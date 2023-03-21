// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/gitprotocolio"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	gitBinary string
	// *managedRepository map keyed by a cached repository path.
	managedRepos sync.Map
)

func init() {
	var err error
	gitBinary, err = exec.LookPath("git")
	if err != nil {
		log.Fatal("Cannot find the git binary: ", err)
	}
}

func urlFixer(u *url.URL) *url.URL {
	result := url.URL{}
	result.Scheme = "https"
	result.Host = u.Host
	result.Path = u.Path

	if strings.HasSuffix(result.Path, "/info/refs") {
		result.Path = strings.TrimSuffix(result.Path, "/info/refs")
	} else if strings.HasSuffix(result.Path, "/git-receive-pack") {
		result.Path = strings.TrimSuffix(result.Path, "/git-receive-pack")
	} else if strings.HasSuffix(result.Path, "/git-upload-pack") {
		result.Path = strings.TrimSuffix(result.Path, "/git-upload-pack")
	}

	// Doesn't seem to apply to Gitlab
	//result.Path = strings.TrimSuffix(result.Path, ".git")
	return &result
}

func stripGitSuffixes(path string) string {
	replacer := strings.NewReplacer("/info/refs", "", "/git-receive-pack", "", "/git-upload-pack", "", ".git", "")
	return replacer.Replace(path)
}

func getLocalGitRepositoryPath(config *ServerConfig, u *url.URL) string {
	return filepath.Join(config.LocalDiskCacheRoot, u.Host, stripGitSuffixes(u.Path))
}

func managedRepositoryExists(config *ServerConfig, u *url.URL) bool {
	_, loaded := managedRepos.Load(getLocalGitRepositoryPath(config, u))
	return loaded
}

func createManagedRepository(config *ServerConfig, u *url.URL, auth string) *managedRepository {
	u = urlFixer(u)
	localRepositoryPath := getLocalGitRepositoryPath(config, u)

	repository := &managedRepository{
		localDiskPath: localRepositoryPath,
		upstreamURL:   u,
		config:        config,
		isPublic:      true,
		accessList:    []string{},
	}
	if auth != "" {
		repository.isPublic = false
		repository.accessList = append(repository.accessList, auth)
	}

	managedRepos.Store(localRepositoryPath, repository)
	return repository
}

func getManagedRepository(config *ServerConfig, u *url.URL) (*managedRepository, bool) {
	repo, ok := managedRepos.Load(getLocalGitRepositoryPath(config, u))
	if !ok {
		return nil, ok
	}

	return repo.(*managedRepository), ok
}

type managedRepository struct {
	localDiskPath string
	lastUpdate    time.Time
	upstreamURL   *url.URL
	config        *ServerConfig
	mu            sync.RWMutex
	isPublic      bool
	accessList    []string
}

func (r *managedRepository) open() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := os.Stat(r.localDiskPath); err != nil {
		if !os.IsNotExist(err) {
			return status.Errorf(codes.Internal, "error while initializing local Git repoitory: %v", err)
		}

		if err := os.MkdirAll(r.localDiskPath, 0750); err != nil {
			return status.Errorf(codes.Internal, "cannot create a cache dir: %v", err)
		}

		op := noopOperation{}
		runGit(op, r.localDiskPath, "", "init", "--bare")
		runGit(op, r.localDiskPath, "", "config", "protocol.version", "2")
		runGit(op, r.localDiskPath, "", "config", "uploadpack.allowfilter", "1")
		runGit(op, r.localDiskPath, "", "config", "uploadpack.allowrefinwant", "1")
		runGit(op, r.localDiskPath, "", "config", "repack.writebitmaps", "1")
		// It seems there's a bug in libcurl and HTTP/2 doens't work.
		runGit(op, r.localDiskPath, "", "config", "http.version", "HTTP/1.1")
		runGit(op, r.localDiskPath, "", "remote", "add", "--mirror=fetch", "origin", r.upstreamURL.String())
	}
	return nil
}

func (r *managedRepository) lsRefsUpstream(command []*gitprotocolio.ProtocolV2RequestChunk, authentication string) ([]*gitprotocolio.ProtocolV2ResponseChunk, error) {
	req, err := http.NewRequest("POST", r.upstreamURL.String()+"/git-upload-pack", newGitRequest(command))
	if err != nil {
		return nil, fmt.Errorf("cannot construct a request object: %v", err)
	}

	req.Header.Add("Content-Type", "application/x-git-upload-pack-request")
	req.Header.Add("Accept", "application/x-git-upload-pack-result")
	req.Header.Add("Git-Protocol", "version=2")

	if authentication != "" {
		req.Header.Add("Authorization", authentication)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot send a request to the upstream: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errMessage := ""
		if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
			bs, err := ioutil.ReadAll(resp.Body)
			if err == nil {
				errMessage = string(bs)
			}
		}
		return nil, fmt.Errorf("got a non-OK response from the upstream: %v %s", resp.StatusCode, errMessage)
	}

	chunks := []*gitprotocolio.ProtocolV2ResponseChunk{}
	v2Resp := gitprotocolio.NewProtocolV2Response(resp.Body)
	for v2Resp.Scan() {
		chunks = append(chunks, copyResponseChunk(v2Resp.Chunk()))
	}
	if err := v2Resp.Err(); err != nil {
		return nil, fmt.Errorf("cannot parse the upstream response: %v", err)
	}
	return chunks, nil
}

func (r *managedRepository) fetchUpstream(authentication string) (err error) {
	op := r.startOperation("FetchUpstream")
	defer func() {
		op.Done(err)
	}()

	// Because of
	// https://public-inbox.org/git/20190915211802.207715-1-masayasuzuki@google.com/T/#t,
	// the initial git-fetch can be very slow. Split the fetch if there's no
	// reference (== an empty repo).
	g, err := git.PlainOpen(r.localDiskPath)
	if err != nil {
		return fmt.Errorf("cannot open the local cached repository: %v", err)
	}
	splitGitFetch := false
	if _, err := g.Reference("HEAD", true); err == plumbing.ErrReferenceNotFound {
		splitGitFetch = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if splitGitFetch {
		// Fetch heads and changes first.
		err = runGit(op, r.localDiskPath, authentication, "fetch", "--progress", "-f", "-n", "origin", "refs/heads/*:refs/heads/*", "refs/changes/*:refs/changes/*")
	}
	if err == nil {
		err = runGit(op, r.localDiskPath, authentication, "fetch", "--progress", "-f", "origin")
	}
	return err
}

func (r *managedRepository) UpstreamURL() *url.URL {
	u := *r.upstreamURL
	return &u
}

func (r *managedRepository) LastUpdateTime() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastUpdate
}

func (r *managedRepository) Add(auth string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.accessList = append(r.accessList, auth)
}

func (r *managedRepository) HasAccess(auth string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Contains(r.accessList, auth)
}

func (r *managedRepository) RecoverFromBundle(bundlePath string, authentication string) (err error) {
	op := r.startOperation("ReadBundle")
	defer func() {
		op.Done(err)
	}()

	r.mu.Lock()
	defer r.mu.Unlock()
	err = runGit(op, r.localDiskPath, authentication, "fetch", "--progress", "-f", bundlePath, "refs/*:refs/*")
	return
}

func (r *managedRepository) WriteBundle(w io.Writer, authentication string) (err error) {
	op := r.startOperation("CreateBundle")
	defer func() {
		op.Done(err)
	}()
	err = runGitWithStdOut(op, w, r.localDiskPath, authentication, "bundle", "create", "-", "--all")
	return
}

func (r *managedRepository) hasAnyUpdate(refs map[string]plumbing.Hash) (bool, error) {
	g, err := git.PlainOpen(r.localDiskPath)
	if err != nil {
		return false, fmt.Errorf("cannot open the local cached repository: %v", err)
	}
	for refName, hash := range refs {
		ref, err := g.Reference(plumbing.ReferenceName(refName), true)
		if err == plumbing.ErrReferenceNotFound {
			return true, nil
		} else if err != nil {
			return false, fmt.Errorf("cannot open the reference: %v", err)
		}
		if ref.Hash() != hash {
			return true, nil
		}
	}
	return false, nil
}

func (r *managedRepository) hasAllWants(hashes []plumbing.Hash, refs []string) (bool, error) {
	g, err := git.PlainOpen(r.localDiskPath)
	if err != nil {
		return false, fmt.Errorf("cannot open the local cached repository: %v", err)
	}

	for _, hash := range hashes {
		if _, err := g.Object(plumbing.AnyObject, hash); err == plumbing.ErrObjectNotFound {
			return false, nil
		} else if err != nil {
			return false, fmt.Errorf("error while looking up an object for want check: %v", err)
		}
	}

	for _, refName := range refs {
		if _, err := g.Reference(plumbing.ReferenceName(refName), true); err == plumbing.ErrReferenceNotFound {
			return false, nil
		} else if err != nil {
			return false, fmt.Errorf("error while looking up a reference for want check: %v", err)
		}
	}

	return true, nil
}

func (r *managedRepository) serveFetchLocal(command []*gitprotocolio.ProtocolV2RequestChunk, w io.Writer) error {
	// If fetch-upstream is running, it's possible that Git returns
	// incomplete set of objects when the refs being fetched is updated and
	// it uses ref-in-want.
	cmd := exec.Command(gitBinary, "upload-pack", "--stateless-rpc", r.localDiskPath)
	cmd.Env = []string{"GIT_PROTOCOL=version=2"}
	cmd.Dir = r.localDiskPath
	cmd.Stdin = newGitRequest(command)
	cmd.Stdout = w
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (r *managedRepository) startOperation(op string) RunningOperation {
	if r.config.LongRunningOperationLogger != nil {
		return r.config.LongRunningOperationLogger(op, r.upstreamURL)
	}
	return noopOperation{}
}

func runGit(op RunningOperation, gitDir string, authentication string, arg ...string) error {
	cmd := exec.Command(gitBinary, arg...)
	cmd.Env = []string{}
	cmd.Dir = gitDir
	cmd.Stderr = &operationWriter{op}
	cmd.Stdout = &operationWriter{op}

	if authentication != "" {
		cmd.Args = append([]string{"-c", fmt.Sprintf("Authorization: %s", authentication)}, cmd.Args...)
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to run a git command: %v", err)
	}
	return nil
}

func runGitWithStdOut(op RunningOperation, w io.Writer, gitDir string, authentication string, arg ...string) error {
	cmd := exec.Command(gitBinary, arg...)
	cmd.Env = []string{}
	cmd.Dir = gitDir
	cmd.Stdout = w
	cmd.Stderr = &operationWriter{op}

	if authentication != "" {
		cmd.Args = append([]string{"-c", fmt.Sprintf("Authorization: %s", authentication)}, cmd.Args...)
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to run a git command: %v", err)
	}
	return nil
}

func newGitRequest(command []*gitprotocolio.ProtocolV2RequestChunk) io.Reader {
	b := new(bytes.Buffer)
	for _, c := range command {
		b.Write(c.EncodeToPktLine())
	}
	return b
}

type noopOperation struct{}

func (noopOperation) Printf(string, ...interface{}) {}
func (noopOperation) Done(error)                    {}

type operationWriter struct {
	op RunningOperation
}

func (w *operationWriter) Write(p []byte) (int, error) {
	w.op.Printf("%s", string(p))
	return len(p), nil
}
