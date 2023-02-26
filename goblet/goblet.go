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

package goblet

import (
	"io"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/oauth2"
)

type ServerConfig struct {
	LocalDiskCacheRoot string

	URLCanonializer func(*url.URL) (*url.URL, error)

	RequestAuthorizer func(*http.Request) error

	TokenSource oauth2.TokenSource

	ErrorReporter func(*http.Request, error)

	RequestLogger func(r *http.Request, status int, requestSize, responseSize int64, latency time.Duration)

	LongRunningOperationLogger func(string, *url.URL) RunningOperation
}

type RunningOperation interface {
	Printf(format string, a ...interface{})

	Done(error)
}

type ManagedRepository interface {
	UpstreamURL() *url.URL

	LastUpdateTime() time.Time

	RecoverFromBundle(string) error

	WriteBundle(io.Writer) error
}

func HTTPHandler(config *ServerConfig) http.Handler {
	return &httpProxyServer{config}
}

func OpenManagedRepository(config *ServerConfig, u *url.URL) (ManagedRepository, error) {
	return openManagedRepository(config, u)
}

func ListManagedRepositories(fn func(ManagedRepository)) {
	managedRepos.Range(func(key, value interface{}) bool {
		m := value.(*managedRepository)
		fn(m)
		return true
	})
}
