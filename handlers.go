package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/elazarl/goproxy"
	"github.com/google/gitprotocolio"
)

func handleInfoRefs(r *http.Request, ctx *goproxy.ProxyCtx, config *ServerConfig) (*http.Request, *http.Response) {
	if r.URL.Query().Get("service") != "git-upload-pack" {
		return r, goproxy.NewResponse(r, goproxy.ContentTypeText, http.StatusBadRequest, "accepts only git-fetch")
	}

	// Let's check if the repository already exists
	// If it does not exist, let's forward the request to the Git provider
	// which will force authentication if required (401)
	if !managedRepositoryExists(config, r.URL) {
		return r, nil
	}

	// From this point onwards - the managed repository should already exist
	repository, exists := getManagedRepository(config, r.URL)
	if !exists {
		return r, goproxy.NewResponse(r, goproxy.ContentTypeText, http.StatusInternalServerError, "could not verify if repository exists")
	}

	// If repository is cached, deemed not public & no authorization is provided, force Git to send Basic Auth
	if !repository.isPublic && r.Header.Get("Authorization") == "" {
		return r, goproxy.NewResponse(r, goproxy.ContentTypeText, http.StatusUnauthorized, "unauthorized - please send basic auth credentials")
	}

	// Send an info refs response for cached requests
	responseChunks := []*gitprotocolio.InfoRefsResponseChunk{
		{ProtocolVersion: 2},
		{Capabilities: []string{"ls-refs"}},
		// See managed_repositories.go for not having ref-in-want.
		{Capabilities: []string{"fetch=filter shallow"}},
		{Capabilities: []string{"server-option"}},
		{EndOfRequest: true},
	}

	var b bytes.Buffer
	buff := bufio.NewWriter(&b)
	for _, pkt := range responseChunks {
		if err := writePacket(buff, pkt); err != nil {
			return r, goproxy.NewResponse(r, goproxy.ContentTypeText, http.StatusInternalServerError, "client IO error")
		}
	}
	buff.Flush()

	return r, goproxy.NewResponse(r, "application/x-git-upload-pack-advertisement", http.StatusOK, b.String())
}

func handleGitReceivePack(r *http.Request, ctx *goproxy.ProxyCtx, config *ServerConfig) (*http.Request, *http.Response) {
	return r, goproxy.NewResponse(r, goproxy.ContentTypeText, http.StatusBadRequest, "git-receive-pack is not supported")
}

func handleGitUploadPack(r *http.Request, ctx *goproxy.ProxyCtx, config *ServerConfig) (*http.Request, *http.Response) {
	// Check if call is authenticated with Basic auth
	regex, _ := regexp.Compile(`^Basic ([A-Za-z0-9\\+=]*)$`)
	auth := regex.FindString(r.Header.Get("Authorization"))

	// /git-upload-pack doesn't recognize text/plain error. Send an error with ErrorPacket
	if r.Header.Get("Content-Encoding") == "gzip" {
		var err error
		if r.Body, err = gzip.NewReader(r.Body); err != nil {
			return r, goproxy.NewResponse(r, "application/x-git-upload-pack-result", http.StatusBadRequest, fmt.Sprintf("cannot ungzip: %v", err))
		}
	}

	// HTTP is strictly speaking a request-response protocol, and a server
	// cannot send a non-error response until the entire request is read.
	// We need to compromise and either drain the entire request first or
	// buffer the entire response.
	//
	// Because this server supports only ls-refs and fetch commands, valid
	// protocol V2 requests are relatively small in practice compared to the
	// response. A request with many wants and haves can be large, but
	// practically there's a limit on the number of haves a client would
	// send. Compared to that the fetch response can contain a packfile, and
	// this can easily get large. Read the entire request upfront.
	commands, err := parseAllCommands(r.Body)
	if err != nil {
		return r, goproxy.NewResponse(r, "application/x-git-upload-pack-result", http.StatusBadRequest, fmt.Sprintf("error while parsing commands: %v", err))
	}

	repository, exists := getManagedRepository(config, r.URL)
	if !exists {
		repository = createManagedRepository(config, r.URL, auth)
	}

	if err := repository.open(); err != nil {
		return r, goproxy.NewResponse(r, "application/x-git-upload-pack-result", http.StatusBadRequest, fmt.Sprintf("error while opening managed repository: %v", err))
	}

	var b bytes.Buffer
	buff := bufio.NewWriter(&b)
	for _, command := range commands {
		if err := handleV2Command(r.Context(), repository, command, buff, auth); err != nil {
			statusCode := http.StatusInternalServerError
			// Extract popular status codes for better errors
			if strings.Contains(err.Error(), "401") {
				statusCode = http.StatusUnauthorized
			}
			if strings.Contains(err.Error(), "404") {
				statusCode = http.StatusNotFound
			}

			return r, goproxy.NewResponse(r, "application/x-git-upload-pack-result", statusCode, fmt.Sprintf("error while processing commands: %v", err))
		}
	}
	buff.Flush()

	return r, goproxy.NewResponse(r, "application/x-git-upload-pack-result", http.StatusOK, b.String())
}

func parseAllCommands(r io.Reader) ([][]*gitprotocolio.ProtocolV2RequestChunk, error) {
	commands := [][]*gitprotocolio.ProtocolV2RequestChunk{}
	v2Req := gitprotocolio.NewProtocolV2Request(r)
	for {
		chunks := []*gitprotocolio.ProtocolV2RequestChunk{}
		for v2Req.Scan() {
			c := copyRequestChunk(v2Req.Chunk())
			if c.EndRequest {
				break
			}
			chunks = append(chunks, c)
		}
		if len(chunks) == 0 || v2Req.Err() != nil {
			break
		}

		switch chunks[0].Command {
		case "ls-refs":
		case "fetch":
			// Do nothing.
		default:
			return nil, fmt.Errorf("unrecognized command: %v", chunks[0])
		}
		commands = append(commands, chunks)
	}

	if err := v2Req.Err(); err != nil {
		return nil, fmt.Errorf("cannot parse the request: %v", err)
	}
	return commands, nil
}
