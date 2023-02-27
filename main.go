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
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/elazarl/goproxy"
)

var (
	port      = flag.Int("port", 8080, "port to listen to")
	cacheRoot = flag.String("cache_root", "", "Root directory of cached repositories")
)

func main() {
	// Setup Goblet instance to handle Git requests
	var requestLogger func(r *http.Request, status int, requestSize, responseSize int64, latency time.Duration) = func(r *http.Request, status int, requestSize, responseSize int64, latency time.Duration) {
		dump, err := httputil.DumpRequest(r, false)
		if err != nil {
			return
		}
		log.Printf("%q %d request size: %d, response size %d, latency: %v", dump, status, requestSize, responseSize, latency)
	}

	var longRunningOperationLogger func(string, *url.URL) RunningOperation = func(action string, u *url.URL) RunningOperation {
		log.Printf("Starting %s for %s", action, u.String())
		return &logBasedOperation{action, u}
	}

	config := &ServerConfig{
		LocalDiskCacheRoot:         *cacheRoot,
		RequestLogger:              requestLogger,
		LongRunningOperationLogger: longRunningOperationLogger,
	}

	proxy := goproxy.NewProxyHttpServer()
	proxy.Verbose = true

	// Routes
	proxy.OnRequest().HandleConnect(goproxy.AlwaysMitm)
	proxy.OnRequest().DoFunc(
		func(r *http.Request, ctx *goproxy.ProxyCtx) (*http.Request, *http.Response) {
			// Check if request is Git -> refuse if not

			// Create response writer
			resp := goproxy.NewResponse(r, "", 200, "")
			resp.Write()

			// switch
			// If /info/refs w/ service=git-upload-pack -> forward to git handler
			// If /git-receive-pack -> not supported, reject
			// If /git-upload-pack -> go through caching mechanism
			// Else -> reject

			return r, nil
		},
	)

	// Filter specific requests
	/*proxy.OnRequest(goproxy.ReqHostMatches(regexp.MustCompile(`.*github.*`))).DoFunc(
		func(r *http.Request, ctx *goproxy.ProxyCtx) (*http.Request, *http.Response) {
			ctx.Logf("Uh ohhhh, its github again: %s", r.URL.String())
			return r, nil
		},
	)*/

	// Run server
	log.Fatal(http.ListenAndServe(":8080", proxy))
}

type LongRunningOperation struct {
	Action          string `json:"action"`
	URL             string `json:"url"`
	DurationMs      int    `json:"duration_msec,omitempty"`
	Error           string `json:"error,omitempty"`
	ProgressMessage string `json:"progress_message,omitempty"`
}

type logBasedOperation struct {
	action string
	u      *url.URL
}

func (op *logBasedOperation) Printf(format string, a ...interface{}) {
	log.Printf("Progress %s (%s): %s", op.action, op.u.String(), fmt.Sprintf(format, a...))
}

func (op *logBasedOperation) Done(err error) {
	log.Printf("Finished %s for %s: %v", op.action, op.u.String(), err)
}
