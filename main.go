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
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	goblet "github.com/google/goblet/git"
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

	var longRunningOperationLogger func(string, *url.URL) goblet.RunningOperation = func(action string, u *url.URL) goblet.RunningOperation {
		log.Printf("Starting %s for %s", action, u.String())
		return &logBasedOperation{action, u}
	}

	config := &goblet.ServerConfig{
		LocalDiskCacheRoot:         *cacheRoot,
		RequestLogger:              requestLogger,
		LongRunningOperationLogger: longRunningOperationLogger,
	}

	// Routes
	http.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "ok\n")
	})
	http.Handle("/", goblet.HTTPHandler(config))

	// Run server
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
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
