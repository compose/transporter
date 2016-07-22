// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"net/http"
	"time"
)

const (
	// DefaultMaxIdleConnsPerHost indicates the default maximum idle connection
	// count maintained between proxy and each member. We set it to 128 to
	// let proxy handle 128 concurrent requests in long term smoothly.
	// If the number of concurrent requests is bigger than this value,
	// proxy needs to create one new connection when handling each request in
	// the delta, which is bad because the creation consumes resource and
	// may eat up ephemeral ports.
	DefaultMaxIdleConnsPerHost = 128
)

// GetProxyURLs is a function which should return the current set of URLs to
// which client requests should be proxied. This function will be queried
// periodically by the proxy Handler to refresh the set of available
// backends.
type GetProxyURLs func() []string

// NewHandler creates a new HTTP handler, listening on the given transport,
// which will proxy requests to an etcd cluster.
// The handler will periodically update its view of the cluster.
func NewHandler(t *http.Transport, urlsFunc GetProxyURLs, failureWait time.Duration, refreshInterval time.Duration) http.Handler {
	return &reverseProxy{
		director:  newDirector(urlsFunc, failureWait, refreshInterval),
		transport: t,
	}
}

// NewReadonlyHandler wraps the given HTTP handler to allow only GET requests
func NewReadonlyHandler(hdlr http.Handler) http.Handler {
	readonly := readonlyHandlerFunc(hdlr)
	return http.HandlerFunc(readonly)
}

func readonlyHandlerFunc(next http.Handler) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			w.WriteHeader(http.StatusNotImplemented)
			return
		}

		next.ServeHTTP(w, req)
	}
}
