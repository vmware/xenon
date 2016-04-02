// Copyright (c) 2015-2016 VMware, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, without warranties or
// conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
// specific language governing permissions and limitations under the License.

package errors

import (
	"fmt"
	"net/http"
	"strings"
)

// RFC 2616, 10.4.6:
// The method specified in the Request-Line is not allowed for the resource
// identified by the Request-URI. The response MUST include an Allow header
// containing a list of valid methods for the requested resource.
//
// RFC 2616, 14.7 (excerpt):
// The Allow entity-header field lists the set of methods supported by the
// resource identified by the Request-URI. The purpose of this field is
// strictly to inform the recipient of valid methods associated with the
// resource. An Allow header field MUST be present in a 405 (Method Not
// Allowed) response.
//
//   Allow   = "Allow" ":" #Method
//
// Example of use:
//
//   Allow: GET, HEAD, PUT
//
type MethodNotAllowed struct {
	Allowed []string
}

func (e MethodNotAllowed) Error() string {
	return fmt.Sprintf("http: %s", http.StatusText(http.StatusMethodNotAllowed))
}

func (e MethodNotAllowed) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ms := []string{}
	for _, m := range e.Allowed {
		ms = append(ms, strings.ToUpper(m))
	}

	rw.Header().Add("Allow", strings.Join(ms, ", "))
	rw.WriteHeader(http.StatusMethodNotAllowed)
}
