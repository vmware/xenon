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
