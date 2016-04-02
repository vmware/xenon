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

package uri

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"path/filepath"
	"xenon/common"
)

var xenon common.AddressFlag

func init() {
	flag.Var(&xenon, "xenon", "XENON address")
}

// URI wraps net/url.URL
//
// We expect to be able to unmarshal an URI field from JSON, but the standard library implementation
// of url.URL doesn't implement JSON unmarshalling functions.
//
type URI struct {
	*url.URL
}

func Empty() URI {
	return URI{new(url.URL)}
}

func Parse(s string) (URI, error) {
	u, err := url.Parse(s)
	return URI{u}, err
}

func New(host, port string) URI {
	u, _ := Parse(fmt.Sprintf("http://%s:%s/", host, port))
	return u
}

func Local() URI {
	return New(xenon.Host(), xenon.Port())
}

func Copy(u URI) URI {
	// Copy URL itself
	cu := *u.URL
	u.URL = &cu

	// Copy Userinfo if present
	if u.User != nil {
		username := u.User.Username()
		password, ok := u.User.Password()
		if ok {
			u.User = url.UserPassword(username, password)
		} else {
			u.User = url.User(username)
		}
	}

	return u
}

func Normalize(u URI) URI {
	v := Copy(u)
	if len(v.Path) > 0 && v.Path[0] != '/' {
		v.Path = "/" + v.Path
	}
	return v
}

func Extend(u URI, path string) URI {
	v := Normalize(u)
	v.Path = filepath.Join(v.Path, path)
	return v
}

func ExtendQuery(u URI, key, value string) URI {
	v := Copy(u)
	q := v.Query()
	q.Set(key, value)
	v.RawQuery = q.Encode()
	return v
}

// UnmarshalJSON unmarshals a string and parses an URI.
func (u *URI) UnmarshalJSON(data []byte) error {
	var s string

	err := json.Unmarshal(data, &s)
	ref, err := url.Parse(s)
	if err != nil {
		return err
	}

	*u = URI{ref}
	return nil
}

// MarshalJSON marshals the URI as a string.
func (u URI) MarshalJSON() ([]byte, error) {
	var s string

	if u.URL != nil {
		s = u.URL.String()
	}

	return json.Marshal(s)
}
