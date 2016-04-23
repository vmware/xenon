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
	"testing"
)

func TestMarshalOnValue(t *testing.T) {
	var u1, u2 URI

	u1 = New("localhost", "80")
	b, _ := json.Marshal(u1)
	json.Unmarshal(b, &u2)

	if u1.Host != u2.Host {
		t.Errorf("Encoding/decoding error")
	}
}

func TestExtendQuery(t *testing.T) {
	u := New("localhost", "80")
	u = ExtendQuery(u, "foo", "bar")

	expect := "http://localhost:80/?foo=bar"
	if u.String() != expect {
		t.Errorf("expected '%s', got '%s'", expect, u)
	}
}
