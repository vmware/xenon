/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package client

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/pborman/uuid"
)

var (
	testURL    string
	selfLink   string
	testOutput = new(bytes.Buffer)
)

func init() {
	defaultOutput = testOutput

	http.DefaultClient.Transport = &http.Transport{
		DisableKeepAlives: true,
	}
}

// reset all vars/flags before calling run(args)
func testRun(args []string) error {
	method = ""
	service = ""

	fs.VisitAll(func(f *flag.Flag) {
		_ = f.Value.Set(f.DefValue)
	})

	testOutput.Reset()
	defer func() {
		defaultInput = os.Stdin
	}()

	*xenon = testURL
	*nonl = true

	selfLink = uuid.New()
	data := map[string]interface{}{
		"selfLink": selfLink,
	}

	// expand input args (mainly for unique selfLink)
	var xargs []string

	for _, arg := range args {
		b, err := templateExecute(arg, data)
		if err != nil {
			panic(err)
		}

		xargs = append(xargs, b.String())
	}

	// expand input body templates
	if body, ok := defaultInput.(*bytes.Buffer); ok {
		xbody, err := templateExecute(body.String(), data)
		if err != nil {
			panic(err)
		}

		defaultInput = xbody
	}

	return run(xargs)
}

func setDefaultInput(body map[string]interface{}) {
	jbody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	defaultInput = bytes.NewBuffer(jbody)
}

func validateComputeDescription(t *testing.T, args []string) {
	err := testRun(args)
	if err != nil {
		t.Fatal(err)
	}

	res := make(map[string]interface{})
	err = json.Unmarshal(testOutput.Bytes(), &res)
	if err != nil {
		t.Fatal(err)
	}

	val := path.Base(res["documentSelfLink"].(string))
	if val != selfLink {
		t.Errorf("expected '%s', got '%s'", selfLink, val)
	}

	val = res["zoneId"].(string)
	u := uuid.Parse(val)

	if u == nil {
		t.Errorf("failed to parse id='%s'", val)
	}
}

func validateSelectSelfLink(t *testing.T, args []string) {
	args = append([]string{"-s", ".documentSelfLink"}, args...)

	err := testRun(args)
	if err != nil {
		t.Fatal(err)
	}

	res := testOutput.String()
	if path.Base(res) != selfLink {
		t.Errorf("unexpected output: '%s'", res)
	}
}

func TestGet(t *testing.T) {
	s := NewTestServiceHost(t, "/core/management")
	defer s.Stop()

	args := []string{"get", s.service}
	err := testRun(args)
	if err != nil {
		t.Error(err)
	}

	var m struct {
		Address string `json:"bindAddress"`
		Port    int    `json:"httpPort"`
	}

	err = json.Unmarshal(testOutput.Bytes(), &m)
	if err != nil {
		t.Fatal(err)
	}

	u := fmt.Sprintf("http://%s:%d", m.Address, m.Port)
	if u != testURL {
		t.Errorf("expected '%s', got '%s'", testURL, u)
	}

	// same document with field option
	args = append([]string{"-s", ".bindAddress"}, args...)
	err = testRun(args)
	if err != nil {
		t.Error(err)
	}

	addr := testOutput.String()
	if addr != m.Address {
		t.Errorf("expected '%s', got '%s'", m.Address, addr)
	}
}

func TestUsage(t *testing.T) {
	args := []string{
		"-xenon", "http://nowhere",
	}

	err := testRun(args)
	if err != flag.ErrHelp {
		t.Error("expected usage error")
	}

	args = append(args, "post")
	err = testRun(args)
	if err != flag.ErrHelp {
		t.Error("expected usage error")
	}

	args = append(args, "post", "/foo", "bar")
	err = testRun(args)
	if err != ErrInvalidFlag {
		t.Errorf("expected invalid flag error")
	}
}

func TestExampleService(t *testing.T) {
	s := NewTestServiceHost(t, "/core/examples")
	defer s.Stop()

	args := []string{
		"-i", "test/examples.yml",
		"-s", ".name",
	}

	err := testRun(args)
	if err != nil {
		t.Fatal(err)
	}

	name := testOutput.String()

	if name != "My example service" {
		t.Errorf("failed to match name='%s'", name)
	}
}

func TestAddress(t *testing.T) {
	addr, err := lookupHost("www.vmware.com")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		val, expect string
	}{
		{"ssh://192.168.1.111:22", "192.168.1.111"},
		{"192.168.1.111", "192.168.1.111"},
		{"192.168.1.111:22", "192.168.1.111"},
		{"", ""},
		{"localhost", "127.0.0.1"},
		{"https://www.vmware.com/sdk", addr},
		{"https://www.vmware.com:18443/sdk", addr},
	}

	for _, test := range tests {
		a, err := address(test.val)
		if err != nil {
			t.Fatalf("error parsing %s: %s", test.val, err)
		}

		if a != test.expect {
			t.Errorf("expected '%s', got '%s'", test.expect, a)
		}
	}
}

func TestUUID(t *testing.T) {
	input := "one two three"
	if id(input) != id(input) {
		t.Error("id not stable")
	}

	if id() == id() {
		t.Error("uuid not random")
	}
}

// Test that command line arguments override template contents
func TestOverride(t *testing.T) {
	args := []string{
		"-xenon", "http://nowhere",
		"-i", "test/examples.yml",
		"-x",
		"patch",
	}

	err := testRun(args)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(testOutput.String(), "PATCH ") {
		t.Error("failed to override action")
	}
}
