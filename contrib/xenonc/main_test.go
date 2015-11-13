package main

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

	"gopkg.in/yaml.v2"

	"code.google.com/p/go-uuid/uuid"
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

func TestComputeDescriptionYAML(t *testing.T) {
	s := NewTestServiceHost(t, "/resources/compute-descriptions")
	defer s.Stop()

	args := []string{
		"-i", "test/compute_description_test.yml",
		"--",
		"--documentSelfLink={{ .selfLink }}",
	}

	validateComputeDescription(t, args)
	validateSelectSelfLink(t, args)
}

func TestComputeDescriptionJSON(t *testing.T) {
	s := NewTestServiceHost(t, "/resources/compute-descriptions")
	defer s.Stop()

	args := []string{
		"post", s.service,
	}

	body := map[string]interface{}{
		"documentSelfLink": "{{ .selfLink }}",
		"zoneId":           "{{ uuid }}",
	}

	setDefaultInput(body)
	validateComputeDescription(t, args)

	setDefaultInput(body)
	validateSelectSelfLink(t, args)
}

func TestComputeDescriptionFlags(t *testing.T) {
	s := NewTestServiceHost(t, "/resources/compute-descriptions")
	defer s.Stop()

	args := []string{
		"post", s.service,
		"--documentSelfLink={{ .selfLink }}",
		"--zoneId={{ uuid }}",
	}

	validateComputeDescription(t, args)
	validateSelectSelfLink(t, args)
}

// test that we can unmarshal embedded yaml (cloud config)
func TestDiskYAML(t *testing.T) {
	s := NewTestServiceHost(t, "/resources/disks")
	defer s.Stop()

	args := []string{
		"-i", "test/disk_test.yml",
		// select the cloud config as output
		"-s", "(index .bootConfig.files 0).contents",
	}

	err := testRun(args)
	if err != nil {
		t.Fatal(err)
	}

	// verify we maintain the cloud config structure
	cc := struct {
		AuthorizedKeys []string `yaml:"ssh_authorized_keys"`
		CoreOS         struct {
			Units []struct {
				Name    string `yaml:"name"`
				Command string `yaml:"command"`
				Mask    string `yaml:"mask"`
				Content string `yaml:"content"`
			} `yaml:"units"`
		} `yaml:"coreos"`
		WriteFiles []struct {
			Path    string `yaml:"path"`
			Content string `yaml:"content"`
		} `yaml:"write_files"`
	}{}

	err = yaml.Unmarshal(testOutput.Bytes(), &cc)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		expect, actual int
	}{
		{len(cc.AuthorizedKeys), 1},
		{len(cc.CoreOS.Units), 4},
		{len(cc.WriteFiles), 1},
	}

	for _, test := range tests {
		if test.actual != test.expect {
			t.Errorf("expected %d, got %d", test.expect, test.actual)
		}
	}
}

// test that we can embed a raw json body with a yaml document
func TestEmbeddedJSON(t *testing.T) {
	s := NewTestServiceHost(t, "/provisioning/gce/client_secrets")
	defer s.Stop()

	args := []string{
		"-i", "test/gce_client_secrets_test.yml",
	}

	err := testRun(args)
	if err != nil {
		t.Fatal(err)
	}

	// get .authCredentialsLink
	link := testOutput.String()
	err = testRun([]string{"get", link})
	if err != nil {
		t.Error(err)
	}

	res := make(map[string]interface{})
	err = json.Unmarshal(testOutput.Bytes(), &res)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		key, expect string
	}{
		{"userLink", "client_id.apps.googleusercontent.com"},
		{"type", "service_account"},
	}

	for _, test := range tests {
		val := res[test.key]
		if val != test.expect {
			t.Errorf("expected '%s', got '%s'", test.expect, val)
		}
	}

	key := res["privateKey"].(string)
	const begin = "-----BEGIN RSA PRIVATE KEY-----\n"
	const end = "-----END RSA PRIVATE KEY-----\n"
	if !strings.HasPrefix(key, begin) || !strings.HasSuffix(key, end) {
		t.Error("invalid privateKey")
	}
}

func TestDiskNestedSelect(t *testing.T) {
	s := NewTestServiceHost(t, "/resources/disks")
	defer s.Stop()

	args := []string{
		"-i", "test/disk_test.yml",
		"-s", ".bootConfig.label",
	}

	err := testRun(args)
	if err != nil {
		t.Fatal(err)
	}

	id := testOutput.String()
	u := uuid.Parse(id)

	if u == nil {
		t.Errorf("failed to parse id='%s'", id)
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
		"-i", "test/disk_test.yml",
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
