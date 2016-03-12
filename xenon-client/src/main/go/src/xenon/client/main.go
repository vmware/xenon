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
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/ghodss/yaml"
	"github.com/pborman/uuid"
)

var (
	fs = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	xenon   = fs.String("xenon", os.Getenv("DCP"), "Root URI of DCP node")
	input   = fs.String("i", "", "Body input")
	oselect = fs.String("s", "", "Output field selector")
	nonl    = fs.Bool("n", false, "Do not print a trailing newline character")
	execute = fs.Bool("x", false, "Execute body template and print request to stdout")
	trace   = fs.String("t", os.Getenv("DCPC_TRACE"), "Enables trace dump of all HTTP data, to the given output file")

	defaultInput  io.Reader = os.Stdin
	defaultOutput io.Writer = os.Stdout
	traceWriter   io.Writer

	method, service string
)

func init() {
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] METHOD SERVICE [FLAGS]...\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		fs.PrintDefaults()
		os.Exit(1)
	}
}

func setenv() {
	vars := []string{"HTTP_PROXY", "HTTPS_PROXY"}

	for _, key := range vars {
		val := os.Getenv("DCP_" + key)

		if val != "" {
			os.Setenv(key, val)
		}
	}
}

func main() {
	setenv()

	err := run(os.Args[1:])

	if err != nil {
		if err == flag.ErrHelp {
			fs.Usage()
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	err := fs.Parse(args)
	if err != nil {
		fs.Usage()
	}

	if *xenon == "" {
		fs.Usage()
	}

	if *trace != "" {
		if *trace == "-" {
			traceWriter = os.Stdout
		} else {
			traceWriter, err = os.OpenFile(*trace, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600)
			if err != nil {
				return err
			}
			defer traceWriter.(*os.File).Close()
		}
	}

	args = fs.Args()

	for _, arg := range []*string{&method, &service} {
		if len(args) == 0 {
			break
		}
		if strings.HasPrefix(args[0], "--") {
			break
		}

		*arg = args[0]
		args = args[1:]
	}

	var body io.Reader

	switch strings.ToLower(method) {
	case "get":
	default:
		body, err = createBody(args)
		if err != nil {
			return err
		}
	}

	req, err := newRequest(body)
	if err != nil {
		return err
	}

	if *execute {
		b, err := httputil.DumpRequest(req, true)
		if err != nil {
			return err
		}

		fmt.Fprint(defaultOutput, string(b))
		if *nonl == false {
			fmt.Fprintln(defaultOutput)
		}

		return nil
	}

	return doRequest(req)
}

func jsonMarshal(data interface{}) (*bytes.Buffer, error) {
	buf, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(buf), nil
}

func bodyFromArgs(args []string) (io.Reader, error) {
	m, err := Map(args)
	if err != nil {
		return nil, err
	}

	return jsonMarshal(m)
}

func newRequest(body io.Reader) (*http.Request, error) {
	if method == "" || service == "" {
		return nil, flag.ErrHelp
	}

	u, err := url.Parse(*xenon)
	if err != nil {
		return nil, err
	}

	// The result is cleaned so duplicate /'s are removed
	u.Path = filepath.Join(u.Path, service)

	req, err := http.NewRequest(strings.ToUpper(method), u.String(), body)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, nil
}

func traceRequest(req *http.Request) error {
	if traceWriter == nil {
		return nil
	}

	b, err := httputil.DumpRequest(req, true)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(traceWriter, string(b))

	if req.Body != nil {
		_, _ = fmt.Fprintln(traceWriter)
	}

	return err
}

func traceResponse(res *http.Response) error {
	if traceWriter == nil {
		return nil
	}

	b, err := httputil.DumpResponse(res, true)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(traceWriter, string(b))

	_, _ = fmt.Fprintln(traceWriter)

	return err
}

func doRequest(req *http.Request) error {
	if err := traceRequest(req); err != nil {
		return err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if err := traceResponse(res); err != nil {
		return err
	}

	if res.StatusCode > http.StatusInternalServerError {
		io.Copy(os.Stderr, res.Body)
		return fmt.Errorf("%s %s http status code: %d", method, service, res.StatusCode)
	}

	switch res.StatusCode {
	case http.StatusNotModified:
		return nil
	}

	raw := &bytes.Buffer{}
	_, err = io.Copy(raw, res.Body)
	if err != nil {
		return err
	}

	if raw.Len() != 0 {
		if *oselect == "" {
			err = outputAll(raw.Bytes())
		} else {
			err = outputSelect(raw.Bytes())
		}

		if err != nil {
			return err
		}
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s http status code: %d", method, service, res.StatusCode)
	}

	return nil
}

func outputAll(body []byte) error {
	indented := &bytes.Buffer{}
	err := json.Indent(indented, body, "", "  ")
	if err != nil {
		return err
	}

	_, err = io.Copy(defaultOutput, indented)
	if err != nil {
		return err
	}

	// Trail JSON payload with newline such that errors printed to stderr are
	// printed on their own lines.
	if *nonl == false {
		fmt.Fprint(defaultOutput, "\n")
	}

	return nil
}

func outputSelect(body []byte) error {
	m := make(map[string]interface{})
	err := json.Unmarshal(body, &m)
	if err != nil {
		return err
	}

	contents := fmt.Sprintf("{{ %s }}", *oselect)
	t, err := template.New("xenonc-oselect").Parse(contents)
	if err != nil {
		return err
	}

	b := &bytes.Buffer{}
	err = t.Execute(b, m)
	if err != nil {
		return err
	}

	fmt.Fprint(defaultOutput, string(b.Bytes()))
	if *nonl == false {
		fmt.Fprintln(defaultOutput)
	}

	return nil
}

func hasInputFile() bool {
	return *input != "-" && *input != ""
}

func createBodyFromYAML(input []byte) (io.Reader, error) {
	m := make(map[string]interface{})
	err := yaml.Unmarshal(input, &m)
	if err != nil {
		return nil, err
	}

	// .yml input can set these vars + body
	vars := []struct {
		key string
		val *string
	}{
		{"action", &method},
		{"path", &service},
		{"select", oselect},
	}

	for _, v := range vars {
		if p, ok := m[v.key]; ok {
			if *v.val == "" {
				*v.val = p.(string)
			}
		}
	}

	body, ok := m["body"]
	if !ok {
		return nil, nil
	}

	if s, ok := body.(string); ok {
		// read raw json body from string
		m = make(map[string]interface{})
		err = json.Unmarshal([]byte(s), &m)
		if err != nil {
			return nil, err
		}

		body = m
	}

	return jsonMarshal(body)
}

func createBody(args []string) (io.Reader, error) {
	if len(args) != 0 && *input == "" {
		return bodyFromArgs(args)
	}

	reader := defaultInput
	if hasInputFile() {
		var err error
		reader, err = os.Open(*input)
		if err != nil {
			return nil, err
		}
	}

	body, err := templateBody(reader, args)
	if err != nil {
		return nil, err
	}

	b := body.Bytes()
	if len(b) > 0 && b[0] != '{' {
		return createBodyFromYAML(body.Bytes())
	}

	return body, nil
}

// host returns the host of the given URL
func host(val string) (string, error) {
	u, err := url.Parse(val)
	if err != nil {
		return "", err
	}

	host := u.Host
	if host == "" {
		host = u.Path
	}

	if host == "" {
		return host, nil
	}

	if strings.Contains(host, ":") {
		h, _, err := net.SplitHostPort(host)
		if err != nil {
			return "", err
		}
		host = h
	}

	return host, nil
}

func lookupHost(host string) (string, error) {
	if net.ParseIP(host) != nil {
		return host, nil
	}

	addrs, err := net.LookupIP(host)
	if err != nil {
		return "", err
	}

	// prefer ipv4
	for _, addr := range addrs {
		if len(addr.To4()) == net.IPv4len {
			return addr.String(), nil
		}
	}

	return addrs[0].String(), nil
}

// address returns the IP address of the given URL host.
// The main use case for this template function is to populate the
// ComputeState.address for an existing compute.
func address(val string) (string, error) {
	host, err := host(val)
	if err != nil {
		return "", err
	}

	if host == "" {
		return host, nil
	}

	return lookupHost(host)
}

func exists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func include(name string) (string, error) {
	// When including a relative file that does not exist,
	// try to resolve using the directory of the intput file if given.
	if !exists(name) && !filepath.IsAbs(name) && hasInputFile() {
		base := filepath.Dir(*input)
		abs := filepath.Join(base, name)
		if exists(abs) {
			name = abs
		}
	}

	contents, err := ioutil.ReadFile(name)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

// id returns a stable uuid with a hash the given data or
// a random uuid if no data is provided.
func id(data ...string) string {
	if len(data) == 0 {
		return uuid.New()
	}

	b := []byte(strings.Join(data, ""))
	return uuid.NewSHA1(uuid.NameSpace_OID, b).String()
}

// go-yaml does not preserve all \n's if we embed yaml within quotes.
// It does work fine when using a yaml literal block, but we must have the proper indentation.
// Rather than indent the cloud_config.yml file on disk, provide a function to indent.
func indent(n int, val string) string {
	lines := strings.Split(val, "\n")

	for i, line := range lines[1:] {
		lines[i+1] = strings.Repeat(" ", n) + line
	}

	return strings.Join(lines, "\n")
}

var funcMap = template.FuncMap{
	"address": address,
	"host":    host,
	"include": include,
	"indent":  indent,
	"uuid":    id,
}

func templateExecute(contents string, data map[string]interface{}) (*bytes.Buffer, error) {
	t, err := template.New("xenonc").Funcs(funcMap).Parse(string(contents))
	if err != nil {
		return nil, err
	}

	b := &bytes.Buffer{}
	err = t.Execute(b, data)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func templateBody(in io.Reader, args []string) (*bytes.Buffer, error) {
	data, err := Map(args)
	if err != nil {
		return nil, err
	}

	contents, err := ioutil.ReadAll(in)
	if err != nil {
		return nil, err
	}

	return templateExecute(string(contents), data)
}
