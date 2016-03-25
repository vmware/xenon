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

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

var jar = flag.String("xenon.jar", os.Getenv("XENON_JAR"), "Path to Xenon or enatai jar")

type TestServiceHost struct {
	dir     string
	service string
	cmd     *exec.Cmd
	t       *testing.T
}

// NewTestServiceHost starts the given xenon.jar and waits for:
// - server to be listening on serviceHostState.httpPort: error on timeout
// - service to started: skip test on timeout
func NewTestServiceHost(t *testing.T, service string) *TestServiceHost {
	if *jar == "" {
		t.SkipNow()
	}

	s := &TestServiceHost{
		service: service,
		t:       t,
	}

	err := s.init()
	if err != nil {
		s.Stop() // cleanup
		t.Fatal(err)
	}

	return s
}

func (s *TestServiceHost) readState() error {
	name := filepath.Join(s.dir, "0", "serviceHostState.json")
	b, err := ioutil.ReadFile(name)
	if err != nil {
		return err
	}

	var state struct {
		Port int `json:"httpPort"`
	}

	err = json.Unmarshal(b, &state)
	if err != nil {
		return err
	}

	testURL = fmt.Sprintf("http://127.0.0.1:%d", state.Port)

	return nil
}

func (s *TestServiceHost) poll() error {
	timer := time.NewTimer(time.Minute)
	ticker := time.NewTicker(time.Millisecond * 100)
	http.DefaultClient.Timeout = time.Second * 10

	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			_, err := http.Get(testURL)
			if err != nil {
				return errors.New("timeout waiting for server to start")
			}
			s.t.Logf("timeout waiting for service %s to start", s.service)
			s.t.SkipNow()
			return nil
		case <-ticker.C:
			if err := s.readState(); err != nil {
				continue
			}

			res, err := http.Get(testURL + s.service)
			if err != nil || res.StatusCode != http.StatusOK {
				continue
			}
			_ = res.Body.Close()
			return nil
		}
	}
}

func (s *TestServiceHost) init() error {
	var err error
	s.dir, err = ioutil.TempDir("", "xenon-client-linux")
	if err != nil {
		return err
	}

	cmd := exec.Command("java", "-Dxenon.sandbox="+s.dir, "-Dxenon.port=0", "-jar", *jar)

	if testing.Verbose() {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	err = cmd.Start()
	if err != nil {
		return err
	}
	s.cmd = cmd

	return s.poll()
}

func (s *TestServiceHost) Stop() {
	if s.cmd != nil {
		err := s.cmd.Process.Kill()
		if err != nil {
			log.Printf("failed to kill %s: %s", s.cmd.Args, err)
		}
		_ = s.cmd.Wait()
	}

	if s.dir != "" {
		err := os.RemoveAll(s.dir)
		if err != nil {
			log.Printf("failed to remove %s: %s", s.dir, err)
		}
	}
}
