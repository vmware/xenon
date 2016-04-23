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

package common

import (
	"fmt"
	"net"
)

type AddressFlag struct {
	host string
	port string
}

func (f *AddressFlag) Host() string {
	host := f.host
	if host == "" {
		host = "127.0.0.1"
	}

	return host
}

func (f *AddressFlag) Port() string {
	port := f.port
	if port == "" {
		port = "8000"
	}

	return port
}

func (f *AddressFlag) String() string {
	return fmt.Sprintf("%s:%s", f.Host(), f.Port())
}

func (f *AddressFlag) Set(v string) error {
	host, port, err := net.SplitHostPort(v)
	if err != nil {
		return err
	}

	f.host = host
	f.port = port
	return nil
}
