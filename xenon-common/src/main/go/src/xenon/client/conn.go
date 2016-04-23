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

package client

import (
	"net"
	"xenon"

	"github.com/golang/glog"
)

var MaximumConnections = 64

type Dialer struct {
	sem     semaphore
	dialer  *net.Dialer
	ConnMax int
}

func NewDialer(dialer *net.Dialer, connMax int) *Dialer {
	if connMax == 0 {
		connMax = MaximumConnections
	}

	return &Dialer{
		sem:     make(semaphore, connMax),
		dialer:  dialer,
		ConnMax: connMax,
	}
}

func (d *Dialer) Dial(network, address string) (net.Conn, error) {
	if glog.V(xenon.Debug) && (len(d.sem) == d.ConnMax) {
		glog.Warningf("dialer is at %d max connections (dialing %s)", len(d.sem), address)
	}

	d.sem.P(1)
	c, err := d.dialer.Dial(network, address)
	if err != nil {
		d.sem.V(1)
		return nil, err
	}
	return Conn{c, d.sem.V}, nil
}

type Conn struct {
	net.Conn
	v func(n int)
}

func (c Conn) Close() error {
	e := c.Conn.Close()
	c.v(1)
	return e
}
