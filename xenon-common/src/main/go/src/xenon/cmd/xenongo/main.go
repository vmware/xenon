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

package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"xenon/common"
	"xenon/host"
	"xenon/operation"
	"xenon/uri"

	"golang.org/x/net/context"

	"github.com/golang/glog"
)

var (
	// bindAddress is the bind address of this service host
	bindAddress common.AddressFlag

	// authToken is the system user authorization token
	authToken string
)

func init() {
	flag.Var(&bindAddress, "bind", "Bind address")
	flag.StringVar(&authToken, "auth-token", "", "Authorization token")
}

func main() {
	services := []struct {
		uri string
		svc host.Service
	}{
		// Examples
		{
			"/core/examples",
			host.NewFactoryServiceContext(&host.ExampleFactoryService{}),
		},
		// Examples
		{
			"/core/ping",
			host.NewPingService(),
		},
	}

	var err error

	flag.Parse()

	glog.Infof("Started with %s", os.Args[1:])

	h := host.NewServiceHost()
	err = h.Initialize(bindAddress.String())
	if err != nil {
		glog.Fatalf("Error initializing: %s\n", err)
	}

	ctx := operation.SetAuthorizationToken(context.Background(), authToken)
	_, err = host.GetServiceHostManagementState(ctx)
	if err != nil {
		glog.Fatalf("Error getting ServiceHostState: %s\n", err)
	}

	var ops []*operation.Operation
	for _, s := range services {
		op := operation.NewPost(ctx, uri.Extend(uri.Empty(), s.uri), nil)
		ops = append(ops, op)
		h.StartService(op, s.svc)
	}

	_, err = operation.Join(ops)
	if err != nil {
		glog.Fatalf("Error starting services: %s", err)
	}

	start(h)
}

func start(h *host.ServiceHost) {
	signals := []syscall.Signal{
		syscall.SIGTERM,
		syscall.SIGINT,
	}

	sigchan := make(chan os.Signal, 1)
	for _, signum := range signals {
		signal.Notify(sigchan, signum)
	}

	go func() {
		signal := <-sigchan
		glog.Errorf("Received %s, exiting...", signal)
		h.Stop()
	}()

	h.Start()
}
