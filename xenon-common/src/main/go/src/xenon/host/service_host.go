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

package host

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"path"
	"strconv"
	"sync"
	"xenon"
	"xenon/client"
	"xenon/common"
	"xenon/operation"
	"xenon/uri"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

type ServiceHost struct {
	sync.RWMutex

	tcpListener net.Listener

	services map[string]Service

	selfURI uri.URI
}

func NewServiceHost() *ServiceHost {
	h := ServiceHost{
		services: make(map[string]Service),
	}

	return &h
}

func (h *ServiceHost) Initialize(tcpAddr string) error {
	var err error

	h.tcpListener, err = net.Listen("tcp4", tcpAddr)
	if err != nil {
		return err
	}

	glog.Infof("Listening on %s", tcpAddr)

	return h.setURI()
}

func (h *ServiceHost) Start() error {
	return http.Serve(h.tcpListener, h)
}

func (h *ServiceHost) Stop() error {
	return h.tcpListener.Close()
}

func (h *ServiceHost) TCPAddr() (*net.TCPAddr, error) {
	if h.tcpListener == nil {
		return nil, errors.New("no listener")
	}

	switch addr := h.tcpListener.Addr().(type) {
	case *net.TCPAddr:
		return addr, nil
	default:
		return nil, fmt.Errorf("unknown address type: %#v", addr)
	}
}

func (h *ServiceHost) setURI() error {
	addr, err := h.TCPAddr()
	if err != nil {
		return err
	}

	var host string
	if addr.IP.Equal(net.IPv4zero) {
		// Use localhost if address is INADDR_ANY
		host = "127.0.0.1"
	} else {
		host = addr.IP.String()
	}

	h.selfURI = uri.New(host, strconv.Itoa(addr.Port))
	return nil
}

func (h *ServiceHost) URI() uri.URI {
	return h.selfURI
}

// ServeHTTP implements the host's core http entry point.
func (h *ServiceHost) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ctx := context.Background()
	op := operation.NewOperation(ctx)
	op.SetRequest(req)

	if glog.V(xenon.Debug) {
		glog.Infof("host: %s %s", req.Method, req.URL.Path)
	}

	// Create context for handling this request. The request associated with
	// this operation might complete long before the workflow that it kicked
	// off completes. This workflow needs to flow parameters such as those
	// related to authorization to other operations and workflows.
	// On the Java side this is solved by using thread locals that are carefully
	// set when an operation is handled, making sure they can automatically be
	// flowed to newly created operations. On the Go side there is no equivalent
	// for thread locals so this context has to be passed around explicitly.
	if t := req.Header.Get(operation.RequestAuthTokenHeader); t != "" {
		ctx = operation.SetAuthorizationToken(ctx, t)
	}

	// Operations may be marked complete before this function returns.
	// Handle the request in a separate routine to return to the client ASAP.
	op.Start()

	go h.HandleRequest(ctx, op)

	if err := op.Wait(); err != nil {
		if handler, ok := err.(http.Handler); ok {
			if glog.V(xenon.Debug) {
				glog.Warningf("host: %s %s: custom error handler", req.Method, req.URL.Path)
			}

			handler.ServeHTTP(rw, req)
			return
		}

		body := &common.ServiceErrorResponse{
			Message: err.Error(),
		}

		op.SetStatusCode(http.StatusInternalServerError)
		op.SetBody(body)
	}

	// Default status code to 200 OK
	code := op.GetStatusCode()
	if code == 0 {
		code = http.StatusOK
	}

	if glog.V(xenon.Debug) {
		glog.Infof("host: %s %s: status %d", req.Method, req.URL.Path, code)
	}

	if !op.IsStreamResponse() {
		rw.Header().Set(operation.ContentType, operation.ApplicationJSON)
	}

	rw.WriteHeader(code)
	if err := op.EncodeBody(rw); err != nil && err != operation.ErrBodyNotSet {
		glog.Warningf("host: failure writing body: %s\n", err)
	}
}

func (h *ServiceHost) HandleRequest(ctx context.Context, op *operation.Operation) {
	selfLink := path.Clean(op.GetRequest().URL.Path)

	h.RLock()
	s, ok := h.services[selfLink]
	h.RUnlock()

	if !ok {
		// TODO(PN): Check if the path points to a utility service
		op.SetStatusCode(http.StatusNotFound).Complete()
		return
	}

	// Queue request if service is not yet available
	if s.Stage() < StageAvailable {
		select {
		case <-op.Done():
			return // Operation is already done, no need to complete.
		case <-s.StageBarrier(StageAvailable):
			// Continue
		}
	}

	s.HandleRequest(ctx, op)
}

// StartServiceSync starts the specified service, synchronously.
func (h *ServiceHost) StartServiceSync(path string, s Service) error {
	ctx := context.Background()
	startOp := operation.NewPost(ctx, uri.Extend(h.selfURI, path), nil)
	h.StartService(startOp, s)
	return startOp.Wait()
}

// StartService starts the specified service. The operation parameter is used
// for the context of the start; the service's URI, the referrer, signaling
// completion of the service start, etc.
//
// Upon returning, either the operation has failed, or the service is still
// going through the motions of being started. In the latter case, the caller
// can wait for completion of the operation to ensure the service is fully
// started.
//
func (h *ServiceHost) StartService(op *operation.Operation, s Service) {
	s.SetHost(h)

	// The selflink is expected to be either set on the service externally
	// (before starting the service), or as the URI path on the operation.
	selfLink := s.SelfLink()
	if selfLink == "" {
		// Prefix path with / to make sure it is absolute.
		// The clean function removes double /'s and the trailing /, if any.
		selfLink = path.Clean("/" + op.GetURI().Path)
		s.SetSelfLink(selfLink)
	}

	// Add service to the host's service map.
	h.Lock()
	_, ok := h.services[selfLink]
	if ok {
		h.Unlock()
		op.Fail(errors.New("host: service is already bound"))
		return
	}
	h.services[selfLink] = s
	h.Unlock()

	// Service is now attached to host; move to initialized state.
	if err := s.SetStage(StageInitialized); err != nil {
		op.Fail(err)
		return
	}

	// Start service asynchronously.
	go h.startService(op, s)
}

// startService executes the necessary actions to move a service from
// the available stage, to the started stage, to the available stage.
func (h *ServiceHost) startService(op *operation.Operation, s Service) {
	ctx := context.Background()
	err := op.CreateChild(ctx).Go(ctx, s.HandleStart).Wait()
	if err != nil {
		op.Fail(err)
		return
	}

	if err := s.SetStage(StageStarted); err != nil {
		op.Fail(err)
		return
	}

	// Stuff may happen between the started and available stages.
	// This separation is kept here for parity with the Java XENON implementation.

	if err := s.SetStage(StageAvailable); err != nil {
		op.Fail(err)
		return
	}

	op.Complete()
}

// GetServiceHostManagementState returns the local node's NodeState
func GetServiceHostManagementState(ctx context.Context) (*common.ServiceHostState, error) {
	u := uri.Extend(uri.Local(), common.Management)
	p := operation.NewGet(ctx, u)
	client.Send(p)
	if err := p.Wait(); err != nil {
		return nil, err
	}

	var state common.ServiceHostState
	err := p.DecodeBody(&state)
	if err != nil {
		return nil, err
	}
	return &state, nil
}
