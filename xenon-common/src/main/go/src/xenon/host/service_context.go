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
	"xenon/errors"
	"xenon/operation"

	"golang.org/x/net/context"
)

type ServiceContextHandler interface {
	GetState() interface{}
}

type ServiceContext struct {
	MinimalService

	h ServiceContextHandler

	// These functions are extracted from the service handler, if available.
	// If they are unavailable in the handler, a default implementation is used.
	hGet    operation.HandlerFunc
	hPost   operation.HandlerFunc
	hPatch  operation.HandlerFunc
	hPut    operation.HandlerFunc
	hDelete operation.HandlerFunc
}

func NewServiceContext(h ServiceContextHandler) *ServiceContext {
	s := &ServiceContext{
		h: h,
	}

	if x, ok := h.(GetHandler); ok {
		s.hGet = x.HandleGet
	} else {
		// Only GET has a default
		s.hGet = s.HandleGet
	}

	if x, ok := h.(PostHandler); ok {
		s.hPost = x.HandlePost
	}

	if x, ok := h.(PatchHandler); ok {
		s.hPatch = x.HandlePatch
	}

	if x, ok := h.(PutHandler); ok {
		s.hPut = x.HandlePut
	}

	if x, ok := h.(DeleteHandler); ok {
		s.hDelete = x.HandleDelete
	}

	return s
}

func (s *ServiceContext) allowedMethods() []string {
	ms := []string{}

	if s.hGet != nil {
		ms = append(ms, "GET")
	}

	if s.hPost != nil {
		ms = append(ms, "POST")
	}

	if s.hPatch != nil {
		ms = append(ms, "PATCH")
	}

	if s.hPut != nil {
		ms = append(ms, "PUT")
	}

	if s.hDelete != nil {
		ms = append(ms, "DELETE")
	}

	return ms
}

func (s *ServiceContext) HandleStart(ctx context.Context, op *operation.Operation) {
	err := op.DecodeBody(s.h.GetState())
	if err != nil && err != operation.ErrBodyNotSet {
		op.Fail(err)
		return
	}

	// Run the service handler's start handler if available
	if h, ok := s.h.(StartHandler); ok {
		err = op.CreateChild(ctx).Go(ctx, h.HandleStart).Wait()
		if err != nil {
			op.Fail(err)
			return
		}
	}

	op.Complete()
}

func (s *ServiceContext) HandleRequest(ctx context.Context, op *operation.Operation) {
	switch op.GetRequest().Method {
	case "GET":
		if s.hGet != nil {
			s.hGet(ctx, op)
			return
		}
	case "POST":
		if s.hPost != nil {
			s.hPost(ctx, op)
			return
		}
	case "PATCH":
		if s.hPatch != nil {
			s.hPatch(ctx, op)
			return
		}
	case "PUT":
		if s.hPut != nil {
			s.hPut(ctx, op)
			return
		}
	case "DELETE":
		if s.hDelete != nil {
			s.hDelete(ctx, op)
			return
		}
	}

	op.Fail(errors.MethodNotAllowed{Allowed: s.allowedMethods()})
	return
}

func (s *ServiceContext) HandleGet(ctx context.Context, op *operation.Operation) {
	op.SetBody(s.h.GetState()).Complete()
}
