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
	"io"
	"path"
	"xenon/common"
	"xenon/errors"
	"xenon/operation"
	"xenon/uri"

	"golang.org/x/net/context"

	"github.com/pborman/uuid"
)

type ServiceDocumentGetter interface {
	GetServiceDocument() *common.ServiceDocument
}

type FactoryServiceContextHandler interface {
	CreateDocument() ServiceDocumentGetter
	CreateService() Service
}

type FactoryServiceContext struct {
	MinimalService

	h FactoryServiceContextHandler
}

func NewFactoryServiceContext(h FactoryServiceContextHandler) Service {
	f := FactoryServiceContext{
		h: h,
	}

	return &f
}

func (f *FactoryServiceContext) HandleStart(ctx context.Context, op *operation.Operation) {
	op.Complete()
}

func (f *FactoryServiceContext) HandleRequest(ctx context.Context, op *operation.Operation) {
	// TODO(PN): Support GET (need a way to get elements) and DELETE
	switch op.GetRequest().Method {
	case "POST":
	default:
		err := errors.MethodNotAllowed{Allowed: []string{"POST"}}
		op.Fail(err)
		return
	}

	f.handlePost(ctx, op)
}

// handlePost calls out to the factory service implementation's POST handler,
// if it exists, and waits for completion. If this runs and completes without
// error, the returned body is passed to the start operation for the service
// created by this factory.
func (f *FactoryServiceContext) handlePost(ctx context.Context, op *operation.Operation) {
	var err error
	var sd *common.ServiceDocument

	if h, ok := f.h.(PostHandler); ok {
		// Run the factory service's POST handler and wait for completion.
		err = op.CreateChild(ctx).Go(ctx, h.HandlePost).Wait()
		if err != nil {
			op.Fail(err)
			return
		}
	}

	doc := f.h.CreateDocument()
	err = op.DecodeBody(doc)
	if err != nil && err != io.EOF {
		op.Fail(err)
		return
	}

	sd = doc.GetServiceDocument()
	sd.SelfLink = path.Join(f.SelfLink(), uuid.New())
	op.SetBody(doc)

	buf, err := op.EncodeBodyAsBuffer()
	if err != nil {
		op.Fail(err)
		return
	}

	// Start child service at service document's selflink
	startOp := op.NewPost(ctx, uri.Extend(uri.Local(), sd.SelfLink), buf)
	f.Host().StartService(startOp, f.h.CreateService())
	err = startOp.Wait()
	if err != nil {
		op.Fail(err)
		return
	}

	op.Complete()
}
