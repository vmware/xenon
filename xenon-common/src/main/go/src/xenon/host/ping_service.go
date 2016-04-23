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
	"net/http"

	"xenon/client"
	"xenon/operation"
	"xenon/uri"

	"golang.org/x/net/context"
)

type pingService struct{}

type pingRequest struct {
	URI uri.URI `json:"uri"`
}

func (s *pingService) GetState() interface{} {
	return &pingService{}
}

func NewPingService() Service {
	return NewServiceContext(&pingService{})
}

func (s *pingService) HandlePatch(ctx context.Context, op *operation.Operation) {
	req := &pingRequest{}
	err := op.DecodeBody(req)
	if err != nil {
		op.Fail(err)
		return
	}

	op.SetStatusCode(http.StatusCreated)
	op.Complete()

	// Ping specified URL
	pingOp := operation.NewPatch(ctx, req.URI, nil)
	pingOp.SetBody(req)
	client.Send(pingOp)
}
