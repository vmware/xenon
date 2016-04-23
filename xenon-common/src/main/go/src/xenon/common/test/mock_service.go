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

package test

import (
	"io"
	"xenon/host"
	"xenon/operation"

	"golang.org/x/net/context"
)

type MockService struct {
	data interface{}
}

func NewMockService(data interface{}) host.Service {
	s := MockService{
		data: data,
	}

	return host.NewServiceContext(&s)
}

func (s *MockService) GetState() interface{} {
	return s.data
}

func (s *MockService) HandleGet(ctx context.Context, op *operation.Operation) {
	op.SetBody(s.data).Complete()
}

func (s *MockService) HandlePatch(ctx context.Context, op *operation.Operation) {
	err := op.DecodeBody(&s.data)
	if err != nil && err != io.EOF {
		op.Fail(err)
		return
	}

	op.SetBody(s.data).Complete()
}

func (s *MockService) HandlePost(ctx context.Context, op *operation.Operation) {
	s.HandlePatch(ctx, op)
}

func (s *MockService) HandleDelete(ctx context.Context, op *operation.Operation) {
	// TODO: stop/unregister this service
	s.data = nil
	op.Complete()
}
