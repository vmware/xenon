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
	"xenon/operation"

	"golang.org/x/net/context"
)

// MinimalService is a bare bones implementation of the Stateless Service interface.
type MinimalService struct {
	StageContainer

	host     *ServiceHost
	selfLink string
}

func (m *MinimalService) Host() *ServiceHost {
	return m.host
}

func (m *MinimalService) SetHost(h *ServiceHost) {
	m.host = h
}

func (m *MinimalService) SelfLink() string {
	return m.selfLink
}

func (m *MinimalService) SetSelfLink(s string) {
	m.selfLink = s
}

func (m *MinimalService) HandleStart(ctx context.Context, op *operation.Operation) {
	op.Complete()
}

func (m *MinimalService) HandleRequest(ctx context.Context, op *operation.Operation) {
	op.Fail(errors.New("host: service not implemented"))
}
