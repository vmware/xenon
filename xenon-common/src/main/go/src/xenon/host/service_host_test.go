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

package host_test

import (
	"fmt"
	"os"
	"testing"
	"time"
	"xenon/client"
	"xenon/common/test"
	"xenon/host"
	"xenon/operation"
	"xenon/uri"

	"golang.org/x/net/context"

	"github.com/stretchr/testify/assert"
)

type sleepingService struct {
	host.MinimalService

	sleep time.Duration
}

func (s *sleepingService) HandleStart(ctx context.Context, op *operation.Operation) {
	time.Sleep(s.sleep)
	op.Complete()
}

func (s *sleepingService) HandleRequest(ctx context.Context, op *operation.Operation) {
	op.Complete()
}

func TestTwoStageStart(t *testing.T) {
	ctx := context.Background()
	th := test.NewServiceHost(t)
	defer th.Stop()

	// Create 10 sleeping services
	for i := 0; i < 10; i++ {
		s := &sleepingService{sleep: time.Millisecond * time.Duration(10+i)}
		uri := uri.Extend(uri.Empty(), fmt.Sprintf("/%02d", i))
		op := operation.NewPost(ctx, uri, nil)
		th.StartService(op, s)

		// Ignore completion of the start operation
	}

	// Create requests (the services are still starting at this point)
	ops := make([]*operation.Operation, 0)
	for i := 0; i < 10; i++ {
		op := operation.NewGet(ctx, uri.Extend(th.URI(), fmt.Sprintf("/%02d", i)))
		ops = append(ops, op)
		go client.Send(op)
	}

	_, err := operation.Join(ops)
	if err != nil {
		t.Error(err)
	}
}

func TestGetServiceHostManagementState(t *testing.T) {
	ctx := context.Background()
	computeHostID := os.Getenv("DCP_TEST_COMPUTE_HOST")
	if computeHostID == "" {
		t.SkipNow()
	}

	op := operation.NewOperation(ctx)
	op.SetExpiration(time.Now().Add(time.Second * 5))

	go func() {
		op.Start()
		nodeState, err := host.GetServiceHostManagementState(ctx)
		if err != nil {
			op.Fail(err)
			return
		}
		assert.True(t, nodeState.ID != "")
		op.Complete()
	}()

	if err := op.Wait(); err != nil {
		t.Error(err)
	}
}

func TestContentType(t *testing.T) {
	ctx := context.Background()
	th := test.NewServiceHost(t)
	defer th.Stop()

	inState := &host.ExampleServiceDocument{}
	inState.String = "TEST STRING"
	descURI := th.StartMock(inState)

	outState := &host.ExampleServiceDocument{}
	op := operation.NewGet(ctx, descURI)
	err := client.Send(op).Wait()
	assert.Nil(t, err)

	err = op.DecodeBody(outState)
	assert.Nil(t, err)
	assert.Equal(t, inState.String, outState.String)

	contentType := op.GetResponseHeader("Content-Type")
	assert.NotNil(t, contentType)
	assert.Equal(t, contentType, operation.ApplicationJSON)
}
