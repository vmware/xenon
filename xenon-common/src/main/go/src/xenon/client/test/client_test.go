package test

// Note: we have an import cycle if we put this test in the ../client package

import (
	"testing"
	"xenon/client"
	"xenon/common/test"
	"xenon/host"
	"xenon/operation"

	"golang.org/x/net/context"
)

func TestRetry(t *testing.T) {
	th := test.NewServiceHost(t)

	state := &host.ExampleServiceDocument{}
	stateURI := th.StartMock(state)

	client.DefaultTransport.DisableKeepAlives = true

	get := func(retry int) error {
		ctx := context.Background()
		op := operation.NewGet(ctx, stateURI)
		op.SetRetryCount(retry)
		return client.Send(op).Wait()
	}

	if err := get(0); err != nil {
		t.Fatal(err)
	}

	client.DefaultTransport.CloseIdleConnections()
	if err := th.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := get(1); err == nil {
		t.Fatal("expected error")
	}
}
