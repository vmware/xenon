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
	computeHostID := os.Getenv("XENON_TEST_COMPUTE_HOST")
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
