package operation

import (
	"sync"
	"time"

	"github.com/golang/glog"

	"golang.org/x/net/context"
)

// Operations can finish through one of two ways. Either:
//
//   * The operation finishes by itself, with or without an error (inside out)
//   * The operation is cancelled (outside in)
//
// For the inside-out completion, the operation has a done channel field. This
// channel is closed as soon as the operation is set to be completed (with or
// without failure, through Complete() or Fail(err error) respectively). Users
// can get a reference to this channel through the Done() function and use it
// directly to monitor for operation completion.
//
// For the outside-in completion, the operation starts a separate goroutine
// when Start() is called. This routine monitors the context for being done and
// fails the operation when it does. By failing the operation when its context
// is cancelled, the user only has to monitor a single channel to catch both
// the inside-out and the outside-in completion. Note: the done channel of the
// context is only closed when the context is cancelled, either through a
// manual cancellation call, or a timeout.
//
// In all cases an operation can only be used once.  Once the done channel is
// closed, the operation cannot be reissued.

const (
	defaultRetryCount = 2
	DefaultTimeout    = 1 * time.Minute
)

type HandlerFunc func(ctx context.Context, op *Operation)

type Operation struct {
	complete sync.Once
	start    sync.Once
	donec    chan struct{}
	err      error

	state *State
	vars  context.Context

	retryCount       int
	retriesRemaining int

	expirationTime time.Time
}

func NewOperation(vars context.Context) *Operation {
	op := Operation{
		donec: make(chan struct{}, 0),
		err:   nil,

		state: &State{},
		vars:  vars,

		retryCount:       defaultRetryCount,
		retriesRemaining: defaultRetryCount,

		expirationTime: time.Now().Add(DefaultTimeout),
	}

	return &op
}

func (op *Operation) Done() <-chan struct{} {
	return op.donec
}

func (op *Operation) Complete() {
	op.complete.Do(func() {
		close(op.donec)
	})
}

func (op *Operation) Err() error {
	return op.err
}

func (op *Operation) Fail(err error) {
	op.complete.Do(func() {
		op.err = err
		close(op.donec)
	})
}

func (op *Operation) CreateChild(ctx context.Context) *Operation {
	child := NewOperation(ctx)
	child.state = op.state
	return child
}

func (op *Operation) Go(ctx context.Context, fn HandlerFunc) *Operation {
	go fn(ctx, op)
	return op
}

func (op *Operation) Wait() error {
	<-op.Done()
	return op.Err()
}

func (op *Operation) GetRetryCount() int {
	return op.retryCount
}

func (op *Operation) SetRetryCount(retryCount int) *Operation {
	op.retryCount = retryCount
	op.retriesRemaining = retryCount
	return op
}

func (op *Operation) GetRetriesRemaining() int {
	return op.retriesRemaining
}

func (op *Operation) DecrementRetriesRemaining() int {
	n := op.retriesRemaining
	op.retriesRemaining--
	return n
}

func (op *Operation) WaitForRetryDelay(cause error) {
	retryCount := op.GetRetryCount() - op.GetRetriesRemaining()
	delaySeconds := time.Duration(retryCount) * time.Second

	glog.Warningf("retrying request in %s: %s", delaySeconds, cause)

	ticker := time.NewTicker(delaySeconds)
	<-ticker.C
	ticker.Stop()
}

// SetExpiration sets the Expiration (in absolute time) of the operation.  The
// Operation is started when Start() is called and ended when either Complete()
// or Fail() is called.
func (op *Operation) SetExpiration(t time.Time) *Operation {
	op.expirationTime = t
	return op
}

func (op *Operation) GetExpiration() time.Time {
	return op.expirationTime
}

// Start starts an operation.  If the operation is being passed to
// dcp/client.Send() to execute the operation on a remote URI, the client will
// call this for the user.  Likewise, the service host calls Start() for every
// incoming operation before calling the service handler.  Otherwise, if
// implementing an operation client or service handler idependently, the
// operation Start() routine needs to be called in order to properly check for
// timeout/error.
func (op *Operation) Start() {
	// Fail the operation if its context is cancelled.
	f := func(ctx context.Context, op *Operation) {
		select {
		case <-op.Done():
			// Operation completed, no need to wait for context.
		case <-ctx.Done():
			op.Fail(ctx.Err())
		}
	}

	op.start.Do(func() {
		duration := op.expirationTime.Sub(time.Now())
		ctx, _ := context.WithTimeout(context.Background(), duration)
		go f(ctx, op)
	})
}
