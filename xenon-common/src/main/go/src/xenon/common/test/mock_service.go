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
