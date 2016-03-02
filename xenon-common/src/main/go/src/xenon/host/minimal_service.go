package host

import (
	"errors"
	"xenon/operation"

	"golang.org/x/net/context"
)

// MinimalService is a bare bones implementation of the MicroService interface.
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
