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
