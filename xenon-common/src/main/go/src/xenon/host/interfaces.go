package host

import (
	"xenon/operation"

	"golang.org/x/net/context"
)

type StartHandler interface {
	HandleStart(ctx context.Context, op *operation.Operation)
}

type RequestHandler interface {
	HandleRequest(ctx context.Context, op *operation.Operation)
}

type GetHandler interface {
	HandleGet(ctx context.Context, op *operation.Operation)
}

type PostHandler interface {
	HandlePost(ctx context.Context, op *operation.Operation)
}

type PatchHandler interface {
	HandlePatch(ctx context.Context, op *operation.Operation)
}

type PutHandler interface {
	HandlePut(ctx context.Context, op *operation.Operation)
}

type DeleteHandler interface {
	HandleDelete(ctx context.Context, op *operation.Operation)
}
