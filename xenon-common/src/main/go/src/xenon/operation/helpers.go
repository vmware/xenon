package operation

import (
	"io"
	"xenon/uri"

	"golang.org/x/net/context"
)

// contextKey is a private, unexported type so that no code outside this
// package can have a key collide on the values associated with a context.
type contextKey int

const authorizationCookieKey contextKey = 1

const RequestAuthTokenHeader = "x-xenon-auth-token"

// Set authorization token and return new context.
func SetAuthorizationToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, authorizationCookieKey, token)
}

// Get authorization token on operation from the specified context.
func GetAuthorizationToken(ctx context.Context) string {
	token := ctx.Value(authorizationCookieKey)
	if token == nil {
		return ""
	}

	return token.(string)
}

func NewGet(ctx context.Context, u uri.URI) *Operation {
	return NewOperation(ctx).
		SetMethod("GET").
		SetURI(u)
}

func NewPost(ctx context.Context, u uri.URI, r io.Reader) *Operation {
	return NewOperation(ctx).
		SetMethod("POST").
		SetURI(u).
		SetBody(r)
}

func NewPatch(ctx context.Context, u uri.URI, r io.Reader) *Operation {
	return NewOperation(ctx).
		SetMethod("PATCH").
		SetURI(u).
		SetBody(r)
}

func NewPut(ctx context.Context, u uri.URI, r io.Reader) *Operation {
	return NewOperation(ctx).
		SetMethod("PUT").
		SetURI(u).
		SetBody(r)
}

func NewDelete(ctx context.Context, u uri.URI) *Operation {
	return NewOperation(ctx).
		SetMethod("DELETE").
		SetURI(u)
}

func (o *Operation) NewGet(ctx context.Context, u uri.URI) *Operation {
	return NewGet(ctx, u)
}

func (o *Operation) NewPost(ctx context.Context, u uri.URI, r io.Reader) *Operation {
	return NewPost(ctx, u, r)
}

func (o *Operation) NewPatch(ctx context.Context, u uri.URI, r io.Reader) *Operation {
	return NewPatch(ctx, u, r)
}

func (o *Operation) NewPut(ctx context.Context, u uri.URI, r io.Reader) *Operation {
	return NewPut(ctx, u, r)
}

func (o *Operation) NewDelete(ctx context.Context, u uri.URI) *Operation {
	return NewDelete(ctx, u)
}
