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
