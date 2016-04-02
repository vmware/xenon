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
