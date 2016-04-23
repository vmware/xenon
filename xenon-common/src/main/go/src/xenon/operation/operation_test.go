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
	"testing"

	"golang.org/x/net/context"
)

type exampleBody struct {
	body string
}

func TestDecodeBodyWithPresetBody(t *testing.T) {
	var src, dst *exampleBody
	var err error

	src = &exampleBody{body: "hello"}
	op := NewOperation(context.Background()).SetBody(src)

	go func() {
		op.Start()

		err = op.DecodeBody(1)
		if err == nil {
			t.Error("Expected error, got nil")
		}

		err = op.DecodeBody(&dst)
		if err != nil {
			t.Error(err)
		}

		op.Complete()
	}()

	if err = op.Wait(); err != nil {
		t.Error(err)
	}
}
