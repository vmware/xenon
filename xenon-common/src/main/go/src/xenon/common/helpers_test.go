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

package common

import (
	"os"
	"testing"
)

func TestToServerErrorResponse(t *testing.T) {
	res := ToServerErrorResponse(os.ErrInvalid, 400)

	if res.Message != os.ErrInvalid.Error() {
		t.Error(res.Message)
	}

	if len(res.StackTrace) != 9 {
		t.Errorf("len(res.StackTrace) == %d", len(res.StackTrace))
	}

	if res.StatusCode != 400 {
		t.Errorf("res.StatusCode  != %d", 400)
	}

}
