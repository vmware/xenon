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
	"bufio"
	"bytes"
	"runtime"
)

func ToServerErrorResponse(err error, statusCode int) *ServiceErrorResponse {
	var stackTrace []string
	buf := make([]byte, 4096)
	buf = buf[:runtime.Stack(buf, false)]

	r := bufio.NewReader(bytes.NewReader(buf))
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		stackTrace = append(stackTrace, string(line[:len(line)-2]))
	}

	return &ServiceErrorResponse{
		Message:    err.Error(),
		StackTrace: stackTrace,
		StatusCode: statusCode,
	}
}
