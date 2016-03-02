package common

import (
	"bufio"
	"bytes"
	"runtime"
)

func ToServerErrorResponse(err error) *ServiceErrorResponse {
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
	}
}
