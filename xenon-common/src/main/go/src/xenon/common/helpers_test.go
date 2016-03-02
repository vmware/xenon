package common

import (
	"os"
	"testing"
)

func TestToServerErrorResponse(t *testing.T) {
	res := ToServerErrorResponse(os.ErrInvalid)

	if res.Message != os.ErrInvalid.Error() {
		t.Error(res.Message)
	}

	if len(res.StackTrace) != 9 {
		t.Errorf("len(res.StackTrace) == %d", len(res.StackTrace))
	}
}
