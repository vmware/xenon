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
