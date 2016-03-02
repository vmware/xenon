package test

import (
	"fmt"
	"time"
	"xenon/client"
	"xenon/operation"
	"xenon/uri"

	"golang.org/x/net/context"
)

type pollFunc func() (bool, error)

func poll(ctx context.Context, fn pollFunc) error {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			ok, err := fn()
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		}
	}
}

func WaitForNonEmptyBody(ctx context.Context, u uri.URI) error {
	return poll(ctx, func() (bool, error) {
		op := operation.NewGet(ctx, u)
		if err := client.Send(op).Wait(); err != nil {
			return false, fmt.Errorf("Error issuing GET: %s", err)
		}

		var m map[string]interface{}
		if err := op.DecodeBody(&m); err != nil {
			return false, fmt.Errorf("Error decoding body: %s", err)
		}

		return len(m) > 0, nil
	})
}
