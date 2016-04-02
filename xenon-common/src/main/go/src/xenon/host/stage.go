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
	"errors"
	"sync"
)

var ErrStageBackwards = errors.New("service: processing stage cannot move backwards")

type Stage int

const (
	// Service object is instantiated. This is the initial stage.
	StageCreated = iota + 1

	// Service is attached to service host.
	StageInitialized

	// Service is visible to other services (its URI is registered) and can
	// process self issued-operations and durable store has its state available
	// for access. Operations issued by other services or clients are queued.
	StageStarted

	// Service is ready for operation processing. Any operations received while
	// in the STARTED or INITIALIZED stage will be dequeued.
	StageAvailable

	// Service is stopped and its resources have been released.
	StageStopped
)

type StageContainer struct {
	sync.Mutex

	s Stage
	c map[Stage][]chan struct{}
}

type StageHandler interface {
	Stage() Stage
	SetStage(s Stage) error
	StageBarrier(s Stage) chan struct{}
}

func (sc *StageContainer) Stage() Stage {
	return sc.s
}

func (sc *StageContainer) SetStage(s Stage) error {
	sc.Lock()
	defer sc.Unlock()

	if sc.s >= s {
		return ErrStageBackwards
	}

	sc.s = s

	if sc.c != nil {
		// Notify waiters
		for _, c := range sc.c[s] {
			close(c)
		}

		// Remove waiters
		delete(sc.c, s)
	}

	return nil
}

// StageBarrier returns a channel that is closed as soon as the underlying
// stage moves to the specified stage. If the stage is already there, a closed
// channel is returned. It is up to the caller of this function to select on
// this channel to wait for the stage transition.
func (sc *StageContainer) StageBarrier(s Stage) chan struct{} {
	sc.Lock()
	defer sc.Unlock()

	ch := make(chan struct{})
	if sc.s >= s {
		close(ch)
	} else {
		if sc.c == nil {
			sc.c = make(map[Stage][]chan struct{})
		}
		sc.c[s] = append(sc.c[s], ch)
	}

	return ch
}
