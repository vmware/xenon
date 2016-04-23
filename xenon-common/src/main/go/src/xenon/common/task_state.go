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

type TaskStage string

const (
	TaskStageCreated   = TaskStage("CREATED")
	TaskStageStarted   = TaskStage("STARTED")
	TaskStageFinished  = TaskStage("FINISHED")
	TaskStageFailed    = TaskStage("FAILED")
	TaskStageCancelled = TaskStage("CANCELLED")
)

type TaskState struct {
	Stage   TaskStage             `json:"stage,omitempty"`
	Direct  bool                  `json:"isDirect,omitempty"`
	Failure *ServiceErrorResponse `json:"failure,omitempty"`
}

func NewTaskState() *TaskState {
	return &TaskState{}
}

func (t *TaskState) SetStage(stage TaskStage) {
	t.Stage = stage
}

func (t *TaskState) SetFailure(err error) {
	t.Stage = TaskStageFailed
	t.Failure = ToServerErrorResponse(err, 400)
}
