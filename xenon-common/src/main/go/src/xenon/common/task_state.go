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
	t.Failure = ToServerErrorResponse(err)
}
