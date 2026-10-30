// Package models описывает доменные типы минимального HTTP API BusyBee.
package models

import (
	"busybee/core/api/httpx"
	"time"
)

// PipelineGroup описывает запись из busybee_pipeline_group.
type PipelineGroup struct {
	ID          int       `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
}

// PipelineListItem описывает pipeline в списке и в ответе GET by id.
type PipelineListItem struct {
	ID              int       `json:"id"`
	Name            string    `json:"name"`
	Description     string    `json:"description"`
	PipelineGroupID *int      `json:"pipeline_group_id"`
	Schedule        *string   `json:"schedule"`
	IsActive        bool      `json:"is_active"`
	MaxActiveRuns   int       `json:"max_active_runs"`
	NodeCount       int       `json:"node_count"`
	HasActiveRuns   bool      `json:"has_active_runs"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// Pipeline возвращает подробную метаинформацию pipeline.
type Pipeline = PipelineListItem

// PipelineListFilters задает пагинацию списка pipeline.
type PipelineListFilters struct {
	Limit  int
	Offset int
}

// CreatePipelineInput описывает payload создания pipeline.
type CreatePipelineInput struct {
	Name            string
	Description     string
	PipelineGroupID *int
	Schedule        *string
	IsActive        bool
	MaxActiveRuns   int
}

// UpdatePipelineInput описывает partial update pipeline.
type UpdatePipelineInput struct {
	Name            httpx.Optional[string]
	Description     httpx.Optional[string]
	PipelineGroupID httpx.Optional[*int]
	Schedule        httpx.Optional[*string]
	IsActive        httpx.Optional[bool]
	MaxActiveRuns   httpx.Optional[int]
}

// HasChanges сообщает, содержит ли patch pipeline хотя бы одно изменяемое поле.
func (u UpdatePipelineInput) HasChanges() bool {
	return u.Name.Set ||
		u.Description.Set ||
		u.PipelineGroupID.Set ||
		u.Schedule.Set ||
		u.IsActive.Set ||
		u.MaxActiveRuns.Set
}

// PipelineNode описывает узел pipeline в CRUD API и graph endpoint.
type PipelineNode struct {
	ID             int            `json:"id"`
	PipelineID     int            `json:"pipeline_id"`
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	Command        string         `json:"command"`
	ExecutorType   string         `json:"executor_type"`
	MaxRetries     int            `json:"max_retries"`
	TimeoutSeconds int            `json:"timeout_seconds"`
	Meta           map[string]any `json:"meta"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// PipelineNodeListItem описывает узел pipeline в списке.
type PipelineNodeListItem = PipelineNode

// CreatePipelineNodeInput описывает payload создания узла pipeline.
type CreatePipelineNodeInput struct {
	Name           string
	Description    string
	Command        string
	ExecutorType   string
	MaxRetries     int
	TimeoutSeconds int
	Meta           map[string]any
}

// UpdatePipelineNodeInput описывает partial update узла pipeline.
type UpdatePipelineNodeInput struct {
	Name           httpx.Optional[string]
	Description    httpx.Optional[string]
	Command        httpx.Optional[string]
	ExecutorType   httpx.Optional[string]
	MaxRetries     httpx.Optional[int]
	TimeoutSeconds httpx.Optional[int]
	Meta           httpx.Optional[map[string]any]
}

// HasChanges сообщает, содержит ли patch узла pipeline хотя бы одно изменяемое поле.
func (u UpdatePipelineNodeInput) HasChanges() bool {
	return u.Name.Set ||
		u.Description.Set ||
		u.Command.Set ||
		u.ExecutorType.Set ||
		u.MaxRetries.Set ||
		u.TimeoutSeconds.Set ||
		u.Meta.Set
}

// PipelineEdge описывает связь между двумя node одного pipeline.
type PipelineEdge struct {
	ParentNodeID int `json:"parent_node_id"`
	ChildNodeID  int `json:"child_node_id"`
}

// CreatePipelineEdgeInput описывает payload создания ребра pipeline.
type CreatePipelineEdgeInput struct {
	ParentNodeID int
	ChildNodeID  int
}

// DeletePipelineEdgeInput описывает payload удаления ребра pipeline.
type DeletePipelineEdgeInput struct {
	ParentNodeID int
	ChildNodeID  int
}

// PipelineGraph возвращает pipeline вместе с его node и edge.
type PipelineGraph struct {
	Pipeline Pipeline       `json:"pipeline"`
	Nodes    []PipelineNode `json:"nodes"`
	Edges    []PipelineEdge `json:"edges"`
}

// PipelineRunListFilters задает фильтры списка горячих запусков pipeline.
type PipelineRunListFilters struct {
	PipelineID  *int
	Status      *string
	TriggerType *string
	CreatedFrom *time.Time
	CreatedTo   *time.Time
}

// PipelineRunStats описывает агрегированное состояние нод внутри одного run.
type PipelineRunStats struct {
	TotalNodes      int `json:"total_nodes"`
	NotStartedNodes int `json:"not_started_nodes"`
	PendingNodes    int `json:"pending_nodes"`
	QueuedNodes     int `json:"queued_nodes"`
	RunningNodes    int `json:"running_nodes"`
	SuccessNodes    int `json:"success_nodes"`
	FailedNodes     int `json:"failed_nodes"`
	StoppedNodes    int `json:"stopped_nodes"`
	TotalAttempts   int `json:"total_attempts"`
}

// PipelineRunListItem описывает один горячий запуск pipeline в списке.
type PipelineRunListItem struct {
	ID           string           `json:"id"`
	PipelineID   int              `json:"pipeline_id"`
	PipelineName string           `json:"pipeline_name"`
	Status       string           `json:"status"`
	TriggerType  string           `json:"trigger_type"`
	CreatedAt    time.Time        `json:"created_at"`
	FinishedAt   *time.Time       `json:"finished_at"`
	Stats        PipelineRunStats `json:"stats"`
}

// PipelineRun описывает один горячий запуск pipeline.
type PipelineRun = PipelineRunListItem

// PipelineNodeRunAttempt описывает одну попытку выполнения ноды внутри run.
type PipelineNodeRunAttempt struct {
	ID             int        `json:"id"`
	PipelineNodeID int        `json:"pipeline_node_id"`
	PipelineRunID  string     `json:"pipeline_run_id"`
	Status         string     `json:"status"`
	TriggerType    string     `json:"trigger_type"`
	PID            *int       `json:"pid"`
	ExitCode       *int       `json:"exit_code"`
	RetryCount     int        `json:"retry_count"`
	ScheduledAt    *time.Time `json:"scheduled_at"`
	StartedAt      *time.Time `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at"`
	ErrorMsg       *string    `json:"error_msg"`
	LogPath        *string    `json:"log_path"`
	CreatedAt      time.Time  `json:"created_at"`
}

// PipelineRunNode описывает одну ноду pipeline в детальном снимке run.
type PipelineRunNode struct {
	PipelineNodeID  int                      `json:"pipeline_node_id"`
	NodeName        string                   `json:"node_name"`
	NodeDescription string                   `json:"node_description"`
	Command         string                   `json:"command"`
	ExecutorType    string                   `json:"executor_type"`
	MaxRetries      int                      `json:"max_retries"`
	TimeoutSeconds  int                      `json:"timeout_seconds"`
	Meta            map[string]any           `json:"meta"`
	Status          string                   `json:"status"`
	Attempts        []PipelineNodeRunAttempt `json:"attempts"`
}

// PipelineRunDetail возвращает run вместе с агрегированной картиной по его нодам.
type PipelineRunDetail struct {
	Run        PipelineRun       `json:"run"`
	Stats      PipelineRunStats  `json:"stats"`
	Nodes      []PipelineRunNode `json:"nodes"`
	SnapshotAt time.Time         `json:"snapshot_at"`
}

// PipelineNodeRunLog содержит лог конкретного node_run для text/plain endpoint.
type PipelineNodeRunLog struct {
	NodeRunID int
	LogPath   string
	Content   string
}

// PipelineSummary описывает pipeline в расширенном списке групп.
type PipelineSummary struct {
	ID        int     `json:"id"`
	Name      string  `json:"name"`
	Schedule  *string `json:"schedule"`
	IsActive  bool    `json:"is_active"`
	NodeCount int     `json:"node_count"`
}

// PipelineGroupDetailed возвращает группу вместе со связанными pipeline.
type PipelineGroupDetailed struct {
	ID          int               `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	CreatedAt   time.Time         `json:"created_at"`
	Pipelines   []PipelineSummary `json:"pipelines"`
}

// PipelineGroupListFilters задает пагинацию списка групп pipeline.
type PipelineGroupListFilters struct {
	Limit  int
	Offset int
}

// CreatePipelineGroupInput описывает payload создания группы pipeline.
type CreatePipelineGroupInput struct {
	Name        string
	Description string
}

// UpdatePipelineGroupInput описывает partial update группы pipeline.
type UpdatePipelineGroupInput struct {
	Name        httpx.Optional[string]
	Description httpx.Optional[string]
}

// HasChanges сообщает, содержит ли patch группы хотя бы одно изменяемое поле.
func (u UpdatePipelineGroupInput) HasChanges() bool {
	return u.Name.Set || u.Description.Set
}
