package requests

import "busybee/core/api/httpx"

// CreatePipelineNodeRequest описывает JSON body для создания узла pipeline.
type CreatePipelineNodeRequest struct {
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	Command        string         `json:"command"`
	ExecutorType   string         `json:"executor_type"`
	MaxRetries     int            `json:"max_retries"`
	TimeoutSeconds int            `json:"timeout_seconds"`
	Meta           map[string]any `json:"meta"`
}

// UpdatePipelineNodeRequest описывает JSON body для partial update узла pipeline.
type UpdatePipelineNodeRequest struct {
	Name           httpx.Optional[string]         `json:"name"`
	Description    httpx.Optional[string]         `json:"description"`
	Command        httpx.Optional[string]         `json:"command"`
	ExecutorType   httpx.Optional[string]         `json:"executor_type"`
	MaxRetries     httpx.Optional[int]            `json:"max_retries"`
	TimeoutSeconds httpx.Optional[int]            `json:"timeout_seconds"`
	Meta           httpx.Optional[map[string]any] `json:"meta"`
}
