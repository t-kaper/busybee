package requests

import "busybee/core/api/httpx"

// CreatePipelineRequest описывает JSON body для создания pipeline.
type CreatePipelineRequest struct {
	Name            string  `json:"name"`
	Description     string  `json:"description"`
	PipelineGroupID *int    `json:"pipeline_group_id"`
	Schedule        *string `json:"schedule"`
	IsActive        bool    `json:"is_active"`
	MaxActiveRuns   int     `json:"max_active_runs"`
}

// UpdatePipelineRequest описывает JSON body для partial update pipeline.
type UpdatePipelineRequest struct {
	Name            httpx.Optional[string]  `json:"name"`
	Description     httpx.Optional[string]  `json:"description"`
	PipelineGroupID httpx.Optional[*int]    `json:"pipeline_group_id"`
	Schedule        httpx.Optional[*string] `json:"schedule"`
	IsActive        httpx.Optional[bool]    `json:"is_active"`
	MaxActiveRuns   httpx.Optional[int]     `json:"max_active_runs"`
}
