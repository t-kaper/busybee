package requests

import "busybee/core/api/httpx"

// CreatePipelineGroupRequest описывает JSON body для создания группы pipeline.
type CreatePipelineGroupRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// UpdatePipelineGroupRequest описывает JSON body для partial update группы pipeline.
type UpdatePipelineGroupRequest struct {
	Name        httpx.Optional[string] `json:"name"`
	Description httpx.Optional[string] `json:"description"`
}
