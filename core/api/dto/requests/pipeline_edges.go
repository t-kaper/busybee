package requests

// CreatePipelineEdgeRequest описывает JSON body для создания ребра pipeline.
type CreatePipelineEdgeRequest struct {
	ParentNodeID int `json:"parent_node_id"`
	ChildNodeID  int `json:"child_node_id"`
}

// DeletePipelineEdgeRequest описывает JSON body для удаления ребра pipeline.
type DeletePipelineEdgeRequest struct {
	ParentNodeID int `json:"parent_node_id"`
	ChildNodeID  int `json:"child_node_id"`
}
