package dto

import "context"

// Service описывает минимальный контракт, который нужен HTTP-хендлерам.
type Service interface {
	ListPipelineGroups(ctx context.Context, filters PipelineGroupListFilters) ([]PipelineGroup, int, error)
	ListPipelineGroupsDetailed(ctx context.Context, filters PipelineGroupListFilters) ([]PipelineGroupDetailed, int, error)
	ListPipelines(ctx context.Context, filters PipelineListFilters) ([]PipelineListItem, int, error)
	ListPipelineRuns(ctx context.Context, filters PipelineRunListFilters) ([]PipelineRunListItem, int, error)
	ListPipelineNodes(ctx context.Context, pipelineID int) ([]PipelineNodeListItem, error)
	ListPipelineEdges(ctx context.Context, pipelineID int) ([]PipelineEdge, error)
	GetPipeline(ctx context.Context, pipelineID int) (Pipeline, error)
	GetPipelineRun(ctx context.Context, runID string) (PipelineRunDetail, error)
	GetPipelineNode(ctx context.Context, pipelineID int, nodeID int) (PipelineNode, error)
	GetPipelineNodeRunLog(ctx context.Context, nodeRunID int) (PipelineNodeRunLog, error)
	CreatePipelineGroup(ctx context.Context, input CreatePipelineGroupInput) (PipelineGroup, error)
	CreatePipeline(ctx context.Context, input CreatePipelineInput) (Pipeline, error)
	CreatePipelineNode(ctx context.Context, pipelineID int, input CreatePipelineNodeInput) (PipelineNode, error)
	CreatePipelineEdge(ctx context.Context, pipelineID int, input CreatePipelineEdgeInput) (PipelineEdge, error)
	UpdatePipelineGroup(ctx context.Context, groupID int, input UpdatePipelineGroupInput) (PipelineGroup, error)
	UpdatePipeline(ctx context.Context, pipelineID int, input UpdatePipelineInput) (Pipeline, error)
	UpdatePipelineNode(ctx context.Context, pipelineID int, nodeID int, input UpdatePipelineNodeInput) (PipelineNode, error)
	DeletePipelineGroup(ctx context.Context, groupID int) error
	DeletePipeline(ctx context.Context, pipelineID int) error
	DeletePipelineNode(ctx context.Context, pipelineID int, nodeID int) error
	DeletePipelineEdge(ctx context.Context, pipelineID int, input DeletePipelineEdgeInput) error
	GetPipelineGraph(ctx context.Context, pipelineID int) (PipelineGraph, error)
}
