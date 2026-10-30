package api

import (
	"context"

	"busybee/core/api/dto"
	"busybee/core/api/services"
)

// handlerServiceAdapter — мост между хендлерами и сервисным слоем.
type handlerServiceAdapter struct {
	service *services.Service
}

func newHandlerServiceAdapter(service *services.Service) *handlerServiceAdapter {
	return &handlerServiceAdapter{service: service}
}

func (a *handlerServiceAdapter) ListPipelineGroups(ctx context.Context, filters dto.PipelineGroupListFilters) ([]dto.PipelineGroup, int, error) {
	return a.service.ListPipelineGroups(ctx, filters)
}

func (a *handlerServiceAdapter) ListPipelineGroupsDetailed(ctx context.Context, filters dto.PipelineGroupListFilters) ([]dto.PipelineGroupDetailed, int, error) {
	return a.service.ListPipelineGroupsDetailed(ctx, filters)
}

func (a *handlerServiceAdapter) ListPipelines(ctx context.Context, filters dto.PipelineListFilters) ([]dto.PipelineListItem, int, error) {
	return a.service.ListPipelines(ctx, filters)
}

func (a *handlerServiceAdapter) ListPipelineRuns(ctx context.Context, filters dto.PipelineRunListFilters) ([]dto.PipelineRunListItem, int, error) {
	return a.service.ListPipelineRuns(ctx, filters)
}

func (a *handlerServiceAdapter) ListPipelineNodes(ctx context.Context, pipelineID int) ([]dto.PipelineNodeListItem, error) {
	return a.service.ListPipelineNodes(ctx, pipelineID)
}

func (a *handlerServiceAdapter) ListPipelineEdges(ctx context.Context, pipelineID int) ([]dto.PipelineEdge, error) {
	return a.service.ListPipelineEdges(ctx, pipelineID)
}

func (a *handlerServiceAdapter) GetPipeline(ctx context.Context, pipelineID int) (dto.Pipeline, error) {
	return a.service.GetPipeline(ctx, pipelineID)
}

func (a *handlerServiceAdapter) GetPipelineRun(ctx context.Context, runID string) (dto.PipelineRunDetail, error) {
	return a.service.GetPipelineRun(ctx, runID)
}

func (a *handlerServiceAdapter) GetPipelineNode(ctx context.Context, pipelineID int, nodeID int) (dto.PipelineNode, error) {
	return a.service.GetPipelineNode(ctx, pipelineID, nodeID)
}

func (a *handlerServiceAdapter) GetPipelineNodeRunLog(ctx context.Context, nodeRunID int) (dto.PipelineNodeRunLog, error) {
	return a.service.GetPipelineNodeRunLog(ctx, nodeRunID)
}

func (a *handlerServiceAdapter) CreatePipelineGroup(ctx context.Context, input dto.CreatePipelineGroupInput) (dto.PipelineGroup, error) {
	return a.service.CreatePipelineGroup(ctx, input)
}

func (a *handlerServiceAdapter) CreatePipeline(ctx context.Context, input dto.CreatePipelineInput) (dto.Pipeline, error) {
	return a.service.CreatePipeline(ctx, input)
}

func (a *handlerServiceAdapter) CreatePipelineNode(ctx context.Context, pipelineID int, input dto.CreatePipelineNodeInput) (dto.PipelineNode, error) {
	return a.service.CreatePipelineNode(ctx, pipelineID, input)
}

func (a *handlerServiceAdapter) CreatePipelineEdge(ctx context.Context, pipelineID int, input dto.CreatePipelineEdgeInput) (dto.PipelineEdge, error) {
	return a.service.CreatePipelineEdge(ctx, pipelineID, input)
}

func (a *handlerServiceAdapter) UpdatePipelineGroup(ctx context.Context, groupID int, input dto.UpdatePipelineGroupInput) (dto.PipelineGroup, error) {
	return a.service.UpdatePipelineGroup(ctx, groupID, input)
}

func (a *handlerServiceAdapter) UpdatePipeline(ctx context.Context, pipelineID int, input dto.UpdatePipelineInput) (dto.Pipeline, error) {
	return a.service.UpdatePipeline(ctx, pipelineID, input)
}

func (a *handlerServiceAdapter) UpdatePipelineNode(ctx context.Context, pipelineID int, nodeID int, input dto.UpdatePipelineNodeInput) (dto.PipelineNode, error) {
	return a.service.UpdatePipelineNode(ctx, pipelineID, nodeID, input)
}

func (a *handlerServiceAdapter) DeletePipelineGroup(ctx context.Context, groupID int) error {
	return a.service.DeletePipelineGroup(ctx, groupID)
}

func (a *handlerServiceAdapter) DeletePipeline(ctx context.Context, pipelineID int) error {
	return a.service.DeletePipeline(ctx, pipelineID)
}

func (a *handlerServiceAdapter) DeletePipelineNode(ctx context.Context, pipelineID int, nodeID int) error {
	return a.service.DeletePipelineNode(ctx, pipelineID, nodeID)
}

func (a *handlerServiceAdapter) DeletePipelineEdge(ctx context.Context, pipelineID int, input dto.DeletePipelineEdgeInput) error {
	return a.service.DeletePipelineEdge(ctx, pipelineID, input)
}

func (a *handlerServiceAdapter) GetPipelineGraph(ctx context.Context, pipelineID int) (dto.PipelineGraph, error) {
	return a.service.GetPipelineGraph(ctx, pipelineID)
}
