package services

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
)

// ListPipelineEdges читает все edge одного pipeline после валидации id.
func (s *Service) ListPipelineEdges(ctx context.Context, pipelineID int) ([]models.PipelineEdge, error) {
	if pipelineID <= 0 {
		return nil, httpx.ValidationError("pipeline id must be greater than 0")
	}
	if _, err := s.repo.GetPipeline(ctx, pipelineID); err != nil {
		return nil, err
	}

	return s.repo.ListPipelineEdges(ctx, pipelineID)
}

// CreatePipelineEdge валидирует вход и создает новое edge pipeline.
func (s *Service) CreatePipelineEdge(ctx context.Context, pipelineID int, input models.CreatePipelineEdgeInput) (models.PipelineEdge, error) {
	if pipelineID <= 0 {
		return models.PipelineEdge{}, httpx.ValidationError("pipeline id must be greater than 0")
	}
	if input.ParentNodeID <= 0 {
		return models.PipelineEdge{}, httpx.ValidationError("parent_node_id must be greater than 0")
	}
	if input.ChildNodeID <= 0 {
		return models.PipelineEdge{}, httpx.ValidationError("child_node_id must be greater than 0")
	}
	if input.ParentNodeID == input.ChildNodeID {
		return models.PipelineEdge{}, httpx.ValidationError("pipeline graph must not contain self-loop edges")
	}

	pipeline, err := s.repo.GetPipeline(ctx, pipelineID)
	if err != nil {
		return models.PipelineEdge{}, err
	}
	if err := s.ensurePipelineGraphMutable(ctx, pipelineID); err != nil {
		return models.PipelineEdge{}, err
	}
	if _, err := s.repo.GetPipelineNode(ctx, pipelineID, input.ParentNodeID); err != nil {
		return models.PipelineEdge{}, err
	}
	if _, err := s.repo.GetPipelineNode(ctx, pipelineID, input.ChildNodeID); err != nil {
		return models.PipelineEdge{}, err
	}

	graph, err := s.repo.GetPipelineGraph(ctx, pipelineID)
	if err != nil {
		return models.PipelineEdge{}, err
	}

	edge := models.PipelineEdge{
		ParentNodeID: input.ParentNodeID,
		ChildNodeID:  input.ChildNodeID,
	}
	edges := addEdgeToGraph(graph.Edges, edge)
	if err := validatePipelineGraphDefinition(graph.Nodes, edges); err != nil {
		return models.PipelineEdge{}, err
	}
	if pipeline.IsActive {
		if err := validatePipelineGraphActivatable(graph.Nodes, edges); err != nil {
			return models.PipelineEdge{}, err
		}
	}

	return s.repo.CreatePipelineEdge(ctx, edge)
}

// DeletePipelineEdge удаляет одно edge pipeline после проверки runtime-ограничений.
func (s *Service) DeletePipelineEdge(ctx context.Context, pipelineID int, input models.DeletePipelineEdgeInput) error {
	if pipelineID <= 0 {
		return httpx.ValidationError("pipeline id must be greater than 0")
	}
	if input.ParentNodeID <= 0 {
		return httpx.ValidationError("parent_node_id must be greater than 0")
	}
	if input.ChildNodeID <= 0 {
		return httpx.ValidationError("child_node_id must be greater than 0")
	}

	pipeline, err := s.repo.GetPipeline(ctx, pipelineID)
	if err != nil {
		return err
	}
	if err := s.ensurePipelineGraphMutable(ctx, pipelineID); err != nil {
		return err
	}

	graph, err := s.repo.GetPipelineGraph(ctx, pipelineID)
	if err != nil {
		return err
	}

	edge := models.PipelineEdge{
		ParentNodeID: input.ParentNodeID,
		ChildNodeID:  input.ChildNodeID,
	}
	edges, removed := removeEdgeFromGraph(graph.Edges, edge)
	if !removed {
		return httpx.NotFoundError(
			"pipeline edge not found",
		)
	}
	if pipeline.IsActive {
		if err := validatePipelineGraphActivatable(graph.Nodes, edges); err != nil {
			return err
		}
	}

	return s.repo.DeletePipelineEdge(ctx, pipelineID, edge)
}
