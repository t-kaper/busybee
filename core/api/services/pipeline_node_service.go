package services

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
	"strings"
)

// ListPipelineNodes читает все node одного pipeline после валидации id.
func (s *Service) ListPipelineNodes(ctx context.Context, pipelineID int) ([]models.PipelineNodeListItem, error) {
	if pipelineID <= 0 {
		return nil, httpx.ValidationError("pipeline id must be greater than 0")
	}
	if _, err := s.repo.GetPipeline(ctx, pipelineID); err != nil {
		return nil, err
	}

	return s.repo.ListPipelineNodes(ctx, pipelineID)
}

// GetPipelineNode читает один node pipeline после валидации id.
func (s *Service) GetPipelineNode(ctx context.Context, pipelineID int, nodeID int) (models.PipelineNode, error) {
	if pipelineID <= 0 {
		return models.PipelineNode{}, httpx.ValidationError("pipeline id must be greater than 0")
	}
	if nodeID <= 0 {
		return models.PipelineNode{}, httpx.ValidationError("node id must be greater than 0")
	}
	if _, err := s.repo.GetPipeline(ctx, pipelineID); err != nil {
		return models.PipelineNode{}, err
	}

	return s.repo.GetPipelineNode(ctx, pipelineID, nodeID)
}

// CreatePipelineNode валидирует вход и создает новый node pipeline.
func (s *Service) CreatePipelineNode(ctx context.Context, pipelineID int, input models.CreatePipelineNodeInput) (models.PipelineNode, error) {
	if pipelineID <= 0 {
		return models.PipelineNode{}, httpx.ValidationError("pipeline id must be greater than 0")
	}

	if _, err := s.repo.GetPipeline(ctx, pipelineID); err != nil {
		return models.PipelineNode{}, err
	}
	if err := s.ensurePipelineGraphMutable(ctx, pipelineID); err != nil {
		return models.PipelineNode{}, err
	}

	name := strings.TrimSpace(input.Name)
	if name == "" {
		return models.PipelineNode{}, httpx.ValidationError("name is required")
	}
	command := strings.TrimSpace(input.Command)
	if command == "" {
		return models.PipelineNode{}, httpx.ValidationError("command is required")
	}
	executorType := strings.TrimSpace(input.ExecutorType)
	if executorType == "" {
		executorType = "local"
	}
	if input.MaxRetries < 0 {
		return models.PipelineNode{}, httpx.ValidationError("max_retries must be greater than or equal to 0")
	}
	if input.TimeoutSeconds == 0 {
		input.TimeoutSeconds = 3600
	}
	if input.TimeoutSeconds <= 0 {
		return models.PipelineNode{}, httpx.ValidationError("timeout_seconds must be greater than 0")
	}
	if input.Meta == nil {
		input.Meta = map[string]any{}
	}

	return s.repo.CreatePipelineNode(ctx, pipelineID, models.CreatePipelineNodeInput{
		Name:           name,
		Description:    strings.TrimSpace(input.Description),
		Command:        command,
		ExecutorType:   executorType,
		MaxRetries:     input.MaxRetries,
		TimeoutSeconds: input.TimeoutSeconds,
		Meta:           input.Meta,
	})
}

// UpdatePipelineNode применяет partial update к node pipeline.
func (s *Service) UpdatePipelineNode(ctx context.Context, pipelineID int, nodeID int, input models.UpdatePipelineNodeInput) (models.PipelineNode, error) {
	if pipelineID <= 0 {
		return models.PipelineNode{}, httpx.ValidationError("pipeline id must be greater than 0")
	}
	if nodeID <= 0 {
		return models.PipelineNode{}, httpx.ValidationError("node id must be greater than 0")
	}
	if !input.HasChanges() {
		return models.PipelineNode{}, httpx.ValidationError("request body must contain at least one field")
	}

	if _, err := s.repo.GetPipeline(ctx, pipelineID); err != nil {
		return models.PipelineNode{}, err
	}
	if err := s.ensurePipelineGraphMutable(ctx, pipelineID); err != nil {
		return models.PipelineNode{}, err
	}

	node, err := s.repo.GetPipelineNode(ctx, pipelineID, nodeID)
	if err != nil {
		return models.PipelineNode{}, err
	}

	if input.Name.Set {
		node.Name = strings.TrimSpace(input.Name.Value)
		if node.Name == "" {
			return models.PipelineNode{}, httpx.ValidationError("name must not be empty")
		}
	}
	if input.Description.Set {
		node.Description = strings.TrimSpace(input.Description.Value)
	}
	if input.Command.Set {
		node.Command = strings.TrimSpace(input.Command.Value)
		if node.Command == "" {
			return models.PipelineNode{}, httpx.ValidationError("command must not be empty")
		}
	}
	if input.ExecutorType.Set {
		node.ExecutorType = strings.TrimSpace(input.ExecutorType.Value)
		if node.ExecutorType == "" {
			return models.PipelineNode{}, httpx.ValidationError("executor_type must not be empty")
		}
	}
	if input.MaxRetries.Set {
		if input.MaxRetries.Value < 0 {
			return models.PipelineNode{}, httpx.ValidationError("max_retries must be greater than or equal to 0")
		}
		node.MaxRetries = input.MaxRetries.Value
	}
	if input.TimeoutSeconds.Set {
		if input.TimeoutSeconds.Value <= 0 {
			return models.PipelineNode{}, httpx.ValidationError("timeout_seconds must be greater than 0")
		}
		node.TimeoutSeconds = input.TimeoutSeconds.Value
	}
	if input.Meta.Set {
		node.Meta = input.Meta.Value
		if node.Meta == nil {
			node.Meta = map[string]any{}
		}
	}

	return s.repo.UpdatePipelineNode(ctx, node)
}

// DeletePipelineNode удаляет node pipeline после проверки runtime-ограничений.
func (s *Service) DeletePipelineNode(ctx context.Context, pipelineID int, nodeID int) error {
	if pipelineID <= 0 {
		return httpx.ValidationError("pipeline id must be greater than 0")
	}
	if nodeID <= 0 {
		return httpx.ValidationError("node id must be greater than 0")
	}

	pipeline, err := s.repo.GetPipeline(ctx, pipelineID)
	if err != nil {
		return err
	}
	if err := s.ensurePipelineGraphMutable(ctx, pipelineID); err != nil {
		return err
	}
	if _, err := s.repo.GetPipelineNode(ctx, pipelineID, nodeID); err != nil {
		return err
	}

	if pipeline.IsActive {
		graph, err := s.repo.GetPipelineGraph(ctx, pipelineID)
		if err != nil {
			return err
		}
		nodes, edges := removeNodeFromGraph(graph.Nodes, graph.Edges, nodeID)
		if err := validatePipelineGraphActivatable(nodes, edges); err != nil {
			return err
		}
	}

	return s.repo.DeletePipelineNode(ctx, pipelineID, nodeID)
}
