package services

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
	"strings"

	"github.com/robfig/cron/v3"
)

const maxPipelinesPageSize = 500

// ListPipelines валидирует фильтры списка и делегирует чтение репозиторию.
func (s *Service) ListPipelines(ctx context.Context, filters models.PipelineListFilters) ([]models.PipelineListItem, int, error) {
	if filters.Limit <= 0 {
		filters.Limit = 100
	}
	if filters.Limit > maxPipelinesPageSize {
		return nil, 0, httpx.ValidationError("limit must be less than or equal to 500")
	}
	if filters.Offset < 0 {
		return nil, 0, httpx.ValidationError("offset must be greater than or equal to 0")
	}

	return s.repo.ListPipelines(ctx, filters)
}

// GetPipeline читает один pipeline по id после валидации.
func (s *Service) GetPipeline(ctx context.Context, pipelineID int) (models.Pipeline, error) {
	if pipelineID <= 0 {
		return models.Pipeline{}, httpx.ValidationError("pipeline id must be greater than 0")
	}
	return s.repo.GetPipeline(ctx, pipelineID)
}

// CreatePipeline валидирует вход и создает новый pipeline definition.
func (s *Service) CreatePipeline(ctx context.Context, input models.CreatePipelineInput) (models.Pipeline, error) {
	name := strings.TrimSpace(input.Name)
	if name == "" {
		return models.Pipeline{}, httpx.ValidationError("name is required")
	}
	if input.MaxActiveRuns < 0 {
		return models.Pipeline{}, httpx.ValidationError("max_active_runs must be greater than or equal to 0")
	}

	groupID, err := s.normalizePipelineGroupID(ctx, input.PipelineGroupID)
	if err != nil {
		return models.Pipeline{}, err
	}
	schedule, err := normalizePipelineSchedule(input.Schedule)
	if err != nil {
		return models.Pipeline{}, err
	}
	if input.IsActive {
		return models.Pipeline{}, httpx.ValidationError("pipeline cannot be created as active before graph is configured")
	}

	return s.repo.CreatePipeline(ctx, models.CreatePipelineInput{
		Name:            name,
		Description:     strings.TrimSpace(input.Description),
		PipelineGroupID: groupID,
		Schedule:        schedule,
		IsActive:        false,
		MaxActiveRuns:   input.MaxActiveRuns,
	})
}

// UpdatePipeline применяет partial update к pipeline с учетом runtime-ограничений.
func (s *Service) UpdatePipeline(ctx context.Context, pipelineID int, input models.UpdatePipelineInput) (models.Pipeline, error) {
	if pipelineID <= 0 {
		return models.Pipeline{}, httpx.ValidationError("pipeline id must be greater than 0")
	}
	if !input.HasChanges() {
		return models.Pipeline{}, httpx.ValidationError("request body must contain at least one field")
	}

	pipeline, err := s.repo.GetPipeline(ctx, pipelineID)
	if err != nil {
		return models.Pipeline{}, err
	}

	activeRuns, err := s.repo.CountActivePipelineRuns(ctx, pipelineID)
	if err != nil {
		return models.Pipeline{}, err
	}
	if activeRuns > 0 {
		return s.updatePipelineWithActiveRuns(ctx, pipeline, input)
	}

	if input.Name.Set {
		pipeline.Name = strings.TrimSpace(input.Name.Value)
		if pipeline.Name == "" {
			return models.Pipeline{}, httpx.ValidationError("name must not be empty")
		}
	}
	if input.Description.Set {
		pipeline.Description = strings.TrimSpace(input.Description.Value)
	}
	if input.PipelineGroupID.Set {
		groupID, err := s.normalizePipelineGroupID(ctx, input.PipelineGroupID.Value)
		if err != nil {
			return models.Pipeline{}, err
		}
		pipeline.PipelineGroupID = groupID
	}
	if input.Schedule.Set {
		schedule, err := normalizePipelineSchedule(input.Schedule.Value)
		if err != nil {
			return models.Pipeline{}, err
		}
		pipeline.Schedule = schedule
	}
	if input.MaxActiveRuns.Set {
		if input.MaxActiveRuns.Value < 0 {
			return models.Pipeline{}, httpx.ValidationError("max_active_runs must be greater than or equal to 0")
		}
		pipeline.MaxActiveRuns = input.MaxActiveRuns.Value
	}

	activating := !pipeline.IsActive && input.IsActive.Set && input.IsActive.Value
	if input.IsActive.Set {
		pipeline.IsActive = input.IsActive.Value
	}
	if activating {
		if err := s.validatePipelineGraphActivation(ctx, pipelineID); err != nil {
			return models.Pipeline{}, err
		}
	}

	return s.repo.UpdatePipeline(ctx, pipeline)
}

// DeletePipeline удаляет pipeline после проверки активных run.
func (s *Service) DeletePipeline(ctx context.Context, pipelineID int) error {
	if pipelineID <= 0 {
		return httpx.ValidationError("pipeline id must be greater than 0")
	}

	activeRuns, err := s.repo.CountActivePipelineRuns(ctx, pipelineID)
	if err != nil {
		return err
	}
	if activeRuns > 0 {
		return httpx.ConflictError("pipeline with active runs cannot be deleted")
	}

	return s.repo.DeletePipeline(ctx, pipelineID)
}

// GetPipelineGraph читает graph pipeline по id после валидации.
func (s *Service) GetPipelineGraph(ctx context.Context, pipelineID int) (models.PipelineGraph, error) {
	if pipelineID <= 0 {
		return models.PipelineGraph{}, httpx.ValidationError("pipeline id must be greater than 0")
	}
	return s.repo.GetPipelineGraph(ctx, pipelineID)
}

func (s *Service) updatePipelineWithActiveRuns(ctx context.Context, pipeline models.Pipeline, input models.UpdatePipelineInput) (models.Pipeline, error) {
	if input.Name.Set || input.Description.Set || input.PipelineGroupID.Set || input.Schedule.Set || input.MaxActiveRuns.Set {
		return models.Pipeline{}, httpx.ConflictError("pipeline with active runs can only be deactivated")
	}
	if !input.IsActive.Set || input.IsActive.Value {
		return models.Pipeline{}, httpx.ConflictError("pipeline with active runs can only be deactivated")
	}
	if !pipeline.IsActive {
		return pipeline, nil
	}

	pipeline.IsActive = false
	return s.repo.UpdatePipeline(ctx, pipeline)
}

func (s *Service) normalizePipelineGroupID(ctx context.Context, groupID *int) (*int, error) {
	if groupID == nil {
		return nil, nil
	}
	if *groupID <= 0 {
		return nil, httpx.ValidationError("pipeline_group_id must be greater than 0")
	}

	exists, err := s.repo.PipelineGroupExists(ctx, *groupID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, httpx.ValidationError("pipeline_group_id references unknown pipeline group")
	}

	return groupID, nil
}

func normalizePipelineSchedule(schedule *string) (*string, error) {
	if schedule == nil {
		return nil, nil
	}

	normalized := strings.TrimSpace(*schedule)
	if normalized == "" {
		return nil, nil
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	if _, err := parser.Parse(normalized); err != nil {
		return nil, httpx.ValidationError("schedule must be a valid cron expression")
	}

	return &normalized, nil
}
