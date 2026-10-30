package services

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"busybee/core/api/repositories"
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxPipelineGroupsPageSize = 500

// Service реализует минимальную бизнес-логику HTTP API.
type Service struct {
	repo *repositories.Repository
}

// New создает сервисный слой поверх общего pgx pool.
func New(pool *pgxpool.Pool) *Service {
	return &Service{
		repo: repositories.New(pool),
	}
}

// ListPipelineGroups валидирует фильтры списка и делегирует чтение репозиторию.
func (s *Service) ListPipelineGroups(ctx context.Context, filters models.PipelineGroupListFilters) ([]models.PipelineGroup, int, error) {
	if filters.Limit <= 0 {
		filters.Limit = 100
	}
	if filters.Limit > maxPipelineGroupsPageSize {
		return nil, 0, httpx.ValidationError("limit must be less than or equal to 500")
	}
	if filters.Offset < 0 {
		return nil, 0, httpx.ValidationError("offset must be greater than or equal to 0")
	}

	return s.repo.ListPipelineGroups(ctx, filters)
}

// ListPipelineGroupsDetailed возвращает группы вместе со связанными pipeline.
func (s *Service) ListPipelineGroupsDetailed(ctx context.Context, filters models.PipelineGroupListFilters) ([]models.PipelineGroupDetailed, int, error) {
	if filters.Limit <= 0 {
		filters.Limit = 100
	}
	if filters.Limit > maxPipelineGroupsPageSize {
		return nil, 0, httpx.ValidationError("limit must be less than or equal to 500")
	}
	if filters.Offset < 0 {
		return nil, 0, httpx.ValidationError("offset must be greater than or equal to 0")
	}

	return s.repo.ListPipelineGroupsDetailed(ctx, filters)
}

// CreatePipelineGroup валидирует вход и создает новую группу pipeline.
func (s *Service) CreatePipelineGroup(ctx context.Context, input models.CreatePipelineGroupInput) (models.PipelineGroup, error) {
	name := strings.TrimSpace(input.Name)
	if name == "" {
		return models.PipelineGroup{}, httpx.ValidationError("name is required")
	}

	return s.repo.CreatePipelineGroup(ctx, models.PipelineGroup{
		Name:        name,
		Description: strings.TrimSpace(input.Description),
	})
}

// UpdatePipelineGroup применяет partial update к группе pipeline.
func (s *Service) UpdatePipelineGroup(ctx context.Context, groupID int, input models.UpdatePipelineGroupInput) (models.PipelineGroup, error) {
	if groupID <= 0 {
		return models.PipelineGroup{}, httpx.ValidationError("pipeline group id must be greater than 0")
	}
	if !input.HasChanges() {
		return models.PipelineGroup{}, httpx.ValidationError("request body must contain at least one field")
	}

	group, err := s.repo.GetPipelineGroup(ctx, groupID)
	if err != nil {
		return models.PipelineGroup{}, err
	}

	if input.Name.Set {
		group.Name = strings.TrimSpace(input.Name.Value)
		if group.Name == "" {
			return models.PipelineGroup{}, httpx.ValidationError("name must not be empty")
		}
	}

	if input.Description.Set {
		group.Description = strings.TrimSpace(input.Description.Value)
	}

	return s.repo.UpdatePipelineGroup(ctx, group)
}

// DeletePipelineGroup удаляет группу pipeline и каскадно все её pipeline.
// Запрещено если хотя бы один pipeline группы имеет активный run.
func (s *Service) DeletePipelineGroup(ctx context.Context, groupID int) error {
	if groupID <= 0 {
		return httpx.ValidationError("pipeline group id must be greater than 0")
	}

	hasActiveRuns, err := s.repo.HasActiveRunsInGroup(ctx, groupID)
	if err != nil {
		return err
	}
	if hasActiveRuns {
		return httpx.ConflictError("pipeline group with active pipeline runs cannot be deleted")
	}

	return s.repo.DeletePipelineGroup(ctx, groupID)
}
