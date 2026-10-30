package repositories

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const dbTimeout = 5 * time.Second

type rowScanner interface {
	Scan(dest ...any) error
}

// Repository инкапсулирует SQL-доступ к конфигурационным таблицам API.
type Repository struct {
	pool *pgxpool.Pool
}

// New создает репозиторий с общим pgx pool.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// ListPipelineGroups читает список групп pipeline с пагинацией.
func (r *Repository) ListPipelineGroups(ctx context.Context, filters models.PipelineGroupListFilters) ([]models.PipelineGroup, int, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `--sql
		SELECT id,
		       name,
		       COALESCE(description, ''),
		       created_at,
		       COUNT(*) OVER() AS total_count
		FROM busybee_pipeline_group
		ORDER BY id ASC
		LIMIT $1 OFFSET $2
	`, filters.Limit, filters.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list pipeline groups: %w", err)
	}
	defer rows.Close()

	total := 0
	items := make([]models.PipelineGroup, 0, filters.Limit)
	for rows.Next() {
		var (
			group    models.PipelineGroup
			rowTotal int
		)
		if err := rows.Scan(&group.ID, &group.Name, &group.Description, &group.CreatedAt, &rowTotal); err != nil {
			return nil, 0, fmt.Errorf("scan pipeline group: %w", err)
		}
		total = rowTotal
		items = append(items, group)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate pipeline groups: %w", err)
	}

	return items, total, nil
}

// ListPipelineGroupsDetailed читает список групп вместе со связанными pipeline.
func (r *Repository) ListPipelineGroupsDetailed(ctx context.Context, filters models.PipelineGroupListFilters) ([]models.PipelineGroupDetailed, int, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `--sql
		SELECT id,
		       name,
		       COALESCE(description, ''),
		       created_at,
		       COUNT(*) OVER() AS total_count
		FROM busybee_pipeline_group
		ORDER BY id ASC
		LIMIT $1 OFFSET $2
	`, filters.Limit, filters.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list detailed pipeline groups: %w", err)
	}
	defer rows.Close()

	total := 0
	items := make([]models.PipelineGroupDetailed, 0, filters.Limit)
	groupIndex := make(map[int]int, filters.Limit)
	groupIDs := make([]int32, 0, filters.Limit)

	for rows.Next() {
		var (
			group    models.PipelineGroupDetailed
			rowTotal int
		)
		if err := rows.Scan(&group.ID, &group.Name, &group.Description, &group.CreatedAt, &rowTotal); err != nil {
			return nil, 0, fmt.Errorf("scan detailed pipeline group: %w", err)
		}
		group.Pipelines = make([]models.PipelineSummary, 0)
		total = rowTotal
		groupIndex[group.ID] = len(items)
		groupIDs = append(groupIDs, int32(group.ID))
		items = append(items, group)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate detailed pipeline groups: %w", err)
	}
	if len(items) == 0 {
		return items, total, nil
	}

	pipelineRows, err := r.pool.Query(ctx, `--sql
		SELECT p.id,
		       p.pipeline_group_id,
		       p.name,
		       p.schedule,
		       p.is_active,
		       COUNT(n.id)::int AS node_count
		FROM busybee_pipeline p
		LEFT JOIN busybee_pipeline_node n ON n.pipeline_id = p.id
		WHERE p.pipeline_group_id = ANY($1)
		GROUP BY p.id, p.pipeline_group_id, p.name, p.schedule, p.is_active
		ORDER BY p.pipeline_group_id ASC, p.id ASC
	`, groupIDs)
	if err != nil {
		return nil, 0, fmt.Errorf("list pipelines for groups: %w", err)
	}
	defer pipelineRows.Close()

	for pipelineRows.Next() {
		var (
			pipeline models.PipelineSummary
			groupID  int
		)
		if err := pipelineRows.Scan(&pipeline.ID, &groupID, &pipeline.Name, &pipeline.Schedule, &pipeline.IsActive, &pipeline.NodeCount); err != nil {
			return nil, 0, fmt.Errorf("scan group pipeline summary: %w", err)
		}

		idx, ok := groupIndex[groupID]
		if !ok {
			continue
		}
		items[idx].Pipelines = append(items[idx].Pipelines, pipeline)
	}

	if err := pipelineRows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate group pipelines: %w", err)
	}

	return items, total, nil
}

// GetPipelineGroup читает одну группу pipeline по id.
func (r *Repository) GetPipelineGroup(ctx context.Context, groupID int) (models.PipelineGroup, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	group, err := scanPipelineGroup(r.pool.QueryRow(ctx, `--sql
		SELECT id, name, COALESCE(description, ''), created_at
		FROM busybee_pipeline_group
		WHERE id = $1
	`, groupID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.PipelineGroup{}, httpx.NotFoundError(fmt.Sprintf("pipeline group not found: %d", groupID))
		}
		return models.PipelineGroup{}, fmt.Errorf("get pipeline group: %w", err)
	}

	return group, nil
}

// CreatePipelineGroup вставляет новую группу pipeline и возвращает сохраненную запись.
func (r *Repository) CreatePipelineGroup(ctx context.Context, group models.PipelineGroup) (models.PipelineGroup, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	created, err := scanPipelineGroup(r.pool.QueryRow(ctx, `--sql
		INSERT INTO busybee_pipeline_group (name, description)
		VALUES ($1, NULLIF($2, ''))
		RETURNING id, name, COALESCE(description, ''), created_at
	`, group.Name, group.Description))
	if err != nil {
		return models.PipelineGroup{}, mapPgError("create pipeline group", err)
	}

	return created, nil
}

// UpdatePipelineGroup обновляет группу pipeline и возвращает итоговую запись.
func (r *Repository) UpdatePipelineGroup(ctx context.Context, group models.PipelineGroup) (models.PipelineGroup, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	updated, err := scanPipelineGroup(r.pool.QueryRow(ctx, `--sql
		UPDATE busybee_pipeline_group
		SET name = $1, description = NULLIF($2, '')
		WHERE id = $3
		RETURNING id, name, COALESCE(description, ''), created_at
	`, group.Name, group.Description, group.ID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.PipelineGroup{}, httpx.NotFoundError(fmt.Sprintf("pipeline group not found: %d", group.ID))
		}
		return models.PipelineGroup{}, mapPgError("update pipeline group", err)
	}

	return updated, nil
}

// HasActiveRunsInGroup возвращает true если хотя бы один pipeline группы имеет активный run.
func (r *Repository) HasActiveRunsInGroup(ctx context.Context, groupID int) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var exists bool
	err := r.pool.QueryRow(ctx, `--sql
		SELECT EXISTS (
		    SELECT 1
		    FROM busybee_pipeline_runs pr
		    JOIN busybee_pipeline p ON p.id = pr.pipeline_id
		    WHERE p.pipeline_group_id = $1
		      AND pr.status = 'active'
		)
	`, groupID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check active runs in group: %w", err)
	}
	return exists, nil
}

// DeletePipelineGroup удаляет группу pipeline по id.
func (r *Repository) DeletePipelineGroup(ctx context.Context, groupID int) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	result, err := r.pool.Exec(ctx, `DELETE FROM busybee_pipeline_group WHERE id = $1`, groupID)
	if err != nil {
		return fmt.Errorf("delete pipeline group: %w", err)
	}
	if result.RowsAffected() == 0 {
		return httpx.NotFoundError(fmt.Sprintf("pipeline group not found: %d", groupID))
	}
	return nil
}

func scanPipelineGroup(scanner rowScanner) (models.PipelineGroup, error) {
	var group models.PipelineGroup
	err := scanner.Scan(&group.ID, &group.Name, &group.Description, &group.CreatedAt)
	return group, err
}

func mapPgError(action string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503":
			return httpx.ValidationError("referenced entity does not exist")
		case "23505":
			return httpx.ConflictError("entity already exists")
		}
	}
	return fmt.Errorf("%s: %w", action, err)
}
