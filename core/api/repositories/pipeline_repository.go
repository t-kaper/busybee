package repositories

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ListPipelines читает список pipeline с пагинацией и агрегированной информацией.
func (r *Repository) ListPipelines(ctx context.Context, filters models.PipelineListFilters) ([]models.PipelineListItem, int, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `--sql
		SELECT p.id,
		       p.name,
		       COALESCE(p.description, ''),
		       p.pipeline_group_id,
		       p.schedule,
		       p.is_active,
		       p.max_active_runs,
		       COALESCE(node_stats.node_count, 0) AS node_count,
		       COALESCE(run_stats.has_active_runs, false) AS has_active_runs,
		       p.created_at,
		       p.updated_at,
		       COUNT(*) OVER() AS total_count
		FROM busybee_pipeline p
		LEFT JOIN (
		    SELECT pipeline_id, COUNT(*)::int AS node_count
		    FROM busybee_pipeline_node
		    GROUP BY pipeline_id
		) node_stats ON node_stats.pipeline_id = p.id
		LEFT JOIN (
		    SELECT pipeline_id, true AS has_active_runs
		    FROM busybee_pipeline_runs
		    WHERE status = 'active'
		    GROUP BY pipeline_id
		) run_stats ON run_stats.pipeline_id = p.id
		ORDER BY p.id ASC
		LIMIT $1 OFFSET $2
	`, filters.Limit, filters.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list pipelines: %w", err)
	}
	defer rows.Close()

	total := 0
	items := make([]models.PipelineListItem, 0, filters.Limit)
	for rows.Next() {
		var (
			item     models.PipelineListItem
			groupID  sql.NullInt64
			schedule sql.NullString
			rowTotal int
		)
		if err := rows.Scan(
			&item.ID,
			&item.Name,
			&item.Description,
			&groupID,
			&schedule,
			&item.IsActive,
			&item.MaxActiveRuns,
			&item.NodeCount,
			&item.HasActiveRuns,
			&item.CreatedAt,
			&item.UpdatedAt,
			&rowTotal,
		); err != nil {
			return nil, 0, fmt.Errorf("scan pipeline list item: %w", err)
		}
		item.PipelineGroupID = nullableInt(groupID)
		item.Schedule = nullableString(schedule)
		total = rowTotal
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate pipelines: %w", err)
	}

	return items, total, nil
}

// GetPipeline читает один pipeline по id.
func (r *Repository) GetPipeline(ctx context.Context, pipelineID int) (models.Pipeline, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	pipeline, err := scanPipeline(r.pool.QueryRow(ctx, `--sql
		SELECT p.id,
		       p.name,
		       COALESCE(p.description, ''),
		       p.pipeline_group_id,
		       p.schedule,
		       p.is_active,
		       p.max_active_runs,
		       COALESCE(node_stats.node_count, 0) AS node_count,
		       EXISTS (
		           SELECT 1
		           FROM busybee_pipeline_runs wr
		           WHERE wr.pipeline_id = p.id
		             AND wr.status = 'active'
		       ) AS has_active_runs,
		       p.created_at,
		       p.updated_at
		FROM busybee_pipeline p
		LEFT JOIN (
		    SELECT pipeline_id, COUNT(*)::int AS node_count
		    FROM busybee_pipeline_node
		    GROUP BY pipeline_id
		) node_stats ON node_stats.pipeline_id = p.id
		WHERE p.id = $1
	`, pipelineID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Pipeline{}, httpx.NotFoundError(fmt.Sprintf("pipeline not found: %d", pipelineID))
		}
		return models.Pipeline{}, fmt.Errorf("get pipeline: %w", err)
	}

	return pipeline, nil
}

// CreatePipeline вставляет новый pipeline и возвращает сохраненную запись.
func (r *Repository) CreatePipeline(ctx context.Context, input models.CreatePipelineInput) (models.Pipeline, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var pipelineID int
	err := r.pool.QueryRow(ctx, `--sql
		INSERT INTO busybee_pipeline (
		    name,
		    description,
		    pipeline_group_id,
		    schedule,
		    is_active,
		    max_active_runs
		)
		VALUES ($1, NULLIF($2, ''), $3, $4, $5, $6)
		RETURNING id
	`, input.Name, input.Description, input.PipelineGroupID, input.Schedule, input.IsActive, input.MaxActiveRuns).Scan(&pipelineID)
	if err != nil {
		return models.Pipeline{}, mapPgError("create pipeline", err)
	}

	return r.GetPipeline(ctx, pipelineID)
}

// UpdatePipeline обновляет pipeline и возвращает итоговую запись.
func (r *Repository) UpdatePipeline(ctx context.Context, pipeline models.Pipeline) (models.Pipeline, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var updatedID int
	err := r.pool.QueryRow(ctx, `--sql
		UPDATE busybee_pipeline
		SET name = $1,
		    description = NULLIF($2, ''),
		    pipeline_group_id = $3,
		    schedule = $4,
		    is_active = $5,
		    max_active_runs = $6,
		    updated_at = NOW()
		WHERE id = $7
		RETURNING id
	`, pipeline.Name, pipeline.Description, pipeline.PipelineGroupID, pipeline.Schedule, pipeline.IsActive, pipeline.MaxActiveRuns, pipeline.ID).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Pipeline{}, httpx.NotFoundError(fmt.Sprintf("pipeline not found: %d", pipeline.ID))
		}
		return models.Pipeline{}, mapPgError("update pipeline", err)
	}

	return r.GetPipeline(ctx, updatedID)
}

// DeletePipeline удаляет pipeline по id.
func (r *Repository) DeletePipeline(ctx context.Context, pipelineID int) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	result, err := r.pool.Exec(ctx, `DELETE FROM busybee_pipeline WHERE id = $1`, pipelineID)
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}
	if result.RowsAffected() == 0 {
		return httpx.NotFoundError(fmt.Sprintf("pipeline not found: %d", pipelineID))
	}
	return nil
}

// CountActivePipelineRuns возвращает число активных запусков pipeline.
func (r *Repository) CountActivePipelineRuns(ctx context.Context, pipelineID int) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var count int
	if err := r.pool.QueryRow(ctx, `--sql
		SELECT COUNT(*)
		FROM busybee_pipeline_runs
		WHERE pipeline_id = $1
		  AND status = 'active'
	`, pipelineID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active pipeline runs: %w", err)
	}

	return count, nil
}

// PipelineGroupExists проверяет существование группы pipeline.
func (r *Repository) PipelineGroupExists(ctx context.Context, groupID int) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var exists bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM busybee_pipeline_group WHERE id = $1)`, groupID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check pipeline group exists: %w", err)
	}

	return exists, nil
}

// GetPipelineGraph читает pipeline вместе с его node и edge.
func (r *Repository) GetPipelineGraph(ctx context.Context, pipelineID int) (models.PipelineGraph, error) {
	pipeline, err := r.GetPipeline(ctx, pipelineID)
	if err != nil {
		return models.PipelineGraph{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	nodeRows, err := r.pool.Query(ctx, `--sql
		SELECT id,
		       pipeline_id,
		       name,
		       COALESCE(description, ''),
		       command,
		       executor_type,
		       max_retries,
		       timeout_seconds,
		       meta,
		       created_at,
		       updated_at
		FROM busybee_pipeline_node
		WHERE pipeline_id = $1
		ORDER BY id ASC
	`, pipelineID)
	if err != nil {
		return models.PipelineGraph{}, fmt.Errorf("list pipeline nodes: %w", err)
	}
	defer nodeRows.Close()

	nodes := make([]models.PipelineNode, 0)
	for nodeRows.Next() {
		var (
			node     models.PipelineNode
			metaJSON []byte
		)
		if err := nodeRows.Scan(
			&node.ID,
			&node.PipelineID,
			&node.Name,
			&node.Description,
			&node.Command,
			&node.ExecutorType,
			&node.MaxRetries,
			&node.TimeoutSeconds,
			&metaJSON,
			&node.CreatedAt,
			&node.UpdatedAt,
		); err != nil {
			return models.PipelineGraph{}, fmt.Errorf("scan pipeline node: %w", err)
		}
		node.Meta = map[string]any{}
		if len(metaJSON) > 0 {
			if err := json.Unmarshal(metaJSON, &node.Meta); err != nil {
				return models.PipelineGraph{}, fmt.Errorf("decode pipeline node meta: %w", err)
			}
		}
		nodes = append(nodes, node)
	}

	if err := nodeRows.Err(); err != nil {
		return models.PipelineGraph{}, fmt.Errorf("iterate pipeline nodes: %w", err)
	}

	edgeRows, err := r.pool.Query(ctx, `--sql
		SELECT e.parent_node_id,
		       e.child_node_id
		FROM busybee_pipeline_edge e
		JOIN busybee_pipeline_node parent ON parent.id = e.parent_node_id
		WHERE parent.pipeline_id = $1
		ORDER BY e.parent_node_id ASC, e.child_node_id ASC
	`, pipelineID)
	if err != nil {
		return models.PipelineGraph{}, fmt.Errorf("list pipeline edges: %w", err)
	}
	defer edgeRows.Close()

	edges := make([]models.PipelineEdge, 0)
	for edgeRows.Next() {
		var edge models.PipelineEdge
		if err := edgeRows.Scan(&edge.ParentNodeID, &edge.ChildNodeID); err != nil {
			return models.PipelineGraph{}, fmt.Errorf("scan pipeline edge: %w", err)
		}
		edges = append(edges, edge)
	}

	if err := edgeRows.Err(); err != nil {
		return models.PipelineGraph{}, fmt.Errorf("iterate pipeline edges: %w", err)
	}

	return models.PipelineGraph{
		Pipeline: pipeline,
		Nodes:    nodes,
		Edges:    edges,
	}, nil
}

func scanPipeline(scanner rowScanner) (models.Pipeline, error) {
	var (
		pipeline models.Pipeline
		groupID  sql.NullInt64
		schedule sql.NullString
	)

	err := scanner.Scan(
		&pipeline.ID,
		&pipeline.Name,
		&pipeline.Description,
		&groupID,
		&schedule,
		&pipeline.IsActive,
		&pipeline.MaxActiveRuns,
		&pipeline.NodeCount,
		&pipeline.HasActiveRuns,
		&pipeline.CreatedAt,
		&pipeline.UpdatedAt,
	)
	if err != nil {
		return models.Pipeline{}, err
	}

	pipeline.PipelineGroupID = nullableInt(groupID)
	pipeline.Schedule = nullableString(schedule)
	return pipeline, nil
}

func nullableInt(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	result := int(value.Int64)
	return &result
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}
