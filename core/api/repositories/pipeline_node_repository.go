package repositories

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ListPipelineNodes читает все node одного pipeline.
func (r *Repository) ListPipelineNodes(ctx context.Context, pipelineID int) ([]models.PipelineNode, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `--sql
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
		return nil, fmt.Errorf("list pipeline nodes: %w", err)
	}
	defer rows.Close()

	nodes := make([]models.PipelineNode, 0)
	for rows.Next() {
		node, err := scanPipelineNode(rows)
		if err != nil {
			return nil, fmt.Errorf("scan pipeline node: %w", err)
		}
		nodes = append(nodes, node)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pipeline nodes: %w", err)
	}

	return nodes, nil
}

// GetPipelineNode читает один node pipeline по id.
func (r *Repository) GetPipelineNode(ctx context.Context, pipelineID int, nodeID int) (models.PipelineNode, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	node, err := scanPipelineNode(r.pool.QueryRow(ctx, `--sql
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
		  AND id = $2
	`, pipelineID, nodeID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.PipelineNode{}, httpx.NotFoundError(fmt.Sprintf("pipeline node not found: %d", nodeID))
		}
		return models.PipelineNode{}, fmt.Errorf("get pipeline node: %w", err)
	}

	return node, nil
}

// CreatePipelineNode вставляет новый node pipeline и возвращает сохраненную запись.
func (r *Repository) CreatePipelineNode(ctx context.Context, pipelineID int, input models.CreatePipelineNodeInput) (models.PipelineNode, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	metaJSON, err := json.Marshal(input.Meta)
	if err != nil {
		return models.PipelineNode{}, fmt.Errorf("marshal pipeline node meta: %w", err)
	}

	node, err := scanPipelineNode(r.pool.QueryRow(ctx, `--sql
		INSERT INTO busybee_pipeline_node (
		    pipeline_id,
		    name,
		    description,
		    command,
		    executor_type,
		    max_retries,
		    timeout_seconds,
		    meta
		)
		VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6, $7, $8)
		RETURNING id,
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
	`, pipelineID, input.Name, input.Description, input.Command, input.ExecutorType, input.MaxRetries, input.TimeoutSeconds, metaJSON))
	if err != nil {
		return models.PipelineNode{}, mapPgError("create pipeline node", err)
	}

	return node, nil
}

// UpdatePipelineNode обновляет node pipeline и возвращает итоговую запись.
func (r *Repository) UpdatePipelineNode(ctx context.Context, node models.PipelineNode) (models.PipelineNode, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	metaJSON, err := json.Marshal(node.Meta)
	if err != nil {
		return models.PipelineNode{}, fmt.Errorf("marshal pipeline node meta: %w", err)
	}

	updated, err := scanPipelineNode(r.pool.QueryRow(ctx, `--sql
		UPDATE busybee_pipeline_node
		SET name = $1,
		    description = NULLIF($2, ''),
		    command = $3,
		    executor_type = $4,
		    max_retries = $5,
		    timeout_seconds = $6,
		    meta = $7,
		    updated_at = NOW()
		WHERE pipeline_id = $8
		  AND id = $9
		RETURNING id,
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
	`, node.Name, node.Description, node.Command, node.ExecutorType, node.MaxRetries, node.TimeoutSeconds, metaJSON, node.PipelineID, node.ID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.PipelineNode{}, httpx.NotFoundError(fmt.Sprintf("pipeline node not found: %d", node.ID))
		}
		return models.PipelineNode{}, mapPgError("update pipeline node", err)
	}

	return updated, nil
}

// DeletePipelineNode удаляет node pipeline по id.
func (r *Repository) DeletePipelineNode(ctx context.Context, pipelineID int, nodeID int) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	result, err := r.pool.Exec(ctx, `DELETE FROM busybee_pipeline_node WHERE pipeline_id = $1 AND id = $2`, pipelineID, nodeID)
	if err != nil {
		return fmt.Errorf("delete pipeline node: %w", err)
	}
	if result.RowsAffected() == 0 {
		return httpx.NotFoundError(fmt.Sprintf("pipeline node not found: %d", nodeID))
	}
	return nil
}

func scanPipelineNode(scanner rowScanner) (models.PipelineNode, error) {
	var (
		node     models.PipelineNode
		metaJSON []byte
	)

	err := scanner.Scan(
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
	)
	if err != nil {
		return models.PipelineNode{}, err
	}

	node.Meta = map[string]any{}
	if len(metaJSON) > 0 {
		if err := json.Unmarshal(metaJSON, &node.Meta); err != nil {
			return models.PipelineNode{}, fmt.Errorf("decode pipeline node meta: %w", err)
		}
	}

	return node, nil
}
