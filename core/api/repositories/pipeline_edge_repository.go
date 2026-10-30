package repositories

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
	"fmt"
)

// ListPipelineEdges читает все edge одного pipeline.
func (r *Repository) ListPipelineEdges(ctx context.Context, pipelineID int) ([]models.PipelineEdge, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `--sql
		SELECT e.parent_node_id,
		       e.child_node_id
		FROM busybee_pipeline_edge e
		JOIN busybee_pipeline_node parent ON parent.id = e.parent_node_id
		WHERE parent.pipeline_id = $1
		ORDER BY e.parent_node_id ASC, e.child_node_id ASC
	`, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("list pipeline edges: %w", err)
	}
	defer rows.Close()

	edges := make([]models.PipelineEdge, 0)
	for rows.Next() {
		var edge models.PipelineEdge
		if err := rows.Scan(&edge.ParentNodeID, &edge.ChildNodeID); err != nil {
			return nil, fmt.Errorf("scan pipeline edge: %w", err)
		}
		edges = append(edges, edge)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pipeline edges: %w", err)
	}

	return edges, nil
}

// CreatePipelineEdge вставляет новое edge pipeline и возвращает сохраненную запись.
func (r *Repository) CreatePipelineEdge(ctx context.Context, edge models.PipelineEdge) (models.PipelineEdge, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	if _, err := r.pool.Exec(ctx, `--sql
		INSERT INTO busybee_pipeline_edge (parent_node_id, child_node_id)
		VALUES ($1, $2)
	`, edge.ParentNodeID, edge.ChildNodeID); err != nil {
		return models.PipelineEdge{}, mapPgError("create pipeline edge", err)
	}

	return edge, nil
}

// DeletePipelineEdge удаляет одно edge pipeline по паре node id.
func (r *Repository) DeletePipelineEdge(ctx context.Context, pipelineID int, edge models.PipelineEdge) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	result, err := r.pool.Exec(ctx, `--sql
		DELETE FROM busybee_pipeline_edge e
		USING busybee_pipeline_node parent, busybee_pipeline_node child
		WHERE e.parent_node_id = $1
		  AND e.child_node_id = $2
		  AND parent.id = e.parent_node_id
		  AND child.id = e.child_node_id
		  AND parent.pipeline_id = $3
		  AND child.pipeline_id = $3
	`, edge.ParentNodeID, edge.ChildNodeID, pipelineID)
	if err != nil {
		return fmt.Errorf("delete pipeline edge: %w", err)
	}
	if result.RowsAffected() == 0 {
		return httpx.NotFoundError(fmt.Sprintf("pipeline edge not found: %d -> %d", edge.ParentNodeID, edge.ChildNodeID))
	}

	return nil
}
