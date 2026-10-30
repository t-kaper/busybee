package repositories

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ListPipelineRuns читает горячие pipeline_run с агрегированной статистикой по нодам.
func (r *Repository) ListPipelineRuns(ctx context.Context, filters models.PipelineRunListFilters) ([]models.PipelineRunListItem, int, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `--sql
		WITH filtered_runs AS (
			SELECT wr.id,
			       wr.pipeline_id,
			       p.name AS pipeline_name,
			       wr.status,
			       wr.trigger_type,
			       wr.created_at,
			       wr.finished_at,
			       COUNT(*) OVER() AS total_count
			FROM busybee_pipeline_runs wr
			JOIN busybee_pipeline p ON p.id = wr.pipeline_id
			WHERE ($1::int IS NULL OR wr.pipeline_id = $1)
			  AND ($2::text IS NULL OR wr.status = $2)
			  AND ($3::text IS NULL OR wr.trigger_type = $3)
			  AND ($4::timestamptz IS NULL OR wr.created_at >= $4)
			  AND ($5::timestamptz IS NULL OR wr.created_at <= $5)
		),
		pipeline_nodes AS (
			SELECT fr.id AS pipeline_run_id,
			       COUNT(n.id)::int AS total_nodes
			FROM filtered_runs fr
			LEFT JOIN busybee_pipeline_node n ON n.pipeline_id = fr.pipeline_id
			GROUP BY fr.id
		),
		latest_runs AS (
			SELECT DISTINCT ON (nr.pipeline_run_id, nr.pipeline_node_id)
			       nr.pipeline_run_id,
			       nr.pipeline_node_id,
			       nr.status
			FROM busybee_pipeline_node_runs nr
			JOIN filtered_runs fr ON fr.id = nr.pipeline_run_id
			ORDER BY nr.pipeline_run_id, nr.pipeline_node_id, nr.created_at DESC, nr.id DESC
		),
		run_stats AS (
			SELECT fr.id AS pipeline_run_id,
			       COALESCE(pn.total_nodes, 0)::int AS total_nodes,
			       GREATEST(COALESCE(pn.total_nodes, 0)::int - COUNT(lr.pipeline_node_id)::int, 0) AS not_started_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'pending')::int AS pending_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'queued')::int AS queued_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'running')::int AS running_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'success')::int AS success_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'failed')::int AS failed_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'stopped')::int AS stopped_nodes
			FROM filtered_runs fr
			LEFT JOIN pipeline_nodes pn ON pn.pipeline_run_id = fr.id
			LEFT JOIN latest_runs lr ON lr.pipeline_run_id = fr.id
			GROUP BY fr.id, pn.total_nodes
		),
		attempt_stats AS (
			SELECT fr.id AS pipeline_run_id,
			       COUNT(nr.id)::int AS total_attempts
			FROM filtered_runs fr
			LEFT JOIN busybee_pipeline_node_runs nr ON nr.pipeline_run_id = fr.id
			GROUP BY fr.id
		)
		SELECT fr.id,
		       fr.pipeline_id,
		       fr.pipeline_name,
		       fr.status,
		       fr.trigger_type,
		       fr.created_at,
		       fr.finished_at,
		       rs.total_nodes,
		       rs.not_started_nodes,
		       rs.pending_nodes,
		       rs.queued_nodes,
		       rs.running_nodes,
		       rs.success_nodes,
		       rs.failed_nodes,
		       rs.stopped_nodes,
		       COALESCE(ast.total_attempts, 0) AS total_attempts,
		       fr.total_count
		FROM filtered_runs fr
		JOIN run_stats rs ON rs.pipeline_run_id = fr.id
		LEFT JOIN attempt_stats ast ON ast.pipeline_run_id = fr.id
		ORDER BY CASE WHEN fr.status = 'active' THEN 0 ELSE 1 END,
		         fr.created_at DESC,
		         fr.id DESC
	`, filters.PipelineID, filters.Status, filters.TriggerType, filters.CreatedFrom, filters.CreatedTo)
	if err != nil {
		return nil, 0, fmt.Errorf("list pipeline runs: %w", err)
	}
	defer rows.Close()

	total := 0
	items := make([]models.PipelineRunListItem, 0)
	for rows.Next() {
		var (
			item       models.PipelineRunListItem
			finishedAt sql.NullTime
			totalCount int
		)
		if err := rows.Scan(
			&item.ID,
			&item.PipelineID,
			&item.PipelineName,
			&item.Status,
			&item.TriggerType,
			&item.CreatedAt,
			&finishedAt,
			&item.Stats.TotalNodes,
			&item.Stats.NotStartedNodes,
			&item.Stats.PendingNodes,
			&item.Stats.QueuedNodes,
			&item.Stats.RunningNodes,
			&item.Stats.SuccessNodes,
			&item.Stats.FailedNodes,
			&item.Stats.StoppedNodes,
			&item.Stats.TotalAttempts,
			&totalCount,
		); err != nil {
			return nil, 0, fmt.Errorf("scan pipeline run list item: %w", err)
		}
		item.FinishedAt = nullableTime(finishedAt)
		total = totalCount
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate pipeline runs: %w", err)
	}

	return items, total, nil
}

// GetPipelineRun читает один горячий pipeline_run вместе с агрегированной статистикой.
func (r *Repository) GetPipelineRun(ctx context.Context, runID string) (models.PipelineRun, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var (
		run        models.PipelineRun
		finishedAt sql.NullTime
	)

	err := r.pool.QueryRow(ctx, `--sql
		WITH base_run AS (
			SELECT wr.id,
			       wr.pipeline_id,
			       p.name AS pipeline_name,
			       wr.status,
			       wr.trigger_type,
			       wr.created_at,
			       wr.finished_at
			FROM busybee_pipeline_runs wr
			JOIN busybee_pipeline p ON p.id = wr.pipeline_id
			WHERE wr.id = $1
		),
		pipeline_nodes AS (
			SELECT br.id AS pipeline_run_id,
			       COUNT(n.id)::int AS total_nodes
			FROM base_run br
			LEFT JOIN busybee_pipeline_node n ON n.pipeline_id = br.pipeline_id
			GROUP BY br.id
		),
		latest_runs AS (
			SELECT DISTINCT ON (nr.pipeline_node_id)
			       nr.pipeline_node_id,
			       nr.status
			FROM busybee_pipeline_node_runs nr
			WHERE nr.pipeline_run_id = $1
			ORDER BY nr.pipeline_node_id, nr.created_at DESC, nr.id DESC
		),
		run_stats AS (
			SELECT br.id AS pipeline_run_id,
			       COALESCE(pn.total_nodes, 0)::int AS total_nodes,
			       GREATEST(COALESCE(pn.total_nodes, 0)::int - COUNT(lr.pipeline_node_id)::int, 0) AS not_started_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'pending')::int AS pending_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'queued')::int AS queued_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'running')::int AS running_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'success')::int AS success_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'failed')::int AS failed_nodes,
			       COUNT(*) FILTER (WHERE lr.status = 'stopped')::int AS stopped_nodes
			FROM base_run br
			LEFT JOIN pipeline_nodes pn ON pn.pipeline_run_id = br.id
			LEFT JOIN latest_runs lr ON true
			GROUP BY br.id, pn.total_nodes
		),
		attempt_stats AS (
			SELECT COUNT(*)::int AS total_attempts
			FROM busybee_pipeline_node_runs
			WHERE pipeline_run_id = $1
		)
		SELECT br.id,
		       br.pipeline_id,
		       br.pipeline_name,
		       br.status,
		       br.trigger_type,
		       br.created_at,
		       br.finished_at,
		       rs.total_nodes,
		       rs.not_started_nodes,
		       rs.pending_nodes,
		       rs.queued_nodes,
		       rs.running_nodes,
		       rs.success_nodes,
		       rs.failed_nodes,
		       rs.stopped_nodes,
		       COALESCE(ast.total_attempts, 0) AS total_attempts
		FROM base_run br
		JOIN run_stats rs ON rs.pipeline_run_id = br.id
		CROSS JOIN attempt_stats ast
	`, runID).Scan(
		&run.ID,
		&run.PipelineID,
		&run.PipelineName,
		&run.Status,
		&run.TriggerType,
		&run.CreatedAt,
		&finishedAt,
		&run.Stats.TotalNodes,
		&run.Stats.NotStartedNodes,
		&run.Stats.PendingNodes,
		&run.Stats.QueuedNodes,
		&run.Stats.RunningNodes,
		&run.Stats.SuccessNodes,
		&run.Stats.FailedNodes,
		&run.Stats.StoppedNodes,
		&run.Stats.TotalAttempts,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.PipelineRun{}, httpx.NotFoundError(fmt.Sprintf("pipeline run not found: %s", runID))
		}
		return models.PipelineRun{}, fmt.Errorf("get pipeline run: %w", err)
	}

	run.FinishedAt = nullableTime(finishedAt)
	return run, nil
}

// ListPipelineNodeRunAttempts читает все попытки node_run для одного pipeline_run.
func (r *Repository) ListPipelineNodeRunAttempts(ctx context.Context, runID string) ([]models.PipelineNodeRunAttempt, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, `--sql
		SELECT id,
		       pipeline_node_id,
		       pipeline_run_id,
		       status,
		       trigger_type,
		       pid,
		       exit_code,
		       retry_count,
		       scheduled_at,
		       started_at,
		       finished_at,
		       error_msg,
		       log_path,
		       created_at
		FROM busybee_pipeline_node_runs
		WHERE pipeline_run_id = $1
		ORDER BY pipeline_node_id ASC, created_at ASC, id ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list pipeline node run attempts: %w", err)
	}
	defer rows.Close()

	attempts := make([]models.PipelineNodeRunAttempt, 0)
	for rows.Next() {
		var (
			attempt     models.PipelineNodeRunAttempt
			pid         sql.NullInt64
			exitCode    sql.NullInt64
			scheduledAt sql.NullTime
			startedAt   sql.NullTime
			finishedAt  sql.NullTime
			errorMsg    sql.NullString
			logPath     sql.NullString
		)
		if err := rows.Scan(
			&attempt.ID,
			&attempt.PipelineNodeID,
			&attempt.PipelineRunID,
			&attempt.Status,
			&attempt.TriggerType,
			&pid,
			&exitCode,
			&attempt.RetryCount,
			&scheduledAt,
			&startedAt,
			&finishedAt,
			&errorMsg,
			&logPath,
			&attempt.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pipeline node run attempt: %w", err)
		}
		attempt.PID = nullableOptionalInt(pid)
		attempt.ExitCode = nullableOptionalInt(exitCode)
		attempt.ScheduledAt = nullableTime(scheduledAt)
		attempt.StartedAt = nullableTime(startedAt)
		attempt.FinishedAt = nullableTime(finishedAt)
		attempt.ErrorMsg = nullableOptionalString(errorMsg)
		attempt.LogPath = nullableOptionalString(logPath)
		attempts = append(attempts, attempt)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pipeline node run attempts: %w", err)
	}

	return attempts, nil
}

// GetPipelineNodeRunLogPath возвращает путь к логу конкретного горячего node_run.
func (r *Repository) GetPipelineNodeRunLogPath(ctx context.Context, nodeRunID int) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var logPath sql.NullString
	if err := r.pool.QueryRow(ctx, `--sql
		SELECT log_path
		FROM busybee_pipeline_node_runs
		WHERE id = $1
	`, nodeRunID).Scan(&logPath); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", httpx.NotFoundError(fmt.Sprintf("pipeline node run not found: %d", nodeRunID))
		}
		return "", fmt.Errorf("get pipeline node run log path: %w", err)
	}

	if !logPath.Valid || logPath.String == "" {
		return "", httpx.ConflictError(fmt.Sprintf("log is not available for pipeline node run: %d", nodeRunID))
	}

	return logPath.String, nil
}

func nullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}

func nullableOptionalInt(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	result := int(value.Int64)
	return &result
}

func nullableOptionalString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}
