package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrPipelineRunLimitReached сигнализирует, что max_active_runs достигнут.
// Вызывающий код может проверить errors.Is для специфичной обработки.
var ErrPipelineRunLimitReached = errors.New("лимит активных pipeline_run достигнут")

// ErrPipelineInactive сигнализирует, что pipeline деактивирован и не должен запускаться.
var ErrPipelineInactive = errors.New("pipeline is inactive")

// CreatePipelineRun атомарно создаёт новый pipeline_run и стартовые node_run.
//
// Стартовые ноды — это ноды без входящих рёбер в busybee_pipeline_edge (корни DAG-а).
// Для них создаются pending node_run с указанным triggerType.
//
// Возвращает (pipelineRunID, startNodeCount, error).
// Если max_active_runs превышен — возвращает ErrPipelineRunLimitReached.
//
// Используется Ticker (triggerType="scheduled") и ручной запуск через сервисный слой
// или будущий runtime API (triggerType="manual").
func CreatePipelineRun(ctx context.Context, pool *pgxpool.Pool, pipelineID int, triggerType string) (string, int, error) {
	pipelineRunID, err := GeneratePipelineRunID()
	if err != nil {
		return "", 0, fmt.Errorf("generate pipeline run id: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	startNodeCount, err := createPipelineRunTx(ctx, tx, pipelineID, pipelineRunID, triggerType)
	if err != nil {
		return "", 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", 0, fmt.Errorf("commit pipeline run: %w", err)
	}

	return pipelineRunID, startNodeCount, nil
}

// createPipelineRunTx выполняет создание pipeline_run внутри переданной транзакции.
//
// Алгоритм:
//  1. FOR UPDATE на pipeline — лочит строку, чтобы гарантировать корректный подсчёт active run.
//  2. Считает текущие active run, сравнивает с max_active_runs.
//  3. INSERT pipeline_run со статусом active.
//  4. INSERT стартовые node_run: ноды, у которых нет входящих рёбер.
func createPipelineRunTx(ctx context.Context, tx pgx.Tx, pipelineID int, pipelineRunID string, triggerType string) (int, error) {
	// Лочим pipeline, чтобы параллельные Ticker / manual-запуски
	// корректно считали количество active run.
	var (
		isActive      bool
		maxActiveRuns int
	)
	err := tx.QueryRow(ctx, `--sql
		SELECT is_active,
		       max_active_runs
		FROM busybee_pipeline
		WHERE id = $1
		FOR UPDATE
	`, pipelineID).Scan(&isActive, &maxActiveRuns)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("pipeline not found: %d", pipelineID)
		}
		return 0, fmt.Errorf("lock pipeline: %w", err)
	}
	if !isActive {
		return 0, ErrPipelineInactive
	}

	// Проверяем лимит параллельных run (0 = без ограничений).
	if maxActiveRuns > 0 {
		var activeCount int
		err = tx.QueryRow(ctx, `--sql
			SELECT COUNT(*)
			FROM busybee_pipeline_runs
			WHERE pipeline_id = $1
			  AND status = 'active'
		`, pipelineID).Scan(&activeCount)
		if err != nil {
			return 0, fmt.Errorf("count active pipeline runs: %w", err)
		}
		if activeCount >= maxActiveRuns {
			return 0, ErrPipelineRunLimitReached
		}
	}

	// Создаём pipeline_run.
	_, err = tx.Exec(ctx, `--sql
		INSERT INTO busybee_pipeline_runs (id, pipeline_id, status, trigger_type, created_at)
		VALUES ($1, $2, 'active', $3, NOW())
	`, pipelineRunID, pipelineID, triggerType)
	if err != nil {
		return 0, fmt.Errorf("insert pipeline run: %w", err)
	}

	// Создаём стартовые node_run: ноды без входящих рёбер (корни DAG-а).
	// NOT EXISTS (edge WHERE child_node_id = n.id) — нода не является чьим-то child.
	rows, err := tx.Query(ctx, `--sql
		INSERT INTO busybee_pipeline_node_runs (
			pipeline_node_id,
			pipeline_run_id,
			status,
			trigger_type,
			created_at
		)
		SELECT
			n.id,
			$1,
			'pending',
			$3,
			NOW()
		FROM busybee_pipeline_node n
		WHERE n.pipeline_id = $2
		  AND NOT EXISTS (
		      SELECT 1
		      FROM busybee_pipeline_edge e
		      WHERE e.child_node_id = n.id
		  )
		RETURNING id
	`, pipelineRunID, pipelineID, triggerType)
	if err != nil {
		return 0, fmt.Errorf("insert start node runs: %w", err)
	}
	defer rows.Close()

	startNodeIDs, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return 0, fmt.Errorf("collect start node runs: %w", err)
	}
	if len(startNodeIDs) == 0 {
		return 0, fmt.Errorf("pipeline %d has no start nodes", pipelineID)
	}

	return len(startNodeIDs), nil
}

// GeneratePipelineRunID создаёт человекочитаемый ID формата:
//
//	pipeline_run_YYYYMMDD_HHMMSS_XXXX
//
// XXXX — 4 hex-символа (2 случайных байта) для уникальности
// при одновременных запусках в одну секунду.
func GeneratePipelineRunID() (string, error) {
	now := time.Now()
	randomBytes := make([]byte, 2)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}
	return fmt.Sprintf("pipeline_run_%s_%s",
		now.Format("20060102_150405"),
		hex.EncodeToString(randomBytes),
	), nil
}
