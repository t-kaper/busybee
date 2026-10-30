package engine

import (
	"busybee/core/common"
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// StartHistoryArchiver запускает цикл архивации завершённых pipeline_run.
//
// Каждые interval:
//  1. Находит до 100 завершённых pipeline_run старше retentionDays дней.
//  2. Копирует их node_run в busybee_history_pipeline_node_runs.
//  3. Копирует pipeline_run в busybee_history_pipeline_runs.
//  4. Удаляет оригиналы из busybee_pipeline_runs (каскадно удаляет node_run).
//
// Архивные таблицы не имеют FK — определение pipeline может быть удалено,
// а история исполнения сохраняется как аудит.
//
// Первая архивация запускается сразу при старте (не ждёт interval).
func StartHistoryArchiver(ctx context.Context, pool *pgxpool.Pool, interval time.Duration, retentionDays int) {
	common.ZapLogger.Info("HistoryArchiver запущен",
		zap.Duration("interval", interval),
		zap.Int("retention_days", retentionDays))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	archiveOldPipelineRuns(ctx, pool, retentionDays)

	for {
		select {
		case <-ctx.Done():
			common.ZapLogger.Info("HistoryArchiver остановлен")
			return
		case <-ticker.C:
			archiveOldPipelineRuns(ctx, pool, retentionDays)
		}
	}
}

// archiveOldPipelineRuns переносит завершённые pipeline_run в архивные таблицы.
//
// Порядок операций (одна транзакция):
//  1. INSERT node_run → history (ON CONFLICT DO NOTHING — идемпотентность).
//  2. INSERT pipeline_run → history.
//  3. DELETE pipeline_run (каскадно удаляет node_run из горячих таблиц).
//
// Это сохраняет горячие таблицы компактными для Ticker, Dispatcher и API.
func archiveOldPipelineRuns(ctx context.Context, pool *pgxpool.Pool, retentionDays int) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		common.ZapLogger.Error("HistoryArchiver: ошибка начала транзакции", zap.Error(err))
		return
	}
	defer tx.Rollback(ctx)

	// Выбираем пачку уже внутри транзакции и лочим её, чтобы несколько архиваторов
	// не пытались одновременно переносить одни и те же pipeline_run.
	rows, err := tx.Query(ctx, `--sql
		WITH candidates AS (
			SELECT id
			FROM busybee_pipeline_runs
			WHERE status IN ('completed', 'failed', 'stopped')
			  AND finished_at IS NOT NULL
			  AND finished_at < NOW() - MAKE_INTERVAL(days => $1)
			ORDER BY finished_at ASC, id ASC
			LIMIT 100
			FOR UPDATE SKIP LOCKED
		)
		SELECT id
		FROM candidates
		ORDER BY id
	`, retentionDays)
	if err != nil {
		common.ZapLogger.Error("HistoryArchiver: ошибка поиска pipeline_run", zap.Error(err))
		return
	}
	defer rows.Close()

	pipelineRunIDs := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			common.ZapLogger.Error("HistoryArchiver: ошибка сканирования pipeline_run", zap.Error(err))
			continue
		}
		pipelineRunIDs = append(pipelineRunIDs, id)
	}
	if err := rows.Err(); err != nil {
		common.ZapLogger.Error("HistoryArchiver: ошибка чтения pipeline_run", zap.Error(err))
		return
	}

	if len(pipelineRunIDs) == 0 {
		return
	}

	common.ZapLogger.Info("HistoryArchiver: найдены pipeline_run для архивации",
		zap.Int("count", len(pipelineRunIDs)))

	// Шаг 1: копируем node_run в архив.
	// ON CONFLICT DO NOTHING — если повторная архивация (краш на шаге 3), дубли не создаются.
	nodeResult, err := tx.Exec(ctx, `--sql
		INSERT INTO busybee_history_pipeline_node_runs (
			id,
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
			created_at,
			archived_at
		)
		SELECT
			id,
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
			created_at,
			NOW()
		FROM busybee_pipeline_node_runs
		WHERE pipeline_run_id = ANY($1)
		ON CONFLICT (id) DO NOTHING
	`, pipelineRunIDs)
	if err != nil {
		common.ZapLogger.Error("HistoryArchiver: ошибка копирования pipeline_node_runs", zap.Error(err))
		return
	}

	// Шаг 2: копируем pipeline_run в архив.
	pipelineResult, err := tx.Exec(ctx, `--sql
		INSERT INTO busybee_history_pipeline_runs (
			id,
			pipeline_id,
			status,
			trigger_type,
			created_at,
			finished_at,
			archived_at
		)
		SELECT
			id,
			pipeline_id,
			status,
			trigger_type,
			created_at,
			finished_at,
			NOW()
		FROM busybee_pipeline_runs
		WHERE id = ANY($1)
		ON CONFLICT (id) DO NOTHING
	`, pipelineRunIDs)
	if err != nil {
		common.ZapLogger.Error("HistoryArchiver: ошибка копирования pipeline_runs", zap.Error(err))
		return
	}

	// Шаг 3: удаляем оригиналы. FK ON DELETE CASCADE удалит node_run автоматически.
	_, err = tx.Exec(ctx, `--sql
		DELETE FROM busybee_pipeline_runs
		WHERE id = ANY($1)
	`, pipelineRunIDs)
	if err != nil {
		common.ZapLogger.Error("HistoryArchiver: ошибка удаления pipeline_runs", zap.Error(err))
		return
	}

	if err := tx.Commit(ctx); err != nil {
		common.ZapLogger.Error("HistoryArchiver: ошибка коммита", zap.Error(err))
		return
	}

	common.ZapLogger.Info("HistoryArchiver: архивация завершена",
		zap.Int("pipeline_runs_archived", int(pipelineResult.RowsAffected())),
		zap.Int("pipeline_node_runs_archived", int(nodeResult.RowsAffected())),
		zap.Strings("pipeline_run_ids", pipelineRunIDs))
}
