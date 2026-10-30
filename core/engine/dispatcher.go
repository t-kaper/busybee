// Package engine реализует runtime-движок BusyBee: планирование, диспетчеризацию,
// исполнение и архивацию pipeline DAG-ов.
//
// Основные компоненты:
//
//   - Ticker        — cron-планировщик, создаёт pipeline_run по расписанию.
//   - Dispatcher     — подбирает pending node_run с удовлетворёнными DAG-зависимостями
//     и передаёт их в очередь воркеров.
//   - WorkerPool      — пул горутин, исполняющих node_run (bash-команды).
//   - HistoryArchiver — переносит завершённые pipeline_run в архивные таблицы.
//
// Жизненный цикл node_run:
//
//	pending → queued → running → success / failed / stopped
//
// Жизненный цикл pipeline_run:
//
//	active → completed / failed / stopped
package engine

import (
	"busybee/core/common"
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const dispatchBatchSize = 256

// RecoverZombieNodeRuns сбрасывает зависшие pipeline_node_run после рестарта сервиса.
//
// При крахе процесса некоторые node_run могут остаться в running/queued.
// Функция переводит их обратно в pending, чтобы Dispatcher подобрал их заново.
// Вызывается один раз при старте сервиса, ДО запуска WorkerPool и Dispatcher.
func RecoverZombieNodeRuns(ctx context.Context, pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Массовый сброс: running/queued → pending, обнуляем pid и started_at.
	result, err := pool.Exec(ctx, `--sql
		UPDATE busybee_pipeline_node_runs
		SET status = 'pending',
		    started_at = NULL,
		    pid = NULL
		WHERE status IN ('running', 'queued')
	`)
	if err != nil {
		common.ZapLogger.Error("RecoverZombieNodeRuns: ошибка сброса pipeline_node_run", zap.Error(err))
		return err
	}

	recoveredCount := result.RowsAffected()
	if recoveredCount > 0 {
		common.ZapLogger.Warn("RecoverZombieNodeRuns: восстановлены pipeline_node_run",
			zap.Int64("count", recoveredCount))
	} else {
		common.ZapLogger.Info("RecoverZombieNodeRuns: зомби pipeline_node_run не найдено")
	}

	// Логируем первые 10 восстановленных node_run для диагностики.
	if recoveredCount > 0 {
		rows, err := pool.Query(ctx, `--sql
			SELECT nr.id, nr.pipeline_node_id, nr.pipeline_run_id, n.name
			FROM busybee_pipeline_node_runs nr
			JOIN busybee_pipeline_node n ON n.id = nr.pipeline_node_id
			WHERE nr.status = 'pending'
			  AND nr.created_at < NOW() - INTERVAL '1 minute'
			ORDER BY nr.created_at DESC
			LIMIT 10
		`)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var nodeRunID, nodeID int
				var pipelineRunID, name string
				if rows.Scan(&nodeRunID, &nodeID, &pipelineRunID, &name) == nil {
					common.ZapLogger.Info("RecoverZombieNodeRuns: восстановлен node_run",
						zap.Int("node_run_id", nodeRunID),
						zap.Int("pipeline_node_id", nodeID),
						zap.String("pipeline_run_id", pipelineRunID),
						zap.String("name", name))
				}
			}
			if err := rows.Err(); err != nil {
				common.ZapLogger.Warn("RecoverZombieNodeRuns: ошибка чтения диагностических строк", zap.Error(err))
			}
		}
	}

	return nil
}

// StartDispatcher запускает цикл диспетчеризации node_run.
//
// Каждые interval секунд:
//  1. recoverOrphanedChildren — находит ноды без node_run, у которых все parent уже success,
//     и создаёт для них pending node_run (восстановление после частичного сбоя).
//  2. dispatchPendingNodeRuns — находит pending node_run с удовлетворёнными DAG-зависимостями,
//     переводит их в queued и отправляет в очередь воркеров.
func StartDispatcher(ctx context.Context, pool *pgxpool.Pool, queue *NodeRunQueue, interval time.Duration) {
	common.ZapLogger.Info("Dispatcher запущен", zap.Duration("interval", interval))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			common.ZapLogger.Info("Dispatcher остановлен")
			return
		case <-ticker.C:
			recoverOrphanedChildren(ctx, pool)
			repairMissingRetryNodeRuns(ctx, pool)
			dispatchPendingNodeRuns(ctx, pool, queue)
		}
	}
}

// recoverOrphanedChildren создаёт pending node_run для нод, которые «потерялись».
//
// Сценарий: воркер пометил parent-ноду как success, но до TriggerDependentPipelineNodeRuns
// произошёл краш (или INSERT child-ноды упал по таймауту). В результате child-нода
// никогда не получила node_run, и pipeline_run завис бы как active навсегда.
//
// Эта функция исправляет ситуацию: для каждого active pipeline_run находит ноды,
// у которых нет ни одного node_run, но все parent-ноды уже success, и создаёт
// для них pending node_run с trigger_type='recovery'.
func recoverOrphanedChildren(ctx context.Context, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Один INSERT...SELECT: находит «сирот» и создаёт для них pending node_run.
	// Условия:
	//   1. pipeline_run ещё active
	//   2. у ноды нет ни одного node_run в этом pipeline_run
	//   3. у ноды нет ни одного parent, который ещё не success
	//      (для стартовых нод без parent — условие автоматически выполнено)
	// ON CONFLICT гарантирует идемпотентность при параллельных вызовах.
	rows, err := pool.Query(ctx, `--sql
		INSERT INTO busybee_pipeline_node_runs (
			pipeline_node_id, pipeline_run_id, status, trigger_type, created_at
		)
		SELECT n.id, pr.id, 'pending', 'recovery', NOW()
		FROM busybee_pipeline_runs pr
		JOIN busybee_pipeline_node n ON n.pipeline_id = pr.pipeline_id
		WHERE pr.status = 'active'
		  AND NOT EXISTS (
		      SELECT 1
		      FROM busybee_pipeline_node_runs nr
		      WHERE nr.pipeline_node_id = n.id
		        AND nr.pipeline_run_id = pr.id
		  )
		  AND NOT EXISTS (
		      SELECT 1
		      FROM busybee_pipeline_edge e
		      WHERE e.child_node_id = n.id
		        AND NOT EXISTS (
		            SELECT 1
		            FROM busybee_pipeline_node_runs pnr
		            WHERE pnr.pipeline_node_id = e.parent_node_id
		              AND pnr.pipeline_run_id = pr.id
		              AND pnr.status = 'success'
		        )
		  )
		ON CONFLICT (pipeline_node_id, pipeline_run_id)
		    WHERE status IN ('pending', 'queued', 'running')
		    DO NOTHING
		RETURNING id, pipeline_node_id, pipeline_run_id
	`)
	if err != nil {
		common.ZapLogger.Error("recoverOrphanedChildren: ошибка recovery-запроса", zap.Error(err))
		return
	}
	defer rows.Close()

	for rows.Next() {
		var nodeRunID, pipelineNodeID int
		var pipelineRunID string
		if rows.Scan(&nodeRunID, &pipelineNodeID, &pipelineRunID) == nil {
			common.ZapLogger.Warn("recoverOrphanedChildren: создан recovery node_run",
				zap.Int("node_run_id", nodeRunID),
				zap.Int("pipeline_node_id", pipelineNodeID),
				zap.String("pipeline_run_id", pipelineRunID))
		}
	}
	if err := rows.Err(); err != nil {
		common.ZapLogger.Error("recoverOrphanedChildren: ошибка чтения RETURNING-строк", zap.Error(err))
	}
}

// repairMissingRetryNodeRuns чинит редкий, но неприятный сценарий:
// failed-статус уже записан, а retry не создался из-за кратковременного сбоя БД.
// Берём только latest failed run без активного successor-run и с оставшимися retry.
func repairMissingRetryNodeRuns(ctx context.Context, pool *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := pool.Query(ctx, `--sql
		WITH latest_runs AS (
			SELECT DISTINCT ON (nr.pipeline_node_id, nr.pipeline_run_id)
				nr.pipeline_node_id,
				nr.pipeline_run_id,
				nr.status,
				nr.retry_count
			FROM busybee_pipeline_node_runs nr
			JOIN busybee_pipeline_runs pr ON pr.id = nr.pipeline_run_id
			WHERE pr.status = 'active'
			ORDER BY nr.pipeline_node_id, nr.pipeline_run_id, nr.created_at DESC, nr.id DESC
		)
		SELECT lr.pipeline_node_id, lr.pipeline_run_id, lr.retry_count, n.max_retries
		FROM latest_runs lr
		JOIN busybee_pipeline_node n ON n.id = lr.pipeline_node_id
		WHERE lr.status = 'failed'
		  AND lr.retry_count < n.max_retries
		  AND NOT EXISTS (
		      SELECT 1
		      FROM busybee_pipeline_node_runs active
		      WHERE active.pipeline_node_id = lr.pipeline_node_id
		        AND active.pipeline_run_id = lr.pipeline_run_id
		        AND active.status IN ('pending', 'queued', 'running')
		  )
		LIMIT 100
	`)
	if err != nil {
		common.ZapLogger.Error("repairMissingRetryNodeRuns: ошибка поиска потерянных retry", zap.Error(err))
		return
	}
	defer rows.Close()

	for rows.Next() {
		var (
			pipelineNodeID int
			pipelineRunID  string
			retryCount     int
			maxRetries     int
		)
		if err := rows.Scan(&pipelineNodeID, &pipelineRunID, &retryCount, &maxRetries); err != nil {
			common.ZapLogger.Error("repairMissingRetryNodeRuns: ошибка чтения кандидата", zap.Error(err))
			continue
		}

		createRetryNodeRun(ctx, pool, pipelineNodeID, pipelineRunID, retryCount, maxRetries)
	}
	if err := rows.Err(); err != nil {
		common.ZapLogger.Error("repairMissingRetryNodeRuns: ошибка чтения результатов", zap.Error(err))
	}
}

// dispatchPendingNodeRuns находит pending node_run, у которых:
//   - pipeline_run ещё active
//   - scheduled_at наступил (или не задан)
//   - все parent-ноды в этом pipeline_run завершились success
//
// Переводит их в queued (атомарно, в одной транзакции) и отправляет в NodeRunQueue,
// откуда воркеры заберут их на исполнение.
func dispatchPendingNodeRuns(ctx context.Context, pool *pgxpool.Pool, queue *NodeRunQueue) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		common.ZapLogger.Error("Dispatcher: ошибка начала транзакции", zap.Error(err))
		return
	}
	defer tx.Rollback(ctx)

	// Один batched-запрос одновременно:
	//   1. выбирает готовые pending node_run;
	//   2. лочит только их через FOR UPDATE SKIP LOCKED;
	//   3. массово переводит их в queued;
	//   4. возвращает ровно тот набор ID, который успешно попал в очередь.
	//
	// Это убирает N отдельных UPDATE внутри транзакции и уменьшает время удержания локов.
	rows, err := tx.Query(ctx, `--sql
		WITH ready AS (
			SELECT nr.id, nr.pipeline_node_id, nr.pipeline_run_id
			FROM busybee_pipeline_node_runs nr
			JOIN busybee_pipeline_runs wr ON wr.id = nr.pipeline_run_id
			WHERE nr.status = 'pending'
			  AND wr.status = 'active'
			  AND (nr.scheduled_at IS NULL OR nr.scheduled_at <= NOW())
			  AND NOT EXISTS (
			      SELECT 1
			      FROM busybee_pipeline_edge e
			      WHERE e.child_node_id = nr.pipeline_node_id
			        AND NOT EXISTS (
			            SELECT 1
			            FROM busybee_pipeline_node_runs pr
			            WHERE pr.pipeline_node_id = e.parent_node_id
			              AND pr.pipeline_run_id = nr.pipeline_run_id
			              AND pr.status = 'success'
			        )
			  )
			ORDER BY COALESCE(nr.scheduled_at, nr.created_at), nr.id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		),
		queued AS (
			UPDATE busybee_pipeline_node_runs nr
			SET status = 'queued'
			FROM ready
			WHERE nr.id = ready.id
			RETURNING nr.id
		)
		SELECT ready.id, ready.pipeline_node_id, ready.pipeline_run_id
		FROM ready
		JOIN queued ON queued.id = ready.id
		ORDER BY ready.id
	`, dispatchBatchSize)
	if err != nil {
		common.ZapLogger.Error("Dispatcher: ошибка получения pending node_run", zap.Error(err))
		return
	}
	defer rows.Close()

	type nodeRunToDispatch struct {
		nodeRunID      int
		pipelineNodeID int
		pipelineRunID  string
	}

	items := make([]nodeRunToDispatch, 0)
	for rows.Next() {
		var item nodeRunToDispatch
		if err := rows.Scan(&item.nodeRunID, &item.pipelineNodeID, &item.pipelineRunID); err != nil {
			common.ZapLogger.Error("Dispatcher: ошибка сканирования node_run", zap.Error(err))
			continue
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		common.ZapLogger.Error("Dispatcher: ошибка чтения batched queued node_run", zap.Error(err))
		return
	}

	if len(items) == 0 {
		return
	}

	if err := tx.Commit(ctx); err != nil {
		common.ZapLogger.Error("Dispatcher: ошибка коммита", zap.Error(err))
		return
	}

	// После коммита отправляем ID в in-memory очередь воркеров.
	for _, item := range items {
		queue.Push(item.nodeRunID)
		common.ZapLogger.Debug("Dispatcher: node_run отправлен воркерам",
			zap.Int("node_run_id", item.nodeRunID),
			zap.Int("pipeline_node_id", item.pipelineNodeID),
			zap.String("pipeline_run_id", item.pipelineRunID))
	}
}

// TriggerDependentPipelineNodeRuns создаёт pending node_run для child-нод
// после успешного завершения parent-ноды.
//
// Вызывается воркером сразу после пометки node_run как success.
//
// Один INSERT...SELECT атомарно:
//  1. проверяет, что pipeline_run ещё active (через JOIN);
//  2. находит child-ноды completedNodeID;
//  3. пропускает child, у которого уже есть активный run (pending/queued/running);
//  4. проверяет, что все parent child-ноды завершились success;
//  5. создаёт pending node_run с trigger_type='dependency'.
//
// ON CONFLICT + partial unique index гарантируют отсутствие дублей.
func TriggerDependentPipelineNodeRuns(ctx context.Context, pool *pgxpool.Pool, completedNodeID int, pipelineRunID string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Один запрос: JOIN на pipeline_runs проверяет active-статус,
	// INSERT создаёт child node_run только для готовых нод.
	rows, err := pool.Query(ctx, `--sql
		INSERT INTO busybee_pipeline_node_runs (
			pipeline_node_id, pipeline_run_id, status, trigger_type, created_at
		)
		SELECT e.child_node_id, $2, 'pending', 'dependency', NOW()
		FROM busybee_pipeline_edge e
		JOIN busybee_pipeline_runs pr ON pr.id = $2 AND pr.status = 'active'
		WHERE e.parent_node_id = $1
		  AND NOT EXISTS (
		      SELECT 1
		      FROM busybee_pipeline_node_runs existing
		      WHERE existing.pipeline_node_id = e.child_node_id
		        AND existing.pipeline_run_id = $2
		        AND existing.status IN ('pending', 'queued', 'running')
		  )
		  AND NOT EXISTS (
		      SELECT 1
		      FROM busybee_pipeline_edge parents
		      WHERE parents.child_node_id = e.child_node_id
		        AND NOT EXISTS (
		            SELECT 1
		            FROM busybee_pipeline_node_runs pnr
		            WHERE pnr.pipeline_node_id = parents.parent_node_id
		              AND pnr.pipeline_run_id = $2
		              AND pnr.status = 'success'
		        )
		  )
		ON CONFLICT (pipeline_node_id, pipeline_run_id)
		    WHERE status IN ('pending', 'queued', 'running')
		    DO NOTHING
		RETURNING id, pipeline_node_id
	`, completedNodeID, pipelineRunID)
	if err != nil {
		common.ZapLogger.Error("TriggerDependentPipelineNodeRuns: ошибка создания child node_run",
			zap.Int("parent_node_id", completedNodeID),
			zap.String("pipeline_run_id", pipelineRunID),
			zap.Error(err))
		return
	}
	defer rows.Close()

	for rows.Next() {
		var nodeRunID, childNodeID int
		if rows.Scan(&nodeRunID, &childNodeID) == nil {
			common.ZapLogger.Info("TriggerDependentPipelineNodeRuns: создан child node_run",
				zap.Int("parent_node_id", completedNodeID),
				zap.Int("child_node_id", childNodeID),
				zap.String("pipeline_run_id", pipelineRunID),
				zap.Int("node_run_id", nodeRunID))
		}
	}
	if err := rows.Err(); err != nil {
		common.ZapLogger.Error("TriggerDependentPipelineNodeRuns: ошибка чтения RETURNING-строк", zap.Error(err))
	}
}

// UpdatePipelineRunStatus закрывает pipeline_run, когда DAG больше не может двигаться:
//   - completed: все ноды представлены latest_run и нет active/stopped статусов;
//   - failed: есть хотя бы один failed и больше нет active/stopped статусов;
//   - stopped: есть хотя бы один stopped и не осталось active статусов.
//
// Вызывается воркером после каждого завершения node_run (success или failed).
func UpdatePipelineRunStatus(ctx context.Context, pool *pgxpool.Pool, pipelineRunID string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var (
		pipelineRunStatus string
		successCount      int
		failedCount       int
		stoppedCount      int
	)

	err := pool.QueryRow(ctx, `--sql
		WITH total_nodes AS (
			SELECT COUNT(*) AS cnt
			FROM busybee_pipeline_node
			WHERE pipeline_id = (
				SELECT pipeline_id FROM busybee_pipeline_runs WHERE id = $1
			)
		),
		latest_runs AS (
			SELECT DISTINCT ON (pipeline_node_id)
				pipeline_node_id,
				status
			FROM busybee_pipeline_node_runs
			WHERE pipeline_run_id = $1
			ORDER BY pipeline_node_id, created_at DESC, id DESC
		),
		stats AS (
			SELECT
				COUNT(*)                                                AS scheduled,
				COUNT(*) FILTER (WHERE status IN ('pending', 'queued')) AS pending,
				COUNT(*) FILTER (WHERE status = 'running')             AS running,
				COUNT(*) FILTER (WHERE status = 'success')             AS success,
				COUNT(*) FILTER (WHERE status = 'failed')              AS failed,
				COUNT(*) FILTER (WHERE status = 'stopped')             AS stopped
			FROM latest_runs
		)
		UPDATE busybee_pipeline_runs wr
		SET status = CASE
			WHEN stats.stopped > 0 THEN 'stopped'
			WHEN stats.failed > 0 THEN 'failed'
			ELSE 'completed'
		END,
		finished_at = NOW()
		FROM stats, total_nodes
		WHERE wr.id = $1
		  AND wr.status = 'active'
		  AND stats.pending = 0
		  AND stats.running = 0
		  AND (
		      stats.stopped > 0
		      OR stats.scheduled = total_nodes.cnt
		  )
		RETURNING wr.status, stats.success, stats.failed, stats.stopped
	`, pipelineRunID).Scan(&pipelineRunStatus, &successCount, &failedCount, &stoppedCount)

	if err == pgx.ErrNoRows {
		return
	}
	if err != nil {
		common.ZapLogger.Error("UpdatePipelineRunStatus: ошибка обновления pipeline_run",
			zap.String("pipeline_run_id", pipelineRunID),
			zap.Error(err))
		return
	}

	common.ZapLogger.Info("UpdatePipelineRunStatus: pipeline_run завершён",
		zap.String("pipeline_run_id", pipelineRunID),
		zap.String("status", pipelineRunStatus),
		zap.Int("success", successCount),
		zap.Int("failed", failedCount),
		zap.Int("stopped", stoppedCount))
}
