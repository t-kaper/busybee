package engine

import (
	"busybee/core/common"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const (
	nodeRunWriteAttempts = 5
	nodeRunWriteTimeout  = 5 * time.Second
	nodeRunWriteBackoff  = 300 * time.Millisecond
)

// StartWorkerPool запускает пул из maxWorkers горутин, которые исполняют node_run.
//
// Каждый воркер в цикле:
//  1. Pop из NodeRunQueue (блокирующий, ждёт появления работы).
//  2. Загружает параметры node_run из БД.
//  3. Запускает bash-команду в отдельной process group.
//  4. По завершении — помечает node_run как success/failed, тригерит child-ноды
//     и обновляет статус pipeline_run.
//
// Функция блокирует до завершения всех воркеров (ctx отменён + очередь опустела).
func StartWorkerPool(ctx context.Context, pool *pgxpool.Pool, queue *NodeRunQueue, maxWorkers int) {
	var wg sync.WaitGroup

	common.ZapLogger.Info("WorkerPool запущен", zap.Int("max_workers", maxWorkers))

	for i := 1; i <= maxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker(ctx, workerID, pool, queue)
		}(i)
	}

	wg.Wait()
	common.ZapLogger.Info("WorkerPool остановлен")
}

// worker — цикл одного воркера. Берёт node_run из очереди и исполняет.
// Завершается, когда очередь закрыта и пуста, или ctx отменён.
func worker(ctx context.Context, id int, pool *pgxpool.Pool, queue *NodeRunQueue) {
	common.ZapLogger.Debug("Воркер запущен", zap.Int("worker_id", id))

	for {
		nodeRunID, ok := queue.Pop(ctx)
		if !ok {
			common.ZapLogger.Debug("Воркер остановлен", zap.Int("worker_id", id))
			return
		}

		common.ZapLogger.Debug("Воркер взял node_run",
			zap.Int("worker_id", id),
			zap.Int("node_run_id", nodeRunID))

		executeNodeRun(ctx, pool, nodeRunID)
	}
}

// executeNodeRun — полный цикл исполнения одного node_run:
//
//  1. Читает из БД: команду, timeout, max_retries, retry_count.
//  2. Создаёт лог-файл logs/run_<id>.log.
//  3. Запускает bash -c <command> в отдельной process group (Setpgid).
//  4. CAS queued→running: записывает PID и started_at.
//     Если CAS провалился (node_run уже не queued) — убивает процесс.
//  5. cmd.Wait() — ждёт завершения процесса.
//  6. При success: помечает node_run, создаёт child node_run, проверяет завершение pipeline.
//     При failed: помечает node_run, создаёт retry (если разрешён), проверяет завершение pipeline.
func executeNodeRun(ctx context.Context, pool *pgxpool.Pool, nodeRunID int) {
	var (
		pipelineID     int
		pipelineNodeID int
		pipelineRunID  string
		commandStr     string
		timeoutSec     int
		maxRetries     int
		retryCount     int
	)

	// Загружаем параметры node_run + определение ноды (команда, timeout, retries).
	err := pool.QueryRow(ctx, `--sql
		SELECT
			n.pipeline_id,
			nr.pipeline_node_id,
			nr.pipeline_run_id,
			nr.retry_count,
			n.command,
			n.timeout_seconds,
			n.max_retries
		FROM busybee_pipeline_node_runs nr
		JOIN busybee_pipeline_node n ON n.id = nr.pipeline_node_id
		WHERE nr.id = $1
	`, nodeRunID).Scan(
		&pipelineID,
		&pipelineNodeID,
		&pipelineRunID,
		&retryCount,
		&commandStr,
		&timeoutSec,
		&maxRetries,
	)
	if err != nil {
		common.ZapLogger.Error("Worker: ошибка получения node_run",
			zap.Int("node_run_id", nodeRunID),
			zap.Error(err))
		return
	}

	common.ZapLogger.Info("Worker: начинаю выполнение node_run",
		zap.Int("node_run_id", nodeRunID),
		zap.Int("pipeline_id", pipelineID),
		zap.Int("pipeline_node_id", pipelineNodeID),
		zap.String("pipeline_run_id", pipelineRunID),
		zap.Int("retry_count", retryCount),
		zap.Int("max_retries", maxRetries),
		zap.Int("timeout_sec", timeoutSec))

	// Таймаут на исполнение команды (из определения ноды).
	nodeRunCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	_ = os.MkdirAll("logs", os.ModePerm)
	logFileName := fmt.Sprintf("logs/run_%d.log", nodeRunID)

	logFile, err := os.Create(logFileName)
	if err != nil {
		failNodeRunWithRetry(ctx, pool, nodeRunID, pipelineNodeID, pipelineRunID, -1, fmt.Sprintf("log error: %v", err), retryCount, maxRetries)
		UpdatePipelineRunStatus(ctx, pool, pipelineRunID)
		return
	}
	defer logFile.Close()

	absPath, err := filepath.Abs(logFileName)
	if err != nil {
		failNodeRunWithRetry(ctx, pool, nodeRunID, pipelineNodeID, pipelineRunID, -1, fmt.Sprintf("path error: %v", err), retryCount, maxRetries)
		UpdatePipelineRunStatus(ctx, pool, pipelineRunID)
		return
	}

	// Запуск bash -c в отдельной process group (Setpgid).
	// getSysProcAttr() платформозависим: на Linux добавляет Pdeathsig.
	cmd := exec.CommandContext(nodeRunCtx, "bash", "-c", commandStr)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = getSysProcAttr()
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("BUSYBEE_PIPELINE_ID=%d", pipelineID),
		fmt.Sprintf("BUSYBEE_PIPELINE_NODE_ID=%d", pipelineNodeID),
		fmt.Sprintf("BUSYBEE_PIPELINE_RUN_ID=%s", pipelineRunID),
		fmt.Sprintf("BUSYBEE_NODE_RUN_ID=%d", nodeRunID),
		fmt.Sprintf("BUSYBEE_RETRY_COUNT=%d", retryCount),
	)

	if err := cmd.Start(); err != nil {
		failNodeRunWithRetry(ctx, pool, nodeRunID, pipelineNodeID, pipelineRunID, -1, err.Error(), retryCount, maxRetries)
		UpdatePipelineRunStatus(ctx, pool, pipelineRunID)
		return
	}

	// CAS queued→running: атомарно обновляем статус и записываем PID.
	// WHERE status = 'queued' защищает от повторного подбора одного и того же node_run
	// после recovery или конкурентного исполнения.
	result, err := pool.Exec(ctx, `--sql
		UPDATE busybee_pipeline_node_runs
		SET pid = $1,
		    status = 'running',
		    started_at = NOW(),
		    log_path = $2
		WHERE id = $3
		  AND status = 'queued'
	`, cmd.Process.Pid, absPath, nodeRunID)
	if err != nil {
		common.ZapLogger.Error("Worker: ошибка CAS queued->running для node_run",
			zap.Int("node_run_id", nodeRunID),
			zap.Error(err))
		terminateStartedCommand(cmd)
		// Если CAS не записался из-за сбоя БД, не оставляем node_run навечно в queued.
		failNodeRunWithRetry(ctx, pool, nodeRunID, pipelineNodeID, pipelineRunID, -1, fmt.Sprintf("queued->running CAS error: %v", err), retryCount, maxRetries)
		UpdatePipelineRunStatus(ctx, pool, pipelineRunID)
		return
	}
	if result.RowsAffected() != 1 {
		common.ZapLogger.Warn("Worker: node_run уже не queued, выполнение отменено",
			zap.Int("node_run_id", nodeRunID))
		terminateStartedCommand(cmd)
		return
	}

	waitErr := cmd.Wait()
	// Убиваем всю process group (на случай fork-нутых дочерних процессов).
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)

	exitCode := 0
	status := "success"
	errMsg := ""

	if waitErr != nil {
		status = "failed"
		errMsg = waitErr.Error()
		if exitError, ok := waitErr.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			exitCode = -1
		}
	}

	common.ZapLogger.Info("Worker: node_run завершён",
		zap.Int("node_run_id", nodeRunID),
		zap.Int("pipeline_node_id", pipelineNodeID),
		zap.String("pipeline_run_id", pipelineRunID),
		zap.String("status", status),
		zap.Int("exit_code", exitCode),
		zap.Int("retry_count", retryCount))

	if status == "success" {
		updated, err := updateNodeRunStatus(ctx, pool, nodeRunID, "success", 0, "")
		if err != nil {
			common.ZapLogger.Error("Worker: ошибка финального success-обновления node_run",
				zap.Int("node_run_id", nodeRunID),
				zap.Error(err))
		} else if updated {
			// Child-ноды можно триггерить только после подтверждённого running->success.
			TriggerDependentPipelineNodeRuns(ctx, pool, pipelineNodeID, pipelineRunID)
		}
	} else {
		failNodeRunWithRetry(ctx, pool, nodeRunID, pipelineNodeID, pipelineRunID, exitCode, errMsg, retryCount, maxRetries)
	}

	UpdatePipelineRunStatus(ctx, pool, pipelineRunID)
}

// failNodeRunWithRetry гарантированно фиксирует terminal-status failed
// отдельным CAS-обновлением без транзакции. Это исключает сценарий, в котором
// коммит падает после завершения процесса и откатывает terminal-state обратно в running.
func failNodeRunWithRetry(
	ctx context.Context,
	pool *pgxpool.Pool,
	nodeRunID int,
	pipelineNodeID int,
	pipelineRunID string,
	exitCode int,
	errMsg string,
	retryCount int,
	maxRetries int,
) {
	updated, err := markNodeRunFailed(pool, nodeRunID, exitCode, errMsg)
	if err != nil {
		common.ZapLogger.Error("Worker: ошибка обновления failed статуса",
			zap.Int("node_run_id", nodeRunID),
			zap.Error(err))
		return
	}
	if !updated {
		common.ZapLogger.Info("Worker: node_run уже не active, fail+retry пропущены",
			zap.Int("node_run_id", nodeRunID))
		return
	}

	if maxRetries > 0 && retryCount < maxRetries {
		createRetryNodeRun(ctx, pool, pipelineNodeID, pipelineRunID, retryCount, maxRetries)
		return
	}

	common.ZapLogger.Info("Worker: node_run failed, retry исчерпаны",
		zap.Int("node_run_id", nodeRunID),
		zap.Int("pipeline_node_id", pipelineNodeID),
		zap.String("pipeline_run_id", pipelineRunID),
		zap.Int("retry_count", retryCount),
		zap.Int("max_retries", maxRetries))
}

// createRetryNodeRun создаёт отложенный retry после того, как failed-статус уже
// зафиксирован в БД. ON CONFLICT защищает от дублей при повторных попытках recovery.
func createRetryNodeRun(
	ctx context.Context,
	pool *pgxpool.Pool,
	pipelineNodeID int,
	pipelineRunID string,
	retryCount int,
	maxRetries int,
) {
	delay := calculateRetryBackoff(retryCount)
	scheduledAt := time.Now().Add(delay)
	newRetryCount := retryCount + 1

	created, newNodeRunID, err := insertRetryNodeRun(pool, pipelineNodeID, pipelineRunID, newRetryCount, scheduledAt)
	if err != nil {
		common.ZapLogger.Error("Worker: ошибка создания retry node_run",
			zap.Int("pipeline_node_id", pipelineNodeID),
			zap.String("pipeline_run_id", pipelineRunID),
			zap.Int("retry_count", newRetryCount),
			zap.Int("max_retries", maxRetries),
			zap.Error(err))
		return
	}
	if !created {
		common.ZapLogger.Info("Worker: retry не создан, потому что уже существует active node_run",
			zap.Int("pipeline_node_id", pipelineNodeID),
			zap.String("pipeline_run_id", pipelineRunID),
			zap.Int("retry_count", newRetryCount),
			zap.Int("max_retries", maxRetries))
		return
	}

	common.ZapLogger.Info("Worker: node_run failed + создан retry",
		zap.Int("pipeline_node_id", pipelineNodeID),
		zap.String("pipeline_run_id", pipelineRunID),
		zap.Int("new_node_run_id", newNodeRunID),
		zap.Int("retry_count", newRetryCount),
		zap.Int("max_retries", maxRetries),
		zap.Duration("delay", delay),
		zap.Time("scheduled_at", scheduledAt))
}

// calculateRetryBackoff возвращает задержку перед retry-попыткой.
// Используется табличный backoff: 1m, 5m, 15m, 30m, 60m.
// При retryCount >= len(delays) возвращает максимальную задержку (60m).
func calculateRetryBackoff(retryCount int) time.Duration {
	delays := []time.Duration{
		1 * time.Minute,
		5 * time.Minute,
		15 * time.Minute,
		30 * time.Minute,
		60 * time.Minute,
	}

	if retryCount >= len(delays) {
		return delays[len(delays)-1]
	}
	return delays[retryCount]
}

// updateNodeRunStatus помечает node_run как success (CAS: только если running)
// и делает несколько повторных попыток записи, чтобы пережить кратковременный сбой БД
// уже после завершения процесса.
func updateNodeRunStatus(ctx context.Context, pool *pgxpool.Pool, nodeRunID int, status string, exitCode int, errMsg string) (bool, error) {
	result, err := execWithWriteRetry(func(writeCtx context.Context) (pgconn.CommandTag, error) {
		return pool.Exec(writeCtx, `--sql
			UPDATE busybee_pipeline_node_runs
			SET status = $1,
			    exit_code = $2,
			    error_msg = $3,
			    finished_at = NOW()
			WHERE id = $4
			  AND status = 'running'
		`, status, exitCode, errMsg, nodeRunID)
	})
	if err != nil {
		common.ZapLogger.Error("Worker: ошибка обновления статуса node_run",
			zap.Int("node_run_id", nodeRunID),
			zap.Error(err))
		return false, err
	}
	if result.RowsAffected() == 0 {
		common.ZapLogger.Info("Worker: node_run уже не running, success-обновление пропущено",
			zap.Int("node_run_id", nodeRunID),
			zap.String("target_status", status))
		return false, nil
	}
	return true, nil
}

// markNodeRunFailed выполняет CAS active->failed с повторными попытками записи.
func markNodeRunFailed(pool *pgxpool.Pool, nodeRunID int, exitCode int, errMsg string) (bool, error) {
	result, err := execWithWriteRetry(func(writeCtx context.Context) (pgconn.CommandTag, error) {
		return pool.Exec(writeCtx, `--sql
			UPDATE busybee_pipeline_node_runs
			SET status = 'failed',
			    exit_code = $1,
			    error_msg = $2,
			    finished_at = NOW()
			WHERE id = $3
			  AND status IN ('queued', 'running')
		`, exitCode, errMsg, nodeRunID)
	})
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

// insertRetryNodeRun делает несколько попыток записи retry, чтобы кратковременный сбой БД
// не лишал задачу автоматического повтора.
func insertRetryNodeRun(pool *pgxpool.Pool, pipelineNodeID int, pipelineRunID string, retryCount int, scheduledAt time.Time) (bool, int, error) {
	var newNodeRunID int
	for attempt := 1; attempt <= nodeRunWriteAttempts; attempt++ {
		writeCtx, cancel := context.WithTimeout(context.Background(), nodeRunWriteTimeout)
		err := pool.QueryRow(writeCtx, `--sql
			INSERT INTO busybee_pipeline_node_runs (
				pipeline_node_id,
				pipeline_run_id,
				status,
				trigger_type,
				retry_count,
				scheduled_at,
				created_at
			)
			VALUES ($1, $2, 'pending', 'retry', $3, $4, NOW())
			ON CONFLICT (pipeline_node_id, pipeline_run_id)
			    WHERE status IN ('pending', 'queued', 'running')
			    DO NOTHING
			RETURNING id
		`, pipelineNodeID, pipelineRunID, retryCount, scheduledAt).Scan(&newNodeRunID)
		cancel()
		if errors.Is(err, pgx.ErrNoRows) {
			return false, 0, nil
		}
		if err == nil {
			return true, newNodeRunID, nil
		}
		if attempt == nodeRunWriteAttempts {
			return false, 0, err
		}
		time.Sleep(nodeRunWriteBackoff * time.Duration(attempt))
	}
	return false, 0, nil
}

// execWithWriteRetry даёт финальным DB-записям несколько шансов пережить
// кратковременные сетевые/базовые сбои после завершения процесса.
func execWithWriteRetry(execFn func(context.Context) (pgconn.CommandTag, error)) (pgconn.CommandTag, error) {
	var (
		tag     pgconn.CommandTag
		lastErr error
	)
	for attempt := 1; attempt <= nodeRunWriteAttempts; attempt++ {
		writeCtx, cancel := context.WithTimeout(context.Background(), nodeRunWriteTimeout)
		tag, lastErr = execFn(writeCtx)
		cancel()
		if lastErr == nil {
			return tag, nil
		}
		if attempt < nodeRunWriteAttempts {
			time.Sleep(nodeRunWriteBackoff * time.Duration(attempt))
		}
	}
	return tag, lastErr
}

// terminateStartedCommand best-effort завершает процесс и дожидается его reap,
// чтобы после раннего выхода воркера не оставлять zombie-процессы.
func terminateStartedCommand(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	_ = cmd.Wait()
}
