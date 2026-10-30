package main

import (
	"busybee/core/api"
	"busybee/core/common"
	"busybee/core/config"
	"busybee/core/engine"
	"busybee/internal/dtb" // Путь к твоему пакету
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func main() {

	var config config.ServiceConfig
	config.ReadConfig("config.yaml")
	if err := config.Validate(); err != nil {
		log.Fatalf("CONFIG ERROR: %v", err)
	}
	// ---------- 2. Инициализируем логгер ----------
	// InitLogger настраивает глобальный ZapLogger в пакете common.
	common.InitLogger(config.App.Mode)
	defer func(ZapLogger *zap.Logger) {
		// Sync сбрасывает буферы логгера на диск/stdout.
		// IMPORTANT: в некоторых окружениях (Docker/CI) Sync() на stdout/stderr может вернуть EINVAL/ENOTTY.
		// Это известный кейс, его нельзя считать критическим (см. практику zap).
		if err := ZapLogger.Sync(); err != nil {
			if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTTY) {
				return
			}
			log.Printf("WARN: zap logger sync failed: %v\n", err)
		}
	}(common.ZapLogger)
	common.ZapLogger.Info(
		"starting busybee scheduler",
		zap.String("version", config.App.Version),
		zap.String("mode", config.App.Mode),
		zap.String("timezone", config.App.TimeTimezone),
	)

	// ---------- 4. Контекст приложения + graceful shutdown ----------

	// ctx отменится, когда мы получим SIGINT/SIGTERM (Ctrl+C, docker stop и т.п.)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---------- 5. Подключаемся к Postgres (если включен) ----------

	var pgPool *pgxpool.Pool

	// Формируем DSN вида:
	// storage://user:password@host:port/database?sslmode=disable
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		config.Postgres.User,
		url.QueryEscape(config.Postgres.Password),
		config.Postgres.Host,
		config.Postgres.Port,
		config.Postgres.Database,
		config.Postgres.SSLMode,
	)

	// NewClientPgx — твой helper из tools/dtb/psql/pgxpool.go
	// timeoutConnect = 5 секунд, попыток = 5, размер пула — из конфига.
	var err error
	pgPool, err = dtb.NewClientPgx(ctx, 5, 5*time.Second, dsn, int(config.Postgres.MaxPool))
	if err != nil {
		log.Fatalf("CRITICAL: failed to connect to PostgreSQL: %v\n", err)
	}
	defer dtb.ClosePostgresConnection(pgPool)

	common.ZapLogger.Info("storage connected",
		zap.String("host", config.Postgres.Host),
		zap.Int("port", config.Postgres.Port),
		zap.String("db", config.Postgres.Database),
	)

	runtimeLockConn, err := acquireRuntimeLeaderLock(ctx, pgPool)
	if err != nil {
		log.Fatalf("CRITICAL: %v\n", err)
	}
	defer releaseRuntimeLeaderLock(runtimeLockConn)

	// ---------- 6. Recovery зомби node_run (ВАЖНО: до старта воркеров!) ----------
	// Сбрасываем pipeline_node_run, которые остались в running/queued после краша
	if err := engine.RecoverZombieNodeRuns(ctx, pgPool); err != nil {
		common.ZapLogger.Error("Recovery failed, но продолжаем работу", zap.Error(err))
	}

	// 2. Очередь node_run и WaitGroup
	nodeRunQueue := engine.NewNodeRunQueue()
	var wg sync.WaitGroup

	// 3. Запуск компонентов (горутин)
	// Передаем пул и очередь во все компоненты
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.StartWorkerPool(ctx, pgPool, nodeRunQueue, 10)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := api.StartHTTPServer(ctx, config.App.HTTPAddr, config.API.BearerToken, pgPool); err != nil {
			common.ZapLogger.Error("HTTP server failed", zap.Error(err))
			stop()
		}
	}()

	// Запуск тикера pipeline (сканер cron-расписаний, проверка каждые 30 сек)
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.StartTicker(ctx, pgPool, 30*time.Second, config.App.Location)
	}()

	// Запуск диспетчера (берёт pending pipeline_node_run, проверяет DAG-зависимости и отправляет воркерам)
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.StartDispatcher(ctx, pgPool, nodeRunQueue, 1*time.Second)
	}()

	// Запуск архиватора истории (переносит завершённые pipeline_run старше 7 дней
	// из busybee_pipeline_runs/busybee_pipeline_node_runs в архивные таблицы, проверка каждый час)
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.StartHistoryArchiver(ctx, pgPool, 1*time.Hour, 7)
	}()

	// Ожидание завершения (ctx отменится по SIGINT/SIGTERM)
	<-ctx.Done()
	common.ZapLogger.Info("Получен сигнал остановки, завершаем...")

	// Закрываем очередь — воркеры завершатся после обработки оставшихся node_run
	nodeRunQueue.Close()

	// Даём горутинам время на graceful shutdown
	wg.Wait()
	common.ZapLogger.Info("Сервис остановлен")
}

const runtimeLeaderLockID int64 = 0x42555359424545

// acquireRuntimeLeaderLock удерживает advisory lock на отдельном соединении весь срок жизни процесса.
// Это явно запрещает одновременный запуск нескольких runtime-инстансов поверх одной БД.
func acquireRuntimeLeaderLock(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, error) {
	lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := pool.Acquire(lockCtx)
	if err != nil {
		return nil, fmt.Errorf("acquire runtime lock connection: %w", err)
	}

	var locked bool
	if err := conn.QueryRow(lockCtx, `SELECT pg_try_advisory_lock($1)`, runtimeLeaderLockID).Scan(&locked); err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquire advisory lock: %w", err)
	}
	if !locked {
		conn.Release()
		return nil, fmt.Errorf("another BusyBee runtime instance already holds the database lock")
	}

	common.ZapLogger.Info("runtime leader lock acquired")
	return conn, nil
}

func releaseRuntimeLeaderLock(conn *pgxpool.Conn) {
	if conn == nil {
		return
	}

	unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, runtimeLeaderLockID); err != nil {
		common.ZapLogger.Warn("failed to release runtime leader lock", zap.Error(err))
	}
	conn.Release()
}
