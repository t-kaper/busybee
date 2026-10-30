package engine

import (
	"busybee/core/common"
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

// pipelineSchedule — строка из busybee_pipeline + агрегированное количество active run.
type pipelineSchedule struct {
	ID            int
	Name          string
	Schedule      string
	MaxActiveRuns int
	ActiveRuns    int
	parser        cron.Schedule
}

// StartTicker запускает cron-планировщик на уровне pipeline.
//
// Каждые interval секунд:
//  1. Загружает все active pipeline с непустым schedule.
//  2. Для каждого проверяет, наступил ли следующий cron-момент с прошлого тика.
//  3. Если наступил и лимит max_active_runs не превышен — создаёт pipeline_run
//     вместе со стартовыми node_run (через CreatePipelineRun).
//
// lastCheck хранит время последней проверки для каждого pipeline_id,
// чтобы не пропустить и не дублировать запуски.
func StartTicker(ctx context.Context, pool *pgxpool.Pool, interval time.Duration, location *time.Location) {
	common.ZapLogger.Info("Ticker запущен",
		zap.Duration("interval", interval),
		zap.String("timezone", location.String()))

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastCheck := make(map[int]time.Time)

	for {
		select {
		case <-ctx.Done():
			common.ZapLogger.Info("Ticker остановлен")
			return
		case now := <-ticker.C:
			scheduleActivePipelines(ctx, pool, parser, lastCheck, now.In(location))
		}
	}
}

// scheduleActivePipelines загружает active pipeline и создаёт pipeline_run для тех,
// у которых наступил cron-момент.
func scheduleActivePipelines(
	ctx context.Context,
	pool *pgxpool.Pool,
	parser cron.Parser,
	lastCheck map[int]time.Time,
	now time.Time,
) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Загружаем все active pipeline с cron-расписанием + количество их active run.
	// LEFT JOIN — чтобы pipeline без active run тоже попали в выборку.
	rows, err := pool.Query(ctx, `--sql
		SELECT
			w.id,
			w.name,
			w.schedule,
			w.max_active_runs,
			COUNT(wr.id) AS active_runs
		FROM busybee_pipeline w
		LEFT JOIN busybee_pipeline_runs wr
		       ON wr.pipeline_id = w.id
		      AND wr.status = 'active'
		WHERE w.is_active = true
		  AND w.schedule IS NOT NULL
		  AND w.schedule <> ''
		GROUP BY w.id
	`)
	if err != nil {
		common.ZapLogger.Error("Ticker: ошибка получения pipeline", zap.Error(err))
		return
	}
	defer rows.Close()

	pipelines := make([]pipelineSchedule, 0)
	for rows.Next() {
		var wf pipelineSchedule
		if err := rows.Scan(&wf.ID, &wf.Name, &wf.Schedule, &wf.MaxActiveRuns, &wf.ActiveRuns); err != nil {
			common.ZapLogger.Error("Ticker: ошибка сканирования pipeline", zap.Error(err))
			continue
		}

		schedule, err := parser.Parse(wf.Schedule)
		if err != nil {
			common.ZapLogger.Warn("Ticker: невалидный cron pipeline",
				zap.Int("pipeline_id", wf.ID),
				zap.String("schedule", wf.Schedule),
				zap.Error(err))
			continue
		}
		wf.parser = schedule
		pipelines = append(pipelines, wf)
	}
	if err := rows.Err(); err != nil {
		common.ZapLogger.Error("Ticker: ошибка чтения pipeline", zap.Error(err))
		return
	}

	for _, pipeline := range pipelines {
		last, exists := lastCheck[pipeline.ID]
		if !exists {
			// Первый тик — считаем, что предыдущая проверка была минуту назад.
			last = now.Add(-time.Minute)
		}

		activeRuns := pipeline.ActiveRuns
		cursor := last

		// Обрабатываем все просроченные cron-слоты до текущего now.
		// Это устраняет схлопывание нескольких запусков в один, если тикер отставал.
		for {
			nextRun := pipeline.parser.Next(cursor)
			if nextRun.After(now) {
				break
			}

			if pipeline.MaxActiveRuns > 0 && activeRuns >= pipeline.MaxActiveRuns {
				common.ZapLogger.Debug("Ticker: лимит pipeline_run достигнут",
					zap.Int("pipeline_id", pipeline.ID),
					zap.String("name", pipeline.Name),
					zap.Int("active", activeRuns),
					zap.Int("max", pipeline.MaxActiveRuns))
				// Не продвигаем lastCheck дальше непройденного cron-слота:
				// когда активных run станет меньше, попробуем догнать пропущенное окно.
				lastCheck[pipeline.ID] = cursor
				break
			}

			pipelineRunID, startNodeCount, err := CreatePipelineRun(ctx, pool, pipeline.ID, "scheduled")
			if err != nil {
				if errors.Is(err, ErrPipelineRunLimitReached) {
					common.ZapLogger.Debug("Ticker: лимит pipeline_run достигнут (race condition)",
						zap.Int("pipeline_id", pipeline.ID),
						zap.String("name", pipeline.Name),
						zap.Int("active", activeRuns),
						zap.Int("max", pipeline.MaxActiveRuns))
				} else if errors.Is(err, ErrPipelineInactive) {
					common.ZapLogger.Debug("Ticker: pipeline был деактивирован до создания run",
						zap.Int("pipeline_id", pipeline.ID),
						zap.String("name", pipeline.Name))
				} else {
					common.ZapLogger.Error("Ticker: ошибка создания pipeline_run",
						zap.Int("pipeline_id", pipeline.ID),
						zap.String("name", pipeline.Name),
						zap.Error(err))
				}
				// На ошибке не продвигаем cursor, чтобы не потерять конкретное окно расписания.
				lastCheck[pipeline.ID] = cursor
				break
			}

			activeRuns++
			cursor = nextRun
			common.ZapLogger.Info("Ticker: новый pipeline_run запущен",
				zap.String("pipeline_run_id", pipelineRunID),
				zap.Int("pipeline_id", pipeline.ID),
				zap.String("pipeline_name", pipeline.Name),
				zap.Time("scheduled_for", nextRun),
				zap.Int("start_node_count", startNodeCount),
				zap.Int("active_runs", activeRuns),
				zap.Int("max_active_runs", pipeline.MaxActiveRuns))
		}

		// Watermark всегда хранит последний реально обработанный cron-слот.
		// Это не даёт смещать расписание к "времени тика" и терять окна при catch-up.
		lastCheck[pipeline.ID] = cursor
	}
}
