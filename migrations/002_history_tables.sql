-- +goose Up
-- +goose StatementBegin

-- ============================================================================
-- 7. Архив завершённых pipeline_run
-- ============================================================================
-- HistoryArchiver переносит сюда завершённые pipeline_run и их node_run, чтобы
-- горячие runtime-таблицы оставались компактными для Ticker, Dispatcher и UI.
-- FK здесь не используются: определение pipeline может быть пересоздано или удалено,
-- а история исполнения должна сохраняться как аудит.
CREATE TABLE busybee_history_pipeline_runs
(
    id           VARCHAR(64) PRIMARY KEY,
    pipeline_id  INT                      NOT NULL,
    status       VARCHAR(50)              NOT NULL,
    trigger_type VARCHAR(50)              NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE,
    finished_at  TIMESTAMP WITH TIME ZONE,
    archived_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE busybee_history_pipeline_runs IS 'Архив завершённых запусков pipeline без внешних ключей на runtime-таблицы.';
COMMENT ON COLUMN busybee_history_pipeline_runs.id IS 'Идентификатор исходного запуска pipeline.';
COMMENT ON COLUMN busybee_history_pipeline_runs.pipeline_id IS 'Идентификатор pipeline на момент выполнения, без внешнего ключа.';
COMMENT ON COLUMN busybee_history_pipeline_runs.status IS 'Финальный статус завершённого запуска pipeline.';
COMMENT ON COLUMN busybee_history_pipeline_runs.trigger_type IS 'Источник запуска: scheduled, manual, dependency и т.д.';
COMMENT ON COLUMN busybee_history_pipeline_runs.created_at IS 'Время создания исходного запуска pipeline.';
COMMENT ON COLUMN busybee_history_pipeline_runs.finished_at IS 'Время завершения исходного запуска pipeline.';
COMMENT ON COLUMN busybee_history_pipeline_runs.archived_at IS 'Время переноса записи в архивную таблицу.';

CREATE INDEX idx_busybee_history_pipeline_runs_pipeline
    ON busybee_history_pipeline_runs (pipeline_id);

CREATE INDEX idx_busybee_history_pipeline_runs_finished
    ON busybee_history_pipeline_runs (finished_at);

-- ============================================================================
-- 8. Архив завершённых pipeline_node_run
-- ============================================================================
-- Повторяет структуру busybee_pipeline_node_runs, но без FK.
CREATE TABLE busybee_history_pipeline_node_runs
(
    id               INT PRIMARY KEY,
    pipeline_node_id INT                      NOT NULL,
    pipeline_run_id  VARCHAR(64)              NOT NULL,
    status           VARCHAR(50)              NOT NULL,
    trigger_type     VARCHAR(50)              NOT NULL,
    pid              INT,
    exit_code        INT,
    retry_count      INT                      NOT NULL DEFAULT 0,
    scheduled_at     TIMESTAMP WITH TIME ZONE,
    started_at       TIMESTAMP WITH TIME ZONE,
    finished_at      TIMESTAMP WITH TIME ZONE,
    error_msg        TEXT,
    log_path         TEXT,
    created_at       TIMESTAMP WITH TIME ZONE,
    archived_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE busybee_history_pipeline_node_runs IS 'Архив завершённых запусков узлов pipeline без внешних ключей.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.id IS 'Идентификатор исходного запуска узла.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.pipeline_node_id IS 'Идентификатор узла pipeline на момент выполнения, без внешнего ключа.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.pipeline_run_id IS 'Идентификатор запуска pipeline, к которому относился запуск узла.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.status IS 'Финальный статус завершённого запуска узла.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.trigger_type IS 'Причина запуска узла: scheduled, dependency, manual, retry и т.д.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.pid IS 'PID процесса, если он был зафиксирован во время выполнения.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.exit_code IS 'Код завершения процесса после окончания выполнения узла.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.retry_count IS 'Количество повторных попыток, с которым завершился запуск узла.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.scheduled_at IS 'Время планирования или постановки запуска узла в очередь.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.started_at IS 'Фактическое время старта выполнения узла.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.finished_at IS 'Фактическое время завершения выполнения узла.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.error_msg IS 'Текст ошибки или причина неуспешного завершения.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.log_path IS 'Путь к файлу лога, связанному с запуском узла.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.created_at IS 'Время создания исходной записи о запуске узла.';
COMMENT ON COLUMN busybee_history_pipeline_node_runs.archived_at IS 'Время переноса записи о запуске узла в архив.';

CREATE INDEX idx_busybee_history_pipeline_node_runs_pipeline_run
    ON busybee_history_pipeline_node_runs (pipeline_run_id);

CREATE INDEX idx_busybee_history_pipeline_node_runs_node
    ON busybee_history_pipeline_node_runs (pipeline_node_id);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS busybee_history_pipeline_node_runs;
DROP TABLE IF EXISTS busybee_history_pipeline_runs;
-- +goose StatementEnd
