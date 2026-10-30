-- +goose Up
-- +goose StatementBegin

-- ============================================================================
-- 1. Группы pipeline (опциональная каталогизация для UI и документации)
-- ============================================================================
CREATE TABLE busybee_pipeline_group
(
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE busybee_pipeline_group IS 'Группа pipeline для UI-каталогизации и логического объединения.';
COMMENT ON COLUMN busybee_pipeline_group.id IS 'Уникальный идентификатор группы pipeline.';
COMMENT ON COLUMN busybee_pipeline_group.name IS 'Уникальное имя группы, отображаемое в UI и API.';
COMMENT ON COLUMN busybee_pipeline_group.description IS 'Свободное текстовое описание назначения группы.';
COMMENT ON COLUMN busybee_pipeline_group.created_at IS 'Время создания записи о группе.';

-- ============================================================================
-- 2. Pipeline definition
-- ============================================================================
-- Pipeline — это шаблон целого DAG-а.
-- Cron, активность и лимит параллельных запусков относятся ко всему pipeline,
-- а не к отдельному узлу.
CREATE TABLE busybee_pipeline
(
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255)             NOT NULL UNIQUE,
    description     TEXT,
    pipeline_group_id INT REFERENCES busybee_pipeline_group (id) ON DELETE CASCADE,
    schedule        VARCHAR(100),
    is_active       BOOLEAN                  NOT NULL DEFAULT true,
    max_active_runs INT                      NOT NULL DEFAULT 1 CHECK (max_active_runs >= 0),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE busybee_pipeline IS 'Определение DAG pipeline. Cron и лимит параллелизма живут на уровне pipeline.';
COMMENT ON COLUMN busybee_pipeline.id IS 'Уникальный идентификатор pipeline.';
COMMENT ON COLUMN busybee_pipeline.name IS 'Уникальное имя pipeline.';
COMMENT ON COLUMN busybee_pipeline.description IS 'Текстовое описание назначения и поведения pipeline.';
COMMENT ON COLUMN busybee_pipeline.pipeline_group_id IS 'Ссылка на группу pipeline для каталогизации; может быть NULL. Удаление группы каскадно удаляет pipeline.';
COMMENT ON COLUMN busybee_pipeline.schedule IS 'Cron-выражение для планового запуска pipeline.';
COMMENT ON COLUMN busybee_pipeline.is_active IS 'Флаг, определяющий доступность pipeline для планировщика и ручного запуска.';
COMMENT ON COLUMN busybee_pipeline.max_active_runs IS 'Максимальное число одновременно активных запусков данного pipeline.';
COMMENT ON COLUMN busybee_pipeline.created_at IS 'Время создания определения pipeline.';
COMMENT ON COLUMN busybee_pipeline.updated_at IS 'Время последнего обновления определения pipeline.';

-- ============================================================================
-- 3. Узлы pipeline
-- ============================================================================
-- Узел — это исполняемая команда внутри pipeline.
-- Стартовые узлы определяются отсутствием входящих рёбер в busybee_pipeline_edge.
CREATE TABLE busybee_pipeline_node
(
    id              SERIAL PRIMARY KEY,
    pipeline_id     INT                      NOT NULL REFERENCES busybee_pipeline (id) ON DELETE CASCADE,
    name            VARCHAR(255)             NOT NULL,
    description     TEXT,
    command         TEXT                     NOT NULL,
    executor_type   VARCHAR(50)              NOT NULL DEFAULT 'local',
    max_retries     INT                      NOT NULL DEFAULT 0 CHECK (max_retries >= 0),
    timeout_seconds INT                      NOT NULL DEFAULT 3600 CHECK (timeout_seconds > 0),
    meta            JSONB                    NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pipeline_id, name)
);
COMMENT ON TABLE busybee_pipeline_node IS 'Исполняемый узел DAG-а. Сам по себе не стартует pipeline, а живёт внутри одного определения.';
COMMENT ON COLUMN busybee_pipeline_node.id IS 'Уникальный идентификатор узла pipeline.';
COMMENT ON COLUMN busybee_pipeline_node.pipeline_id IS 'Ссылка на pipeline, которому принадлежит узел.';
COMMENT ON COLUMN busybee_pipeline_node.name IS 'Уникальное внутри pipeline имя узла.';
COMMENT ON COLUMN busybee_pipeline_node.description IS 'Текстовое описание назначения узла.';
COMMENT ON COLUMN busybee_pipeline_node.command IS 'Команда, которую нужно выполнить для данного узла.';
COMMENT ON COLUMN busybee_pipeline_node.executor_type IS 'Тип исполнителя команды, например local.';
COMMENT ON COLUMN busybee_pipeline_node.max_retries IS 'Максимальное количество автоматических повторных запусков узла.';
COMMENT ON COLUMN busybee_pipeline_node.timeout_seconds IS 'Таймаут выполнения узла в секундах.';
COMMENT ON COLUMN busybee_pipeline_node.meta IS 'Дополнительные параметры узла в формате JSONB.';
COMMENT ON COLUMN busybee_pipeline_node.created_at IS 'Время создания узла.';
COMMENT ON COLUMN busybee_pipeline_node.updated_at IS 'Время последнего обновления узла.';

-- ============================================================================
-- 4. DAG-рёбра pipeline
-- ============================================================================
-- Потомок может быть поставлен к запуску только после success всех parent-узлов
-- в рамках одного pipeline_run.
CREATE TABLE busybee_pipeline_edge
(
    parent_node_id INT NOT NULL REFERENCES busybee_pipeline_node (id) ON DELETE CASCADE,
    child_node_id  INT NOT NULL REFERENCES busybee_pipeline_node (id) ON DELETE CASCADE,
    PRIMARY KEY (parent_node_id, child_node_id),
    CHECK (parent_node_id <> child_node_id)
);
COMMENT ON TABLE busybee_pipeline_edge IS 'DAG pipeline: child-узел допускается к исполнению после success всех parent-узлов в одном pipeline_run.';
COMMENT ON COLUMN busybee_pipeline_edge.parent_node_id IS 'Идентификатор родительского узла, который должен завершиться раньше.';
COMMENT ON COLUMN busybee_pipeline_edge.child_node_id IS 'Идентификатор дочернего узла, зависящего от parent-узла.';

CREATE OR REPLACE FUNCTION busybee_check_pipeline_edge_same_pipeline()
RETURNS trigger AS $$
DECLARE
    parent_pipeline_id INT;
    child_pipeline_id  INT;
BEGIN
    -- Читаем обе ноды одним запросом, чтобы не делать два отдельных lookup
    -- на каждую вставку/обновление ребра.
    SELECT parent.pipeline_id, child.pipeline_id
    INTO parent_pipeline_id, child_pipeline_id
    FROM busybee_pipeline_node parent
    JOIN busybee_pipeline_node child ON child.id = NEW.child_node_id
    WHERE parent.id = NEW.parent_node_id;

    IF parent_pipeline_id IS NULL OR child_pipeline_id IS NULL THEN
        RAISE EXCEPTION 'pipeline edge references unknown node';
    END IF;

    IF parent_pipeline_id <> child_pipeline_id THEN
        RAISE EXCEPTION 'pipeline edge must connect nodes from the same pipeline';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_busybee_pipeline_edge_same_pipeline
BEFORE INSERT OR UPDATE ON busybee_pipeline_edge
FOR EACH ROW
EXECUTE FUNCTION busybee_check_pipeline_edge_same_pipeline();

-- ============================================================================
-- 5. Запуски pipeline
-- ============================================================================
-- Один pipeline_run представляет запуск целого DAG-а.
-- Внутри него могут существовать несколько стартовых узлов без общего root.
CREATE TABLE busybee_pipeline_runs
(
    id           VARCHAR(64) PRIMARY KEY,
    pipeline_id  INT                      NOT NULL REFERENCES busybee_pipeline (id) ON DELETE CASCADE,
    status       VARCHAR(50)              NOT NULL DEFAULT 'active',
    trigger_type VARCHAR(50)              NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at  TIMESTAMP WITH TIME ZONE
);
COMMENT ON TABLE busybee_pipeline_runs IS 'Один execution run для целого pipeline.';
COMMENT ON COLUMN busybee_pipeline_runs.id IS 'Уникальный идентификатор запуска pipeline.';
COMMENT ON COLUMN busybee_pipeline_runs.pipeline_id IS 'Ссылка на pipeline, экземпляром выполнения которого является этот run.';
COMMENT ON COLUMN busybee_pipeline_runs.status IS 'Текущее состояние запуска pipeline.';
COMMENT ON COLUMN busybee_pipeline_runs.trigger_type IS 'Источник запуска: scheduled, manual, dependency и т.д.';
COMMENT ON COLUMN busybee_pipeline_runs.created_at IS 'Время создания запуска pipeline.';
COMMENT ON COLUMN busybee_pipeline_runs.finished_at IS 'Время завершения запуска pipeline, если он уже завершён.';

CREATE INDEX idx_busybee_pipeline_active_schedule
    ON busybee_pipeline (id, schedule)
    WHERE is_active = true AND schedule IS NOT NULL AND schedule <> '';

CREATE INDEX idx_busybee_pipeline_runs_active
    ON busybee_pipeline_runs (pipeline_id, status)
    WHERE status = 'active';

CREATE INDEX idx_busybee_pipeline_runs_status_created
    ON busybee_pipeline_runs (status, created_at DESC);

-- HistoryArchiver отбирает завершённые run по finished_at, поэтому отдельный
-- partial index под terminal-статусы нужен для коротких range scan.
CREATE INDEX idx_busybee_pipeline_runs_finished_terminal
    ON busybee_pipeline_runs (finished_at, id)
    WHERE status IN ('completed', 'failed', 'stopped')
      AND finished_at IS NOT NULL;

-- ============================================================================
-- 6. Запуски узлов pipeline
-- ============================================================================
-- Узел может иметь несколько run-ов внутри одного pipeline_run:
--   - первый dependency/scheduled/manual запуск;
--   - retry;
--   - будущий ручной перезапуск, если он будет возвращён отдельным runtime API.
-- Но одновременно активным может быть только один run конкретного узла.
CREATE TABLE busybee_pipeline_node_runs
(
    id               SERIAL PRIMARY KEY,
    pipeline_node_id INT                      NOT NULL REFERENCES busybee_pipeline_node (id) ON DELETE CASCADE,
    pipeline_run_id  VARCHAR(64)              NOT NULL REFERENCES busybee_pipeline_runs (id) ON DELETE CASCADE,
    status           VARCHAR(50)              NOT NULL DEFAULT 'pending',
    trigger_type     VARCHAR(50)              NOT NULL,
    pid              INT,
    exit_code        INT,
    retry_count      INT                      NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    scheduled_at     TIMESTAMP WITH TIME ZONE,
    started_at       TIMESTAMP WITH TIME ZONE,
    finished_at      TIMESTAMP WITH TIME ZONE,
    error_msg        TEXT,
    log_path         TEXT,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE busybee_pipeline_node_runs IS 'Конкретный запуск узла внутри одного pipeline_run.';
COMMENT ON COLUMN busybee_pipeline_node_runs.id IS 'Уникальный идентификатор запуска узла.';
COMMENT ON COLUMN busybee_pipeline_node_runs.pipeline_node_id IS 'Ссылка на узел pipeline, который был запущен.';
COMMENT ON COLUMN busybee_pipeline_node_runs.pipeline_run_id IS 'Ссылка на общий запуск pipeline, внутри которого выполняется узел.';
COMMENT ON COLUMN busybee_pipeline_node_runs.status IS 'Текущее состояние запуска узла.';
COMMENT ON COLUMN busybee_pipeline_node_runs.trigger_type IS 'Причина запуска узла: scheduled, dependency, manual, retry и т.д.';
COMMENT ON COLUMN busybee_pipeline_node_runs.pid IS 'PID процесса, если узел выполняется как локальный системный процесс.';
COMMENT ON COLUMN busybee_pipeline_node_runs.exit_code IS 'Код завершения процесса после окончания выполнения.';
COMMENT ON COLUMN busybee_pipeline_node_runs.retry_count IS 'Количество уже выполненных повторных попыток для этого запуска узла.';
COMMENT ON COLUMN busybee_pipeline_node_runs.scheduled_at IS 'Время, на которое запуск узла был поставлен в очередь или запланирован.';
COMMENT ON COLUMN busybee_pipeline_node_runs.started_at IS 'Фактическое время старта выполнения узла.';
COMMENT ON COLUMN busybee_pipeline_node_runs.finished_at IS 'Фактическое время завершения выполнения узла.';
COMMENT ON COLUMN busybee_pipeline_node_runs.error_msg IS 'Текст ошибки или причина неуспешного завершения.';
COMMENT ON COLUMN busybee_pipeline_node_runs.log_path IS 'Путь к файлу лога, связанному с этим запуском узла.';
COMMENT ON COLUMN busybee_pipeline_node_runs.created_at IS 'Время создания записи о запуске узла.';

CREATE INDEX idx_busybee_pipeline_node_pipeline
    ON busybee_pipeline_node (pipeline_id, id);

CREATE INDEX idx_busybee_pipeline_edge_parent
    ON busybee_pipeline_edge (parent_node_id);

CREATE INDEX idx_busybee_pipeline_edge_child
    ON busybee_pipeline_edge (child_node_id);

CREATE INDEX idx_busybee_pipeline_node_runs_status
    ON busybee_pipeline_node_runs (status)
    WHERE status IN ('pending', 'queued', 'running');

CREATE INDEX idx_busybee_pipeline_node_runs_pipeline_run
    ON busybee_pipeline_node_runs (pipeline_run_id);

CREATE INDEX idx_busybee_pipeline_node_runs_node
    ON busybee_pipeline_node_runs (pipeline_node_id);

CREATE INDEX idx_busybee_pipeline_node_runs_node_run_status
    ON busybee_pipeline_node_runs (pipeline_node_id, pipeline_run_id, status);

-- Гарантирует, что в одном pipeline_run не будет двух активных run-ов одного узла.
CREATE UNIQUE INDEX unique_active_node_run_in_pipeline_run
    ON busybee_pipeline_node_runs (pipeline_node_id, pipeline_run_id)
    WHERE status IN ('pending', 'queued', 'running');

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS busybee_pipeline_node_runs;
DROP TABLE IF EXISTS busybee_pipeline_runs;
DROP TABLE IF EXISTS busybee_pipeline_edge;
DROP FUNCTION IF EXISTS busybee_check_pipeline_edge_same_pipeline();
DROP TABLE IF EXISTS busybee_pipeline_node;
DROP TABLE IF EXISTS busybee_pipeline;
DROP TABLE IF EXISTS busybee_pipeline_group;
-- +goose StatementEnd
