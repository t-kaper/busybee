# Миграции BusyBee через `goose`

Этот документ описывает, как работать с миграциями схемы BusyBee после перехода
на pipeline-модель.

Если нужен смысл таблиц и runtime-потоков, смотри `docs/architecture.md`.

---

## 1. Где лежат миграции

BusyBee хранит миграции в каталоге:

```text
./migrations
```

Текущие основные миграции:

- `001_initial_schema.sql` — pipeline definition, node definition, DAG-edges, `pipeline_run` и `pipeline_node_run`
- `002_history_tables.sql` — архивные таблицы `busybee_history_pipeline_*`

---

## 2. Что нужно для запуска `goose`

Обычно нужны три вещи:

1. установленный бинарник `goose`
2. доступ к PostgreSQL
3. connection string к базе BusyBee

Пример переменных окружения:

```bash
export GOOSE_DRIVER=postgres
export GOOSE_DBSTRING='postgres://busybee_user:busybee_password@127.0.0.1:5432/busybee?sslmode=disable'
export GOOSE_MIGRATION_DIR=./migrations
```

---

## 3. Базовые команды

```bash
# посмотреть статус миграций
goose status

# применить все новые миграции
goose up

# применить одну следующую миграцию
goose up-by-one

# откатить последнюю миграцию
goose down
```

---

## 4. Что проверять после миграции

- `goose status`
- запуск BusyBee
- создание `pipeline_run`
- продвижение `pipeline_node_run`
- архивацию завершённых запусков

Особенно важно при изменении:

- `busybee_pipeline`
- `busybee_pipeline_node`
- `busybee_pipeline_edge`
- `busybee_pipeline_runs`
- `busybee_pipeline_node_runs`
- `busybee_history_pipeline_*`

---

## 5. Важное примечание

Файлы `001`–`002` уже отражают текущую pipeline-модель.

Это удобно для развёртывания базы с нуля, но для существующих production-баз
обычно нужен отдельный migration-path и отдельная стратегия миграции данных.
