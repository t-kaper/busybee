# BusyBee HTTP API

Текущий HTTP API намеренно минимален.

Он включает:

- публичные проверки состояния `health` и `ready`
- CRUD для `busybee_pipeline_group`
- CRUD для `busybee_pipeline`
- CRUD для `busybee_pipeline_node`
- list/create/delete для `busybee_pipeline_edge`
- graph endpoint для `busybee_pipeline`

## Base URL

```text
http://<host>:8080
```

Адрес настраивается через `BUSYBEE_APP__HTTP_ADDR` или `app.http_addr` в `config.yaml`.

## Content-Type

Все запросы и ответы используют `application/json`.

## Authorization

Все маршруты с префиксом `/api/` требуют заголовок:

```text
Authorization: Bearer <token>
```

Токен задаётся через `BUSYBEE_API__BEARER_TOKEN` или `api.bearer_token` в `config.yaml`.

## Public Endpoints

### `GET /health`

Возвращает состояние HTTP процесса.

Пример ответа:

```json
{"status":"ok"}
```

### `GET /ready`

Проверяет доступность PostgreSQL через `Ping`.

Пример успешного ответа:

```json
{"status":"ok"}
```

Если БД недоступна:

```json
{"status":"unavailable"}
```

## Pipeline Groups API

HTTP API работает с таблицей `busybee_pipeline_group`.

### `GET /api/pipeline-groups`

Возвращает список групп с пагинацией.

Query params:

- `limit` — положительное целое, по умолчанию `100`
- `offset` — целое `>= 0`, по умолчанию `0`

Пример ответа:

```json
{
  "items": [
    {
      "id": 1,
      "name": "default",
      "description": "main pipelines",
      "created_at": "2026-03-23T10:00:00Z"
    }
  ],
  "total": 1
}
```

### `GET /api/pipeline-groups/detailed`

Возвращает список групп с пагинацией и вложенными pipeline для каждой группы.

Для каждого pipeline дополнительно подгружаются:

- `name`
- `schedule` — cron-выражение для планового запуска (может быть `null`)
- `is_active`
- `node_count` — количество записей в `busybee_pipeline_node`, относящихся к pipeline

Query params:

- `limit` — положительное целое, по умолчанию `100`
- `offset` — целое `>= 0`, по умолчанию `0`

Пример ответа:

```json
{
  "items": [
    {
      "id": 1,
      "name": "default",
      "description": "main pipelines",
      "created_at": "2026-03-23T10:00:00Z",
      "pipelines": [
        {
          "id": 10,
          "name": "daily-import",
          "schedule": "0 3 * * *",
          "is_active": true,
          "node_count": 4
        },
        {
          "id": 11,
          "name": "cleanup",
          "schedule": null,
          "is_active": false,
          "node_count": 2
        }
      ]
    }
  ],
  "total": 1
}
```

### `POST /api/pipeline-groups`

Создаёт новую группу.

Пример запроса:

```json
{
  "name": "analytics",
  "description": "analytics pipelines"
}
```

Пример ответа:

```json
{
  "id": 2,
  "name": "analytics",
  "description": "analytics pipelines",
  "created_at": "2026-03-23T10:05:00Z"
}
```

### `PATCH /api/pipeline-groups/{id}`

Частично обновляет группу. Нужно передать хотя бы одно поле.

Пример запроса:

```json
{
  "description": "updated description"
}
```

### `DELETE /api/pipeline-groups/{id}`

Удаляет группу. Успешный ответ: `204 No Content`.

## Pipelines API

HTTP API работает с таблицами:

- `busybee_pipeline`
- `busybee_pipeline_node`
- `busybee_pipeline_edge`

### `GET /api/pipelines`

Возвращает список pipeline с пагинацией.

Query params:

- `limit` — положительное целое, по умолчанию `100`
- `offset` — целое `>= 0`, по умолчанию `0`

Пример ответа:

```json
{
  "items": [
    {
      "id": 10,
      "name": "daily-import",
      "description": "daily import pipeline",
      "pipeline_group_id": 1,
      "schedule": "0 * * * *",
      "is_active": false,
      "max_active_runs": 1,
      "node_count": 4,
      "has_active_runs": false,
      "created_at": "2026-03-24T10:00:00Z",
      "updated_at": "2026-03-24T10:00:00Z"
    }
  ],
  "total": 1
}
```

### `GET /api/pipelines/{id}`

Возвращает один pipeline по id.

Пример ответа:

```json
{
  "id": 10,
  "name": "daily-import",
  "description": "daily import pipeline",
  "pipeline_group_id": 1,
  "schedule": "0 * * * *",
  "is_active": false,
  "max_active_runs": 1,
  "node_count": 4,
  "has_active_runs": false,
  "created_at": "2026-03-24T10:00:00Z",
  "updated_at": "2026-03-24T10:00:00Z"
}
```

### `POST /api/pipelines`

Создаёт новый pipeline definition.

Поддерживаемые поля:

- `name`
- `description`
- `pipeline_group_id`
- `schedule`
- `is_active`
- `max_active_runs`

Пример запроса:

```json
{
  "name": "daily-import",
  "description": "daily import pipeline",
  "pipeline_group_id": 1,
  "schedule": "0 * * * *",
  "is_active": false,
  "max_active_runs": 1
}
```

Пример ответа:

```json
{
  "id": 10,
  "name": "daily-import",
  "description": "daily import pipeline",
  "pipeline_group_id": 1,
  "schedule": "0 * * * *",
  "is_active": false,
  "max_active_runs": 1,
  "node_count": 0,
  "has_active_runs": false,
  "created_at": "2026-03-24T10:00:00Z",
  "updated_at": "2026-03-24T10:00:00Z"
}
```

Важно:

- новый pipeline создаётся как draft definition
- если `is_active=true`, API вернёт `validation_error`, потому что graph ещё не сконфигурирован

### `PATCH /api/pipelines/{id}`

Частично обновляет pipeline. Нужно передать хотя бы одно поле.

Пример запроса:

```json
{
  "description": "updated description",
  "schedule": "*/15 * * * *"
}
```

Правила:

- если у pipeline есть хотя бы один `active` run, разрешена только деактивация через `{"is_active": false}`
- если активных run нет, разрешено менять все поддерживаемые поля
- при переводе `is_active=false -> true` API валидирует graph pipeline

### `DELETE /api/pipelines/{id}`

Выполняет hard delete pipeline. Успешный ответ: `204 No Content`.

Удаляются:

- запись в `busybee_pipeline`
- связанные `busybee_pipeline_node`
- связанные `busybee_pipeline_edge`
- связанные горячие `busybee_pipeline_runs`
- связанные горячие `busybee_pipeline_node_runs`

Не удаляются:

- `busybee_history_pipeline_runs`
- `busybee_history_pipeline_node_runs`

Если у pipeline есть хотя бы один `active` run, API вернёт `conflict`.

### `GET /api/pipelines/{id}/nodes`

Возвращает все node указанного pipeline.

Пример ответа:

```json
[
  {
    "id": 100,
    "pipeline_id": 10,
    "name": "extract",
    "description": "extract raw data",
    "command": "python extract.py",
    "executor_type": "local",
    "max_retries": 1,
    "timeout_seconds": 600,
    "meta": {
      "position_x": 120,
      "position_y": 80
    },
    "created_at": "2026-03-24T10:00:00Z",
    "updated_at": "2026-03-24T10:00:00Z"
  }
]
```

### `GET /api/pipelines/{id}/nodes/{nodeId}`

Возвращает один node указанного pipeline.

Пример ответа:

```json
{
  "id": 100,
  "pipeline_id": 10,
  "name": "extract",
  "description": "extract raw data",
  "command": "python extract.py",
  "executor_type": "local",
  "max_retries": 1,
  "timeout_seconds": 600,
  "meta": {
    "position_x": 120,
    "position_y": 80
  },
  "created_at": "2026-03-24T10:00:00Z",
  "updated_at": "2026-03-24T10:00:00Z"
}
```

### `POST /api/pipelines/{id}/nodes`

Создаёт новый node внутри pipeline.

Поддерживаемые поля:

- `name`
- `description`
- `command`
- `executor_type`
- `max_retries`
- `timeout_seconds`
- `meta`

Пример запроса:

```json
{
  "name": "extract",
  "description": "extract raw data",
  "command": "python extract.py",
  "executor_type": "local",
  "max_retries": 1,
  "timeout_seconds": 600,
  "meta": {
    "position_x": 120,
    "position_y": 80,
    "frontend": {
      "color": "blue"
    }
  }
}
```

Пример ответа:

```json
{
  "id": 100,
  "pipeline_id": 10,
  "name": "extract",
  "description": "extract raw data",
  "command": "python extract.py",
  "executor_type": "local",
  "max_retries": 1,
  "timeout_seconds": 600,
  "meta": {
    "position_x": 120,
    "position_y": 80,
    "frontend": {
      "color": "blue"
    }
  },
  "created_at": "2026-03-24T10:00:00Z",
  "updated_at": "2026-03-24T10:00:00Z"
}
```

Правила:

- `name` обязателен и должен быть уникален внутри pipeline
- `command` обязателен
- `max_retries` должен быть `>= 0`
- `timeout_seconds` должен быть `> 0`
- если `executor_type` не передан или пустой, используется значение `local`
- если `timeout_seconds` не передан или равен `0`, используется значение `3600`
- `meta` сохраняется как произвольный JSON без серверной схемы валидации
- если у pipeline есть хотя бы один `active` run, API вернёт `conflict`

### `PATCH /api/pipelines/{id}/nodes/{nodeId}`

Частично обновляет node. Нужно передать хотя бы одно поле.

Поддерживаемые поля:

- `name`
- `description`
- `command`
- `executor_type`
- `max_retries`
- `timeout_seconds`
- `meta`

Пример запроса:

```json
{
  "description": "extract data from source",
  "meta": {
    "position_x": 220,
    "position_y": 140,
    "frontend": {
      "label": "Extract"
    }
  }
}
```

Нюансы:

- PATCH работает как partial update: непереданные поля не меняются
- `meta` заменяется целиком новым JSON-объектом
- если нужно очистить `meta`, можно передать `"meta": null`, сервер сохранит пустой объект `{}` 
- если у pipeline есть хотя бы один `active` run, API вернёт `conflict`

### `DELETE /api/pipelines/{id}/nodes/{nodeId}`

Удаляет node pipeline. Успешный ответ: `204 No Content`.

Нюансы:

- связанные `busybee_pipeline_edge` удаляются каскадно
- связанные горячие `busybee_pipeline_node_runs` удаляются каскадно
- если pipeline активен и удаление делает graph невалидным для запуска, API вернёт `validation_error`
- если у pipeline есть хотя бы один `active` run, API вернёт `conflict`

### `GET /api/pipelines/{id}/edges`

Возвращает все edge указанного pipeline.

Пример ответа:

```json
[
  {
    "parent_node_id": 100,
    "child_node_id": 101
  }
]
```

### `POST /api/pipelines/{id}/edges`

Создаёт новое edge между двумя node указанного pipeline.

Пример запроса:

```json
{
  "parent_node_id": 100,
  "child_node_id": 101
}
```

Пример ответа:

```json
{
  "parent_node_id": 100,
  "child_node_id": 101
}
```

Правила:

- обе node должны существовать
- обе node должны принадлежать pipeline из URL
- self-loop запрещён
- duplicate edge вернёт `conflict`
- API сразу запрещает создавать цикл, даже у неактивного pipeline
- если pipeline активен, новое edge не должно делать graph невалидным для запуска
- если у pipeline есть хотя бы один `active` run, API вернёт `conflict`

### `DELETE /api/pipelines/{id}/edges`

Удаляет одно edge указанного pipeline. Успешный ответ: `204 No Content`.

Тело запроса:

```json
{
  "parent_node_id": 100,
  "child_node_id": 101
}
```

Нюансы:

- удаление выполняется по паре `parent_node_id` + `child_node_id`
- если такого edge нет внутри pipeline, API вернёт `not_found`
- если pipeline активен, удаление не должно делать graph невалидным для запуска
- если у pipeline есть хотя бы один `active` run, API вернёт `conflict`

### `GET /api/pipelines/{id}/graph`

Возвращает pipeline вместе со всеми node и edge.

Пример ответа:

```json
{
  "pipeline": {
    "id": 10,
    "name": "daily-import",
    "description": "daily import pipeline",
    "pipeline_group_id": 1,
    "schedule": "0 * * * *",
    "is_active": false,
    "max_active_runs": 1,
    "node_count": 2,
    "has_active_runs": false,
    "created_at": "2026-03-24T10:00:00Z",
    "updated_at": "2026-03-24T10:00:00Z"
  },
  "nodes": [
    {
      "id": 100,
      "pipeline_id": 10,
      "name": "extract",
      "description": "extract raw data",
      "command": "python extract.py",
      "executor_type": "local",
      "max_retries": 1,
      "timeout_seconds": 600,
      "meta": {},
      "created_at": "2026-03-24T10:00:00Z",
      "updated_at": "2026-03-24T10:00:00Z"
    }
  ],
  "edges": [
    {
      "parent_node_id": 100,
      "child_node_id": 101
    }
  ]
}
```

Пустой graph допустим:

- `nodes = []`
- `edges = []`

Но пустой graph нельзя активировать.

### Graph Mutation Rules

При работе с node и edge действуют дополнительные правила:

- любые изменения graph запрещены, если у pipeline есть хотя бы один `active` run
- цикл запрещён всегда, даже у неактивного pipeline
- graph может временно не иметь стартовой node, если pipeline неактивен
- disconnected components допустимы
- при активном pipeline любое изменение graph должно оставлять его валидным для запуска

### Graph Activation Rules

При `PATCH` с переводом `is_active=false -> true` graph должен пройти проверки:

- graph не пустой
- есть хотя бы одна стартовая node без входящих рёбер
- нет self-loop
- все edge относятся к node этого pipeline
- граф ацикличен

### Schedule Semantics

Поле `schedule` может быть:

- `null`
- пустой строкой, которая нормализуется в `NULL`
- валидным cron-выражением

Если cron невалиден, API возвращает `validation_error`.

Семантика:

- `schedule = null` означает, что pipeline не запускается по cron
- `is_active = false` означает, что pipeline выключен
- `is_active = true` и `schedule = null` допустимы, но такой pipeline не будет запускаться автоматически по cron

## Pipeline Runs API

Runtime API читает только горячие таблицы:

- `busybee_pipeline_runs`
- `busybee_pipeline_node_runs`

Архивные таблицы `busybee_history_pipeline_runs` и `busybee_history_pipeline_node_runs`
в эти endpoint'ы не входят.

### Runtime Status Semantics

Статусы `pipeline_run`:

- `active` — run ещё не завершён
- `completed` — все запланированные node завершились успешно
- `failed` — run завершён, и среди последних попыток node есть хотя бы одна `failed`
- `stopped` — run завершён вручную или через будущий runtime-control сценарий; этот статус уже поддержан моделью API

Статусы `node` в detail endpoint:

- `not_started` — для node ещё не создано ни одной попытки внутри данного `pipeline_run`
- `pending`
- `queued`
- `running`
- `success`
- `failed`
- `stopped`

Статусы `attempts[]` в `node_run`:

- `pending`
- `queued`
- `running`
- `success`
- `failed`
- `stopped`

Поле `trigger_type` API не ограничивает жёстким enum на уровне HTTP-контракта, но на практике сейчас встречаются:

- `scheduled`
- `manual`
- `dependency`
- `retry`
- `recovery`

### `GET /api/pipeline-runs`

Возвращает список горячих запусков pipeline без пагинации.

Query params:

- `pipeline_id` — положительное целое, опционально
- `status` — одно из `active`, `completed`, `failed`, `stopped`
- `trigger_type` — произвольная непустая строка, например `scheduled`, `manual`, `dependency`, `retry`, `recovery`
- `created_from` — timestamp в формате RFC3339
- `created_to` — timestamp в формате RFC3339

Правила валидации:

- если передан `pipeline_id`, он должен быть положительным целым числом и ссылаться на существующий pipeline
- если передан `status`, он должен быть одним из `active`, `completed`, `failed`, `stopped`
- если передан `trigger_type`, пустая строка после trim игнорируется
- если переданы `created_from` и `created_to`, обе даты должны быть валидными RFC3339 timestamp
- если переданы обе даты, то `created_from <= created_to`

Эндпоинт возвращает:

- все matching run по всем pipeline
- `active` run в начале списка
- далее остальные run по `created_at DESC`

Пример запроса:

```text
GET /api/pipeline-runs?pipeline_id=10&status=active
```

Пример ответа:

```json
{
  "items": [
    {
      "id": "pipeline_run_20260325_120100_ab12",
      "pipeline_id": 10,
      "pipeline_name": "daily-import",
      "status": "active",
      "trigger_type": "scheduled",
      "created_at": "2026-03-25T12:01:00Z",
      "finished_at": null,
      "stats": {
        "total_nodes": 4,
        "not_started_nodes": 1,
        "pending_nodes": 1,
        "queued_nodes": 0,
        "running_nodes": 1,
        "success_nodes": 1,
        "failed_nodes": 1,
        "stopped_nodes": 0,
        "total_attempts": 5
      }
    }
  ],
  "total": 1
}
```

Поля ответа:

- `items[].id` — строковый идентификатор `pipeline_run`
- `items[].pipeline_id` — числовой id pipeline
- `items[].pipeline_name` — имя pipeline на момент чтения из горячей таблицы definitions
- `items[].status` — текущий статус `pipeline_run`
- `items[].trigger_type` — источник запуска
- `items[].created_at` — время создания run
- `items[].finished_at` — время завершения run или `null`, если run ещё активен
- `items[].stats.total_nodes` — число node в текущем определении pipeline
- `items[].stats.not_started_nodes` — число node, для которых в этом run ещё нет ни одной попытки
- `items[].stats.pending_nodes` — число node, у которых последняя попытка имеет статус `pending`
- `items[].stats.queued_nodes` — число node, у которых последняя попытка имеет статус `queued`
- `items[].stats.running_nodes` — число node, у которых последняя попытка имеет статус `running`
- `items[].stats.success_nodes` — число node, у которых последняя попытка имеет статус `success`
- `items[].stats.failed_nodes` — число node, у которых последняя попытка имеет статус `failed`
- `items[].stats.stopped_nodes` — число node, у которых последняя попытка имеет статус `stopped`
- `items[].stats.total_attempts` — общее число строк `busybee_pipeline_node_runs` внутри данного run, включая retry
- `total` — число run после применения фильтров; возвращается для единообразия со style остальных list endpoint'ов API

Нюансы:

- `total_nodes` считается по определению pipeline, а не по количеству созданных `node_run`
- `not_started_nodes` показывает ноды, для которых в текущем `pipeline_run` ещё не было ни одной попытки
- `total_attempts` считает все записи `busybee_pipeline_node_runs` внутри данного `pipeline_run`, включая retry
- list endpoint не поддерживает `limit` и `offset`

### `GET /api/pipeline-runs/{runId}`

Возвращает детальный runtime-снимок одного горячего `pipeline_run`.

Path params:

- `runId` — строковый идентификатор запуска pipeline

Правила валидации:

- `runId` не должен быть пустым после trim
- если горячий `pipeline_run` с таким id не найден, API вернёт `not_found`

Ответ включает:

- `run` — метаданные запуска и агрегированную статистику
- `stats` — дублирует агрегированную статистику отдельным блоком для удобства фронта
- `nodes` — все node pipeline в топологическом порядке графа
- `snapshot_at` — момент формирования ответа

Пример ответа:

```json
{
  "run": {
    "id": "pipeline_run_20260325_120100_ab12",
    "pipeline_id": 10,
    "pipeline_name": "daily-import",
    "status": "active",
    "trigger_type": "scheduled",
    "created_at": "2026-03-25T12:01:00Z",
    "finished_at": null,
    "stats": {
      "total_nodes": 4,
      "not_started_nodes": 1,
      "pending_nodes": 1,
      "queued_nodes": 0,
      "running_nodes": 1,
      "success_nodes": 1,
      "failed_nodes": 1,
      "stopped_nodes": 0,
      "total_attempts": 5
    }
  },
  "stats": {
    "total_nodes": 4,
    "not_started_nodes": 1,
    "pending_nodes": 1,
    "queued_nodes": 0,
    "running_nodes": 1,
    "success_nodes": 1,
    "failed_nodes": 1,
    "stopped_nodes": 0,
    "total_attempts": 5
  },
  "nodes": [
    {
      "pipeline_node_id": 100,
      "node_name": "extract",
      "node_description": "extract raw data",
      "command": "python extract.py",
      "executor_type": "local",
      "max_retries": 1,
      "timeout_seconds": 600,
      "meta": {},
      "status": "failed",
      "attempts": [
        {
          "id": 5001,
          "pipeline_node_id": 100,
          "pipeline_run_id": "pipeline_run_20260325_120100_ab12",
          "status": "failed",
          "trigger_type": "scheduled",
          "pid": 3210,
          "exit_code": 1,
          "retry_count": 0,
          "scheduled_at": null,
          "started_at": "2026-03-25T12:01:02Z",
          "finished_at": "2026-03-25T12:01:15Z",
          "error_msg": "exit status 1",
          "log_path": "/home/ubuntu/busybee/logs/run_5001.log",
          "created_at": "2026-03-25T12:01:00Z"
        }
      ]
    },
    {
      "pipeline_node_id": 101,
      "node_name": "transform",
      "node_description": "transform raw data",
      "command": "python transform.py",
      "executor_type": "local",
      "max_retries": 0,
      "timeout_seconds": 600,
      "meta": {},
      "status": "not_started",
      "attempts": []
    }
  ],
  "snapshot_at": "2026-03-25T12:03:15Z"
}
```

Поля ответа:

- `run` — тот же объект, что и элемент `items[]` в списке run
- `stats` — дублирует `run.stats`; блок оставлен отдельно, чтобы фронту не приходилось читать его из вложенного `run`
- `nodes[]` — ноды pipeline в топологическом порядке DAG
- `nodes[].pipeline_node_id` — id node
- `nodes[].node_name` — имя node
- `nodes[].node_description` — описание node
- `nodes[].command` — команда node definition
- `nodes[].executor_type` — тип executor из definition
- `nodes[].max_retries` — максимальное число retry для node definition
- `nodes[].timeout_seconds` — таймаут node definition
- `nodes[].meta` — JSON meta node definition
- `nodes[].status` — вычисленный статус node внутри данного run
- `nodes[].attempts` — все попытки `node_run` для этой node внутри выбранного `pipeline_run`
- `nodes[].attempts[].id` — id конкретной попытки `node_run`
- `nodes[].attempts[].pipeline_node_id` — id node, к которой относится попытка
- `nodes[].attempts[].pipeline_run_id` — id родительского `pipeline_run`
- `nodes[].attempts[].status` — статус попытки
- `nodes[].attempts[].trigger_type` — причина создания попытки
- `nodes[].attempts[].pid` — PID процесса или `null`
- `nodes[].attempts[].exit_code` — код завершения или `null`
- `nodes[].attempts[].retry_count` — номер retry-счётчика этой попытки
- `nodes[].attempts[].scheduled_at` — время отложенного запуска/постановки в очередь или `null`
- `nodes[].attempts[].started_at` — фактическое время старта или `null`
- `nodes[].attempts[].finished_at` — фактическое время завершения или `null`
- `nodes[].attempts[].error_msg` — текст ошибки или `null`
- `nodes[].attempts[].log_path` — абсолютный путь к лог-файлу или `null`
- `nodes[].attempts[].created_at` — время создания записи `node_run`
- `snapshot_at` — время формирования ответа API

Правила вычисления:

- `nodes` всегда строятся по текущему graph pipeline, а не по порядку `node_run` в БД
- `nodes[].status` берётся из последней попытки этой node внутри текущего run
- если попыток у node нет, `nodes[].status = "not_started"`
- `nodes[].attempts` сортируются по `created_at ASC`, затем по `id ASC`

Нюансы:

- `nodes` включают все node текущего pipeline, даже если для части из них `node_run` ещё не создавался
- если у node нет попыток в рамках run, её `status` будет `not_started`
- `attempts` отсортированы по времени создания попытки внутри node
- `status` на уровне node вычисляется по последней попытке этой node внутри текущего run

### `GET /api/pipeline-node-runs/{nodeRunId}/log`

Возвращает лог конкретного горячего `node_run` как `text/plain`.

Path params:

- `nodeRunId` — положительное целое id `pipeline_node_run`

Правила валидации:

- `nodeRunId` должен быть положительным целым числом
- если запись `pipeline_node_run` не существует, API вернёт `not_found`
- если `log_path` ещё не заполнен или лог для `node_run` пока недоступен, API вернёт `conflict`
- если `log_path` есть, но файла на диске уже нет, API вернёт `not_found`

Успешный ответ:

- `200 OK`
- `Content-Type: text/plain; charset=utf-8`

Пример:

```text
GET /api/pipeline-node-runs/5001/log
```

Если лог ещё не появился или `log_path` не записан, API вернёт `conflict`.

Если запись `node_run` отсутствует или файл лога больше недоступен на диске, API вернёт `not_found`.

API не оборачивает содержимое лога в JSON: тело ответа содержит сырой текст файла целиком.

## Error Format

Ошибки возвращаются в общем формате:

```json
{
  "error": {
    "code": "validation_error",
    "message": "name is required"
  }
}
```

Типовые коды:

- `validation_error`
- `unauthorized`
- `not_found`
- `conflict`
- `method_not_allowed`
- `internal_error`

