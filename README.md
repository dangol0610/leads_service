# Leads Service

FastAPI-сервис для обработки заявок с outbox-паттерном и Kafka.

## Архитектура

```
POST /leads → API → PostgreSQL (leads + outbox)
                       ↓
              Outbox Publisher → Kafka (leads.events.v1)
                                    ↓
              Kafka Consumer ← lead_moderation.events.v1
                       ↓
                  PostgreSQL (inbound_events + status)
```

- **Domain**: `Lead`, `OutboxEvent`, `InboundEvent` — чистые датаклассы
- **App**: сервисы через протоколы (`UnitOfWork`, `MessageProducer`, `MessageConsumer`)
- **API**: FastAPI роуты, Pydantic схемы
- **Infrastructure**: SQLAlchemy async (asyncpg), aiokafka producer/consumer
- **Workers**: outbox publisher + moderation consumer — отдельные процессы

## Структура проекта

```
leads_service/
├── src/
│   ├── domain/                  # Бизнес-логика (чистые датаклассы)
│   │   ├── lead.py              #   Lead, LeadStatus
│   │   └── events.py            #   OutboxEvent, InboundEvent
│   │
│   ├── app/                     # Use cases / сервисы
│   │   ├── interfaces.py        #   Protocol'ы (порты)
│   │   ├── commands.py          #   CQRS команды и запросы
│   │   ├── exceptions.py        #   Доменные исключения
│   │   └── services.py          #   Сервисы (внешних зависимостей не знают)
│   │
│   ├── api/                     # Inbound adapter — HTTP (FastAPI)
│   │   ├── routes/leads.py     #   Эндпоинты
│   │   ├── schemas/             #   Pydantic схемы
│   │   ├── dependencies.py      #   DI
│   │   └── exception_handlers.py
│   │
│   ├── infrastructure/          # Outbound adapters
│   │   ├── database/            #   SQLAlchemy async (asyncpg)
│   │   │   ├── config.py        #     Engine, session factory
│   │   │   ├── models.py        #     ORM модели
│   │   │   ├── repositories.py  #     Репозитории
│   │   │   └── uow.py           #     Unit of Work
│   │   └── kafka/               #   aiokafka
│   │       ├── producer.py
│   │       └── consumer.py
│   │
│   ├── workers/                 # Фоновые процессы
│   │   ├── outbox_publisher.py  #   Outbox publisher
│   │   └── consumer.py          #   Moderation consumer
│   │
│   ├── core/
│   │   └── config.py            # Конфигурация (pydantic-settings)
│   ├── migrations/              # Alembic
│   └── main.py                  # Composition root — FastAPI app
│
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

## Быстрый старт

### Требования

- Docker
- Docker Compose

### Запуск

```bash
docker compose up --build
```

Поднимутся 4 сервиса:

- `postgres` — PostgreSQL 16
- `redpanda` — Kafka-compatible брокер
- `leads-api` — FastAPI на `:8000`
- `leads-publisher` — читает outbox и публикует в Kafka
- `leads-consumer` — слушает moderation-события и обновляет статус

Миграции применяются автоматически при старте `leads-api`.

## API

### POST /leads

```bash
curl -X POST http://localhost:8000/leads \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Иван",
    "phone": "+79991234567",
    "source": "landing",
    "comment": "Хочу консультацию"
  }'
```

Ответ: `201 Created`

```json
{
    "id": "6298f5fe-8057-4d0c-bbb0-87919b6ee838",
    "name": "Иван",
    "phone": "+79991234567",
    "source": "landing",
    "comment": "Хочу консультацию",
    "status": "new",
    "created_at": "2026-07-15T18:22:49.607920Z",
    "updated_at": "2026-07-15T18:22:49.607920Z"
}
```

### GET /leads/{id}

```bash
curl http://localhost:8000/leads/6298f5fe-8057-4d0c-bbb0-87919b6ee838
```

Если заявки нет — `404`:

```json
{
    "error": {
        "code": "lead_not_found",
        "message": "Заявка не найдена",
        "correlation_id": "uuid"
    }
}
```

## Kafka

### Топики

| Топик                       | Тип     | Формат                        |
| --------------------------- | ------- | ----------------------------- |
| `leads.events.v1`           | outbox  | `lead_created.v1`             |
| `lead_moderation.events.v1` | inbound | `lead_moderation_finished.v1` |

### Отправка тестового moderation-события

```bash
docker compose exec redpanda sh -c 'echo '\''{"event_id":"550e8400-e29b-41d4-a716-446655440000","event_type":"lead_moderation_finished.v1","aggregate_id":"6298f5fe-8057-4d0c-bbb0-87919b6ee838","occurred_at":"2026-07-15T17:50:00Z","payload":{"lead_id":"6298f5fe-8057-4d0c-bbb0-87919b6ee838","approved":true,"reason":null}}'\'' | rpk topic produce lead_moderation.events.v1 --key="6298f5fe-8057-4d0c-bbb0-87919b6ee838"'
```

После обработки статус заявки изменится на `approved` или `rejected`. Повторная отправка с тем же `event_id` игнорируется (идемпотентность).

### Пример события для ручной отправки

```json
{
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "event_type": "lead_moderation_finished.v1",
    "aggregate_id": "6298f5fe-8057-4d0c-bbb0-87919b6ee838",
    "occurred_at": "2026-07-15T18:23:00Z",
    "payload": {
        "lead_id": "6298f5fe-8057-4d0c-bbb0-87919b6ee838",
        "approved": true,
        "reason": null
    }
}
```

## Переменные окружения

| Переменная                | По умолчанию                | Описание                                       |
| ------------------------- | --------------------------- | ---------------------------------------------- |
| `POSTGRES_USER`           | `postgres`                  | Пользователь БД                                |
| `POSTGRES_PASSWORD`       | `postgres`                  | Пароль БД                                      |
| `POSTGRES_DB`             | `leads_service`             | Название БД                                    |
| `POSTGRES_HOST`           | `localhost`                 | Хост БД                                        |
| `POSTGRES_PORT`           | `5432`                      | Порт БД                                        |
| `DB_POOL_SIZE`            | `20`                        | Размер пула соединений                         |
| `DB_MAX_OVERFLOW`         | `40`                        | Макс. превышение пула                          |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`            | Адрес Kafka/Redpanda                           |
| `KAFKA_OUTBOX_TOPIC`      | `leads.events.v1`           | Топик для outbox                               |
| `KAFKA_CONSUMER_TOPIC`    | `lead_moderation.events.v1` | Топик для consumer                             |
| `KAFKA_CONSUMER_GROUP_ID` | `leads-moderation-consumer` | Группа consumer'а                              |
| `KAFKA_POLL_INTERVAL`     | `5`                         | Пауза publisher'а при отсутствии событий (сек) |
| `KAFKA_POLL_TIMEOUT_MS`   | `1000`                      | Таймаут poll consumer'а (мс)                   |

## Запуск без Docker

```bash
# 1. Виртуальное окружение
uv sync

# 2. PostgreSQL и Redpanda
docker compose up -d postgres redpanda

# 3. Миграции
alembic upgrade head

# 4. API
uvicorn src.main:app --reload

# 5. Outbox publisher (отдельный терминал)
python -m src.workers.outbox_publisher

# 6. Consumer (отдельный терминал)
python -m src.workers.consumer
```

## Логирование

По умолчанию уровень — `INFO`. Для отладки:

```bash
LOGURU_LEVEL=DEBUG docker compose up
```

## Что сделано

- `POST /leads` — создание заявки, запись в outbox в одной транзакции
- `GET /leads/{id}` — получение заявки, структурированная ошибка 404
- Outbox publisher — читает `outbox`, публикует в Kafka, помечает опубликованным
- Kafka consumer — идемпотентная обработка moderation-событий
- Чистая архитектура (domain → app → infrastructure → api/workers)
- Docker Compose: PostgreSQL + Redpanda + API + workers

## Что можно улучшить

- **Health checks** — эндпоинт `/health` для мониторинга
- **Retry логика** — повтор при временных ошибках Kafka
- **Graceful shutdown воркеров** — более надежная обработка сигналов
- **Prometheus метрики** — количество обработанных событий, ошибок
- **Тесты** — unit + integration тесты
- **Миграции в CI** — проверка при деплое
- **Rate limiting** на POST /leads
- **Tracing** — OpenTelemetry для end-to-end трассировки
