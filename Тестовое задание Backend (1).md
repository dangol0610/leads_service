# Тестовое ТЗ: leads-service

Нужно реализовать небольшой FastAPI-сервис для обработки заявок. Сервис должен сохранять заявку, публиковать событие в Kafka через outbox и обрабатывать входящее Kafka-событие от условного `moderation-service`.

## Функциональность

### 1. POST /leads

Принимает:

```json
{
  "name": "Иван",
  "phone": "+79991234567",
  "source": "landing",
  "comment": "Хочу консультацию"
}
```

Что должен сделать сервис:

- сохранить заявку в PostgreSQL со статусом `new`;
- в той же транзакции записать событие в таблицу `outbox`;
- событие: `lead_created.v1`;
- вернуть созданную заявку.

### 2. GET /leads/{lead_id}

Возвращает заявку по `id`.

Если заявки нет, вернуть структурированную ошибку:

```json
{
  "error": {
    "code": "lead_not_found",
    "message": "Заявка не найдена",
    "correlation_id": "..."
  }
}
```

### 3. Outbox publisher

Отдельный worker должен:

- читать неопубликованные события из `outbox`;
- публиковать их в Kafka topic `leads.events.v1`;
- помечать событие как опубликованное только после успешной отправки.

Пример payload:

```json
{
  "event_id": "uuid",
  "event_type": "lead_created.v1",
  "aggregate_id": "lead_id",
  "occurred_at": "2026-06-26T10:00:00Z",
  "payload": {
    "lead_id": "uuid",
    "name": "Иван",
    "phone": "+79991234567",
    "source": "landing"
  }
}
```

### 4. Kafka consumer

Отдельный worker должен слушать topic `lead_moderation.events.v1`.

Входящее событие:

```json
{
  "event_id": "uuid",
  "event_type": "lead_moderation_finished.v1",
  "aggregate_id": "lead_id",
  "occurred_at": "2026-06-26T10:05:00Z",
  "payload": {
    "lead_id": "uuid",
    "approved": true,
    "reason": null
  }
}
```

Что должен сделать consumer:

- распарсить событие;
- проверить идемпотентность по `event_id`;
- сохранить входящее событие в `inbound_events`;
- обновить статус заявки:
  - `approved=true` -> `approved`;
  - `approved=false` -> `rejected`;
- повторная обработка события с тем же `event_id` не должна менять данные повторно и не должна падать.

## Технические требования

- Python 3.12+.
- FastAPI.
- PostgreSQL.
- SQLAlchemy async / asyncpg.
- Alembic migration.
- Kafka через `aiokafka`.

## Локальный запуск

Нужно приложить `README.md` с командами:

- как поднять PostgreSQL и Kafka/Redpanda через Docker Compose;
- как применить миграции;
- как запустить API;
- как запустить outbox publisher;
- как запустить consumer;
- как вручную отправить тестовое moderation-событие в Kafka.

Redpanda вместо Kafka допустима, если используется Kafka-compatible protocol.

## Что не нужно делать

- Не нужно Redis.
- Не нужно RabbitMQ.
- Не нужно Kubernetes/Helm/CI/CD.
- Не нужно авторизацию.
- Не нужно frontend.

## Что сдавать

- Ссылка на Git-репозиторий.
- `README.md` с запуском и env-переменными.
- Короткий раздел: что сделано, что бы улучшил дальше.
- Пример JSON-события для ручной отправки в Kafka.


