# PayFlow

![Python](https://img.shields.io/badge/Python-3.14-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-async-009688)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-336791)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-event--driven-231F20)
![Redis](https://img.shields.io/badge/Redis-cache-DC382D)
![Tests](https://img.shields.io/badge/tests-108%20passed-brightgreen)

PayFlow — демонстрационная платформа обработки платежей на микросервисной архитектуре.

Система принимает платёж, обращается к внешнему провайдеру, сохраняет результат и передаёт событие в отдельный сервис аналитики через Kafka.

Основной стек: **Python, FastAPI, PostgreSQL, SQLAlchemy, Alembic, Kafka, Redis, Docker Compose**.

## Возможности

- идемпотентное создание платежей;
- интеграция с внешним платёжным провайдером;
- Retry с exponential backoff и jitter;
- Circuit Breaker;
- Transactional Outbox;
- Kafka producer и consumer с ручным commit offset;
- идемпотентная обработка событий;
- Dead Letter Queue для невалидных сообщений;
- Redis cache-aside для аналитических агрегатов;
- liveness/readiness проверки и graceful shutdown;
- JSON-логи с `request_id` и `event_id`;
- автоматические тесты основных сценариев.

## Архитектура

```text
                         ┌──────────────┐
                         │    Клиент    │
                         └──────┬───────┘
                                │ HTTP
                                ▼
                    ┌───────────────────────┐
                    │    Payment Service    │
                    │                       │
                    │ FastAPI               │
                    │ Retry                 │
                    │ Circuit Breaker       │
                    └───────────┬───────────┘
                                │
                         одна транзакция
                                │
                    ┌───────────▼───────────┐
                    │  Payment PostgreSQL   │
                    │                       │
                    │ payments              │
                    │ outbox_events         │
                    └───────────┬───────────┘
                                │ Outbox Relay
                                ▼
                    ┌───────────────────────┐
                    │     Apache Kafka      │
                    │   topic: payments     │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   Analytics Service   │
                    │                       │
                    │ Idempotent Consumer   │
                    │ Analytics API         │
                    └────────┬────────┬─────┘
                             │        │
                             ▼        ▼
                  ┌──────────────┐  ┌─────────┐
                  │ Analytics DB │  │  Redis  │
                  │              │  │  Cache  │
                  │ transactions │  └─────────┘
                  │ processed_   │
                  │ events       │
                  └──────────────┘
```

У каждого сервиса своя PostgreSQL. Payment Service остаётся источником истины по состоянию платежа. Analytics Service хранит отдельную read-модель и обновляет её асинхронно.

## Сервисы

### Payment Service

Отвечает за обработку платежей:

- принимает запросы от клиента;
- защищает от повторного создания платежа через `idempotency_key`;
- вызывает внешний платёжный провайдер;
- сохраняет платёж и Outbox-событие одной транзакцией;
- публикует события в Kafka через Outbox Relay.

### Analytics Service

Отвечает за аналитику:

- получает события из Kafka;
- проверяет структуру сообщений;
- отбрасывает дубликаты по `event_id`;
- обновляет собственную базу;
- отправляет невалидные сообщения в DLQ;
- отдаёт список транзакций и агрегированную статистику;
- кеширует summary в Redis.

## Поток обработки платежа

```text
POST /api/v1/payments
        │
        ▼
Проверка idempotency_key
        │
        ▼
Вызов платёжного провайдера
        │
        ▼
Payment + OutboxEvent
в одной транзакции PostgreSQL
        │
        ▼
Outbox Relay
        │
        ▼
Kafka
        │
        ▼
Analytics Consumer
        │
        ├─ валидация события
        ├─ проверка event_id
        ├─ обновление transactions
        ├─ запись processed_events
        ├─ commit PostgreSQL
        ├─ инвалидация Redis
        └─ commit Kafka offset
```

## Надёжность

| Механизм | Задача |
|---|---|
| Transactional Outbox | Не потерять событие между PostgreSQL и Kafka |
| At-least-once + Idempotent Consumer | Безопасно обработать повторную доставку |
| Manual offset commit | Не подтвердить событие до сохранения в БД |
| Dead Letter Queue | Не блокировать partition невалидным сообщением |
| Retry + Circuit Breaker | Переживать временные сбои провайдера |
| Redis graceful degradation | Продолжать работу аналитики без кеша |
| Eventual consistency | Не связывать доступность двух сервисов |

## API

### Payment Service — `http://localhost:8000`

| Метод | Endpoint | Описание |
|---|---|---|
| `POST` | `/api/v1/payments` | Создать и обработать платёж |
| `GET` | `/api/v1/payments/{payment_id}` | Получить платёж |
| `GET` | `/health/live` | Проверка процесса |
| `GET` | `/health/ready` | Проверка готовности сервиса |

### Analytics Service — `http://localhost:8002`

| Метод | Endpoint | Описание |
|---|---|---|
| `GET` | `/api/v1/analytics/summary` | Получить агрегированную статистику |
| `GET` | `/api/v1/analytics/transactions` | Получить список транзакций |
| `GET` | `/api/v1/analytics/transactions/{payment_id}` | Получить транзакцию по `payment_id` |
| `GET` | `/health/live` | Проверка процесса |
| `GET` | `/health/ready` | Проверка готовности и degraded-состояния |

## Запуск

### Требования

- Docker
- Docker Compose

### Подготовка окружения

```bash
cp .env.example .env
```

### Запуск

```bash
docker compose up --build
```

### Проверка

```bash
curl http://localhost:8000/health/ready
curl http://localhost:8002/health/ready
```

Остановка:

```bash
docker compose down
```

## Структура проекта

```text
payflow/
├── payment-service/
│   ├── app/
│   │   ├── api/
│   │   ├── clients/
│   │   ├── core/
│   │   ├── integrations/
│   │   ├── models/
│   │   ├── repositories/
│   │   ├── schemas/
│   │   └── services/
│   ├── migrations/
│   └── tests/
│
├── analytics-service/
│   ├── app/
│   │   ├── api/
│   │   ├── consumer/
│   │   ├── core/
│   │   ├── models/
│   │   ├── repositories/
│   │   ├── schemas/
│   │   └── services/
│   ├── migrations/
│   └── tests/
│
├── mock-provider/
├── docker-compose.yml
└── README.md
```

## Ключевые решения

### Почему Kafka, а не прямой HTTP

Payment Service не должен зависеть от доступности Analytics Service. Если аналитика временно недоступна, платежи продолжают обрабатываться, а события остаются в Kafka.

### Почему у сервисов разные базы

Каждый сервис владеет своими данными. Analytics Service не читает таблицы Payment Service напрямую, а строит собственную проекцию из событий.

### Почему используется Transactional Outbox

PostgreSQL и Kafka нельзя обновить одной обычной транзакцией. Поэтому платёж и намерение отправить событие сначала сохраняются вместе в PostgreSQL, а публикация выполняется отдельно.

### Почему offset коммитится вручную

Offset фиксируется только после успешного commit в Analytics PostgreSQL. При сбое событие придёт повторно и будет безопасно обработано за счёт идемпотентности.

### Почему Redis не является обязательной зависимостью

Источник данных для аналитики — PostgreSQL. Redis только ускоряет агрегированные запросы. При его падении API продолжает работать напрямую с базой.
