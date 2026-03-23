# kafka-celery-bridge

> Bridge Kafka events to Celery tasks — without the boilerplate.

[![PyPI version](https://badge.fury.io/py/kafka-celery-bridge.svg)](https://badge.fury.io/py/kafka-celery-bridge)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## The Problem

If you use **Kafka** and **Celery** together in a Python microservices stack, you've written this glue code before:

- A Kafka consumer that polls messages
- Manual offset commits (because auto-commit loses messages on crashes)
- Routing each topic to the right handler
- Dispatching Celery tasks from those handlers
- Graceful shutdown on SIGINT/SIGTERM
- Health metrics

Every team writes this from scratch. This package is that missing layer.

---

## Installation

```bash
pip install kafka-celery-bridge
```

---

## Quickstart

```python
from kafka_celery_bridge import KafkaCeleryBridge

bridge = KafkaCeleryBridge(
    kafka_brokers="kafka:9092",
    celery_broker_url="redis://localhost:6379/0",
    consumer_group="my-service-group",
)

@bridge.on("user.created")
def handle_user_created(event):
    bridge.send_task(
        "myapp.tasks.send_welcome_email",
        args=[event["user_id"]],
        priority=8,
    )
    return True

@bridge.on("order.placed")
def handle_order_placed(event):
    bridge.send_task(
        "myapp.tasks.process_order",
        args=[event["order_id"]],
        countdown=2,  # 2s delay — let DB write propagate first
    )
    return True

bridge.run()
```

That's it. No boilerplate.

---

## Features

| Feature | Detail |
|---|---|
| **Decorator-based routing** | `@bridge.on("topic.name")` wires topics to handlers |
| **Manual offset commit** | Offsets committed after each batch, not per-message — no lost events on crash |
| **Graceful shutdown** | SIGINT / SIGTERM handled cleanly with final offset commit |
| **Priority tasks** | `priority=0–9` passed directly to Celery |
| **Countdown tasks** | `countdown=N` delays task execution by N seconds |
| **Queue routing** | Per-task `queue` override or global `default_queue` |
| **Built-in metrics** | Events processed, failed, last event time — logged every N events |
| **Zero magic** | No metaclasses, no hidden globals — just a class you instantiate |

---

## Configuration

```python
bridge = KafkaCeleryBridge(
    # Required
    kafka_brokers="kafka:9092",           # Kafka broker address(es)
    celery_broker_url="redis://...",      # Celery broker URL
    consumer_group="my-group",            # Kafka consumer group ID

    # Optional
    auto_offset_reset="latest",           # "latest" or "earliest"
    max_poll_records=10,                  # Max messages per poll batch
    session_timeout_ms=30000,             # Kafka session timeout
    heartbeat_interval_ms=10000,          # Kafka heartbeat interval
    default_queue="celery",               # Default Celery queue name
    metrics_log_interval=100,             # Log metrics every N events
)
```

---

## Sending Tasks

```python
bridge.send_task(
    "myapp.tasks.do_work",    # Celery task name (dotted path)
    args=["arg1", "arg2"],    # Positional args
    kwargs={"key": "value"},  # Keyword args
    priority=7,               # 0 (low) – 9 (high), default: 5
    queue="high_priority",    # Override default queue
    countdown=3,              # Delay N seconds before execution
)
```

---

## Advanced: Reading State from Redis

A common pattern in ML/data pipelines — read per-user state from Redis before dispatching:

```python
import redis

r = redis.Redis(host="localhost", decode_responses=True)

@bridge.on("user.login")
def handle_login(event):
    user_id = event["user_id"]
    tier = r.get(f"user:{user_id}:tier") or "standard"

    # Power users get higher priority
    priority = 9 if tier == "power_user" else 5

    return bridge.send_task(
        "ml.tasks.warm_recommendations",
        args=[user_id, tier],
        priority=priority,
    )
```

---

## Programmatic Registration (without decorator)

```python
def my_handler(event):
    ...
    return True

bridge.register("some.topic", my_handler)
```

---

## Running in Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY bridge.py .
CMD ["python", "bridge.py"]
```

```yaml
# docker-compose.yml
services:
  kafka-celery-bridge:
    build: .
    environment:
      KAFKA_BROKERS: kafka:9092
      CELERY_BROKER_URL: redis://redis:6379/0
    restart: unless-stopped
    depends_on:
      - kafka
      - redis
```

---

## Error Handling

Handlers should return `True` on success and `False` on failure. The bridge will:
- Increment the `events_failed` metric counter
- Log a warning with elapsed time
- **Still commit the offset** (to avoid infinite retry loops on bad messages)

For proper dead-letter queue (DLQ) handling, catch exceptions inside your handler and route to a DLQ topic or a dedicated Celery error task:

```python
@bridge.on("payment.initiated")
def handle_payment(event):
    try:
        return bridge.send_task("tasks.process_payment", args=[event["payment_id"]])
    except Exception as e:
        # Route to DLQ
        bridge.send_task("tasks.dlq_handler", args=[event, str(e)])
        return False
```

---

## Real-World Architecture

```
┌─────────────┐     Kafka      ┌──────────────────────┐     Celery     ┌──────────────┐
│  Service A  │ ─────────────► │  kafka-celery-bridge │ ─────────────► │   Worker 1   │
│  Service B  │ ─────────────► │                      │ ─────────────► │   Worker 2   │
│  Service C  │ ─────────────► │  @bridge.on(topic)   │ ─────────────► │   Worker N   │
└─────────────┘                └──────────────────────┘                └──────────────┘
                                    Manual offset
                                    Graceful shutdown
                                    Metrics + DLQ
```

---

## Contributing

PRs welcome! Open an issue first for large changes.

```bash
git clone https://github.com/YOUR_USERNAME/kafka-celery-bridge
cd kafka-celery-bridge
pip install -e ".[dev]"
pytest tests/
```

---

## License

MIT — see [LICENSE](LICENSE).
