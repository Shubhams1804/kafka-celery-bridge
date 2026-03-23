"""
Advanced example: Bridge with Redis state reads, priority routing,
and countdown tasks — the pattern used in production ML/feed pipelines.

This mirrors a real-world pattern where:
- Kafka events trigger ML pipeline stages
- Redis stores per-user state (tier, preferences)
- Celery tasks are prioritized by user segment

Run with:
    KAFKA_BROKERS=kafka:9092 \
    CELERY_BROKER_URL=redis://localhost:6379/0 \
    REDIS_HOST=localhost \
    python advanced_with_redis.py
"""

import os
import logging
import redis

from kafka_celery_bridge import KafkaCeleryBridge

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

# Redis client for reading per-user state
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
)

bridge = KafkaCeleryBridge(
    kafka_brokers=os.getenv("KAFKA_BROKERS", "kafka:9092"),
    celery_broker_url=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    consumer_group="ml-pipeline-bridge",
    default_queue="ml_tasks",
)


def get_user_tier(user_id: str) -> str:
    """Read user tier from Redis, defaulting to 'standard'."""
    return r.get(f"user:{user_id}:tier") or "standard"


@bridge.on("user.login")
def handle_login(event):
    """
    On login, update activity tracking and trigger personalization warmup.
    Tier determines Celery priority — power users get priority 9, standard get 5.
    """
    user_id = event.get("user_id")
    if not user_id:
        return False

    tier = get_user_tier(user_id)
    priority = 9 if tier == "power_user" else 5

    # Track last active
    r.setex(f"user:{user_id}:last_active", 86400, event.get("timestamp", ""))

    # Trigger warmup with tier-based priority
    return bridge.send_task(
        "ml.tasks.warm_recommendations",
        args=[user_id, tier],
        priority=priority,
        queue="ml_tasks_high" if tier == "power_user" else "ml_tasks",
    )


@bridge.on("data.invalidated")
def handle_invalidation(event):
    """
    On data invalidation, trigger re-computation with a 1s delay to
    allow upstream writes to propagate before the task reads.
    """
    entity_id = event.get("entity_id")
    reason = event.get("reason", "unknown")
    if not entity_id:
        return False

    return bridge.send_task(
        "ml.tasks.recompute",
        args=[entity_id, reason],
        priority=7,
        countdown=1,  # Small delay to let upstream writes settle
    )


@bridge.on("model.retrained")
def handle_model_retrain(event):
    """Invalidate all cached predictions when the model is retrained."""
    model_id = event.get("model_id")
    version = event.get("version")
    if not model_id:
        return False

    success = bridge.send_task(
        "ml.tasks.invalidate_predictions",
        args=[model_id, version],
        priority=6,
        countdown=5,  # Wait 5s for model artefact to be fully persisted
    )

    if success:
        logging.getLogger(__name__).info(
            f"Prediction invalidation queued for model={model_id} v={version}"
        )
    return success


if __name__ == "__main__":
    bridge.run()
