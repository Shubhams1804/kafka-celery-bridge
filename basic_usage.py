"""
Basic example: Bridge two Kafka topics to Celery tasks.

Run with:
    KAFKA_BROKERS=kafka:9092 CELERY_BROKER_URL=redis://localhost:6379/0 python basic_usage.py
"""

import os
import logging

from kafka_celery_bridge import KafkaCeleryBridge

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

bridge = KafkaCeleryBridge(
    kafka_brokers=os.getenv("KAFKA_BROKERS", "kafka:9092"),
    celery_broker_url=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    consumer_group="my-app-bridge",
    default_queue="my_queue",
)


@bridge.on("user.created")
def handle_user_created(event):
    """Send welcome email when a new user is created."""
    user_id = event.get("user_id")
    if not user_id:
        return False

    return bridge.send_task(
        "myapp.tasks.send_welcome_email",
        args=[user_id],
        priority=8,
    )


@bridge.on("order.placed")
def handle_order_placed(event):
    """Process order with a short delay to allow DB write to propagate."""
    order_id = event.get("order_id")
    if not order_id:
        return False

    return bridge.send_task(
        "myapp.tasks.process_order",
        args=[order_id],
        priority=7,
        countdown=2,  # 2s delay — let DB write settle first
    )


if __name__ == "__main__":
    bridge.run()
