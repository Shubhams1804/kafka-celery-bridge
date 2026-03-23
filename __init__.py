"""
kafka-celery-bridge
~~~~~~~~~~~~~~~~~~~

Bridge Kafka events to Celery tasks — without the boilerplate.

Quickstart::

    from kafka_celery_bridge import KafkaCeleryBridge

    bridge = KafkaCeleryBridge(
        kafka_brokers="kafka:9092",
        celery_broker_url="redis://localhost:6379/0",
        consumer_group="my-service-group",
    )

    @bridge.on("user.created")
    def handle_user_created(event):
        bridge.send_task("tasks.send_welcome_email", args=[event["user_id"]])
        return True

    bridge.run()
"""

from .bridge import KafkaCeleryBridge

__all__ = ["KafkaCeleryBridge"]
__version__ = "0.1.0"
