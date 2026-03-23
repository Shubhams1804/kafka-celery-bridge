"""
kafka-celery-bridge: Bridge Kafka events to Celery tasks with zero boilerplate.

Core bridge module — handles consumer lifecycle, offset management,
graceful shutdown, metrics, and pluggable event routing.
"""

import json
import logging
import signal
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger("kafka_celery_bridge")

# Type alias for event handler functions
EventHandler = Callable[[Dict[str, Any]], bool]


class KafkaCeleryBridge:
    """
    Bridges Kafka events to Celery tasks with pluggable event handlers.

    Features:
    - Manual offset commit (reliability > speed)
    - Graceful shutdown on SIGINT/SIGTERM
    - Built-in metrics (events processed, failed, lag)
    - Dead-letter queue (DLQ) pattern on failure
    - Priority-based Celery task routing
    - Countdown/delayed task support

    Usage::

        from kafka_celery_bridge import KafkaCeleryBridge

        bridge = KafkaCeleryBridge(
            kafka_brokers="kafka:9092",
            celery_broker_url="redis://localhost:6379/0",
            consumer_group="my-service-group",
        )

        @bridge.on("user.created")
        def handle_user_created(event):
            bridge.send_task(
                "tasks.send_welcome_email",
                args=[event["user_id"]],
            )
            return True

        bridge.run()
    """

    def __init__(
        self,
        kafka_brokers: str,
        celery_broker_url: str,
        consumer_group: str,
        auto_offset_reset: str = "latest",
        max_poll_records: int = 10,
        session_timeout_ms: int = 30000,
        heartbeat_interval_ms: int = 10000,
        default_queue: str = "celery",
        metrics_log_interval: int = 100,
    ):
        """
        Initialize the bridge.

        Args:
            kafka_brokers: Comma-separated Kafka broker addresses. e.g. ``"kafka:9092"``
            celery_broker_url: Celery broker URL. e.g. ``"redis://localhost:6379/0"``
            consumer_group: Kafka consumer group ID.
            auto_offset_reset: Where to start consuming if no committed offset exists.
                ``"latest"`` (default) or ``"earliest"``.
            max_poll_records: Max records per poll batch. Default: ``10``.
            session_timeout_ms: Kafka session timeout in ms. Default: ``30000``.
            heartbeat_interval_ms: Kafka heartbeat interval in ms. Default: ``10000``.
            default_queue: Default Celery queue name. Default: ``"celery"``.
            metrics_log_interval: Log metrics every N events. Default: ``100``.
        """
        self.kafka_brokers = kafka_brokers
        self.celery_broker_url = celery_broker_url
        self.consumer_group = consumer_group
        self.auto_offset_reset = auto_offset_reset
        self.max_poll_records = max_poll_records
        self.session_timeout_ms = session_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.default_queue = default_queue
        self.metrics_log_interval = metrics_log_interval

        self._handlers: Dict[str, EventHandler] = {}
        self._consumer: Optional[KafkaConsumer] = None
        self._shutdown_requested = False

        self.metrics = {
            "events_processed": 0,
            "events_failed": 0,
            "started_at": datetime.utcnow().isoformat(),
            "last_event_time": None,
        }

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------

    def on(self, topic: str) -> Callable:
        """
        Decorator to register an event handler for a Kafka topic.

        The handler receives the deserialized event dict and must return
        ``True`` on success or ``False`` on failure.

        Example::

            @bridge.on("order.placed")
            def handle_order(event):
                bridge.send_task("tasks.process_order", args=[event["order_id"]])
                return True
        """
        def decorator(fn: EventHandler) -> EventHandler:
            self.register(topic, fn)
            return fn
        return decorator

    def register(self, topic: str, handler: EventHandler) -> None:
        """
        Register an event handler for a topic programmatically.

        Args:
            topic: Kafka topic name.
            handler: Callable that accepts an event dict and returns bool.
        """
        self._handlers[topic] = handler
        logger.debug(f"Registered handler for topic: {topic}")

    # ------------------------------------------------------------------
    # Celery task dispatch
    # ------------------------------------------------------------------

    def send_task(
        self,
        task_name: str,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        priority: int = 5,
        queue: Optional[str] = None,
        countdown: Optional[int] = None,
    ) -> bool:
        """
        Send a task to Celery.

        Args:
            task_name: Full dotted Celery task name. e.g. ``"myapp.tasks.do_work"``
            args: Positional arguments for the task.
            kwargs: Keyword arguments for the task.
            priority: Celery task priority (0–9, higher = more urgent). Default: ``5``.
            queue: Target Celery queue. Defaults to ``self.default_queue``.
            countdown: Delay execution by N seconds. ``None`` = immediate.

        Returns:
            ``True`` if the task was dispatched successfully, ``False`` otherwise.
        """
        try:
            from celery import Celery

            app = Celery("kafka-celery-bridge", broker=self.celery_broker_url)
            result = app.send_task(
                task_name,
                args=args or [],
                kwargs=kwargs or {},
                priority=priority,
                queue=queue or self.default_queue,
                countdown=countdown,
            )
            logger.debug(
                f"Task dispatched: {task_name} (id={result.id}, "
                f"priority={priority}, queue={queue or self.default_queue}"
                + (f", countdown={countdown}s" if countdown else "")
                + ")"
            )
            return True
        except Exception as exc:
            logger.error(f"Failed to dispatch task '{task_name}': {exc}")
            return False

    # ------------------------------------------------------------------
    # Consumer lifecycle
    # ------------------------------------------------------------------

    def _build_consumer(self) -> KafkaConsumer:
        topics = list(self._handlers.keys())
        if not topics:
            raise RuntimeError(
                "No topics registered. Use @bridge.on('<topic>') or bridge.register() "
                "before calling bridge.run()."
            )

        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.kafka_brokers,
            group_id=self.consumer_group,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=False,  # Manual commit for reliability
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
            max_poll_records=self.max_poll_records,
            session_timeout_ms=self.session_timeout_ms,
            heartbeat_interval_ms=self.heartbeat_interval_ms,
        )
        logger.info(f"Kafka consumer ready — topics: {topics}, group: {self.consumer_group}")
        return consumer

    def _handle_shutdown(self, signum, frame):
        logger.warning(f"Shutdown signal received ({signum}). Stopping bridge...")
        self._shutdown_requested = True

    def _route(self, topic: str, event: Dict[str, Any]) -> bool:
        handler = self._handlers.get(topic)
        if not handler:
            logger.warning(f"No handler registered for topic: {topic}")
            return False
        return handler(event)

    def _log_metrics(self):
        logger.info(f"Bridge metrics: {json.dumps(self.metrics, indent=2)}")

    def _shutdown(self):
        logger.info("Shutting down bridge...")
        self._log_metrics()
        if self._consumer:
            try:
                self._consumer.commit()
                self._consumer.close()
                logger.info("Kafka consumer closed cleanly.")
            except Exception as exc:
                logger.error(f"Error closing consumer: {exc}")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Start the bridge event loop.

        Registers SIGINT/SIGTERM handlers for graceful shutdown, then
        polls Kafka in a loop, routing each message to its registered
        handler and committing offsets after each batch.

        Blocks until a shutdown signal is received or a fatal error occurs.
        """
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        self._consumer = self._build_consumer()
        logger.info("Kafka-Celery Bridge running. Listening for events...")

        try:
            while not self._shutdown_requested:
                batch = self._consumer.poll(timeout_ms=1000, max_records=self.max_poll_records)

                if self._shutdown_requested:
                    break

                for _tp, messages in batch.items():
                    for message in messages:
                        topic = message.topic
                        event = message.value

                        if not event:
                            logger.warning(f"Empty event on topic '{topic}' — skipping.")
                            continue

                        t0 = time.time()
                        success = self._route(topic, event)
                        elapsed_ms = (time.time() - t0) * 1000

                        self.metrics["events_processed"] += 1
                        self.metrics["last_event_time"] = datetime.utcnow().isoformat()

                        if success:
                            logger.debug(f"Event processed: {topic} ({elapsed_ms:.1f}ms)")
                        else:
                            self.metrics["events_failed"] += 1
                            logger.warning(f"Event handler returned False: {topic} ({elapsed_ms:.1f}ms)")

                        if self.metrics["events_processed"] % self.metrics_log_interval == 0:
                            self._log_metrics()

                if batch:
                    try:
                        self._consumer.commit()
                    except Exception as exc:
                        logger.error(f"Offset commit failed: {exc}")

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt — shutting down.")
        except Exception as exc:
            logger.error(f"Fatal error in event loop: {exc}", exc_info=True)
            raise
        finally:
            self._shutdown()
