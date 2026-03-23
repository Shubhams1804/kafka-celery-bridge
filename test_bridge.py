"""
Tests for KafkaCeleryBridge.
Run with: pytest tests/
"""

from unittest.mock import MagicMock, patch
import pytest

from kafka_celery_bridge import KafkaCeleryBridge


@pytest.fixture
def bridge():
    return KafkaCeleryBridge(
        kafka_brokers="kafka:9092",
        celery_broker_url="redis://localhost:6379/0",
        consumer_group="test-group",
    )


class TestHandlerRegistration:
    def test_register_via_decorator(self, bridge):
        @bridge.on("test.topic")
        def handler(event):
            return True

        assert "test.topic" in bridge._handlers
        assert bridge._handlers["test.topic"] is handler

    def test_register_programmatically(self, bridge):
        def handler(event):
            return True

        bridge.register("other.topic", handler)
        assert "other.topic" in bridge._handlers

    def test_multiple_topics(self, bridge):
        bridge.register("topic.a", lambda e: True)
        bridge.register("topic.b", lambda e: True)
        assert len(bridge._handlers) == 2


class TestEventRouting:
    def test_routes_to_correct_handler(self, bridge):
        results = []

        bridge.register("foo.event", lambda e: results.append(e) or True)

        bridge._route("foo.event", {"key": "value"})
        assert results == [{"key": "value"}]

    def test_returns_false_for_unknown_topic(self, bridge):
        result = bridge._route("nonexistent.topic", {})
        assert result is False

    def test_handler_returning_false_is_respected(self, bridge):
        bridge.register("bad.event", lambda e: False)
        result = bridge._route("bad.event", {"x": 1})
        assert result is False


class TestSendTask:
    def test_send_task_success(self, bridge):
        mock_result = MagicMock()
        mock_result.id = "abc-123"

        mock_celery = MagicMock()
        mock_celery.send_task.return_value = mock_result

        with patch("kafka_celery_bridge.bridge.Celery", return_value=mock_celery):
            result = bridge.send_task("myapp.tasks.do_work", args=["arg1"])

        assert result is True
        mock_celery.send_task.assert_called_once()

    def test_send_task_failure(self, bridge):
        with patch("kafka_celery_bridge.bridge.Celery", side_effect=Exception("broker down")):
            result = bridge.send_task("myapp.tasks.do_work")

        assert result is False

    def test_send_task_with_countdown(self, bridge):
        mock_result = MagicMock()
        mock_result.id = "xyz-456"
        mock_celery = MagicMock()
        mock_celery.send_task.return_value = mock_result

        with patch("kafka_celery_bridge.bridge.Celery", return_value=mock_celery):
            bridge.send_task("myapp.tasks.delayed", args=["x"], countdown=5)

        _, kwargs = mock_celery.send_task.call_args
        assert kwargs["countdown"] == 5


class TestMetrics:
    def test_initial_metrics(self, bridge):
        assert bridge.metrics["events_processed"] == 0
        assert bridge.metrics["events_failed"] == 0
        assert bridge.metrics["started_at"] is not None

    def test_no_handlers_raises(self, bridge):
        with pytest.raises(RuntimeError, match="No topics registered"):
            bridge._build_consumer()
