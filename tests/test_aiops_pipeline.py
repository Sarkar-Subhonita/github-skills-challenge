import runpy

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_cpu_anomaly_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:06:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 94,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "High CPU utilization"
    }

    event = detector.detect(record)

    assert event["reasons"] == ["High CPU utilization"]


def test_memory_anomaly_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:06:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 91,
        "log_level": "INFO",
        "message": "High memory utilization"
    }

    event = detector.detect(record)

    assert event["reasons"] == ["High memory utilization"]


def test_error_log_is_detected_with_normal_metrics():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "ERROR",
        "message": "Payment service failed"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["reasons"] == ["Error log detected"]


def test_pipeline_processes_and_consumes_anomalies():
    result = run_pipeline("data/service_data.json")

    assert result["records_processed"] == 10
    assert len(result["anomalies_detected"]) == 2
    assert len(result["events_consumed"]) == 2


def test_pipeline_script_prints_result(capsys, monkeypatch):
    monkeypatch.syspath_prepend("src")

    runpy.run_path("src/aiops_pipeline.py", run_name="__main__")

    output = capsys.readouterr().out
    assert "Records processed: 10" in output
    assert "Anomalies detected: 2" in output
    assert "Events consumed: 2" in output


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_producer_rejects_empty_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert not producer.publish(None)
    assert topic.get_messages() == []


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1


def test_topic_clear_removes_messages():
    topic = EventTopic("anomaly-events")
    topic.publish({"type": "ANOMALY"})

    topic.clear()

    assert topic.get_messages() == []