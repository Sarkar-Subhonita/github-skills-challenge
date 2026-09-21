# AIOps Pipeline Assessment

This repository contains a small Python AIOps pipeline that reads service telemetry, detects rule-based anomalies, creates anomaly events, publishes them to an in-memory topic, and consumes them for final output.

## Architecture

```text
data/service_data.json
	|
	v
load_data() in src/aiops_pipeline.py
	|
	v
AnomalyDetector.detect()
	|
	v
EventProducer.publish()
	|
	v
EventTopic.messages
	|
	v
EventConsumer.consume()
	|
	v
run_pipeline() result and printed output
```

The event topic is an in-memory Python list. This project does not connect to Kafka, RabbitMQ, Redis, or another external message broker.

## Project Structure

- `src/aiops_pipeline.py`: Loads data, coordinates detection and event handling, and provides the command-line entry point.
- `src/anomaly_detector.py`: Applies response-time, CPU, memory, and error-log rules.
- `src/event_producer.py`: Publishes generated events to an `EventTopic`.
- `src/event_topic.py`: Stores events in memory and exposes publish, read, and clear operations.
- `src/event_consumer.py`: Reads events from an `EventTopic`.
- `src/calculations.py`: Contains the separate sample circle-area and Fibonacci functions used by the exercise tests.
- `data/service_data.json`: Checked-in service telemetry and log records.
- `tests/test_aiops_pipeline.py`: Tests anomaly detection, event transport, pipeline processing, and command-line output.
- `tests/calculations_test.py`: Tests the sample calculation functions.
- `.github/workflows/`: GitHub Actions exercise workflows and example Python CI workflows.
- `.devcontainer/devcontainer.json`: Python 3.13 development-container configuration.
- `.coveragerc`: Coverage configuration.

## Requirements

- Python 3.13 or a compatible Python 3 version
- `pytest==8.4.1`
- `coverage`
- `pytest-cov`

The development container installs the requirements automatically. For a local setup, run:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install coverage pytest-cov
```

## Run the Pipeline

From the repository root:

```bash
python3 src/aiops_pipeline.py
```

Expected summary:

```text
Records processed: 10
Anomalies detected: 2
Events consumed: 2
```

The two sample anomalies occur at `2026-09-20T10:05:00` and `2026-09-20T10:06:00`.

## Detection Rules

`AnomalyDetector` creates an anomaly event when at least one of these rules matches:

- `response_time_ms > 500`: High response time
- `cpu_percent > 80`: High CPU utilization
- `memory_percent > 80`: High memory utilization
- `log_level == "ERROR"`: Error log detected

The generated event contains `timestamp`, `service`, `type`, `reasons`, and the original record under `source`.

## Run Tests

Run the complete test suite:

```bash
python3 -m pytest -q
```

Run only the AIOps tests:

```bash
PYTHONPATH=src python3 -m pytest tests/test_aiops_pipeline.py -q
```

Run only the calculation tests:

```bash
python3 -m pytest tests/calculations_test.py -q
```

## Coverage

Generate the coverage report:

```bash
python3 -m pytest --cov=src -q
```

Enforce the repository's 90 percent coverage gate:

```bash
coverage report --fail-under=90
```

The current test suite reaches 100 percent coverage.

## Run Producer and Consumer Independently

The producer and consumer must share the same `EventTopic` instance because the topic is in memory:

```bash
PYTHONPATH=src python3 -c 'from event_topic import EventTopic; from event_producer import EventProducer; from event_consumer import EventConsumer; event={"type":"ANOMALY","service":"payment-service"}; topic=EventTopic("anomaly-events"); producer=EventProducer(topic); consumer=EventConsumer(topic); print("published:", producer.publish(event)); print("consumed:", consumer.consume())'
```

Expected output:

```text
published: True
consumed: [{'type': 'ANOMALY', 'service': 'payment-service'}]
```

## CI Examples

- `.github/workflows/python-package.yml.example` demonstrates linting and pytest execution.
- `.github/workflows/python-coverage.yml.example` runs pytest with coverage and fails below 90 percent.

The files under `.github/steps/` document the GitHub Skills exercise workflow. They are instructional scaffolding rather than runtime application configuration.

## License and Exercise Resources

This repository is part of a GitHub Skills exercise. See [LICENSE](LICENSE) for licensing information and the [GitHub Skills](https://skills.github.com/) site for related exercises.

[Code of Conduct](https://www.contributor-covenant.org/version/2/1/code-of-conduct/code-of-conduct.md)

