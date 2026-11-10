"""A recreated received topic: coordinate-conflict fence, topic identity and failure logs."""

from base64 import urlsafe_b64decode
from concurrent.futures import Future
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock
import logging

import psycopg.errors
import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition, Uuid

from keep.kafka_changed import PartitionRetries
from keep.kafka_source import SourceConsumer, same_topic, topic_identity
from keep.shared.errors import AppError, unavailable
from test_source_partition_batching import TOPIC, applied_offsets, changed, record, source

# The local broker's received and changed topics, as kafka-topics.sh prints them.
RECEIVED = "YnmTZeuHT_iG2FXpjd3lUQ"
CHANGED = "XvLjZHK5QdW5sr9DukPJTg"


def signed(value):
    return value - (1 << 64) if value >= 1 << 63 else value


def truncated(value):
    """What confluent-kafka reports for one half where a C long is 32 bits."""
    low = value & 0xFFFFFFFF
    return low - (1 << 32) if low >= 1 << 31 else low


def halves(text):
    value = int.from_bytes(urlsafe_b64decode(text + "=="), "big")
    return value >> 64, value & ((1 << 64) - 1)


def admin_for(*bits, error=None):
    """An admin client whose describe_topics reports a topic ID from two halves."""
    future = Future()
    if error:
        future.set_exception(error)
    else:
        msb, lsb = bits
        topic_id = SimpleNamespace(
            get_most_significant_bits=lambda: msb, get_least_significant_bits=lambda: lsb
        )
        future.set_result(SimpleNamespace(topic_id=topic_id))
    admin = Mock()
    admin.describe_topics.side_effect = lambda topics, **_: {topics.topic_names[0]: future}
    return admin


def test_topic_identity_is_kafka_text_and_compares_across_clients():
    """Requirements: REV-41; Kafka alert pipeline 8."""
    msb, lsb = halves(RECEIVED)
    full = topic_identity(admin_for(signed(msb), signed(lsb)), TOPIC)
    assert full == RECEIVED
    # Windows clients keep only the low 32 bits of each half, sign extended.
    windows = topic_identity(admin_for(truncated(msb), truncated(lsb)), TOPIC)
    assert windows == "_____-uHT_j_____jd3lUQ"
    assert same_topic(RECEIVED, windows) and same_topic(windows, RECEIVED)
    assert not same_topic(RECEIVED, CHANGED)
    # The installed client on this platform, whichever width it reports.
    installed = Uuid(signed(msb), signed(lsb))
    observed = topic_identity(
        admin_for(installed.get_most_significant_bits(), installed.get_least_significant_bits()),
        TOPIC,
    )
    assert same_topic(RECEIVED, observed)


@pytest.mark.parametrize(
    ("admin", "code"),
    [
        (admin_for(0, 0), "kafka_topic_identity_unavailable"),
        (
            admin_for(error=KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))),
            "kafka_topic_identity_unverified",
        ),
    ],
)
def test_missing_topic_identity_is_never_a_match(admin, code):
    """Requirements: REV-41; Kafka alert pipeline 8."""
    with pytest.raises(AppError) as raised:
        topic_identity(admin, TOPIC)
    assert raised.value.code == code


class Database:
    def __init__(self):
        self.statements = []

    @contextmanager
    def transaction(self, *args, **kwargs):
        conn = Mock()
        conn.execute.side_effect = lambda sql, params=(): self.statements.append((sql, params))
        yield conn


def assigning(monkeypatch, recorded, observed, committed=(7,)):
    """A source worker about to receive its partitions, with stand-in clients."""
    producer = Mock()
    monkeypatch.setattr("confluent_kafka.Producer", producer)
    monkeypatch.setattr("keep.service_clients.kafka_options", lambda settings: {})
    msb, lsb = halves(observed)
    worker = SourceConsumer.__new__(SourceConsumer)
    worker.db = Database()
    worker.settings = SimpleNamespace(environment="test")
    worker.control = {"received_topic_id": recorded, "received_partitions": len(committed)}
    worker.closing = False
    worker.producers = {}
    worker.admin = admin_for(signed(msb), signed(lsb))
    consumer = Mock()
    consumer.list_topics.return_value = SimpleNamespace(
        topics={TOPIC: SimpleNamespace(partitions=dict.fromkeys(range(len(committed))))}
    )
    consumer.committed.side_effect = lambda points, timeout: [
        TopicPartition(p.topic, p.partition, committed[p.partition]) for p in points
    ]
    consumer.get_watermark_offsets.return_value = (0, 100)
    worker.client = consumer
    worker.retries = PartitionRetries(consumer)
    partitions = [TopicPartition(TOPIC, partition) for partition in range(len(committed))]
    return worker, consumer, producer, partitions


def test_recreated_topic_is_refused_before_any_producer_offset_or_input(monkeypatch):
    """Requirements: REV-41; Kafka alert pipeline 3, 8."""
    worker, consumer, producer, partitions = assigning(
        monkeypatch, RECEIVED, CHANGED, committed=(7, -1001)
    )
    with pytest.raises(AppError) as raised:
        worker._assigned(consumer, partitions)
    assert raised.value.code == "source_topic_identity_changed"
    producer.assert_not_called()
    consumer.assign.assert_not_called()
    assert worker.producers == {}
    # One visible fault per assigned partition, at its committed offset.
    assert [params for _, params in worker.db.statements] == [(TOPIC, 0, 7), (TOPIC, 1, 0)]
    assert all("'source_topic_identity_changed'" in sql for sql, _ in worker.db.statements)


def test_unrecorded_topic_identity_refuses_the_assignment(monkeypatch):
    """Requirements: REV-41; Kafka alert pipeline 8."""
    worker, consumer, producer, partitions = assigning(monkeypatch, None, RECEIVED)
    with pytest.raises(AppError) as raised:
        worker._assigned(consumer, partitions)
    assert raised.value.code == "kafka_topic_identity_unrecorded"
    worker.admin.describe_topics.assert_not_called()
    producer.assert_not_called()
    consumer.assign.assert_not_called()
    assert worker.db.statements == []


@pytest.mark.parametrize("recorded", [RECEIVED, "_____-uHT_j_____jd3lUQ"])
def test_recorded_incarnation_proceeds_to_the_normal_assignment(monkeypatch, recorded):
    """Requirements: REV-41; Kafka alert pipeline 3, 8."""
    worker, consumer, producer, partitions = assigning(monkeypatch, recorded, RECEIVED)
    worker._assigned(consumer, partitions)
    producer.return_value.init_transactions.assert_called_once_with(10)
    consumer.assign.assert_called_once_with(partitions)
    assert partitions[0].offset == 7
    assert "SET kafka_confirmed=true" in worker.db.statements[0][0]


def test_coordinate_conflict_fences_the_partition_until_explicit_repair(monkeypatch, caplog):
    """Requirements: REV-41; Kafka alert pipeline 3, 8."""

    def apply(db, *, partition, offset, **_):
        if (partition, offset) == (0, 4):
            raise unavailable("source_coordinate_conflict")
        return changed(partition, offset)

    worker = source(monkeypatch, [[record(0, 3), record(0, 4), record(0, 5), record(1, 9)]], apply)
    caplog.set_level(logging.WARNING, logger="keep.kafka_source")
    assert worker.turn() == 2
    assert applied_offsets() == [(0, 3), (0, 4), (1, 9)]
    point, deadline = worker.retries.failed[(TOPIC, 0)]
    assert point.offset == 4 and deadline == float("inf")
    assert worker.client.seek.call_args.args[0].offset == 4
    prefix, fault, healthy = worker.db.statements()
    assert fault[0][1][:4] == (TOPIC, 0, 4, "source_coordinate_conflict")
    # A timed retry never resumes it; only a reviewed reset and restart do.
    worker.retries.resume_due()
    worker.client.resume.assert_not_called()
    assert worker.retries.timeout(0.1) == 0.1
    [entry] = caplog.records
    assert entry.levelno == logging.ERROR
    assert entry.getMessage() == (
        "source_partition_fenced partition=0 offset=4 reason=source_coordinate_conflict "
        "error_class=keep.shared.errors.AppError error_code=source_coordinate_conflict"
    )


@pytest.mark.parametrize(
    ("error", "error_class", "error_code"),
    [
        (
            psycopg.errors.SerializationFailure("synthetic-private-payload"),
            "psycopg.errors.SerializationFailure",
            "40001",
        ),
        (ValueError("synthetic-private-payload"), "builtins.ValueError", "-"),
        (
            unavailable("cel_compilation_unavailable"),
            "keep.shared.errors.AppError",
            "cel_compilation_unavailable",
        ),
    ],
)
def test_generic_failure_logs_class_and_code_never_text(
    monkeypatch, caplog, error, error_class, error_code
):
    """Requirements: REV-41; Kafka alert pipeline 3, 8."""

    def apply(db, **_):
        raise error

    worker = source(monkeypatch, [[record(0, 8)]], apply)
    caplog.set_level(logging.WARNING, logger="keep.kafka_source")
    assert worker.turn() == 0
    [fault] = worker.db.statements()
    assert fault[0][1][3] == "source_processing_failed"
    assert worker.retries.failed[(TOPIC, 0)][1] != float("inf")
    [entry] = caplog.records
    assert entry.levelno == logging.WARNING
    assert entry.getMessage() == (
        "source_partition_retry partition=0 offset=8 reason=source_processing_failed "
        f"error_class={error_class} error_code={error_code}"
    )
    assert "synthetic-private-payload" not in caplog.text
