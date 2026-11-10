"""Batch confirmation takes the restore guard inside its single transaction."""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

from keep.db import Database
from keep.kafka_changed import PartitionRetries
from keep.kafka_source import SourceConsumer
from test_source_partition_batching import changed, record

XACT_LOCK = "pg_try_advisory_xact_lock_shared"
SESSION_LOCK = "pg_try_advisory_lock_shared"
CONTROL = "SELECT deployment_epoch,fenced FROM restore_control"


class Connection:
    """Record each transaction's statements and answer the guard's reads."""

    def __init__(self):
        self.transactions = []

    @contextmanager
    def transaction(self, *args, **kwargs):
        self.transactions.append([])
        yield

    @contextmanager
    def pipeline(self):
        yield

    def execute(self, query, params=None):
        self.transactions[-1].append(query)
        row = None
        if XACT_LOCK in query or SESSION_LOCK in query:
            row = {"acquired": True}
        elif "pg_advisory_unlock_shared" in query:
            row = {"released": True}
        elif query.startswith(CONTROL):
            row = {"deployment_epoch": "epoch", "fenced": False}
        return SimpleNamespace(fetchone=lambda: row)


class Pool:
    def __init__(self, connection):
        self.held = connection

    @contextmanager
    def connection(self, timeout=None):
        yield self.held


def test_batch_confirmation_is_one_transaction_with_the_guard_inside():
    connection = Connection()
    worker = SourceConsumer.__new__(SourceConsumer)
    worker.db = Database.__new__(Database)
    worker.db.deployment_epoch, worker.db.pool = "epoch", Pool(connection)
    worker.settings = SimpleNamespace(environment="test")
    worker.client = Mock()
    worker.retries = PartitionRetries(worker.client)
    applied = [(record(0, offset), changed(0, offset)) for offset in (10, 11, 12)]

    assert worker._publish(Mock(), applied) == 3

    # No preliminary guard or lock-release transaction around the bookkeeping.
    [statements] = connection.transactions
    pinned = statements.index("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    lock = next(i for i, query in enumerate(statements) if XACT_LOCK in query)
    control = next(i for i, query in enumerate(statements) if query.startswith(CONTROL))
    confirm = next(i for i, query in enumerate(statements) if "kafka_confirmed=true" in query)
    position = next(i for i, query in enumerate(statements) if "kafka_consumer_positions" in query)
    assert pinned < lock < control < confirm < position
    assert not any(SESSION_LOCK in query or "unlock" in query for query in statements)
