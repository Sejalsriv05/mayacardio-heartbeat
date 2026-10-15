import json
from heartbeat_pipeline.storage import Storage
from heartbeat_pipeline.sync import (
    CloudTransport,
    SyncAck,
    SyncRecord,
    Synchronizer,
)


def test_sync_preserves_sensor_session_and_channels(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    # Sensor 1: 3 channels
    storage.save_sample("session-1", 1, 0, "[100, 200, 300]")
    # Sensor 2: 2 channels
    storage.save_sample("session-1", 2, 0, "[50, 60]")

    captured_batches = []

    def mock_transport(batch):
        captured_batches.append(list(batch))
        return True

    synchronizer = Synchronizer(storage, mock_transport)
    stats = synchronizer.sync_session("session-1")

    assert stats.samples_acknowledged == 2
    assert len(captured_batches) == 1
    batch = captured_batches[0]

    assert len(batch) == 2
    # Sensor 1 validation
    assert batch[0].session_id == "session-1"
    assert batch[0].sensor == 1
    assert batch[0].sample_index == 0
    assert batch[0].values == [100, 200, 300]  # Exactly 3 channels preserved

    # Sensor 2 validation
    assert batch[1].session_id == "session-1"
    assert batch[1].sensor == 2
    assert batch[1].sample_index == 0
    assert batch[1].values == [50, 60]  # Exactly 2 channels preserved (NOT merged into 5)

    storage.close()


def test_samples_remain_pending_until_acknowledged(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    storage.save_sample("sess-offline", 1, 10, "[1, 2, 3]")
    storage.save_sample("sess-offline", 1, 11, "[4, 5, 6]")

    def offline_transport(batch):
        # Transport fails to deliver / no durable acknowledgement
        return SyncAck(success=False, error_message="Cloud unreachable")

    synchronizer = Synchronizer(
        storage, offline_transport, max_retries=1, backoff_factor=0.01
    )
    stats = synchronizer.sync_session("sess-offline")

    assert stats.samples_acknowledged == 0
    assert stats.batches_failed == 1

    # Samples MUST remain pending in SQLite
    pending = storage.get_unsynced_samples("sess-offline")
    assert len(pending) == 2
    assert [p[2] for p in pending] == [10, 11]

    storage.close()


def test_samples_marked_synced_after_durable_ack(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    storage.save_sample("sess-ack", 1, 1, "[10, 20, 30]")
    storage.save_sample("sess-ack", 1, 2, "[40, 50, 60]")

    def ack_transport(batch):
        return SyncAck(
            success=True,
            acknowledged_keys=[
                (r.session_id, r.sensor, r.sample_index) for r in batch
            ],
        )

    synchronizer = Synchronizer(storage, ack_transport)
    stats = synchronizer.sync_session("sess-ack")

    assert stats.samples_acknowledged == 2
    assert stats.batches_succeeded == 1

    # No pending unsynced samples left
    pending = storage.get_unsynced_samples("sess-ack")
    assert len(pending) == 0

    # Retained in database with synced = 1
    cursor = storage.connection.execute(
        "SELECT synced FROM samples WHERE session_id = 'sess-ack' ORDER BY sample_index"
    )
    rows = cursor.fetchall()
    assert rows == [(1,), (1,)]

    storage.close()


def test_partial_failure_preserves_unacknowledged_samples(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    storage.save_sample("sess-part", 1, 0, "[1, 1, 1]")
    storage.save_sample("sess-part", 1, 1, "[2, 2, 2]")
    storage.save_sample("sess-part", 1, 2, "[3, 3, 3]")
    storage.save_sample("sess-part", 1, 3, "[4, 4, 4]")

    def partial_ack_transport(batch):
        # Acknowledge only first two samples; fail remaining
        return SyncAck(
            success=False,
            acknowledged_keys=[("sess-part", 1, 0), ("sess-part", 1, 1)],
            error_message="Partial write failure on downstream cluster",
        )

    synchronizer = Synchronizer(
        storage, partial_ack_transport, max_retries=0
    )
    stats = synchronizer.sync_session("sess-part")

    assert stats.samples_acknowledged == 2
    assert stats.batches_failed == 1

    # Indices 0 and 1 marked synced; indices 2 and 3 remain unsynced
    pending = storage.get_unsynced_samples("sess-part")
    assert len(pending) == 2
    assert [p[2] for p in pending] == [2, 3]

    storage.close()


def test_retries_transient_failure_and_succeeds(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    storage.save_sample("sess-retry", 1, 0, "[9, 9, 9]")

    attempts = 0

    def flaky_transport(batch):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionResetError("Connection dropped by peer")
        return True

    synchronizer = Synchronizer(
        storage, flaky_transport, max_retries=3, backoff_factor=0.01
    )
    stats = synchronizer.sync_session("sess-retry")

    assert attempts == 3
    assert stats.samples_acknowledged == 1
    assert stats.batches_succeeded == 1
    assert len(storage.get_unsynced_samples("sess-retry")) == 0

    storage.close()


def test_exhausted_retries_keeps_backlog_safe(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    storage.save_sample("sess-exhaust", 1, 0, "[9, 9, 9]")

    call_count = 0

    def down_transport(batch):
        nonlocal call_count
        call_count += 1
        raise TimeoutError("Gateway timeout")

    synchronizer = Synchronizer(
        storage, down_transport, max_retries=2, backoff_factor=0.01
    )
    stats = synchronizer.sync_session("sess-exhaust")

    assert call_count == 3  # initial attempt + 2 retries
    assert stats.samples_acknowledged == 0
    assert stats.batches_failed == 1
    assert len(storage.get_unsynced_samples("sess-exhaust")) == 1

    storage.close()


def test_duplicate_safe_resynchronization(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    storage.save_sample("sess-idem", 1, 0, "[1, 2, 3]")

    synchronizer = Synchronizer(storage, lambda batch: True)
    stats1 = synchronizer.sync_session("sess-idem")
    assert stats1.samples_acknowledged == 1

    # Running sync again when no pending samples exists is a no-op
    stats2 = synchronizer.sync_session("sess-idem")
    assert stats2.samples_acknowledged == 0
    assert stats2.batches_attempted == 0

    # Idempotent database marking does not crash
    storage.mark_sample_synced("sess-idem", 1, 0)
    assert len(storage.get_unsynced_samples("sess-idem")) == 0

    storage.close()


def test_backlog_recovery_multiple_batches(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    # Insert 25 samples
    for i in range(25):
        storage.save_sample("sess-backlog", 1, i, json.dumps([i, i + 1, i + 2]))

    delivered_count = 0

    def batch_transport(batch):
        nonlocal delivered_count
        delivered_count += len(batch)
        return True

    # Batch size 10 -> will require 3 batches (10, 10, 5)
    synchronizer = Synchronizer(storage, batch_transport, batch_size=10)
    stats = synchronizer.sync_session("sess-backlog")

    assert stats.batches_attempted == 3
    assert stats.batches_succeeded == 3
    assert stats.samples_acknowledged == 25
    assert delivered_count == 25
    assert len(storage.get_unsynced_samples("sess-backlog")) == 0

    storage.close()


def test_sync_all_multiple_sessions(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    storage.save_sample("session-A", 1, 0, "[1, 1, 1]")
    storage.save_sample("session-A", 1, 1, "[2, 2, 2]")
    storage.save_sample("session-B", 2, 0, "[3, 3]")

    synchronizer = Synchronizer(storage, lambda batch: True)
    stats = synchronizer.sync_all()

    assert stats.samples_acknowledged == 3
    assert len(storage.get_unsynced_samples("session-A")) == 0
    assert len(storage.get_unsynced_samples("session-B")) == 0
    assert synchronizer.get_unsynced_sessions() == []

    storage.close()


def test_synchronizer_automatic_mode_switching_and_recovery(tmp_path):
    from heartbeat_pipeline.sync import InMemoryCloudTransport

    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    storage.save_sample("sess-mode", 1, 0, "[1, 2, 3]")
    storage.save_sample("sess-mode", 1, 1, "[4, 5, 6]")

    # Transport configured to fail the first batch, then succeed
    transport = InMemoryCloudTransport(fail_first_n_batches=1)
    synchronizer = Synchronizer(storage, transport, max_retries=0)

    # Initial state is online
    assert synchronizer.mode == "online"
    assert synchronizer.is_online is True

    # 1. First sync attempt fails -> switches mode to offline, preserves backlog
    stats1 = synchronizer.sync_session("sess-mode")
    assert stats1.batches_failed == 1
    assert stats1.samples_acknowledged == 0
    assert synchronizer.mode == "offline"
    assert synchronizer.is_online is False
    assert len(storage.get_unsynced_samples("sess-mode")) == 2

    # 2. Second sync attempt succeeds -> switches mode back to online, drains backlog
    stats2 = synchronizer.sync_session("sess-mode")
    assert stats2.batches_succeeded == 1
    assert stats2.samples_acknowledged == 2
    assert synchronizer.mode == "online"
    assert synchronizer.is_online is True
    assert len(storage.get_unsynced_samples("sess-mode")) == 0
    assert len(transport.received_records) == 2

    storage.close()
