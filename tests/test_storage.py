
import json

from heartbeat_pipeline.storage import Storage


def test_duplicate_sample_is_not_inserted(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    try:
        values = json.dumps([100, 200, 300])

        storage.save_sample("session-1", 1, 0, values)
        storage.save_sample("session-1", 1, 0, values)

        assert storage.get_sample_count("session-1") == 1
    finally:
        storage.close()


def test_unsynced_sample_can_be_retrieved(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    try:
        storage.save_sample(
            "session-1", 2, 0, "[100,200]"
        )

        pending = storage.get_unsynced_samples("session-1")

        assert len(pending) == 1
        assert pending[0][1] == 2
        assert pending[0][2] == 0
    finally:
        storage.close()

def test_missing_sample_range_is_recorded(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    try:
        storage.save_loss(
            "session-1",
            1,
            100,
            104,
            "sensor_gap",
        )

        losses = storage.get_losses("session-1")

        assert len(losses) == 1
        assert losses[0] == (1, 100, 104, "sensor_gap")
    finally:
        storage.close()

def test_sample_survives_database_reopen(tmp_path):
    db_path = tmp_path / "restart.db"

    first = Storage(str(db_path))
    first.save_sample("session-1", 1, 42, "[100,200,300]")
    first.close()

    second = Storage(str(db_path))
    try:
        assert second.get_sample_count("session-1") == 1
        pending = second.get_unsynced_samples("session-1")
        assert len(pending) == 1
        assert pending[0][2] == 42
        assert pending[0][3] == "[100,200,300]"
    finally:
        second.close()



def test_unknown_loss_end_is_supported(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(db_path)

    storage.save_loss(
        session_id="session-1",
        sensor=1,
        from_index=1200,
        to_index=None,
        reason="Sensor session restarted; end of loss is unknown",
    )

    losses = storage.get_losses("session-1")
    assert len(losses) == 1
    assert losses[0][1] == 1200
    assert losses[0][2] is None

    storage.close()


def test_save_samples_batch_and_get_max_sample_index(tmp_path):
    db_path = tmp_path / "batch.db"
    storage = Storage(str(db_path))

    # Empty database queries
    assert storage.get_max_sample_index() is None
    assert storage.get_max_sample_index("sess-1", 1) is None

    # Insert batch
    batch = [
        ("sess-1", 1, 0, "[1, 2, 3]"),
        ("sess-1", 1, 1, "[4, 5, 6]"),
        ("sess-1", 1, 2, "[7, 8, 9]"),
        ("sess-1", 2, 0, "[10, 20]"),
        ("sess-2", 1, 100, "[30, 40, 50]"),
    ]
    storage.save_samples_batch(batch)

    # Deduplication in batch
    storage.save_samples_batch([("sess-1", 1, 1, "[99, 99, 99]")])

    assert storage.get_sample_count("sess-1") == 4
    assert storage.get_sample_count("sess-2") == 1

    # Check max indices
    assert storage.get_max_sample_index("sess-1", 1) == 2
    assert storage.get_max_sample_index("sess-1", 2) == 0
    assert storage.get_max_sample_index("sess-2", 1) == 100
    assert storage.get_max_sample_index() == 100

    storage.close()
