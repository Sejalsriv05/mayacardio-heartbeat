import io
import json
import pytest

from heartbeat_pipeline.cli import main
from heartbeat_pipeline.storage import Storage


def test_cli_status_empty_db(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "test.db")
    storage = Storage(db_path)
    storage.close()

    exit_code = main(["status", "--db", db_path])
    assert exit_code == 0

    captured = capsys.readouterr()
    lines = captured.out.strip().split("\n")
    assert len(lines) == 1

    status = json.loads(lines[0])
    assert status == {
        "mode": "online",
        "samples_saved": 0,
        "samples_in_cloud": 0,
        "backlog": 0,
    }


def test_cli_status_with_samples_and_offline_mode(tmp_path, capsys):
    db_path = str(tmp_path / "test.db")
    storage = Storage(db_path)
    # Save 3 samples: 2 synced, 1 unsynced
    storage.save_sample("sess-1", 1, 0, "[1, 2, 3]")
    storage.save_sample("sess-1", 1, 1, "[4, 5, 6]")
    storage.save_sample("sess-1", 1, 2, "[7, 8, 9]")
    storage.mark_sample_synced("sess-1", 1, 0)
    storage.mark_sample_synced("sess-1", 1, 1)
    storage.close()

    exit_code = main(["status", "--db", db_path, "--mode", "offline"])
    assert exit_code == 0

    captured = capsys.readouterr()
    status = json.loads(captured.out.strip())
    assert status == {
        "mode": "offline",
        "samples_saved": 3,
        "samples_in_cloud": 2,
        "backlog": 1,
    }


def test_cli_export_from_device(tmp_path, capsys):
    db_path = str(tmp_path / "test.db")
    storage = Storage(db_path)

    # 2 samples for sess-A (one synced, one unsynced)
    storage.save_sample("sess-A", 1, 10, "[10, 20, 30]")
    storage.save_sample("sess-A", 2, 10, "[50, 60]")
    storage.mark_sample_synced("sess-A", 1, 10)

    # 1 loss record with null to_index
    storage.save_loss("sess-A", 1, 11, None, "Sensor session restarted; end of loss is unknown")

    # Sample for another session (should be excluded)
    storage.save_sample("sess-B", 1, 0, "[9, 9, 9]")
    storage.close()

    exit_code = main(["export", "--session", "sess-A", "--from", "device", "--db", db_path])
    assert exit_code == 0

    captured = capsys.readouterr()
    lines = [json.loads(line) for line in captured.out.strip().split("\n")]

    # 2 sample records + 1 loss record
    assert len(lines) == 3

    # Sample 1
    assert lines[0] == {"sensor": 1, "index": 10, "values": [10, 20, 30]}
    # Sample 2
    assert lines[1] == {"sensor": 2, "index": 10, "values": [50, 60]}
    # Loss record (preserving null to_index)
    assert lines[2] == {
        "sensor": 1,
        "from_index": 11,
        "to_index": None,
        "reason": "Sensor session restarted; end of loss is unknown",
    }


def test_cli_export_from_cloud(tmp_path, capsys):
    db_path = str(tmp_path / "test.db")
    storage = Storage(db_path)

    # 2 samples: only sample 0 is synced
    storage.save_sample("sess-cloud", 1, 0, "[1, 2, 3]")
    storage.save_sample("sess-cloud", 1, 1, "[4, 5, 6]")
    storage.mark_sample_synced("sess-cloud", 1, 0)
    storage.close()

    exit_code = main(["export", "--session", "sess-cloud", "--from", "cloud", "--db", db_path])
    assert exit_code == 0

    captured = capsys.readouterr()
    lines = [json.loads(line) for line in captured.out.strip().split("\n")]

    # Only sample 0 should be present
    assert len(lines) == 1
    assert lines[0] == {"sensor": 1, "index": 0, "values": [1, 2, 3]}


def test_cli_results_multiple_windows(tmp_path, capsys):
    db_path = str(tmp_path / "test.db")
    storage = Storage(db_path)

    # Populate window 0 (samples 0..399) and window 1 (samples 400..799)
    for i in range(800):
        storage.save_sample("sess-res", 1, i, "[10, 20, 30]")
        storage.save_sample("sess-res", 2, i, "[3, 4]")
    storage.close()

    exit_code = main(["results", "--session", "sess-res", "--mode", "online", "--db", db_path])
    assert exit_code == 0

    captured = capsys.readouterr()
    lines = [json.loads(line) for line in captured.out.strip().split("\n")]

    assert len(lines) == 2
    # Window 0
    assert lines[0]["window_index"] == 0
    assert lines[0]["mode"] == "online"
    assert lines[0]["sensor_1"]["available_samples"] == 400
    assert lines[0]["sensor_1"]["completeness"] == 1.0
    assert lines[0]["sensor_2"]["magnitude"] == 5.0
    assert lines[0]["heart_rate"] is None  # w < 4: insufficient initial window

    # Window 1
    assert lines[1]["window_index"] == 1
    assert lines[1]["sensor_1"]["available_samples"] == 400


def test_cli_results_empty_session(tmp_path, capsys):
    db_path = str(tmp_path / "test.db")
    storage = Storage(db_path)
    storage.close()

    exit_code = main(["results", "--session", "empty-session", "--db", db_path])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert captured.out.strip() == ""


def test_cli_invalid_arguments(capsys):
    # Missing required --session on export
    with pytest.raises(SystemExit):
        main(["export", "--from", "device"])

    # Invalid --from choice
    with pytest.raises(SystemExit):
        main(["export", "--session", "s1", "--from", "invalid_source"])

    # Missing command
    with pytest.raises(SystemExit):
        main([])


def test_cli_export_empty_session(tmp_path, capsys):
    db_path = str(tmp_path / "test.db")
    storage = Storage(db_path)
    storage.close()

    exit_code = main(["export", "--session", "empty-session", "--from", "device", "--db", db_path])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert captured.out.strip() == ""
