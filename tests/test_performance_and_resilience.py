import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

import pytest

from heartbeat_pipeline.collector import Collector
from heartbeat_pipeline.processing import process_window
from heartbeat_pipeline.storage import Storage
from heartbeat_pipeline.sync import (
    HttpCloudTransport,
    InMemoryCloudTransport,
    Synchronizer,
    SyncRecord,
)


def test_end_to_end_online_offline_resilience_and_backlog_recovery(tmp_path):
    """Verify complete lifecycle: online collection -> cloud drop -> offline accumulation -> recovery."""
    db_path = tmp_path / "lifecycle.db"
    storage = Storage(str(db_path))

    # Phase 1: Online state with healthy transport
    transport = InMemoryCloudTransport()
    sync = Synchronizer(storage, transport, batch_size=200, max_retries=0)
    assert sync.is_online is True

    # Ingest 200 samples for Sensor 1 and 200 for Sensor 2
    for i in range(200):
        storage.save_sample("session-resilient", 1, i, json.dumps([i, i * 2, -i]))
        storage.save_sample("session-resilient", 2, i, json.dumps([3, 4]))

    # Sync Phase 1
    stats_p1 = sync.sync_all()
    assert stats_p1.samples_acknowledged == 400
    assert sync.is_online is True
    assert len(storage.get_unsynced_samples("session-resilient")) == 0

    # Phase 2: Cloud goes offline
    transport.fail_permanently = True

    # Ingest 400 more samples while offline
    for i in range(200, 400):
        storage.save_sample("session-resilient", 1, i, json.dumps([i, i * 2, -i]))
        storage.save_sample("session-resilient", 2, i, json.dumps([3, 4]))

    # Sync attempt fails -> triggers offline mode
    stats_p2 = sync.sync_all()
    assert stats_p2.batches_failed >= 1
    assert sync.mode == "offline"
    assert sync.is_online is False

    # Backlog preserved in SQLite (400 samples pending)
    backlog = storage.get_unsynced_samples("session-resilient", limit=1000)
    assert len(backlog) == 400

    # Processing continues locally while offline
    window_0 = process_window(storage, "session-resilient", window_index=0, mode="offline")
    assert window_0["mode"] == "offline"
    assert window_0["sensor_1"]["available_samples"] == 400
    assert window_0["sensor_2"]["available_samples"] == 400

    # Phase 3: Cloud connection restored
    transport.fail_permanently = False
    stats_p3 = sync.sync_all()
    assert stats_p3.samples_acknowledged == 400
    assert sync.mode == "online"
    assert sync.is_online is True

    # Verify backlog completely drained
    assert len(storage.get_unsynced_samples("session-resilient")) == 0
    assert len(transport.received_records) == 800

    storage.close()


def test_processing_latency_budget_measurement(tmp_path):
    """Benchmark window processing latency against real-time budget (1000 ms)."""
    db_path = tmp_path / "benchmark.db"
    storage = Storage(str(db_path))

    # Populate 2400 samples (6 full seconds) with physiological pattern
    samples_s1 = []
    samples_s2 = []
    for i in range(2400):
        # Channel 2 has periodic peaks every 300 samples (80 bpm at 400 Hz)
        is_peak = (i % 300 == 150)
        ch2_val = 50000 if is_peak else 1000
        samples_s1.append(("sess-bench", 1, i, json.dumps([500, ch2_val, -200])))
        samples_s2.append(("sess-bench", 2, i, json.dumps([30, 40])))

    storage.save_samples_batch(samples_s1)
    storage.save_samples_batch(samples_s2)

    # Benchmark window processing on window 5 (full 5s history + peak detection)
    iterations = 50
    latencies_ms = []

    for _ in range(iterations):
        t0 = time.perf_counter()
        res = process_window(storage, "sess-bench", window_index=5, mode="online")
        t1 = time.perf_counter()
        latencies_ms.append((t1 - t0) * 1000.0)

    mean_latency = sum(latencies_ms) / len(latencies_ms)
    max_latency = max(latencies_ms)

    assert res["sensor_1"]["available_samples"] == 400
    assert res["heart_rate"] is not None
    # Real-time budget: window processing runs at 1 Hz (1000 ms budget).
    # Expected latency on modern hardware is < 25 ms.
    assert mean_latency < 25.0, f"Mean latency {mean_latency:.2f}ms exceeds 25ms limit"

    storage.close()


class MockHttpHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress server stderr logging

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length)
        if self.path == "/fail":
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Internal Server Error")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')


def test_http_cloud_transport_success_and_failure():
    """Verify HttpCloudTransport with a local HTTP server fixture."""
    server = HTTPServer(("127.0.0.1", 0), MockHttpHandler)
    host, port = server.server_address
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    records = [
        SyncRecord(session_id="s1", sensor=1, sample_index=0, values=[1, 2, 3])
    ]

    try:
        # Success endpoint
        transport = HttpCloudTransport(f"http://{host}:{port}/ingest", auth_token="test-token")
        ack = transport.send_batch(records)
        assert ack.success is True
        assert len(ack.acknowledged_keys) == 1
        assert ack.acknowledged_keys[0] == ("s1", 1, 0)

        # Failure endpoint (500 error)
        fail_transport = HttpCloudTransport(f"http://{host}:{port}/fail")
        fail_ack = fail_transport.send_batch(records)
        assert fail_ack.success is False
        assert "500" in (fail_ack.error_message or "")

        # Unreachable endpoint (connection refused)
        dead_transport = HttpCloudTransport("http://127.0.0.1:1/unreachable", timeout=0.2)
        dead_ack = dead_transport.send_batch(records)
        assert dead_ack.success is False
    finally:
        server.shutdown()
        server.server_close()
