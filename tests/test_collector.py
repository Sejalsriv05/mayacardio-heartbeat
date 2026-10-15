import json
import socket
import threading
import time
import zlib
from contextlib import contextmanager

import pytest

from heartbeat_pipeline.collector import Collector, TCPCollector
from heartbeat_pipeline.storage import Storage


def make_wire_line(
    session_id="session-1",
    sensor=1,
    first_index=0,
    count=1,
    lost=None,
    session_end=False,
    data_text="[[1,2,3]]",
    crc_override=None,
) -> str:
    """Build a protocol-compliant JSON line for testing."""
    if lost is None:
        lost = []
    checksum = zlib.crc32(data_text.encode("utf-8")) & 0xFFFFFFFF
    if crc_override is not None:
        checksum = crc_override

    lost_json = json.dumps(lost)
    session_end_str = "true" if session_end else "false"

    return (
        f'{{"session_id":"{session_id}",'
        f'"sensor":{sensor},'
        f'"first_index":{first_index},'
        f'"count":{count},'
        f'"lost":{lost_json},'
        f'"session_end":{session_end_str},'
        f'"crc32":"{checksum:08x}",'
        f'"data":{data_text}}}\n'
    )


class FakeSensorServer:
    """Ephemeral fake TCP server for simulating sensor telemetry."""

    def __init__(self, chunks=None, handler=None):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(("127.0.0.1", 0))
        self.host, self.port = self.server_sock.getsockname()
        self.server_sock.listen(5)
        self.server_sock.settimeout(0.1)
        self.chunks = chunks
        self.handler = handler
        self.received_requests: list[dict] = []
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                conn, _ = self.server_sock.accept()
            except (socket.timeout, OSError):
                continue
            if self._stop.is_set():
                try:
                    conn.close()
                except OSError:
                    pass
                break
            try:
                # Read incoming request from client
                conn.settimeout(0.2)
                try:
                    req_bytes = conn.recv(1024)
                    if req_bytes:
                        for line in req_bytes.decode("utf-8", errors="replace").splitlines():
                            if line.strip():
                                try:
                                    self.received_requests.append(json.loads(line.strip()))
                                except Exception:
                                    pass
                except (socket.timeout, OSError):
                    pass

                if self.handler:
                    self.handler(conn)
                elif self.chunks is not None:
                    for chunk in self.chunks:
                        if isinstance(chunk, str):
                            chunk = chunk.encode("utf-8")
                        conn.sendall(chunk)
            except OSError:
                pass
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    def stop(self):
        self._stop.set()
        try:
            # Wake up accept loop immediately
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as dummy:
                dummy.connect((self.host, self.port))
        except OSError:
            pass
        try:
            self.server_sock.close()
        except OSError:
            pass
        self.thread.join(timeout=0.5)


@contextmanager
def fake_tcp_server(chunks=None, handler=None):
    server = FakeSensorServer(chunks=chunks, handler=handler)
    try:
        yield server
    finally:
        server.stop()


def test_collector_sensor_1_3_channels(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line = make_wire_line(
        session_id="sess-alpha",
        sensor=1,
        first_index=0,
        count=2,
        data_text="[[100, 200, 300], [101, 201, 301]]",
        session_end=True,
    )

    with fake_tcp_server(chunks=[line]) as server:
        collector = Collector(
            storage=storage,
            sensor_id=1,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=0,
        )
        batches = collector.collect(stop_on_session_end=True)

        assert batches == 1
        assert storage.get_sample_count("sess-alpha") == 2

        samples = storage.get_unsynced_samples("sess-alpha")
        assert len(samples) == 2
        assert samples[0] == ("sess-alpha", 1, 0, "[100, 200, 300]")
        assert samples[1] == ("sess-alpha", 1, 1, "[101, 201, 301]")

    storage.close()


def test_collector_sensor_2_2_channels(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line = make_wire_line(
        session_id="sess-beta",
        sensor=2,
        first_index=10,
        count=2,
        data_text="[[500, 600], [501, 601]]",
        session_end=True,
    )

    with fake_tcp_server(chunks=[line]) as server:
        collector = TCPCollector(
            storage=storage,
            sensor=2,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=0,
        )
        batches = collector.collect(stop_on_session_end=True)

        assert batches == 1
        assert storage.get_sample_count("sess-beta") == 2

        samples = storage.get_unsynced_samples("sess-beta")
        assert len(samples) == 2
        assert samples[0] == ("sess-beta", 2, 10, "[500, 600]")
        assert samples[1] == ("sess-beta", 2, 11, "[501, 601]")

    storage.close()


def test_collector_rejects_sensor_mismatch(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    # Server sends sensor 2 payload, but collector expects sensor 1
    line = make_wire_line(sensor=2, data_text="[[10, 20]]")

    with fake_tcp_server(chunks=[line]) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=0,
        )
        collector.collect(max_batches=1)

        assert collector.malformed_count == 1
        assert storage.get_sample_count("session-1") == 0

    storage.close()


def test_collector_prevents_duplicate_samples(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line = make_wire_line(
        session_id="sess-dup",
        sensor=1,
        first_index=0,
        count=2,
        data_text="[[1, 2, 3], [4, 5, 6]]",
    )

    # Server sends the exact same batch twice
    with fake_tcp_server(chunks=[line, line]) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=0,
        )
        collector.collect(max_batches=2)

        # Primary key deduplication prevents duplicates
        assert storage.get_sample_count("sess-dup") == 2

    storage.close()


def test_collector_records_explicit_losses_including_unknown_end(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    losses_payload = [
        {"from_index": 10, "to_index": 15, "reason": "radio_noise"},
        {
            "from_index": 50,
            "to_index": None,
            "reason": "Sensor session restarted; end of loss is unknown",
        },
    ]

    line = make_wire_line(
        session_id="sess-loss",
        sensor=1,
        first_index=0,
        count=1,
        lost=losses_payload,
        data_text="[[10, 20, 30]]",
        session_end=True,
    )

    with fake_tcp_server(chunks=[line]) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=0,
        )
        collector.collect(stop_on_session_end=True)

        losses = storage.get_losses("sess-loss")
        assert len(losses) == 2
        assert losses[0] == (1, 10, 15, "radio_noise")
        assert losses[1] == (
            1,
            50,
            None,
            "Sensor session restarted; end of loss is unknown",
        )

    storage.close()


def test_collector_handles_session_change_without_merging_or_inventing_indices(
    tmp_path,
):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    # Session 1: indices 0..1 (not cleanly ended)
    line1 = make_wire_line(
        session_id="sess-1",
        sensor=1,
        first_index=0,
        count=2,
        data_text="[[1, 1, 1], [2, 2, 2]]",
        session_end=False,
    )
    # Session 2: indices 0..2 (starts from 0)
    line2 = make_wire_line(
        session_id="sess-2",
        sensor=1,
        first_index=0,
        count=3,
        data_text="[[10, 10, 10], [20, 20, 20], [30, 30, 30]]",
        session_end=True,
    )

    with fake_tcp_server(chunks=[line1, line2]) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=0,
        )
        collector.collect(max_batches=2)

        # Session 1 has exactly 2 samples: 0 and 1
        assert storage.get_sample_count("sess-1") == 2
        s1 = storage.get_unsynced_samples("sess-1")
        assert [s[2] for s in s1] == [0, 1]

        # Session 2 has exactly 3 samples: 0, 1, 2 (indices NOT offset or merged!)
        assert storage.get_sample_count("sess-2") == 3
        s2 = storage.get_unsynced_samples("sess-2")
        assert [s[2] for s in s2] == [0, 1, 2]

        # Since session-1 ended abruptly, a restart loss is recorded with unknown end
        losses_s1 = storage.get_losses("sess-1")
        assert len(losses_s1) == 1
        assert losses_s1[0] == (
            1,
            2,
            None,
            "Sensor session restarted; end of loss is unknown",
        )

    storage.close()


def test_collector_safe_on_malformed_json_and_corrupt_crc(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line_bad_json = "THIS IS NOT JSON AT ALL\n"
    line_bad_crc = make_wire_line(crc_override=12345)
    line_valid = make_wire_line(
        session_id="sess-good",
        sensor=1,
        first_index=5,
        count=1,
        data_text="[[99, 99, 99]]",
        session_end=True,
    )

    with fake_tcp_server(
        chunks=[line_bad_json, line_bad_crc, line_valid]
    ) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=0,
        )
        collector.collect(stop_on_session_end=True)

        assert collector.malformed_count == 2
        assert storage.get_sample_count("sess-good") == 1
        samples = storage.get_unsynced_samples("sess-good")
        assert samples[0][2] == 5
        assert samples[0][3] == "[99, 99, 99]"

    storage.close()


def test_collector_reconnects_on_connection_failure(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line1 = make_wire_line(
        session_id="sess-recon",
        sensor=1,
        first_index=0,
        count=1,
        data_text="[[1, 2, 3]]",
    )
    line2 = make_wire_line(
        session_id="sess-recon",
        sensor=1,
        first_index=1,
        count=1,
        data_text="[[4, 5, 6]]",
        session_end=True,
    )

    connection_count = 0

    def dynamic_handler(conn):
        nonlocal connection_count
        connection_count += 1
        if connection_count == 1:
            conn.sendall(line1.encode("utf-8"))
            # Close connection immediately to trigger disconnect on collector
            conn.close()
        else:
            conn.sendall(line2.encode("utf-8"))
            conn.close()

    with fake_tcp_server(handler=dynamic_handler) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=3,
            retry_interval=0.1,
        )
        collector.collect(max_batches=2)

        assert connection_count >= 2
        assert storage.get_sample_count("sess-recon") == 2

    storage.close()


def test_collector_handles_socket_timeout(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line = make_wire_line(
        session_id="sess-slow",
        sensor=1,
        first_index=0,
        count=1,
        data_text="[[7, 8, 9]]",
        session_end=True,
    )

    def slow_handler(conn):
        # Pause to trigger read timeout on collector
        time.sleep(0.3)
        conn.sendall(line.encode("utf-8"))
        conn.close()

    with fake_tcp_server(handler=slow_handler) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            timeout=0.1,  # short timeout to induce socket.timeout
            max_retries=0,
        )
        collector.collect(stop_on_session_end=True)

        # Ingestion completes successfully after timeout retry
        assert storage.get_sample_count("sess-slow") == 1

    storage.close()


def test_collector_invalid_sensor_id(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    with pytest.raises(ValueError, match="Sensor must be 1 or 2"):
        Collector(storage=storage, sensor_id=3)

    storage.close()


def test_collector_sends_initial_read_request(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line_s1 = make_wire_line(
        session_id="sess-proto-1",
        sensor=1,
        first_index=0,
        count=1,
        data_text="[[10, 20, 30]]",
        session_end=True,
    )
    with fake_tcp_server(chunks=[line_s1]) as server_s1:
        collector_s1 = Collector(
            storage=storage,
            sensor=1,
            host=server_s1.host,
            port=server_s1.port,
            timeout=1.0,
            max_retries=0,
        )
        collector_s1.collect(stop_on_session_end=True)
        assert len(server_s1.received_requests) >= 1
        assert server_s1.received_requests[0] == {"cmd": "read", "sensor": 1}

    line_s2 = make_wire_line(
        session_id="sess-proto-2",
        sensor=2,
        first_index=0,
        count=1,
        data_text="[[5, 6]]",
        session_end=True,
    )
    with fake_tcp_server(chunks=[line_s2]) as server_s2:
        collector_s2 = Collector(
            storage=storage,
            sensor=2,
            host=server_s2.host,
            port=server_s2.port,
            timeout=1.0,
            max_retries=0,
        )
        collector_s2.collect(stop_on_session_end=True)
        assert len(server_s2.received_requests) >= 1
        assert server_s2.received_requests[0] == {"cmd": "read", "sensor": 2}

    storage.close()


def test_collector_sends_resume_from_index_request(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line = make_wire_line(
        session_id="sess-resume",
        sensor=1,
        first_index=100,
        count=1,
        data_text="[[1, 2, 3]]",
        session_end=True,
    )
    with fake_tcp_server(chunks=[line]) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            from_index=100,
            timeout=1.0,
            max_retries=0,
        )
        collector.collect(stop_on_session_end=True)
        assert len(server.received_requests) >= 1
        assert server.received_requests[0] == {
            "cmd": "read",
            "sensor": 1,
            "from_index": 100,
        }

    storage.close()


def test_collector_reconnect_sends_incremented_resume_index(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line1 = make_wire_line(
        session_id="sess-auto-resume",
        sensor=1,
        first_index=0,
        count=2,
        data_text="[[1, 2, 3], [4, 5, 6]]",
        session_end=False,
    )
    line2 = make_wire_line(
        session_id="sess-auto-resume",
        sensor=1,
        first_index=2,
        count=1,
        data_text="[[7, 8, 9]]",
        session_end=True,
    )

    conn_count = 0

    def dynamic_handler(conn):
        nonlocal conn_count
        conn_count += 1
        if conn_count == 1:
            conn.sendall(line1.encode("utf-8"))
            conn.close()
        else:
            conn.sendall(line2.encode("utf-8"))
            conn.close()

    with fake_tcp_server(handler=dynamic_handler) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=3,
            retry_interval=0.05,
        )
        collector.collect(max_batches=2)

        # Connection 1 sends initial read
        # Connection 2 resumes from next index (index 2)
        assert len(server.received_requests) >= 2
        assert server.received_requests[0] == {"cmd": "read", "sensor": 1}
        assert server.received_requests[1] == {
            "cmd": "read",
            "sensor": 1,
            "from_index": 2,
        }
        assert storage.get_sample_count("sess-auto-resume") == 3

    storage.close()


def test_collector_reconnect_prioritizes_last_sample_index_over_initial_param(tmp_path):
    db_path = tmp_path / "test.db"
    storage = Storage(str(db_path))

    line1 = make_wire_line(
        session_id="sess-param-resume",
        sensor=1,
        first_index=0,
        count=5,
        data_text="[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12], [13, 14, 15]]",
        session_end=False,
    )
    line2 = make_wire_line(
        session_id="sess-param-resume",
        sensor=1,
        first_index=5,
        count=1,
        data_text="[[16, 17, 18]]",
        session_end=True,
    )

    conn_count = 0

    def dynamic_handler(conn):
        nonlocal conn_count
        conn_count += 1
        if conn_count == 1:
            conn.sendall(line1.encode("utf-8"))
            conn.close()
        else:
            conn.sendall(line2.encode("utf-8"))
            conn.close()

    with fake_tcp_server(handler=dynamic_handler) as server:
        collector = Collector(
            storage=storage,
            sensor=1,
            host=server.host,
            port=server.port,
            timeout=1.0,
            max_retries=3,
            retry_interval=0.05,
        )
        # Explicit initial from_index=0 passed to collect()
        collector.collect(from_index=0, max_batches=2)

        assert len(server.received_requests) >= 2
        # First connection used initial parameter
        assert server.received_requests[0] == {"cmd": "read", "sensor": 1, "from_index": 0}
        # Reconnect MUST resume from last_sample_index + 1 (5), NOT restart at 0
        assert server.received_requests[1] == {"cmd": "read", "sensor": 1, "from_index": 5}
        assert storage.get_sample_count("sess-param-resume") == 6

    storage.close()
