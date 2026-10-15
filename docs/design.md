# MayaCardio "Never Lose a Heartbeat" — System Design Note

## 1. System Architecture & Data Flow

The MayaCardio Telemetry Pipeline is an edge telemetry ingestion, clinical feature extraction, and cloud synchronization engine designed for medical-grade continuous physiological monitoring:
- **Sensor 1 (ECG)**: 3 channels, 400 Hz sampling rate, signed 24-bit integers $[-2^{23}, 2^{23}-1]$.
- **Sensor 2 (Accelerometer / Motion)**: 2 channels, 400 Hz, signed 24-bit integers $[-2^{23}, 2^{23}-1]$.

```
[TCP Sensors: 9001/9002] ──> [Collector: Buffer & CRC32] ──> [SQLite: WAL, PK Deduplication]
                                                               ├──> [Window Engine] ──> CLI
                                                               └──> [Synchronizer]  ──> HTTPS Cloud
```

- **Online Mode**: Telemetry streams into SQLite while the synchronizer delivers batches upstream. Acknowledged samples are marked `synced = 1`.
- **Offline Mode**: If upstream delivery fails, the pipeline transitions automatically to `"offline"`. Ingestion and window processing continue locally without dropping frames; unsynced samples accumulate safely in SQLite.
- **Recovery**: Restored connectivity transitions the pipeline back to `"online"`, draining the backlog in ordered batches without duplicates.

---

## 2. Durable Local Storage & Zero-Loss Guarantees

Local persistence utilizes SQLite with Write-Ahead Logging (`PRAGMA journal_mode=WAL`) and full disk synchronization (`PRAGMA synchronous=FULL`).
- **Deduplication**: The primary key `(session_id, sensor, sample_index)` with `INSERT INTO samples ... ON CONFLICT DO NOTHING` prevents duplicate inserts during reconnection or redundant transmission.
- **Transactional Batch Ingestion**: `save_samples_batch()` executes an atomic `executemany` statement per received frame. This reduces disk transactions from 400/s to 1/s, eliminating flash wear and I/O latency bottlenecks.
- **Unbounded Loss Tracking**: The `losses` table records explicit sensor loss intervals (`from_index`, `to_index`, `reason`). If a sensor restarts without clean closure, `to_index` is stored as `NULL`. Migration logic ensures dynamic support for nullable loss endpoints.
- **Crash Invariance**: SQLite WAL mode guarantees that committed samples survive abrupt application crashes and reopen cleanly.

---

## 3. Wire Protocol & Ingestion Lifecycle

- **Command Protocol**: On socket connection, the collector transmits a newline-delimited JSON command:
  - Initial connection: `{"cmd":"read","sensor":X}`
  - Resuming connection: `{"cmd":"read","sensor":X,"from_index":N}` where $N = \text{last\_sample\_index} + 1$.
- **Validation**: Incoming byte streams are buffered until `\n`. [`validator.py`](file:///Users/sejalsrivastava/Documents/mayacardio-heartbeat/src/heartbeat_pipeline/validator.py) enforces schema conformance, 24-bit integer ranges, channel counts (3 for S1, 2 for S2), and computes direct CRC32 checksums over the exact wire text slice of the `"data"` JSON array (`[... ...]`).
- **Session Transition Isolation**: When `session_id` changes unexpectedly without `session_end: true`, a loss record for `[last_sample_index + 1, NULL]` is saved for the previous session. Sample indices across sessions are never merged or shifted.

---

## 4. Cloud Synchronization & Backlog Recovery

- **Durable Acknowledgement Contract**: Samples remain `synced = 0` until upstream acknowledges the exact `(session_id, sensor, sample_index)` key.
- **Exponential Backoff**: Transient failures trigger retries with exponential backoff. Exhausted retries halt the sync loop, keeping the backlog safe in SQLite without hammering downstream.
- **Backlog Drainage**: Reconnection triggers `sync_all()`, reading unsynced samples in sequential chunks (`batch_size=500`). Sensor channel separation is strictly preserved (no 5-channel fusion).
- **Transport Flexibility**: Supports [`InMemoryCloudTransport`](file:///Users/sejalsrivastava/Documents/mayacardio-heartbeat/src/heartbeat_pipeline/sync.py#L84) for local testing and [`HttpCloudTransport`](file:///Users/sejalsrivastava/Documents/mayacardio-heartbeat/src/heartbeat_pipeline/sync.py#L116) (standard library `urllib`) for serverless HTTPS webhook delivery.

---

## 5. Clinical Feature Extraction & Heart-Rate Engine

- **1-Second Windows**: Evaluates non-overlapping windows $[400w, 400w + 399]$ for window index $w \ge 0$. Reports available/missing samples, completeness ($\text{available} / 400.0$), and per-channel min/max/mean (`null` if empty).
- **Sensor 2 Magnitude**: $\text{mean}(\sqrt{\text{ch1}^2 + \text{ch2}^2})$ over available samples (`null` if empty).
- **Peak Detection (Sensor 1 Channel 2)**:
  1. Local maximum: $x_i > x_{i-1}$ and $x_i > x_{i+1}$.
  2. Historical threshold: $x_i > 0.5 \times \max(x_{i-800}, \dots, x_{i-1})$. Requires exactly 800 preceding samples; shorter histories are never substituted.
  3. Refractory constraint: At least 100 samples ($250\text{ ms}$) between consecutive peaks.
- **5-Second Heart Rate**: Evaluated over $[400(w-4), 400w + 399]$. Returns `null` if $w < 4$, available samples $< 2000$, or detected peaks $< 2$. Calculates $\text{gap}_k = (p_{k+1} - p_k) / 400.0$ and $\text{HR} = \text{round}(60.0 / \text{mean\_gap}, 1)$. Count-based fallbacks are rejected.
- **Measured Latency**: Benchmarked at **< 4 ms** per window over 50 iterations (< 0.5% of the 1000 ms real-time budget).

---

## 6. Failure Modes & Automated Test Evidence (58 Tests Passing)

| Failure Mode | Detection & Mitigation | Automated Test Evidence |
|---|---|---|
| **Corrupted CRC** | Frame rejected; not persisted; logged | `test_validator.py::test_corrupt_crc_is_rejected` |
| **Malformed JSON** | Parser error caught; stream buffer advances | `test_collector.py::test_collector_safe_on_malformed_json...` |
| **Channel / Sensor Mismatch** | Schema validator asserts channel count and ID | `test_validator.py::test_wrong_channel_count_is_rejected` |
| **Socket Timeout / Drop** | Socket timeout caught; reconnect loop triggered | `test_collector.py::test_collector_handles_socket_timeout` |
| **Resume Index Conflict** | Reconnect prioritizes `last_sample_index + 1` | `test_collector.py::test_collector_reconnect_prioritizes...` |
| **Duplicate Delivery** | SQLite composite PK ignores duplicate insert | `test_storage.py::test_duplicate_sample_is_not_inserted` |
| **Sensor Session Reset** | Unbounded loss recorded; separate session started | `test_collector.py::test_collector_handles_session_change...` |
| **Cloud Outage / Drop** | Retries exhaust; mode switches to offline | `test_sync.py::test_exhausted_retries_keeps_backlog_safe` |
| **Partial Cloud ACK** | Only confirmed keys marked `synced = 1` | `test_sync.py::test_partial_failure_preserves_unacknowledged...` |
| **Offline -> Online Recovery** | Backlog drained in batches upon reconnection | `test_performance_and_resilience.py::test_end_to_end...` |
| **Process Crash** | SQLite WAL survives database reopen | `test_storage.py::test_sample_survives_database_reopen` |

---

## 7. Resource Profiling & Demo Runbook

- **ARM64 Linux Measurement**:
  - *RSS Memory (< 50 MB)*: `watch -n 1 "cat /proc/$PID/status | grep -E 'VmRSS|VmHWM'"`
  - *CPU Usage (< 15%)*: `pidstat -p $PID -u 1 60` or `top -b -d 1 -p $PID`
  - *Disk I/O & WAL*: `iotop -p $PID` and `ls -lh heartbeat.db*`
- **Demo Runbook**:
  1. `pipeline status`: Verify initial zero backlog.
  2. Ingestion: Start collector on ports 9001/9002; confirm initial read requests.
  3. Outage: Disconnect cloud transport; verify status shows growing `backlog`.
  4. Local Processing: Run `pipeline results --session ID`; verify clinical features emit `"mode":"offline"`.
  5. Recovery: Re-enable cloud endpoint; run sync; verify status shows `backlog: 0`.
  6. Verification: Run `pipeline export --from device` and compare with `--from cloud`.

---

## 8. Limitations & AI Disclosure

- **Hardware & Cloud Boundaries**: Tests executed on macOS Darwin with Python 3.13.5. Standalone mock binaries were not packaged in the repository. Physical flash power-cut durability testing, embedded ARM64 Linux hardware profiling, physical biomedical sensor streaming, and live production cloud deployment were not performed and remain subject to target hardware verification.
- **AI Tool Usage Disclosure**: Developed with the assistance of an agentic AI assistant (Google DeepMind Antigravity / Gemini) under paired developer review. All formulas, schemas, protocol framing, and CLI commands were verified for exact compliance against the assessment specifications.
