# MayaCardio — "Never Lose a Heartbeat" Telemetry Pipeline

An edge telemetry collection, signal processing, and cloud synchronization pipeline built for medical-grade continuous physiological monitoring.

---

## 1. Deliverables Checklist & Compliance Status

Each requirement from the MayaCardio Software Systems Engineer assessment is labeled below:

| Requirement Area | Detailed Specification | Status Label | Automated Test Coverage |
|---|---|:---:|---|
| **Storage Engine** | SQLite WAL mode (`PRAGMA synchronous=FULL`), composite PK deduplication `(session_id, sensor, sample_index)`, unbounded loss storage (`to_index = null`), transactional batch insertion | **Implemented and unit-tested** | `test_storage.py` (6 tests) |
| **Response Validator** | CRC32 data-slice verification, JSON schema validation, signed 24-bit range checks, channel counts (3 for S1, 2 for S2) | **Implemented and unit-tested** | `test_validator.py` (3 tests) |
| **TCP Telemetry Collector** | Line-buffered TCP ingestion, protocol read request (`{"cmd":"read","sensor":X}`), resume with `from_index`, socket timeout/disconnect retries, session restart loss tracking | **Implemented and unit-tested** | `test_collector.py` (14 tests) |
| **Cloud Synchronization** | Durable ACK contract, exponential backoff retries, partial failure safety, sequential batch drainage | **Implemented and unit-tested** | `test_sync.py` (10 tests) |
| **HTTPS Cloud Transport** | Standard library `urllib` client supporting HTTPS POST delivery and authentication tokens | **Implemented and unit-tested** | `test_performance_and_resilience.py` |
| **Online / Offline Switching** | Automatic transition to offline on delivery failure; local buffering; automatic transition to online and backlog recovery | **Integration-tested** | `test_performance_and_resilience.py::test_end_to_end_online_offline_resilience...` |
| **Window Processing** | 1-second windows $[400w, 400w+399]$, missing samples, completeness, channel statistics (min/max/mean) | **Implemented and unit-tested** | `test_processing.py` (14 tests) |
| **Sensor 2 Magnitude** | Mean of $\sqrt{\text{ch1}^2 + \text{ch2}^2}$ over available samples | **Implemented and unit-tested** | `test_processing.py::test_sensor_2_magnitude_calculation` |
| **Sensor 1 Ch2 Peak Rule** | Strict neighbor check ($x_i > x_{i-1}, x_i > x_{i+1}$), 800-sample preceding history check without substitution, 100-sample refractory constraint | **Implemented and unit-tested** | `test_processing.py::test_peak_detection_rules`, `test_peak_requires_full_800_samples...` |
| **5-Second Heart Rate** | $60.0 / \text{mean\_gap\_seconds}$ rounded to 1 decimal place; null conditions for missing data, $w < 4$, or $< 2$ peaks | **Implemented and unit-tested** | `test_processing.py::test_heart_rate_calculation_and_rounding` |
| **Processing Latency** | Mean window processing latency < 25 ms (< 0.5% of 1000 ms real-time budget) | **Integration-tested** | `test_performance_and_resilience.py::test_processing_latency_budget_measurement` |
| **CLI Commands** | `pipeline status`, `pipeline export --from device|cloud`, `pipeline results --session ID` | **Implemented and unit-tested** | `test_cli.py` (8 tests) |
| **ARM64 Linux Deployment** | Resident Set Size < 50 MB, CPU < 15% on physical ARM64 Linux board | **Not implemented or not verified** | Requires physical ARM64 hardware |
| **Power-Cut Durability** | Sudden physical power removal on flash storage during active writes | **Not implemented or not verified** | Requires hardware relay / physical power cycling |
| **Physical Sensor / Live Mock** | End-to-end collection against physical sensors or external proprietary mock binary | **Not implemented or not verified** | External mock binary was not provided in repo |
| **Production Cloud Endpoint** | End-to-end delivery to live production AWS/Azure/GCP cloud service | **Not implemented or not verified** | Abstract & HTTP transports tested locally |

---

## 2. Architecture Overview

```
+---------------------------------------------------------------------------------+
|                                 EDGE SYSTEM                                     |
|                                                                                 |
|   +-------------------+        +--------------------+        +---------------+  |
|   | Sensor 1 (ECG)    |  TCP   | TCP Collector      | SQLite | SQLite Store  |  |
|   | Sensor 2 (Motion) |------->| - CRC32 Validator  |------->| (WAL Mode,    |  |
|   +-------------------+        | - Resume protocol  | BATCH  | Composite PK) |  |
|                                +--------------------+ INSERT +---------------+  |
|                                                                      |          |
|                                       +------------------------------+          |
|                                       |                              |          |
|                                       v                              v          |
|                             +--------------------+        +-------------------+ |
|                             | Window Processor   |        | Cloud Sync Engine | |
|                             | - 1s window metrics|        | - Durable ACKs    | |
|                             | - S2 magnitude     |        | - Backlog drain   | |
|                             | - 800-sample peaks |        | - Backoff retries | |
|                             | - 5s Heart Rate    |        +-------------------+ |
|                             +--------------------+                   |          |
|                                       |                              |          |
+---------------------------------------|------------------------------|----------+
                                        |                              v
                                  CLI Results                   Cloud Transport
                                (JSONL stdout)                 (Pending durable ACK)
```

---

## 3. Quickstart & Testing

### Setup & Installation
The pipeline runtime requires **Python 3.10+** and has **zero external runtime dependencies** (built strictly using Python's standard library: `sqlite3`, `socket`, `json`, `urllib`, `argparse`, `math`, `time`). The only development dependency is `pytest` for the automated test suite.

```bash
# 1. Enter repository
cd mayacardio-heartbeat

# 2. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install test dependency
pip install pytest
```

### Running Test Suite (58 Passing Tests)
```bash
PATH=".venv/bin:$PATH" PYTHONPATH=src python -m pytest -v
```

---

## 4. CLI Commands & Usage

The pipeline CLI is accessible via `pipeline` when `.venv/bin` is in `PATH`, or directly via Python module:
```bash
export PATH=".venv/bin:$PATH"
pipeline <command>

# Or directly without PATH modifications:
PYTHONPATH=src python3 -m heartbeat_pipeline.cli <command>
```

### 1. `pipeline status`
Emits a single JSON line to stdout reporting pipeline mode, total samples saved, acknowledged cloud samples, and pending local backlog:
```bash
.venv/bin/pipeline status
# Output:
# {"mode": "online", "samples_saved": 12000, "samples_in_cloud": 10000, "backlog": 2000}
```

### 2. `pipeline export --session <ID> --from device|cloud`
Exports session telemetry in JSON Lines (JSONL) format:
- `--from device`: Emits all samples saved locally.
- `--from cloud`: Emits only acknowledged samples (`synced = 1`).

Both exports follow sample records with applicable loss records:
```bash
.venv/bin/pipeline export --session session-1 --from device
```
Sample output:
```json
{"sensor": 1, "index": 0, "values": [1024, 2048, -512]}
{"sensor": 1, "index": 1, "values": [1030, 2040, -508]}
{"sensor": 1, "from_index": 450, "to_index": null, "reason": "Sensor session restarted; end of loss is unknown"}
```

### 3. `pipeline results --session <ID>`
Emits clinical window analysis results for each 1-second window $[400w, 400w+399]$ up to the session's maximum sample index:
```bash
.venv/bin/pipeline results --session session-1
```
Sample output:
```json
{"window_index": 4, "session_id": "session-1", "mode": "online", "sensor_1": {"available_samples": 400, "missing_samples": 0, "completeness": 1.0, "channels": {"ch1": {"mean": 1024.5, "min": 980, "max": 1060}, "ch2": {"mean": 2050.1, "min": 1900, "max": 2200}, "ch3": {"mean": -510.2, "min": -550, "max": -480}}}, "sensor_2": {"available_samples": 400, "magnitude": 5.25, "missing_samples": 0, "completeness": 1.0, "channels": {"ch1": {"mean": 3.1, "min": 2.8, "max": 3.4}, "ch2": {"mean": 4.2, "min": 3.9, "max": 4.5}}}, "heart_rate": 72.4}
```

---

## 5. Live Demonstration Plan

To demonstrate the pipeline end-to-end:
1. **Initial Status**: Run `.venv/bin/pipeline status` to verify empty/initial storage.
2. **Start Ingestion**: Connect collector to Sensor 1 (`port 9001`) and Sensor 2 (`port 9002`). The collector sends `{"cmd":"read","sensor":X}` and begins streaming batches into SQLite.
3. **Trigger Cloud Outage**: Disconnect the network interface or point transport to an unreachable endpoint.
4. **Observe Offline Backlog**: Run `.venv/bin/pipeline status`. Verify that `samples_saved` increases while `samples_in_cloud` remains constant and `backlog` grows.
5. **Local Clinical Processing**: Execute `.venv/bin/pipeline results --session <ID>` while offline. Verify that 1-second window completeness, per-channel metrics, Sensor 2 magnitude, and 5-second heart rate are calculated locally and emit `"mode": "offline"`.
6. **Restore Connectivity & Recover Backlog**: Re-enable the cloud endpoint and execute `synchronizer.sync_all()`. Observe batches draining in sequence. Run `.venv/bin/pipeline status` to verify `backlog` returns to 0.
7. **Verification of Exports**: Run `.venv/bin/pipeline export --session <ID> --from device` and compare with `--from cloud` to confirm exact alignment.

---

## 6. Disclosures & Testing Boundaries

- **Local Test Environment**: All 58 automated tests were executed on macOS (Darwin 24.3.0, arm64) using Python 3.13.5 and pytest 9.1.1.
- **Sensor Mock Availability**: An external mock binary or standalone sensor executable was not present in the workspace or environment. Integration testing uses local protocol-compliant `FakeSensorServer` sockets.
- **Target Hardware & Cloud Notice**: Physical power-cut durability testing on flash memory, embedded ARM64 Linux resource profiling (< 50 MB RSS), physical biomedical sensor telemetry, and live production cloud connections have **not** been performed and remain subject to target hardware verification.
- **AI Tool Usage**: Developed with the assistance of Google DeepMind Antigravity / Gemini paired programming agent. All specifications, algorithms, tests, and documentation have been strictly audited against the MayaCardio assessment requirements.
