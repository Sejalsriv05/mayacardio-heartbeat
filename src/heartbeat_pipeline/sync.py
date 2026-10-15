"""Cloud Telemetry Synchronizer for MayaCardio 'Never Lose a Heartbeat' Pipeline.

Reads pending unsynced samples from SQLite storage, delivers them to a durable
cloud transport with retries and backoff, and marks samples as synchronized only
upon explicit durable acknowledgement. Preserves original session IDs, sample
indices, and separate channel values without artificial channel merging.
"""

from dataclasses import dataclass, field
import json
import logging
import time
from typing import Any, Callable, Protocol, Sequence
import urllib.error
import urllib.request

from heartbeat_pipeline.storage import Storage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncRecord:
    """Represents a discrete sensor sample prepared for cloud synchronization."""

    session_id: str
    sensor: int
    sample_index: int
    values: list[int]

    def to_dict(self) -> dict[str, Any]:
        """Convert sample to dictionary for wire transport serialization."""
        return {
            "session_id": self.session_id,
            "sensor": self.sensor,
            "sample_index": self.sample_index,
            "values": list(self.values),
        }


@dataclass
class SyncAck:
    """Durable acknowledgement received from the cloud transport endpoint."""

    success: bool
    acknowledged_keys: list[tuple[str, int, int]] = field(default_factory=list)
    error_message: str | None = None


class CloudTransport(Protocol):
    """Protocol defining the interface for durable cloud telemetry delivery."""

    def send_batch(self, batch: Sequence[SyncRecord]) -> SyncAck | bool:
        """Transmit a batch of samples to the cloud.

        Args:
            batch: Sequence of SyncRecord instances to deliver.

        Returns:
            SyncAck containing durable acknowledgement or boolean success flag.
        """
        ...


@dataclass
class SyncStats:
    """Operational metrics collected during a synchronization run."""

    samples_read: int = 0
    samples_acknowledged: int = 0
    batches_attempted: int = 0
    batches_succeeded: int = 0
    batches_failed: int = 0
    errors: list[str] = field(default_factory=list)

    def merge(self, other: "SyncStats") -> None:
        """Merge statistics from another synchronization run."""
        self.samples_read += other.samples_read
        self.samples_acknowledged += other.samples_acknowledged
        self.batches_attempted += other.batches_attempted
        self.batches_succeeded += other.batches_succeeded
        self.batches_failed += other.batches_failed
        self.errors.extend(other.errors)


class InMemoryCloudTransport:
    """In-memory mock transport for testing and local simulation."""

    def __init__(
        self,
        fail_first_n_batches: int = 0,
        fail_permanently: bool = False,
    ) -> None:
        self.received_records: list[SyncRecord] = []
        self.batches_received: int = 0
        self.fail_first_n_batches = fail_first_n_batches
        self.fail_permanently = fail_permanently

    def send_batch(self, batch: Sequence[SyncRecord]) -> SyncAck:
        self.batches_received += 1
        if self.fail_permanently or self.batches_received <= self.fail_first_n_batches:
            return SyncAck(
                success=False,
                acknowledged_keys=[],
                error_message="Simulated cloud transport failure",
            )
        self.received_records.extend(batch)
        return SyncAck(
            success=True,
            acknowledged_keys=[
                (r.session_id, r.sensor, r.sample_index) for r in batch
            ],
        )


class HttpCloudTransport:
    """Standard HTTPS cloud transport delivering telemetry via HTTP POST."""

    def __init__(
        self,
        endpoint_url: str,
        auth_token: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.auth_token = auth_token
        self.timeout = timeout

    def send_batch(self, batch: Sequence[SyncRecord]) -> SyncAck:
        """Deliver batch of samples to HTTPS endpoint."""
        payload = json.dumps([r.to_dict() for r in batch]).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "MayaCardio-Pipeline/1.0",
        }
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        req = urllib.request.Request(
            self.endpoint_url, data=payload, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                status = resp.status
                body = resp.read().decode("utf-8")
                if 200 <= status < 300:
                    return SyncAck(
                        success=True,
                        acknowledged_keys=[
                            (r.session_id, r.sensor, r.sample_index) for r in batch
                        ],
                    )
                else:
                    return SyncAck(
                        success=False,
                        error_message=f"HTTP {status}: {body}",
                    )
        except urllib.error.HTTPError as exc:
            return SyncAck(
                success=False,
                error_message=f"HTTPError {exc.code}: {exc.reason}",
            )
        except urllib.error.URLError as exc:
            return SyncAck(
                success=False,
                error_message=f"URLError: {exc.reason}",
            )
        except Exception as exc:
            return SyncAck(
                success=False,
                error_message=str(exc),
            )


class Synchronizer:
    """Synchronizes locally persisted sensor samples to cloud endpoints."""

    def __init__(
        self,
        storage: Storage,
        transport: CloudTransport | Callable[[Sequence[SyncRecord]], SyncAck | bool],
        batch_size: int = 500,
        max_retries: int = 3,
        backoff_factor: float = 0.05,
    ) -> None:
        self.storage = storage
        self.transport = transport
        self.batch_size = max(1, batch_size)
        self.max_retries = max(0, max_retries)
        self.backoff_factor = max(0.0, backoff_factor)
        self.mode: str = "online"

    @property
    def is_online(self) -> bool:
        """Return True if synchronizer is currently operating in online mode."""
        return self.mode == "online"

    def set_mode(self, mode: str) -> None:
        """Manually update operational mode ('online' or 'offline')."""
        if mode not in ("online", "offline"):
            raise ValueError("Mode must be 'online' or 'offline'")
        self.mode = mode

    def get_unsynced_sessions(self) -> list[str]:
        """Retrieve a list of all distinct session IDs with pending unsynced samples."""
        with self.storage.connection:
            rows = self.storage.connection.execute(
                """
                SELECT DISTINCT session_id
                FROM samples
                WHERE synced = 0
                ORDER BY session_id
                """
            ).fetchall()
        return [row[0] for row in rows]

    def _invoke_transport(self, batch: Sequence[SyncRecord]) -> SyncAck:
        """Call transport handler and normalize response into a SyncAck."""
        if hasattr(self.transport, "send_batch"):
            result = self.transport.send_batch(batch)
        elif callable(self.transport):
            result = self.transport(batch)
        else:
            raise TypeError("Transport must implement send_batch or be callable")

        if isinstance(result, SyncAck):
            return result
        elif result is True:
            # Full acknowledgement of all records in this batch
            return SyncAck(
                success=True,
                acknowledged_keys=[
                    (r.session_id, r.sensor, r.sample_index) for r in batch
                ],
            )
        else:
            return SyncAck(
                success=False,
                acknowledged_keys=[],
                error_message="Transport returned false or unacknowledged",
            )

    def send_batch_with_retry(self, batch: Sequence[SyncRecord]) -> SyncAck:
        """Send a single batch to transport with exponential backoff on retryable failures."""
        last_error = None
        last_ack = None

        for attempt in range(self.max_retries + 1):
            try:
                ack = self._invoke_transport(batch)
                last_ack = ack
                if ack.success:
                    return ack
                last_error = ack.error_message or "Transport reported failure"
                logger.warning(
                    "Cloud sync batch attempt %d/%d failed: %s",
                    attempt + 1,
                    self.max_retries + 1,
                    last_error,
                )
            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "Cloud sync batch attempt %d/%d encountered exception: %s",
                    attempt + 1,
                    self.max_retries + 1,
                    exc,
                )

            if attempt < self.max_retries and self.backoff_factor > 0:
                sleep_time = self.backoff_factor * (2**attempt)
                time.sleep(sleep_time)

        partial_keys = last_ack.acknowledged_keys if last_ack else []
        return SyncAck(
            success=False,
            acknowledged_keys=partial_keys,
            error_message=f"Exhausted {self.max_retries + 1} attempts. Last error: {last_error}",
        )

    def sync_session(
        self,
        session_id: str,
        max_batches: int | None = None,
    ) -> SyncStats:
        """Synchronize unsynced samples for a specific session to the cloud.

        Samples are strictly marked synced in SQLite only after durable confirmation
        from the transport.

        Args:
            session_id: Target session identifier to sync.
            max_batches: Optional limit on the number of batches to process.

        Returns:
            SyncStats tracking the result of the sync run.
        """
        stats = SyncStats()

        while True:
            if max_batches is not None and stats.batches_attempted >= max_batches:
                break

            rows = self.storage.get_unsynced_samples(
                session_id=session_id,
                limit=self.batch_size,
            )
            if not rows:
                break

            stats.samples_read += len(rows)
            stats.batches_attempted += 1

            batch = [
                SyncRecord(
                    session_id=row[0],
                    sensor=row[1],
                    sample_index=row[2],
                    values=json.loads(row[3]),
                )
                for row in rows
            ]

            valid_batch_keys = {
                (r.session_id, r.sensor, r.sample_index) for r in batch
            }

            ack = self.send_batch_with_retry(batch)

            # Mark only confirmed samples in storage
            confirmed_keys = [
                key for key in ack.acknowledged_keys if key in valid_batch_keys
            ]

            for s_id, sensor, sample_idx in confirmed_keys:
                self.storage.mark_sample_synced(
                    session_id=s_id,
                    sensor=sensor,
                    sample_index=sample_idx,
                )
                stats.samples_acknowledged += 1

            if ack.success and len(confirmed_keys) == len(batch):
                stats.batches_succeeded += 1
                self.mode = "online"
            else:
                stats.batches_failed += 1
                self.mode = "offline"
                error_msg = ack.error_message or "Partial or unacknowledged batch"
                stats.errors.append(error_msg)
                logger.warning(
                    "Sync interrupted for session %s: %s. Remaining backlog preserved.",
                    session_id,
                    error_msg,
                )
                # Break to preserve remaining backlog and avoid hammering downstream while offline
                break

        return stats

    def sync_all(self, max_batches_per_session: int | None = None) -> SyncStats:
        """Synchronize all sessions with pending unsynced samples."""
        total_stats = SyncStats()
        sessions = self.get_unsynced_sessions()

        for session_id in sessions:
            session_stats = self.sync_session(
                session_id=session_id,
                max_batches=max_batches_per_session,
            )
            total_stats.merge(session_stats)

        return total_stats


__all__ = [
    "SyncRecord",
    "SyncAck",
    "CloudTransport",
    "InMemoryCloudTransport",
    "HttpCloudTransport",
    "SyncStats",
    "Synchronizer",
]
