"""TCP Telemetry Collector for MayaCardio 'Never Lose a Heartbeat' Pipeline.

Connects to sensor endpoints over TCP, receives newline-delimited JSON batches,
validates integrity via CRC32 and schema rules, and persists samples and explicit
loss records into SQLite storage.
"""

import json
import logging
import socket
import threading
import time
from typing import Any

from heartbeat_pipeline.storage import Storage
from heartbeat_pipeline.validator import InvalidResponse, validate_response_line

logger = logging.getLogger(__name__)


class Collector:
    """Collects ECG telemetry from a MayaCardio sensor over TCP."""

    def __init__(
        self,
        storage: Storage,
        sensor_id: int | None = None,
        sensor: int | None = None,
        host: str = "127.0.0.1",
        port: int = 9001,
        timeout: float = 5.0,
        max_retries: int = 3,
        retry_interval: float = 0.5,
        from_index: int | None = None,
    ) -> None:
        target_sensor = sensor_id if sensor_id is not None else sensor
        if target_sensor not in (1, 2):
            raise ValueError("Sensor must be 1 or 2")

        self.storage = storage
        self.sensor = target_sensor
        self.sensor_id = target_sensor
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.from_index = from_index

        # State tracking per session
        self.current_session_id: str | None = None
        self.last_sample_index: int | None = None
        self.is_session_ended: bool = False
        self.samples_collected: int = 0
        self.malformed_count: int = 0
        self.losses_recorded: int = 0

        # Concurrency / lifecycle control
        self._stop_event = threading.Event()
        self._sock: socket.socket | None = None

    def process_line(self, raw_line: str) -> dict[str, Any] | None:
        """Validate and persist a single line-delimited sensor response.

        Args:
            raw_line: Raw wire string received from sensor TCP socket.

        Returns:
            Validated payload dict if valid, or None if the response is malformed.
        """
        raw_line = raw_line.strip()
        if not raw_line:
            return None

        try:
            validated = validate_response_line(
                raw_line, expected_sensor=self.sensor
            )
        except InvalidResponse as exc:
            self.malformed_count += 1
            logger.warning(
                "Rejected malformed response for sensor %d: %s",
                self.sensor,
                exc,
            )
            return None

        session_id: str = validated["session_id"]

        # Handle session transition without silently merging or inventing indices
        if (
            self.current_session_id is not None
            and self.current_session_id != session_id
        ):
            # If the previous session was interrupted before session_end and had samples,
            # document the loss for the interrupted session with unknown end index.
            if not self.is_session_ended and self.last_sample_index is not None:
                restart_from = self.last_sample_index + 1
                logger.info(
                    "Session transition (%s -> %s). Recording restart loss for %s from %d.",
                    self.current_session_id,
                    session_id,
                    self.current_session_id,
                    restart_from,
                )
                self.storage.save_loss(
                    session_id=self.current_session_id,
                    sensor=self.sensor,
                    from_index=restart_from,
                    to_index=None,
                    reason="Sensor session restarted; end of loss is unknown",
                )
                self.losses_recorded += 1

            self.current_session_id = session_id
            self.last_sample_index = None
            self.is_session_ended = False
        elif self.current_session_id is None:
            self.current_session_id = session_id
            self.last_sample_index = None
            self.is_session_ended = False

        # Record explicitly reported loss ranges (including unknown end indices)
        for loss in validated["lost"]:
            from_index = loss["from_index"]
            to_index = loss.get("to_index")
            reason = loss["reason"]
            self.storage.save_loss(
                session_id=session_id,
                sensor=self.sensor,
                from_index=from_index,
                to_index=to_index,
                reason=reason,
            )
            self.losses_recorded += 1

        # Persist samples (deduplication handled by storage primary key constraint)
        first_index = validated["first_index"]
        sample_rows = [
            (session_id, self.sensor, first_index + offset, json.dumps(sample))
            for offset, sample in enumerate(validated["data"])
        ]
        if hasattr(self.storage, "save_samples_batch"):
            self.storage.save_samples_batch(sample_rows)
        else:
            for s_id, sens, s_idx, v_json in sample_rows:
                self.storage.save_sample(
                    session_id=s_id,
                    sensor=sens,
                    sample_index=s_idx,
                    values_json=v_json,
                )

        if sample_rows:
            last_in_batch = first_index + len(sample_rows) - 1
            if (
                self.last_sample_index is None
                or last_in_batch > self.last_sample_index
            ):
                self.last_sample_index = last_in_batch
        self.samples_collected += len(sample_rows)

        if validated.get("session_end", False):
            self.is_session_ended = True

        return validated

    def build_read_request(self, from_index: int | None = None) -> dict[str, Any]:
        """Build the protocol-compliant JSON read command.

        Format: {"cmd": "read", "sensor": 1} or {"cmd": "read", "sensor": 1, "from_index": N}.
        """
        req: dict[str, Any] = {"cmd": "read", "sensor": self.sensor}
        if from_index is not None:
            req["from_index"] = from_index
        return req

    def send_read_request(
        self, sock: socket.socket, from_index: int | None = None
    ) -> None:
        """Send newline-delimited JSON read request over connected socket."""
        req = self.build_read_request(from_index=from_index)
        wire = (json.dumps(req, separators=(",", ":")) + "\n").encode("utf-8")
        sock.sendall(wire)

    def collect_from_socket(
        self,
        sock: socket.socket,
        max_batches: int | None = None,
        stop_on_session_end: bool = False,
        send_request: bool = False,
        from_index: int | None = None,
    ) -> int:
        """Stream and process responses from an already-connected socket.

        Args:
            sock: Connected TCP socket.
            max_batches: Maximum number of valid batches to process.
            stop_on_session_end: If True, stop once session_end is True.
            send_request: If True, send initial read command before streaming.
            from_index: Optional starting index for resume command.

        Returns:
            Count of valid batches processed.
        """
        self._sock = sock
        if send_request:
            self.send_read_request(sock, from_index=from_index)

        batches_processed = 0
        buffer = bytearray()

        while not self._stop_event.is_set():
            try:
                chunk = sock.recv(4096)
            except (socket.timeout, TimeoutError):
                logger.warning(
                    "Socket read timed out after %.1fs for sensor %d",
                    self.timeout,
                    self.sensor,
                )
                if self._stop_event.is_set():
                    break
                continue
            except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                logger.warning(
                    "Socket connection lost for sensor %d: %s",
                    self.sensor,
                    exc,
                )
                break

            if not chunk:
                # Connection closed by remote peer
                break

            buffer.extend(chunk)

            while b"\n" in buffer:
                newline_pos = buffer.find(b"\n")
                line_bytes = buffer[:newline_pos]
                del buffer[:newline_pos + 1]

                try:
                    line_str = line_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    self.malformed_count += 1
                    logger.warning(
                        "Invalid UTF-8 sequence received from sensor %d",
                        self.sensor,
                    )
                    continue

                result = self.process_line(line_str)
                if result is not None:
                    batches_processed += 1
                    if (
                        max_batches is not None
                        and batches_processed >= max_batches
                    ):
                        return batches_processed
                    if stop_on_session_end and result.get("session_end", False):
                        return batches_processed

        if buffer:
            try:
                line_str = buffer.decode("utf-8").strip()
                if line_str:
                    result = self.process_line(line_str)
                    if result is not None:
                        batches_processed += 1
            except UnicodeDecodeError:
                self.malformed_count += 1

        return batches_processed

    def collect(
        self,
        max_batches: int | None = None,
        stop_on_session_end: bool = False,
        from_index: int | None = None,
    ) -> int:
        """Connect to sensor and collect responses with automatic reconnection.

        Args:
            max_batches: Maximum valid batches to collect before returning.
            stop_on_session_end: Stop once a response with session_end=True is received.
            from_index: Optional starting sample index override for initial request.

        Returns:
            Total count of valid batches processed.
        """
        self._stop_event.clear()
        total_batches = 0
        retries = 0

        while not self._stop_event.is_set():
            sock = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                logger.info(
                    "Connecting to sensor %d at %s:%d...",
                    self.sensor,
                    self.host,
                    self.port,
                )
                sock.connect((self.host, self.port))
                retries = 0  # Reset counter on successful connect

                if self.last_sample_index is not None:
                    req_from_index = self.last_sample_index + 1
                elif from_index is not None:
                    req_from_index = from_index
                else:
                    req_from_index = self.from_index

                remaining = (
                    max_batches - total_batches
                    if max_batches is not None
                    else None
                )
                processed = self.collect_from_socket(
                    sock,
                    max_batches=remaining,
                    stop_on_session_end=stop_on_session_end,
                    send_request=True,
                    from_index=req_from_index,
                )
                total_batches += processed

                if max_batches is not None and total_batches >= max_batches:
                    break
                if stop_on_session_end and self.is_session_ended:
                    break

            except (
                ConnectionRefusedError,
                ConnectionResetError,
                BrokenPipeError,
                socket.error,
                OSError,
            ) as exc:
                retries += 1
                logger.warning(
                    "Network error connecting to sensor %d (%s:%d): %s (retry %d/%d)",
                    self.sensor,
                    self.host,
                    self.port,
                    exc,
                    retries,
                    self.max_retries,
                )
                if retries > self.max_retries or self._stop_event.is_set():
                    break
                time.sleep(self.retry_interval)
            finally:
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
                self._sock = None

            if self._stop_event.is_set() or self.max_retries == 0:
                break

        return total_batches

    def stop(self) -> None:
        """Signal the collector to terminate collection cleanly."""
        self._stop_event.set()
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._sock.close()
            except OSError:
                pass


TCPCollector = Collector

__all__ = ["Collector", "TCPCollector"]
