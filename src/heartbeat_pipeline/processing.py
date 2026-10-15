"""Signal Processing Engine for MayaCardio 'Never Lose a Heartbeat' Pipeline.

Processes windowed ECG telemetry into clinical features:
- One-second windows [400w, 400w + 399] for Sensor 1 and Sensor 2.
- Missing sample counts, completeness, and per-channel statistics (mean, minimum/min, maximum/max).
- Sensor 2 magnitude as the mean of sqrt(ch1^2 + ch2^2) over available samples.
- Sensor 1 Channel 2 peak detection with exact 800-sample historical threshold
  (without substituting shorter history) and 100-sample refractory constraint.
- 5-second window heart rate calculation using mean consecutive peak gap in seconds
  (60 / mean_gap_seconds) with null conditions (insufficient window, missing data,
  too few peaks) and 1-decimal-place rounding.
- Preserves online/offline mode metadata.
"""

import json
import math
from typing import Any

from heartbeat_pipeline.storage import Storage


def detect_peaks(
    signal_by_index: dict[int, int] | list[tuple[int, int]],
    threshold_history: int = 800,
    min_peak_distance: int = 100,
) -> list[int]:
    """Detect R-peaks in Sensor 1 Channel 2 according to assessment rules.

    Rules:
    1. Local maximum: value strictly greater than both immediate neighbors (i - 1 and i + 1).
    2. Peak-history requirement: strictly exceeds half the maximum of the previous 800 samples.
       Exactly 800 preceding samples must be present; shorter histories are NOT substituted.
    3. Refractory constraint: at least 100 samples between consecutive peaks.

    Args:
        signal_by_index: Mapping or list of (sample_index, value) pairs.
        threshold_history: Exact number of preceding samples required (default 800).
        min_peak_distance: Minimum refractory samples between peaks (default 100).

    Returns:
        List of sample indices where peaks were detected.
    """
    if isinstance(signal_by_index, dict):
        sorted_items = sorted(signal_by_index.items())
    else:
        sorted_items = sorted(signal_by_index, key=lambda x: x[0])

    if len(sorted_items) < threshold_history + 2:
        return []

    val_map = {idx: val for idx, val in sorted_items}
    detected_peaks: list[int] = []
    last_peak_idx: int | None = None

    indices = [idx for idx, _ in sorted_items]

    for idx in indices:
        # 1. Neighbor check: both adjacent samples must exist and be strictly smaller
        prev_val = val_map.get(idx - 1)
        next_val = val_map.get(idx + 1)
        if prev_val is None or next_val is None:
            continue

        current_val = val_map[idx]
        if current_val <= prev_val or current_val <= next_val:
            continue

        # 2. Refractory constraint: at least min_peak_distance from last peak
        if last_peak_idx is not None and (idx - last_peak_idx) < min_peak_distance:
            continue

        # 3. Peak-history requirement: exactly threshold_history preceding samples must exist
        history_start = idx - threshold_history
        if history_start < 0:
            # Fewer than 800 preceding samples exist; do not substitute a shorter history
            continue

        history_samples = [val_map.get(j) for j in range(history_start, idx)]
        if any(v is None for v in history_samples):
            # Preceding 800 samples have gaps/missing samples; do not substitute
            continue

        max_history = max(history_samples)
        if current_val <= 0.5 * max_history:
            continue

        detected_peaks.append(idx)
        last_peak_idx = idx

    return detected_peaks


def calculate_heart_rate(
    peak_indices: list[int],
    total_samples: int,
    expected_samples: int = 2000,
    sampling_rate: int = 400,
) -> float | None:
    """Calculate heart rate from peaks in the specified 5-second window.

    Follows the assessment's exact rule:
    1. Check missing data: total_samples must equal expected_samples (2000).
    2. Check too few peaks: at least 2 peaks required in the window.
    3. Calculate mean gap between consecutive detected peaks in seconds.
    4. Calculate 60 / mean_gap_seconds and round to one decimal place.

    Args:
        peak_indices: List of peak sample indices within the 5-second window.
        total_samples: Available sample count in the 5-second window.
        expected_samples: Required sample count (2000 for 5 seconds at 400 Hz).
        sampling_rate: Sensor sampling frequency in Hz (default 400).

    Returns:
        Heart rate in beats per minute rounded to 1 decimal place,
        or None if conditions (missing data, too few peaks) are not met.
    """
    # Missing data condition: all expected samples must be present
    if total_samples < expected_samples:
        return None

    # Too few peaks condition: at least 2 peaks needed to calculate consecutive gap
    if len(peak_indices) < 2:
        return None

    # Calculate gaps in seconds between consecutive detected peaks
    gaps_seconds = [
        (peak_indices[i + 1] - peak_indices[i]) / float(sampling_rate)
        for i in range(len(peak_indices) - 1)
    ]
    mean_gap_seconds = sum(gaps_seconds) / len(gaps_seconds)

    if mean_gap_seconds <= 0:
        return None

    heart_rate = 60.0 / mean_gap_seconds
    return round(heart_rate, 1)


def calculate_sensor_2_magnitude(samples: list[list[int]]) -> float | None:
    """Calculate Sensor 2 magnitude as the mean of sqrt(ch1^2 + ch2^2)."""
    if not samples:
        return None

    magnitudes = [math.sqrt(s[0] ** 2 + s[1] ** 2) for s in samples]
    return round(sum(magnitudes) / len(magnitudes), 2)


def calculate_channel_stats(
    samples: list[list[int]], channel_count: int
) -> dict[str, dict[str, float | int | None]]:
    """Compute mean, minimum, and maximum for each channel over available samples."""
    stats: dict[str, dict[str, float | int | None]] = {}

    for ch in range(channel_count):
        ch_name = f"ch{ch + 1}"
        if not samples:
            stats[ch_name] = {
                "mean": None,
                "minimum": None,
                "maximum": None,
                "min": None,
                "max": None,
            }
        else:
            values = [s[ch] for s in samples]
            stats[ch_name] = {
                "mean": round(sum(values) / len(values), 2),
                "minimum": min(values),
                "maximum": max(values),
                "min": min(values),
                "max": max(values),
            }

    return stats


def process_window(
    storage: Storage,
    session_id: str,
    window_index: int,
    mode: str = "online",
) -> dict[str, Any]:
    """Process a single 1-second window [400w, 400w + 399] for a given session.

    Args:
        storage: Storage instance containing persisted telemetry.
        session_id: Session identifier to query.
        window_index: 0-based integer window index w.
        mode: Operational mode string ("online" or "offline").

    Returns:
        Structured dictionary matching assessment specifications.
    """
    if window_index < 0:
        raise ValueError("window_index must be non-negative")

    start_idx = 400 * window_index
    end_idx = start_idx + 399

    # Query 1-second window samples for both sensors
    with storage.connection:
        rows = storage.connection.execute(
            """
            SELECT sensor, sample_index, values_json
            FROM samples
            WHERE session_id = ? AND sample_index BETWEEN ? AND ?
            ORDER BY sensor, sample_index
            """,
            (session_id, start_idx, end_idx),
        ).fetchall()

    s1_samples: dict[int, list[int]] = {}
    s2_samples: dict[int, list[int]] = {}

    for sensor, idx, v_json in rows:
        values = json.loads(v_json)
        if sensor == 1:
            s1_samples[idx] = values
        elif sensor == 2:
            s2_samples[idx] = values

    # Metrics for Sensor 1
    s1_avail = len(s1_samples)
    s1_missing = 400 - s1_avail
    s1_completeness = round(s1_avail / 400.0, 4)
    s1_channel_stats = calculate_channel_stats(
        list(s1_samples.values()), channel_count=3
    )

    # Metrics for Sensor 2
    s2_avail = len(s2_samples)
    s2_missing = 400 - s2_avail
    s2_completeness = round(s2_avail / 400.0, 4)
    s2_channel_stats = calculate_channel_stats(
        list(s2_samples.values()), channel_count=2
    )
    s2_mag = calculate_sensor_2_magnitude(list(s2_samples.values()))

    # Heart Rate Calculation over 5-second window [400*(w-4), 400*w + 399]
    heart_rate: float | None = None

    if window_index >= 4:
        # Sufficient initial window available (w >= 4)
        five_sec_start = 400 * (window_index - 4)
        five_sec_end = end_idx  # 400*w + 399

        # Need previous 800 samples before five_sec_start for peak threshold if available
        search_start = max(0, five_sec_start - 800)
        # Fetch up to five_sec_end + 1 for neighbor check on the last sample
        search_end = five_sec_end + 1

        with storage.connection:
            five_sec_rows = storage.connection.execute(
                """
                SELECT sample_index, values_json
                FROM samples
                WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?
                ORDER BY sample_index
                """,
                (session_id, search_start, search_end),
            ).fetchall()

        all_ch2_samples: dict[int, int] = {}
        five_sec_sample_count = 0

        for idx, v_json in five_sec_rows:
            vals = json.loads(v_json)
            all_ch2_samples[idx] = vals[1]  # Sensor 1 Channel 2 is index 1
            if five_sec_start <= idx <= five_sec_end:
                five_sec_sample_count += 1

        # Detect peaks across the signal range
        peaks = detect_peaks(all_ch2_samples, threshold_history=800, min_peak_distance=100)

        # Filter peaks that fall inside the specified 5-second window
        peaks_in_5s = [p for p in peaks if five_sec_start <= p <= five_sec_end]

        heart_rate = calculate_heart_rate(
            peak_indices=peaks_in_5s,
            total_samples=five_sec_sample_count,
            expected_samples=2000,
            sampling_rate=400,
        )

    return {
        "window": window_index,
        "window_index": window_index,
        "session_id": session_id,
        "mode": mode,
        "sensor_1": {
            "available_samples": s1_avail,
            "missing_samples": s1_missing,
            "completeness": s1_completeness,
            "channels": s1_channel_stats,
            "channel_stats": s1_channel_stats,
            "ch1": s1_channel_stats["ch1"],
            "ch2": s1_channel_stats["ch2"],
            "ch3": s1_channel_stats["ch3"],
        },
        "sensor_2": {
            "available_samples": s2_avail,
            "missing_samples": s2_missing,
            "completeness": s2_completeness,
            "channels": s2_channel_stats,
            "channel_stats": s2_channel_stats,
            "ch1": s2_channel_stats["ch1"],
            "ch2": s2_channel_stats["ch2"],
            "magnitude": s2_mag,
        },
        "heart_rate": heart_rate,
    }


class WindowProcessor:
    """Processes sequential windows of telemetry for a recording session."""

    def __init__(
        self,
        storage: Storage,
        session_id: str,
        mode: str = "online",
    ) -> None:
        self.storage = storage
        self.session_id = session_id
        self.mode = mode

    def process_window(self, window_index: int) -> dict[str, Any]:
        """Process a specific 1-second window."""
        return process_window(
            storage=self.storage,
            session_id=self.session_id,
            window_index=window_index,
            mode=self.mode,
        )

    def process_all_available_windows(self) -> list[dict[str, Any]]:
        """Process all available 1-second windows up to the highest sample index."""
        with self.storage.connection:
            row = self.storage.connection.execute(
                """
                SELECT MAX(sample_index)
                FROM samples
                WHERE session_id = ?
                """,
                (self.session_id,),
            ).fetchone()

        max_idx = row[0] if row and row[0] is not None else -1
        if max_idx < 0:
            return []

        max_window = max_idx // 400
        return [self.process_window(w) for w in range(max_window + 1)]


__all__ = [
    "detect_peaks",
    "calculate_heart_rate",
    "calculate_sensor_2_magnitude",
    "calculate_channel_stats",
    "process_window",
    "WindowProcessor",
]
