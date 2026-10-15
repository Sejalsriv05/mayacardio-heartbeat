import json
import math
import pytest
from heartbeat_pipeline.processing import (
    WindowProcessor,
    calculate_channel_stats,
    calculate_heart_rate,
    calculate_sensor_2_magnitude,
    detect_peaks,
    process_window,
)
from heartbeat_pipeline.storage import Storage


def test_window_boundaries_exact(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    # Sample 399 belongs to window 0; sample 400 belongs to window 1
    storage.save_sample("session-1", 1, 399, "[10, 20, 30]")
    storage.save_sample("session-1", 1, 400, "[40, 50, 60]")

    w0 = process_window(storage, "session-1", window_index=0)
    w1 = process_window(storage, "session-1", window_index=1)

    assert w0["window_index"] == 0
    assert w0["sensor_1"]["available_samples"] == 1
    assert w0["sensor_1"]["missing_samples"] == 399
    assert w0["sensor_1"]["channels"]["ch1"]["mean"] == 10.0
    assert w0["sensor_1"]["ch1"]["mean"] == 10.0

    assert w1["window_index"] == 1
    assert w1["sensor_1"]["available_samples"] == 1
    assert w1["sensor_1"]["missing_samples"] == 399
    assert w1["sensor_1"]["channels"]["ch1"]["mean"] == 40.0
    assert w1["sensor_1"]["ch1"]["mean"] == 40.0

    storage.close()


def test_missing_samples_and_completeness(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    # Populate 300 samples for Sensor 1 and 400 samples for Sensor 2 in window 0
    for i in range(300):
        storage.save_sample("session-1", 1, i, "[1, 2, 3]")
    for i in range(400):
        storage.save_sample("session-1", 2, i, "[4, 5]")

    res = process_window(storage, "session-1", window_index=0)

    # Sensor 1
    assert res["sensor_1"]["available_samples"] == 300
    assert res["sensor_1"]["missing_samples"] == 100
    assert res["sensor_1"]["completeness"] == 0.75

    # Sensor 2
    assert res["sensor_2"]["available_samples"] == 400
    assert res["sensor_2"]["missing_samples"] == 0
    assert res["sensor_2"]["completeness"] == 1.0

    storage.close()


def test_empty_window_channel_stats_are_null(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    res = process_window(storage, "session-empty", window_index=0)

    assert res["sensor_1"]["available_samples"] == 0
    assert res["sensor_1"]["missing_samples"] == 400
    assert res["sensor_1"]["completeness"] == 0.0

    for ch in ("ch1", "ch2", "ch3"):
        stats = res["sensor_1"]["channels"][ch]
        assert stats["mean"] is None
        assert stats["minimum"] is None
        assert stats["maximum"] is None
        assert stats["min"] is None
        assert stats["max"] is None

        # Direct channel access
        assert res["sensor_1"][ch]["mean"] is None

    assert res["sensor_2"]["magnitude"] is None
    assert res["heart_rate"] is None

    storage.close()


def test_channel_statistics_calculation():
    samples = [[10, 100, -5], [20, 200, 0], [30, 300, 5]]
    stats = calculate_channel_stats(samples, channel_count=3)

    assert stats["ch1"]["mean"] == 20.0
    assert stats["ch1"]["minimum"] == 10
    assert stats["ch1"]["maximum"] == 30
    assert stats["ch1"]["min"] == 10
    assert stats["ch1"]["max"] == 30

    assert stats["ch2"]["mean"] == 200.0
    assert stats["ch2"]["minimum"] == 100
    assert stats["ch2"]["maximum"] == 300

    assert stats["ch3"]["mean"] == 0.0
    assert stats["ch3"]["minimum"] == -5
    assert stats["ch3"]["maximum"] == 5


def test_sensor_2_magnitude_calculation():
    # sqrt(3^2 + 4^2) = 5; sqrt(6^2 + 8^2) = 10 -> mean = 7.5
    samples = [[3, 4], [6, 8]]
    mag = calculate_sensor_2_magnitude(samples)
    assert mag == 7.5

    assert calculate_sensor_2_magnitude([]) is None


def test_peak_detection_rules():
    # Baseline signal of 1200 samples where max of previous 800 samples is 200
    signal = {}
    for i in range(1200):
        signal[i] = 50

    signal[700] = 200  # Historical max in previous 800 window

    # Test 1: Candidate at 850 exceeds threshold (120 > 0.5 * 200) and is greater than neighbors
    signal[849] = 50
    signal[850] = 120
    signal[851] = 50

    # Test 2: Candidate at 900 rejected by refractory constraint (850 + 50 = 900 < 100 distance)
    signal[899] = 50
    signal[900] = 130
    signal[901] = 50

    # Test 3: Candidate at 960 accepted (850 + 110 = 960 >= 100 distance)
    signal[959] = 50
    signal[960] = 140
    signal[961] = 50

    # Test 4: Candidate at 1080 rejected (value 80 <= half max of 200)
    signal[1079] = 50
    signal[1080] = 80
    signal[1081] = 50

    peaks = detect_peaks(signal)

    assert 850 in peaks
    assert 900 not in peaks  # Rejected: refractory violation
    assert 960 in peaks  # Accepted: distance 110 >= 100
    assert 1080 not in peaks  # Rejected: below threshold (80 <= 100)


def test_peak_requires_full_800_samples_no_short_history():
    signal = {}
    for i in range(1000):
        signal[i] = 50

    # Peak candidate at 500 (has only 500 preceding samples, < 800)
    signal[499] = 50
    signal[500] = 200
    signal[501] = 50

    peaks = detect_peaks(signal)
    # Must NOT detect 500 because full 800 preceding samples are not available
    assert 500 not in peaks

    # Now test sample at 900 with all 800 preceding samples available
    signal[899] = 50
    signal[900] = 250
    signal[901] = 50

    peaks_with_900 = detect_peaks(signal)
    assert 900 in peaks_with_900

    # If even ONE sample is missing in the preceding 800 samples (e.g. sample 450 deleted)
    del signal[450]
    peaks_with_gap = detect_peaks(signal)
    # Must NOT substitute a shorter history; sample 900 should not be detected
    assert 900 not in peaks_with_gap


def test_heart_rate_insufficient_initial_window(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    # For windows w = 0, 1, 2, 3, heart rate MUST be None
    for w in range(4):
        res = process_window(storage, "session-init", window_index=w)
        assert res["heart_rate"] is None

    storage.close()


def test_heart_rate_missing_data_returns_null(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    # In window w = 4, the 5-second window is [0, 1999] (2000 samples).
    # Provide 1999 samples (1 missing sample)
    for i in range(1999):
        storage.save_sample("session-miss", 1, i, "[0, 0, 0]")

    res = process_window(storage, "session-miss", window_index=4)
    assert res["heart_rate"] is None

    storage.close()


def test_heart_rate_too_few_peaks_returns_null(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    # 2000 flat samples (only 1 peak inserted at index 900)
    for i in range(2000):
        val = 150 if i == 900 else 50
        storage.save_sample("session-few", 1, i, f"[0, {val}, 0]")

    res = process_window(storage, "session-few", window_index=4)
    assert res["heart_rate"] is None

    storage.close()


def test_calculate_heart_rate_direct():
    # 2 peaks spaced by 400 samples (1.0 s) -> 60.0 BPM
    hr = calculate_heart_rate([400, 800], total_samples=2000, expected_samples=2000, sampling_rate=400)
    assert hr == 60.0

    # 3 peaks spaced by 350 samples: mean gap = 350 / 400 = 0.875 s -> 60 / 0.875 = 68.5714... -> 68.6 BPM
    hr2 = calculate_heart_rate([100, 450, 800], total_samples=2000, expected_samples=2000, sampling_rate=400)
    assert hr2 == 68.6

    # Missing samples -> None
    assert calculate_heart_rate([400, 800], total_samples=1999, expected_samples=2000) is None

    # Fewer than 2 peaks -> None
    assert calculate_heart_rate([400], total_samples=2000, expected_samples=2000) is None
    assert calculate_heart_rate([], total_samples=2000, expected_samples=2000) is None


def test_heart_rate_calculation_and_rounding(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    # 2000 samples with peaks spaced by 333 samples starting at 850
    # Samples in 5-second window [0, 1999]. Peaks at 850, 1183, 1516, 1849.
    # Each peak has >= 800 preceding samples in [0, 1999].
    # Consecutive gaps: [333, 333, 333]. Mean gap = 333 samples = 333 / 400 = 0.8325 s.
    # Heart rate = 60 / 0.8325 = 72.072... -> rounds to 72.1 BPM.
    peak_indices = {850, 1183, 1516, 1849}

    for i in range(2000):
        if i in peak_indices:
            val = 200
        elif (i + 1) in peak_indices or (i - 1) in peak_indices:
            val = 20
        else:
            val = 30
        storage.save_sample("session-hr", 1, i, f"[10, {val}, 5]")

    res = process_window(storage, "session-hr", window_index=4)
    assert res["heart_rate"] is not None
    assert res["heart_rate"] == 72.1

    storage.close()


def test_online_offline_mode_field(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    w_online = process_window(storage, "session-m", window_index=0, mode="online")
    assert w_online["mode"] == "online"

    w_offline = process_window(storage, "session-m", window_index=0, mode="offline")
    assert w_offline["mode"] == "offline"

    processor = WindowProcessor(storage, "session-m", mode="offline")
    assert processor.process_window(0)["mode"] == "offline"

    storage.close()


def test_refractory_spacing_boundary():
    # Signal with 1200 samples, background 50, historical max 200 at 700
    signal = {i: 50 for i in range(1200)}
    signal[700] = 200

    # First peak at 850
    signal[849] = 50
    signal[850] = 150
    signal[851] = 50

    # Candidate at 949 (distance = 99 < 100) -> rejected
    signal[948] = 50
    signal[949] = 160
    signal[949 + 1] = 50

    # Candidate at 950 (distance = 100 >= 100) -> accepted
    signal[950] = 170
    signal[951] = 50

    peaks = detect_peaks(signal)
    assert 850 in peaks
    assert 949 not in peaks
    assert 950 in peaks
