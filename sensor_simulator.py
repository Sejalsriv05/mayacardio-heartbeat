import json
import math
import socket
import threading
import time
import zlib

HOST = "127.0.0.1"
SESSION = "synthetic-demo-2026"
BATCH_SIZE = 100
SENSOR_PORTS = {1: 9001, 2: 9002}


def make_batch(sensor, start, count):
    data = []

    for index in range(start, start + count):
        t = index / 400.0

        if sensor == 1:
            # Synthetic three-channel ECG-like signal.
            phase = t % 0.83
            peak = math.exp(-((phase - 0.20) / 0.018) ** 2)
            ch1 = int(1200 * math.sin(2 * math.pi * 1.2 * t) + 9000 * peak)
            ch2 = int(800 * math.sin(2 * math.pi * 1.2 * t + 0.3) + 6000 * peak)
            ch3 = int(350 * math.sin(2 * math.pi * 0.25 * t))
            data.append([ch1, ch2, ch3])
        else:
            # Synthetic two-channel motion readings.
            ch1 = int(1200 * math.sin(2 * math.pi * 0.7 * t))
            ch2 = int(900 * math.cos(2 * math.pi * 0.7 * t))
            data.append([ch1, ch2])

    # CRC is calculated over the exact compact JSON array text.
    data_text = json.dumps(data, separators=(",", ":"))
    crc = zlib.crc32(data_text.encode("utf-8")) & 0xFFFFFFFF

    payload = {
        "session_id": SESSION,
        "sensor": sensor,
        "first_index": start,
        "count": len(data),
        "lost": [],
        "session_end": False,
        "crc32": f"{crc:08x}",
        "data": data,
    }

    # Keep the exact data representation used for the CRC.
    prefix = (
        '{"session_id":' + json.dumps(SESSION)
        + ',"sensor":' + str(sensor)
        + ',"first_index":' + str(start)
        + ',"count":' + str(len(data))
        + ',"lost":[],"session_end":false'
        + ',"crc32":' + json.dumps(f"{crc:08x}")
        + ',"data":'
    )
    return (prefix + data_text + "}\n").encode("utf-8")


def serve_sensor(sensor, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, port))
        server.listen()
        print(f"Synthetic Sensor {sensor} listening on {HOST}:{port}", flush=True)

        while True:
            conn, _ = server.accept()
            with conn:
                conn.settimeout(3)
                try:
                    request = b""
                    while b"\n" not in request:
                        chunk = conn.recv(4096)
                        if not chunk:
                            break
                        request += chunk

                    if not request:
                        continue

                    command = json.loads(request.split(b"\n", 1)[0].decode())
                    if command.get("cmd") != "read" or command.get("sensor") != sensor:
                        continue

                    start = int(command.get("from_index", 0))
                    for _ in range(20):
                        conn.sendall(make_batch(sensor, start, BATCH_SIZE))
                        start += BATCH_SIZE
                        time.sleep(0.03)
                except (OSError, ValueError, json.JSONDecodeError):
                    pass


for sensor, port in SENSOR_PORTS.items():
    threading.Thread(
        target=serve_sensor, args=(sensor, port), daemon=True
    ).start()

print("Synthetic-only test server running. Press Ctrl+C to stop.", flush=True)

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping synthetic sensor simulator.")
