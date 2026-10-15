from heartbeat_pipeline.storage import Storage
from heartbeat_pipeline.collector import Collector

DB_PATH = "heartbeat.db"

storage = Storage(DB_PATH)

try:
    for sensor_id, port in [(1, 9001), (2, 9002)]:
        print(f"\nConnecting to synthetic sensor {sensor_id} on port {port}...")

        collector = Collector(
            storage=storage,
            sensor_id=sensor_id,
            host="127.0.0.1",
            port=port,
            timeout=5.0,
            max_retries=2,
            retry_interval=0.5,
        )

        batches = collector.collect(max_batches=5)

        print(f"Sensor: {sensor_id}")
        print(f"Valid batches received: {batches}")
        print(f"Samples collected: {collector.samples_collected}")
        print(f"Malformed batches: {collector.malformed_count}")
        print(f"Loss records: {collector.losses_recorded}")

finally:
    storage.close()

print("\nSynthetic collection test finished.")
