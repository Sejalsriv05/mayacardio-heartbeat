
import sqlite3
from pathlib import Path
from typing import Any


SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    session_id TEXT NOT NULL,
    sensor INTEGER NOT NULL,
    sample_index INTEGER NOT NULL,
    values_json TEXT NOT NULL,
    synced INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, sensor, sample_index)
);

CREATE TABLE IF NOT EXISTS losses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    sensor INTEGER NOT NULL,
    from_index INTEGER NOT NULL,
    to_index INTEGER,
    reason TEXT NOT NULL
);
"""


class Storage:
    def __init__(self, db_path: str = "heartbeat.db"):
        self.db_path = str(Path(db_path))
        self.connection = sqlite3.connect(
            self.db_path,
            timeout=30,
        )
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(SCHEMA)

        # Migrate databases created with the older NOT NULL constraint.
        columns = self.connection.execute(
            "PRAGMA table_info(losses)"
        ).fetchall()
        to_index_column = next(
            column for column in columns if column[1] == "to_index"
        )

        if to_index_column[3] == 1:
            self.connection.executescript(
                """
                CREATE TABLE losses_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    sensor INTEGER NOT NULL,
                    from_index INTEGER NOT NULL,
                    to_index INTEGER,
                    reason TEXT NOT NULL
                );

                INSERT INTO losses_new
                    (id, session_id, sensor, from_index, to_index, reason)
                SELECT
                    id, session_id, sensor, from_index, to_index, reason
                FROM losses;

                DROP TABLE losses;
                ALTER TABLE losses_new RENAME TO losses;
                """
            )

        self.connection.commit()

    def save_sample(
        self,
        session_id: str,
        sensor: int,
        sample_index: int,
        values_json: str,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO samples
                    (session_id, sensor, sample_index, values_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id, sensor, sample_index)
                DO NOTHING
                """,
                (session_id, sensor, sample_index, values_json),
            )

    def save_samples_batch(
        self,
        samples: list[tuple[str, int, int, str]],
    ) -> None:
        """Persist a batch of samples atomically within a single transaction."""
        if not samples:
            return
        with self.connection:
            self.connection.executemany(
                """
                INSERT INTO samples
                    (session_id, sensor, sample_index, values_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id, sensor, sample_index)
                DO NOTHING
                """,
                samples,
            )

    def get_max_sample_index(
        self, session_id: str | None = None, sensor: int | None = None
    ) -> int | None:
        """Query the highest sample index recorded, optionally filtered by session and/or sensor."""
        query = "SELECT MAX(sample_index) FROM samples WHERE 1=1"
        params: list[Any] = []
        if session_id is not None:
            query += " AND session_id = ?"
            params.append(session_id)
        if sensor is not None:
            query += " AND sensor = ?"
            params.append(sensor)
        with self.connection:
            row = self.connection.execute(query, params).fetchone()
            return row[0] if row and row[0] is not None else None

    def get_sample_count(self, session_id: str) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) FROM samples WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        return row[0]

    def get_unsynced_samples(self, session_id: str, limit: int = 1000):
        return self.connection.execute(
            """
            SELECT session_id, sensor, sample_index, values_json
            FROM samples
            WHERE session_id = ? AND synced = 0
            ORDER BY sensor, sample_index
            LIMIT ?
            """,
            (session_id, limit),
        ).fetchall()

    def mark_sample_synced(
        self, session_id: str, sensor: int, sample_index: int
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                UPDATE samples
                SET synced = 1
                WHERE session_id = ? AND sensor = ? AND sample_index = ?
                """,
                (session_id, sensor, sample_index),
            )

    def save_loss(
        self,
        session_id: str,
        sensor: int,
        from_index: int,
        to_index: int | None,
        reason: str,
    ) -> None:
        if from_index < 0:
            raise ValueError("from_index must be non-negative")
        if to_index is not None and to_index < from_index:
            raise ValueError("to_index must be >= from_index")

        with self.connection:
            self.connection.execute(
                """
                INSERT INTO losses
                    (session_id, sensor, from_index, to_index, reason)
                VALUES (?, ?, ?, ?, ?)
                """,
                (session_id, sensor, from_index, to_index, reason),
            )

    def get_losses(self, session_id: str):
        return self.connection.execute(
            """
            SELECT sensor, from_index, to_index, reason
            FROM losses
            WHERE session_id = ?
            ORDER BY sensor, from_index
            """,
            (session_id,),
        ).fetchall()

    def close(self) -> None:
        self.connection.close()


def test_missing_sample_range_is_recorded(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))

    try:
        storage.save_loss(
            "session-1",
            1,
            100,
            104,
            "sensor_gap",
        )

        losses = storage.get_losses("session-1")

        assert len(losses) == 1
        assert losses[0] == (1, 100, 104, "sensor_gap")
    finally:
        storage.close()
