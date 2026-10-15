"""Command-Line Interface for MayaCardio 'Never Lose a Heartbeat' Pipeline.

Supports:
1. pipeline status: emits single JSON line with mode, samples_saved, samples_in_cloud, backlog.
2. pipeline export --session ID --from device|cloud: emits JSONL sample records followed by loss records.
3. pipeline results --session ID: emits JSONL 1-second window processing results.
"""

import argparse
import json
import os
import sys
from typing import Sequence

from heartbeat_pipeline.processing import process_window
from heartbeat_pipeline.storage import Storage


def build_parser() -> argparse.ArgumentParser:
    """Build and configure the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="MayaCardio Telemetry Pipeline CLI",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # 1. pipeline status
    p_status = subparsers.add_parser(
        "status",
        help="Print JSON line containing pipeline status metrics",
    )
    p_status.add_argument(
        "--mode",
        choices=["online", "offline"],
        default="online",
        help="Operational mode (default: online)",
    )
    p_status.add_argument(
        "--session",
        default=None,
        help="Optional session filter",
    )
    p_status.add_argument(
        "--db",
        default=None,
        help="Path to SQLite database file",
    )

    # 2. pipeline export --session ID --from device|cloud
    p_export = subparsers.add_parser(
        "export",
        help="Export session samples and applicable loss records as JSONL",
    )
    p_export.add_argument(
        "--session",
        required=True,
        help="Session identifier to export",
    )
    p_export.add_argument(
        "--from",
        dest="source",
        choices=["device", "cloud"],
        required=True,
        help="Data source scope: device (all saved) or cloud (synced only)",
    )
    p_export.add_argument(
        "--db",
        default=None,
        help="Path to SQLite database file",
    )

    # 3. pipeline results --session ID
    p_results = subparsers.add_parser(
        "results",
        help="Emit JSONL window analysis results for a session",
    )
    p_results.add_argument(
        "--session",
        required=True,
        help="Session identifier to analyze",
    )
    p_results.add_argument(
        "--mode",
        choices=["online", "offline"],
        default="online",
        help="Operational mode to record in results (default: online)",
    )
    p_results.add_argument(
        "--window",
        type=int,
        default=None,
        help="Optional specific window index to process",
    )
    p_results.add_argument(
        "--db",
        default=None,
        help="Path to SQLite database file",
    )

    return parser


def cmd_status(args: argparse.Namespace, storage: Storage) -> int:
    """Handle 'pipeline status' command."""
    with storage.connection:
        if args.session:
            row = storage.connection.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN synced = 1 THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN synced = 0 THEN 1 ELSE 0 END), 0)
                FROM samples
                WHERE session_id = ?
                """,
                (args.session,),
            ).fetchone()
        else:
            row = storage.connection.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN synced = 1 THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN synced = 0 THEN 1 ELSE 0 END), 0)
                FROM samples
                """
            ).fetchone()

        samples_saved = row[0] if row else 0
        samples_in_cloud = row[1] if row else 0
        backlog = row[2] if row else 0

    status_data = {
        "mode": args.mode,
        "samples_saved": samples_saved,
        "samples_in_cloud": samples_in_cloud,
        "backlog": backlog,
    }
    sys.stdout.write(json.dumps(status_data) + "\n")
    sys.stdout.flush()
    return 0


def cmd_export(args: argparse.Namespace, storage: Storage) -> int:
    """Handle 'pipeline export --session ID --from device|cloud' command."""
    session_id = args.session
    source = args.source

    with storage.connection:
        if source == "cloud":
            sample_rows = storage.connection.execute(
                """
                SELECT sensor, sample_index, values_json
                FROM samples
                WHERE session_id = ? AND synced = 1
                ORDER BY sensor, sample_index
                """,
                (session_id,),
            ).fetchall()
        else:  # "device"
            sample_rows = storage.connection.execute(
                """
                SELECT sensor, sample_index, values_json
                FROM samples
                WHERE session_id = ?
                ORDER BY sensor, sample_index
                """,
                (session_id,),
            ).fetchall()

        # Emit sample records in JSONL format
        for sensor, sample_index, values_json in sample_rows:
            sample_record = {
                "sensor": sensor,
                "index": sample_index,
                "values": json.loads(values_json),
            }
            sys.stdout.write(json.dumps(sample_record) + "\n")

        # Emit applicable loss records
        loss_rows = storage.get_losses(session_id)
        for sensor, from_index, to_index, reason in loss_rows:
            loss_record = {
                "sensor": sensor,
                "from_index": from_index,
                "to_index": to_index,
                "reason": reason,
            }
            sys.stdout.write(json.dumps(loss_record) + "\n")

    sys.stdout.flush()
    return 0


def cmd_results(args: argparse.Namespace, storage: Storage) -> int:
    """Handle 'pipeline results --session ID' command."""
    session_id = args.session
    mode = args.mode

    if args.window is not None:
        res = process_window(
            storage=storage,
            session_id=session_id,
            window_index=args.window,
            mode=mode,
        )
        sys.stdout.write(json.dumps(res) + "\n")
        sys.stdout.flush()
        return 0

    with storage.connection:
        row = storage.connection.execute(
            """
            SELECT MAX(sample_index)
            FROM samples
            WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()

    max_idx = row[0] if row and row[0] is not None else None
    if max_idx is None:
        # Empty session with no recorded samples
        return 0

    max_window = max_idx // 400
    for w in range(max_window + 1):
        res = process_window(
            storage=storage,
            session_id=session_id,
            window_index=w,
            mode=mode,
        )
        sys.stdout.write(json.dumps(res) + "\n")

    sys.stdout.flush()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Main CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    db_path = args.db or os.environ.get("HEARTBEAT_DB", "heartbeat.db")
    storage = Storage(db_path=db_path)
    try:
        if args.subcommand == "status":
            return cmd_status(args, storage)
        elif args.subcommand == "export":
            return cmd_export(args, storage)
        elif args.subcommand == "results":
            return cmd_results(args, storage)
        else:
            parser.print_help()
            return 1
    finally:
        storage.close()


if __name__ == "__main__":
    sys.exit(main())
