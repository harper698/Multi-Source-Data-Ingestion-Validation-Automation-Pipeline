#!/usr/bin/env python3
"""Config-driven CLI for Multi-Source Ingestion & Webhook/Report Automation Workflow."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import List, Optional

from src.config import load_config
from src.logging_setup import setup_logging
from src.pipeline import IngestionPipeline

DEFAULT_CONFIG_CANDIDATES = (
    "config/pipeline.yaml",
    "config/pipeline.example.yaml",
)

logger = logging.getLogger("pipeline.cli")


def _resolve_default_config() -> str:
    for candidate in DEFAULT_CONFIG_CANDIDATES:
        if Path(candidate).is_file():
            return candidate
    return DEFAULT_CONFIG_CANDIDATES[-1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generic Multi-Source Ingestion & Webhook/Report Automation Workflow",
    )
    parser.add_argument(
        "--config",
        default=_resolve_default_config(),
        help="Path to YAML pipeline config (default: config/pipeline.yaml or config/pipeline.example.yaml)",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default=None,
        help="Run mode: full or incremental (defaults to config setting)",
    )
    parser.add_argument(
        "--source",
        "--sources",
        dest="sources",
        nargs="+",
        default=None,
        help="Optional subset of configured source names to run (e.g. --source sample_telemetry)",
    )
    parser.add_argument(
        "--mock",
        "--offline",
        dest="mock",
        action="store_true",
        help="Use offline mock/fallback sample data instead of live HTTP calls",
    )
    parser.add_argument(
        "--inject-anomalies",
        action="store_true",
        help="Inject duplicate and invalid records in offline/mock mode to test validation & alerts",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run fetch, processing, and report preview without writing to SQLite or sending live webhooks",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["md", "json", "csv"],
        default=None,
        help="Report formats to generate (e.g. --formats md json csv)",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Override SQLite database path",
    )
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Override report output directory",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Clear saved incremental sync cursors (optionally scoped with --source) and exit",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Run repeatedly every N seconds until interrupted (SIGINT/SIGTERM)",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Optional maximum number of iterations when using --interval",
    )
    parser.add_argument(
        "--list-sources",
        action="store_true",
        help="List all configured sources from the YAML file and exit",
    )
    parser.add_argument(
        "--show-history",
        action="store_true",
        help="Display recent ingestion runs and saved sync cursors from SQLite and exit",
    )
    return parser


def _run_interval_loop(
    pipeline: IngestionPipeline,
    args: argparse.Namespace,
) -> int:
    """Execute the pipeline repeatedly on a fixed interval with clean signal handling."""
    interval_sec = float(args.interval)
    if interval_sec <= 0:
        raise ValueError("--interval must be a positive number of seconds")
    if args.max_runs is not None and args.max_runs <= 0:
        raise ValueError("--max-runs must be a positive integer")

    stop_event = threading.Event()
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    previous_sigint = signal.getsignal(signal.SIGINT)

    def _handle_shutdown(signum: int, _frame: object) -> None:
        sig_name = signal.Signals(signum).name
        logger.info("Received %s; shutting down interval runner cleanly.", sig_name)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    iteration = 0
    last_exit_code = 0
    logger.info(
        "Starting scheduled interval loop (interval=%.1fs, max_runs=%s)",
        interval_sec,
        args.max_runs if args.max_runs is not None else "unbounded",
    )

    try:
        while not stop_event.is_set():
            iteration += 1
            tick_start = time.monotonic()
            logger.info("Interval iteration #%d starting", iteration)
            try:
                summary = pipeline.run(
                    mode=args.mode,
                    selected_sources=args.sources,
                    mock=args.mock,
                    inject_anomalies=args.inject_anomalies,
                    dry_run=args.dry_run,
                )
                last_exit_code = 0 if summary.status in ("SUCCESS", "PARTIAL_SUCCESS") else 1
            except Exception as exc:  # noqa: BLE001
                logger.exception("Unhandled error in interval iteration #%d: %s", iteration, exc)
                last_exit_code = 1

            if args.max_runs is not None and iteration >= args.max_runs:
                logger.info("Reached --max-runs=%d; exiting interval loop.", args.max_runs)
                break

            elapsed = time.monotonic() - tick_start
            wait_seconds = max(0.0, interval_sec - elapsed)
            if wait_seconds > 0 and not stop_event.is_set():
                logger.info("Sleeping %.1fs until next scheduled run", wait_seconds)
                stop_event.wait(timeout=wait_seconds)
    except KeyboardInterrupt:
        logger.info("Interrupted by user; exiting interval loop cleanly.")
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)

    return last_exit_code


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)
    pipeline_cfg = config.get("pipeline", {})
    setup_logging(
        log_dir=str(pipeline_cfg.get("log_dir", "logs")),
        level=str(pipeline_cfg.get("log_level", "INFO")),
    )

    if args.list_sources:
        sources = config.get("sources", {})
        print(f"Configured sources in {args.config}:")
        for name, cfg in sources.items():
            enabled = cfg.get("enabled", True)
            stype = cfg.get("type", name)
            endpoint = cfg.get("endpoint", "(built-in)")
            print(f"  - {name:20s} | type={stype:17s} | enabled={str(enabled):5s} | {endpoint}")
        return 0

    pipeline = IngestionPipeline(
        config=config,
        db_path_override=args.db,
        report_dir_override=args.report_dir,
        report_formats_override=args.formats,
    )

    if args.reset_state:
        cleared = pipeline.reset_state(source_names=args.sources)
        scope = ", ".join(args.sources) if args.sources else "all sources"
        print(f"Cleared {cleared} sync cursor(s) for {scope}.")
        return 0

    if args.show_history:
        runs = pipeline.storage.get_recent_runs(limit=10)
        if not runs:
            print("No ingestion runs recorded yet.")
        else:
            print(f"Recent ingestion runs (total stored metrics: {pipeline.storage.get_record_count()}):")
            for r in runs:
                print(
                    f"  - {r['run_id']} | {r['mode']:11s} | {r['status']:15s} | "
                    f"fetched={r['records_fetched']} valid={r['records_valid']} "
                    f"upserted={r['records_upserted']} invalid={r['records_invalid']} "
                    f"dups={r['duplicates_dropped']}"
                )
        states = pipeline.storage.get_all_sync_states()
        if states:
            print("Saved incremental sync cursors:")
            for s in states:
                print(
                    f"  - {s['source_name']:20s} | last_record_ts={s['last_record_ts']} | "
                    f"last_success_at={s['last_success_at']} | run={s['last_run_id']}"
                )
        return 0

    if args.interval is not None:
        return _run_interval_loop(pipeline, args)

    summary = pipeline.run(
        mode=args.mode,
        selected_sources=args.sources,
        mock=args.mock,
        inject_anomalies=args.inject_anomalies,
        dry_run=args.dry_run,
    )

    return 0 if summary.status in ("SUCCESS", "PARTIAL_SUCCESS") else 1


if __name__ == "__main__":
    sys.exit(main())
