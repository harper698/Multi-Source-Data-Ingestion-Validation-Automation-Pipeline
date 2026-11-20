"""SQLite persistence layer with idempotent upserts, indexes, and run-history logging."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.models import CleanRecord, RunSummary, ValidationIssue

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    sources_requested TEXT NOT NULL DEFAULT '[]',
    sources_succeeded TEXT NOT NULL DEFAULT '[]',
    sources_failed TEXT NOT NULL DEFAULT '[]',
    records_fetched INTEGER NOT NULL DEFAULT 0,
    records_valid INTEGER NOT NULL DEFAULT 0,
    records_upserted INTEGER NOT NULL DEFAULT 0,
    records_invalid INTEGER NOT NULL DEFAULT 0,
    records_flagged INTEGER NOT NULL DEFAULT 0,
    duplicates_dropped INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS metrics_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT NOT NULL DEFAULT '',
    observed_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    is_flagged INTEGER NOT NULL DEFAULT 0,
    flag_reason TEXT,
    first_seen_run_id TEXT NOT NULL,
    last_updated_run_id TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    UNIQUE(source, entity_id, metric_name, observed_at),
    FOREIGN KEY(first_seen_run_id) REFERENCES ingestion_runs(run_id),
    FOREIGN KEY(last_updated_run_id) REFERENCES ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS validation_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    action TEXT NOT NULL,
    message TEXT NOT NULL,
    raw_payload_json TEXT NOT NULL DEFAULT '{}',
    logged_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS sync_state (
    source_name TEXT PRIMARY KEY,
    last_record_ts TEXT NOT NULL,
    last_success_at TEXT NOT NULL,
    last_run_id TEXT NOT NULL,
    FOREIGN KEY(last_run_id) REFERENCES ingestion_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_metrics_source_observed
    ON metrics_data(source, observed_at);

CREATE INDEX IF NOT EXISTS idx_metrics_entity_metric_observed
    ON metrics_data(entity_id, metric_name, observed_at);

CREATE INDEX IF NOT EXISTS idx_runs_started_at
    ON ingestion_runs(started_at);

CREATE INDEX IF NOT EXISTS idx_validation_issues_run_id
    ON validation_issues(run_id);
"""


class SQLiteStorage:
    """Manages SQLite schema, idempotent metric upserts, and ingestion run history."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        if str(self.db_path) != ":memory:":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        if str(self.db_path) != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)

    def start_run(
        self,
        run_id: str,
        mode: str,
        started_at: str,
        sources_requested: List[str],
    ) -> None:
        """Insert an initial RUNNING entry into ingestion_runs."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ingestion_runs (
                    run_id, mode, status, started_at, sources_requested
                ) VALUES (?, ?, 'RUNNING', ?, ?)
                """,
                (run_id, mode, started_at, json.dumps(sources_requested)),
            )

    def finish_run(self, summary: RunSummary) -> None:
        """Update ingestion_runs with final run metrics and status."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_runs
                SET status = ?,
                    finished_at = ?,
                    sources_succeeded = ?,
                    sources_failed = ?,
                    records_fetched = ?,
                    records_valid = ?,
                    records_upserted = ?,
                    records_invalid = ?,
                    records_flagged = ?,
                    duplicates_dropped = ?,
                    error_message = ?
                WHERE run_id = ?
                """,
                (
                    summary.status,
                    summary.finished_at,
                    json.dumps(summary.sources_succeeded),
                    json.dumps(summary.sources_failed),
                    summary.records_fetched,
                    summary.records_valid,
                    summary.records_upserted,
                    summary.records_invalid,
                    summary.records_flagged,
                    summary.duplicates_dropped,
                    summary.error_message,
                    summary.run_id,
                ),
            )

    def upsert_records(self, records: List[CleanRecord], run_id: str) -> int:
        """Idempotently insert or update validated records in metrics_data."""
        if not records:
            return 0

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = [
            (
                rec.source,
                rec.entity_id,
                rec.metric_name,
                rec.value,
                rec.unit,
                rec.observed_at,
                json.dumps(rec.metadata, sort_keys=True),
                1 if rec.is_flagged else 0,
                rec.flag_reason,
                run_id,
                run_id,
                now_iso,
            )
            for rec in records
        ]

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO metrics_data (
                    source, entity_id, metric_name, value, unit, observed_at,
                    metadata_json, is_flagged, flag_reason,
                    first_seen_run_id, last_updated_run_id, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, entity_id, metric_name, observed_at)
                DO UPDATE SET
                    value = excluded.value,
                    unit = excluded.unit,
                    metadata_json = excluded.metadata_json,
                    is_flagged = excluded.is_flagged,
                    flag_reason = excluded.flag_reason,
                    last_updated_run_id = excluded.last_updated_run_id,
                    ingested_at = excluded.ingested_at
                """,
                rows,
            )
        return len(rows)

    def log_validation_issues(self, issues: List[ValidationIssue], run_id: str) -> int:
        """Persist validation issues associated with a run."""
        if not issues:
            return 0

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = [
            (
                run_id,
                issue.source,
                issue.entity_id,
                issue.metric_name,
                issue.issue_type,
                issue.action,
                issue.message,
                json.dumps(issue.raw_payload, default=str, sort_keys=True),
                now_iso,
            )
            for issue in issues
        ]

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO validation_issues (
                    run_id, source, entity_id, metric_name,
                    issue_type, action, message, raw_payload_json, logged_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def get_sync_state(self, source_name: str) -> Optional[Dict[str, Any]]:
        """Return the persisted incremental sync cursor for a source, if any."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT source_name, last_record_ts, last_success_at, last_run_id
                FROM sync_state
                WHERE source_name = ?
                """,
                (source_name,),
            ).fetchone()
            return dict(row) if row else None

    def get_all_sync_states(self) -> List[Dict[str, Any]]:
        """Return all persisted sync_state rows ordered by source_name."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT source_name, last_record_ts, last_success_at, last_run_id
                FROM sync_state
                ORDER BY source_name ASC
                """
            ).fetchall()
            return [dict(r) for r in rows]

    def update_sync_state(
        self,
        source_name: str,
        last_record_ts: str,
        last_success_at: str,
        last_run_id: str,
    ) -> None:
        """Persist or advance the incremental cursor for a source (never moves backwards)."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (
                    source_name, last_record_ts, last_success_at, last_run_id
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(source_name)
                DO UPDATE SET
                    last_record_ts = CASE
                        WHEN excluded.last_record_ts >= sync_state.last_record_ts
                        THEN excluded.last_record_ts
                        ELSE sync_state.last_record_ts
                    END,
                    last_success_at = excluded.last_success_at,
                    last_run_id = excluded.last_run_id
                """,
                (source_name, last_record_ts, last_success_at, last_run_id),
            )

    def reset_sync_state(self, source_names: Optional[List[str]] = None) -> int:
        """Clear saved incremental cursors for specific sources or all sources."""
        with self._connect() as conn:
            if source_names:
                placeholders = ",".join("?" for _ in source_names)
                cursor = conn.execute(
                    f"DELETE FROM sync_state WHERE source_name IN ({placeholders})",
                    tuple(source_names),
                )
            else:
                cursor = conn.execute("DELETE FROM sync_state")
            return cursor.rowcount

    def get_last_successful_run_time(self, source: Optional[str] = None) -> Optional[str]:
        """Return the latest cursor (from sync_state) or fallback to metrics/runs history."""
        if source:
            state = self.get_sync_state(source)
            if state and state.get("last_record_ts"):
                return str(state["last_record_ts"])

        with self._connect() as conn:
            if source:
                row = conn.execute(
                    "SELECT MAX(observed_at) AS last_ts FROM metrics_data WHERE source = ?",
                    (source,),
                ).fetchone()
                if row and row["last_ts"]:
                    return str(row["last_ts"])

            row = conn.execute(
                """
                SELECT MAX(finished_at) AS last_ts
                FROM ingestion_runs
                WHERE status IN ('SUCCESS', 'PARTIAL_SUCCESS')
                """
            ).fetchone()
            return str(row["last_ts"]) if row and row["last_ts"] else None

    def get_record_count(self, source: Optional[str] = None) -> int:
        """Count stored rows in metrics_data."""
        with self._connect() as conn:
            if source:
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM metrics_data WHERE source = ?", (source,)
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS cnt FROM metrics_data").fetchone()
            return int(row["cnt"]) if row else 0

    def get_recent_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieve recent ingestion_runs rows as dictionaries."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
