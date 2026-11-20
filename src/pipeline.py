"""End-to-end orchestrator for multi-source ingestion, processing, storage, reporting, and webhooks."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from src.connectors import build_connectors
from src.models import RawRecord, RunSummary, SourceExecutionResult
from src.notifier import WebhookNotifier
from src.processor import RecordProcessor
from src.reporter import ReportGenerator
from src.storage import SQLiteStorage

logger = logging.getLogger("pipeline.runner")


class IngestionPipeline:
    """Coordinates connectors, processing, SQLite persistence, reporting, and notifications."""

    def __init__(
        self,
        config: Dict[str, Any],
        db_path_override: Optional[str] = None,
        report_dir_override: Optional[str] = None,
        report_formats_override: Optional[Sequence[str]] = None,
    ) -> None:
        self.config = config
        pipeline_cfg = config.get("pipeline", {})
        db_path = db_path_override or pipeline_cfg.get("database_path", "data/pipeline.db")
        report_dir = report_dir_override or pipeline_cfg.get("report_dir", "reports")
        report_formats = report_formats_override or pipeline_cfg.get(
            "report_formats", ["md", "json", "csv"]
        )
        notify_cfg = config.get("notifications") or config.get("alerts") or {}

        self.storage = SQLiteStorage(db_path=db_path)
        self.processor = RecordProcessor(config.get("validation", {}))
        self.reporter = ReportGenerator(report_dir=report_dir, formats=report_formats)
        self.notifier = WebhookNotifier(notify_cfg)

    def reset_state(self, source_names: Optional[List[str]] = None) -> int:
        """Clear persisted incremental sync cursors for all or selected sources."""
        deleted = self.storage.reset_sync_state(source_names=source_names)
        scope = ", ".join(source_names) if source_names else "all sources"
        logger.info("Reset sync_state for %s (%d cursor(s) cleared)", scope, deleted)
        return deleted

    def run(
        self,
        mode: Optional[str] = None,
        selected_sources: Optional[List[str]] = None,
        mock: bool = False,
        inject_anomalies: bool = False,
        dry_run: bool = False,
    ) -> RunSummary:
        """Execute a full or incremental pipeline run with per-source error isolation."""
        run_mode = mode or self.config.get("pipeline", {}).get("default_mode", "incremental")
        if run_mode not in ("full", "incremental"):
            raise ValueError(f"Unsupported run mode '{run_mode}'. Expected 'full' or 'incremental'.")

        started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]

        connectors = build_connectors(self.config, selected_sources=selected_sources)
        source_names = [c.source_name for c in connectors]

        logger.info(
            "Starting pipeline run %s | mode=%s | mock=%s | dry_run=%s | sources=%s",
            run_id,
            run_mode,
            mock,
            dry_run,
            source_names,
        )

        if not dry_run:
            self.storage.start_run(
                run_id=run_id,
                mode=run_mode,
                started_at=started_at,
                sources_requested=source_names,
            )

        summary = RunSummary(
            run_id=run_id,
            mode=run_mode,
            status="RUNNING",
            started_at=started_at,
            finished_at=started_at,
            sources_requested=source_names,
        )

        all_raw_records: List[RawRecord] = []
        errors: List[str] = []
        source_results: Dict[str, SourceExecutionResult] = {}

        # Per-source error isolation: one failing source never aborts the rest
        for connector in connectors:
            try:
                since: Optional[str] = None
                if run_mode == "incremental":
                    state = self.storage.get_sync_state(connector.source_name)
                    if state and state.get("last_record_ts"):
                        since = str(state["last_record_ts"])
                        logger.info(
                            "[%s] Loaded incremental cursor: since=%s (last_run_id=%s)",
                            connector.source_name,
                            since,
                            state.get("last_run_id"),
                        )
                    else:
                        logger.info(
                            "[%s] No saved incremental cursor; fetching without lower bound",
                            connector.source_name,
                        )
                else:
                    logger.info("[%s] Full mode: bypassing saved sync cursor", connector.source_name)

                exec_result = connector.fetch_with_resilience(
                    since=since,
                    mock=mock,
                    inject_anomalies=inject_anomalies,
                )
                source_results[connector.source_name] = exec_result
                if exec_result.success:
                    summary.sources_succeeded.append(connector.source_name)
                    if exec_result.used_fallback:
                        summary.sources_fallback.append(connector.source_name)
                    all_raw_records.extend(exec_result.records)
                else:
                    summary.sources_failed.append(connector.source_name)
                    if exec_result.error:
                        errors.append(f"{connector.source_name}: {exec_result.error}")
            except Exception as exc:  # noqa: BLE001
                logger.exception("[%s] Unexpected connector error (isolated): %s", connector.source_name, exc)
                summary.sources_failed.append(connector.source_name)
                errors.append(f"{connector.source_name}: {exc}")

        summary.records_fetched = len(all_raw_records)

        processed = self.processor.process_batch(all_raw_records)
        summary.records_valid = len(processed.clean_records)
        summary.records_invalid = processed.invalid_dropped
        summary.duplicates_dropped = processed.duplicates_dropped
        summary.records_flagged = processed.flagged_count

        if summary.sources_failed and summary.sources_succeeded:
            summary.status = "PARTIAL_SUCCESS"
        elif summary.sources_failed and not summary.sources_succeeded:
            summary.status = "FAILED"
        else:
            summary.status = "SUCCESS"

        summary.error_message = "; ".join(errors) if errors else None
        summary.finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        if not dry_run:
            summary.records_upserted = self.storage.upsert_records(
                processed.clean_records, run_id=run_id
            )
            self.storage.log_validation_issues(processed.issues, run_id=run_id)
            self._advance_sync_cursors(
                run_id=run_id,
                finished_at=summary.finished_at,
                succeeded_sources=summary.sources_succeeded,
                source_results=source_results,
                clean_records=processed.clean_records,
                mock_mode=mock,
            )
            self.storage.finish_run(summary)
        else:
            logger.info("Dry-run enabled: skipping SQLite writes and sync_state updates.")

        self.notifier.evaluate(summary)
        self.reporter.generate(summary, processed.clean_records, processed.issues)
        self.notifier.notify(summary, dry_run=dry_run)

        return summary

    def _advance_sync_cursors(
        self,
        run_id: str,
        finished_at: str,
        succeeded_sources: List[str],
        source_results: Dict[str, SourceExecutionResult],
        clean_records: List[Any],
        mock_mode: bool = False,
    ) -> None:
        """Advance sync_state only for sources that succeeded via a live fetch and upserted records."""
        max_ts_by_source: Dict[str, str] = {}
        for rec in clean_records:
            current = max_ts_by_source.get(rec.source)
            if current is None or rec.observed_at > current:
                max_ts_by_source[rec.source] = rec.observed_at

        for source_name in succeeded_sources:
            exec_result = source_results.get(source_name)
            if mock_mode:
                logger.info(
                    "[%s] Offline/mock mode enabled; keeping incremental cursor unchanged",
                    source_name,
                )
                continue
            if exec_result is None or exec_result.used_fallback or not exec_result.is_live:
                logger.info(
                    "[%s] Live fetch fell back to sample data; keeping incremental cursor unchanged",
                    source_name,
                )
                continue

            latest_ts = max_ts_by_source.get(source_name)
            if not latest_ts:
                logger.info(
                    "[%s] No valid records upserted; keeping existing sync cursor unchanged",
                    source_name,
                )
                continue
            self.storage.update_sync_state(
                source_name=source_name,
                last_record_ts=latest_ts,
                last_success_at=finished_at,
                last_run_id=run_id,
            )
            logger.info("[%s] Advanced sync cursor to %s", source_name, latest_ts)
