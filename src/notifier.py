"""Webhook and console notifier with threshold evaluation and local log fallback."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from src.models import AlertEvent, RunSummary

logger = logging.getLogger("pipeline.notifier")


class WebhookNotifier:
    """Evaluates run thresholds and dispatches structured Webhook/console notifications."""

    def __init__(self, notify_config: Dict[str, Any]) -> None:
        cfg = notify_config or {}
        self.console_enabled: bool = bool(cfg.get("console_enabled", True))
        self.webhook_enabled: bool = bool(cfg.get("webhook_enabled", True))
        self.webhook_url: str = str(cfg.get("webhook_url", "")).strip()
        self.webhook_timeout: float = float(cfg.get("webhook_timeout_seconds", 5))
        self.fallback_log_path = Path(
            str(cfg.get("fallback_log_path", "logs/webhook_fallback.jsonl"))
        )
        self.notify_on_completion: bool = bool(cfg.get("notify_on_completion", True))
        self.thresholds: Dict[str, Any] = cfg.get("thresholds", {})

    def evaluate(self, summary: RunSummary) -> List[AlertEvent]:
        """Evaluate run metrics against configured failure/anomaly thresholds."""
        alerts: List[AlertEvent] = []

        max_source_failures = int(self.thresholds.get("max_source_failures", 0))
        if len(summary.sources_failed) > max_source_failures:
            alerts.append(
                AlertEvent(
                    severity="CRITICAL",
                    title="Data Source Failure Detected",
                    message=(
                        f"{len(summary.sources_failed)} source(s) failed "
                        f"(threshold: {max_source_failures}): {', '.join(summary.sources_failed)}"
                    ),
                    details={"sources_failed": summary.sources_failed},
                )
            )

        max_invalid_ratio = float(self.thresholds.get("max_invalid_ratio", 0.10))
        if summary.records_fetched > 0 and summary.invalid_ratio > max_invalid_ratio:
            alerts.append(
                AlertEvent(
                    severity="WARNING",
                    title="High Invalid Record Ratio",
                    message=(
                        f"Invalid ratio {summary.invalid_ratio:.1%} exceeded "
                        f"threshold {max_invalid_ratio:.1%} "
                        f"({summary.records_invalid}/{summary.records_fetched} invalid)"
                    ),
                    details={
                        "invalid_ratio": round(summary.invalid_ratio, 4),
                        "records_invalid": summary.records_invalid,
                        "records_fetched": summary.records_fetched,
                    },
                )
            )

        min_records = int(self.thresholds.get("min_records_expected", 1))
        if summary.records_valid < min_records:
            alerts.append(
                AlertEvent(
                    severity="WARNING",
                    title="Low Valid Record Volume",
                    message=(
                        f"Only {summary.records_valid} valid record(s) collected "
                        f"(minimum expected: {min_records})"
                    ),
                    details={"records_valid": summary.records_valid, "min_expected": min_records},
                )
            )

        if summary.sources_fallback:
            alerts.append(
                AlertEvent(
                    severity="INFO",
                    title="Fallback/Mock Data Used",
                    message=(
                        f"Sample/fallback data used for source(s): "
                        f"{', '.join(summary.sources_fallback)}"
                    ),
                    details={"sources_fallback": summary.sources_fallback},
                )
            )

        if self.notify_on_completion:
            alerts.append(
                AlertEvent(
                    severity="INFO" if summary.status == "SUCCESS" else "WARNING",
                    title=f"Pipeline Run {summary.status}",
                    message=(
                        f"Run {summary.run_id} ({summary.mode}) finished: "
                        f"fetched={summary.records_fetched}, valid={summary.records_valid}, "
                        f"upserted={summary.records_upserted}, invalid={summary.records_invalid}, "
                        f"duplicates={summary.duplicates_dropped}"
                    ),
                    details={
                        "run_id": summary.run_id,
                        "mode": summary.mode,
                        "status": summary.status,
                    },
                )
            )

        summary.alerts = alerts
        return alerts

    def build_payload(self, summary: RunSummary) -> Dict[str, Any]:
        """Build a structured JSON payload compatible with Slack/Discord/generic webhooks."""
        lines = [f"[{a.severity}] {a.title}: {a.message}" for a in summary.alerts]
        return {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "text": f"Pipeline Run {summary.run_id} ({summary.status})\n" + "\n".join(lines),
            "run_id": summary.run_id,
            "mode": summary.mode,
            "status": summary.status,
            "sources": {
                "requested": summary.sources_requested,
                "succeeded": summary.sources_succeeded,
                "failed": summary.sources_failed,
                "fallback": summary.sources_fallback,
            },
            "metrics": {
                "fetched": summary.records_fetched,
                "valid": summary.records_valid,
                "upserted": summary.records_upserted,
                "invalid": summary.records_invalid,
                "duplicates": summary.duplicates_dropped,
                "flagged": summary.records_flagged,
            },
            "alerts": [asdict(a) for a in summary.alerts],
            "reports": summary.report_paths,
        }

    def notify(self, summary: RunSummary, dry_run: bool = False) -> Dict[str, Any]:
        """Deliver notifications to console and webhook (or local fallback log)."""
        if not summary.alerts:
            return {"delivered": False, "mode": "none"}

        if self.console_enabled:
            print("\n=== Pipeline Notifications & Summary ===")
            for alert in summary.alerts:
                print(f"[{alert.severity}] {alert.title}: {alert.message}")
            if summary.report_paths:
                formatted = ", ".join(f"{k}={v}" for k, v in summary.report_paths.items())
                print(f"Reports: {formatted}")
            print("========================================\n")

        if not self.webhook_enabled:
            return {"delivered": False, "mode": "console_only"}

        payload = self.build_payload(summary)

        if dry_run or not self.webhook_url:
            reason = "dry_run" if dry_run else "unconfigured_webhook_url"
            self._write_local_fallback(payload, reason=reason)
            return {
                "delivered": True,
                "mode": "local_fallback",
                "reason": reason,
                "path": str(self.fallback_log_path),
            }

        ok = self._post_webhook(payload)
        if not ok:
            self._write_local_fallback(payload, reason="webhook_delivery_failed")
            return {
                "delivered": False,
                "mode": "local_fallback",
                "reason": "webhook_delivery_failed",
                "path": str(self.fallback_log_path),
            }
        return {"delivered": True, "mode": "webhook"}

    def _post_webhook(self, payload: Dict[str, Any]) -> bool:
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "GenericIngestionPipeline/0.2",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.webhook_timeout) as response:
                logger.info("Webhook notification sent (HTTP %s)", response.status)
                return True
        except (urllib.error.URLError, TimeoutError) as exc:
            logger.error("Webhook POST failed (%s); falling back to local log.", exc)
            return False

    def _write_local_fallback(self, payload: Dict[str, Any], reason: str) -> None:
        self.fallback_log_path.parent.mkdir(parents=True, exist_ok=True)
        envelope = {"fallback_reason": reason, "payload": payload}
        with self.fallback_log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(envelope) + "\n")
        logger.info(
            "Webhook payload logged locally to %s (reason=%s)",
            self.fallback_log_path,
            reason,
        )
