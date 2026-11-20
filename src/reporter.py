"""Multi-format summary report generator (Markdown, JSON, and CSV)."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from src.models import CleanRecord, RunSummary, ValidationIssue


class ReportGenerator:
    """Writes per-run summary reports in Markdown, JSON, and CSV formats."""

    SUPPORTED_FORMATS = ("md", "json", "csv")

    def __init__(
        self,
        report_dir: str = "reports",
        formats: Optional[Sequence[str]] = None,
    ) -> None:
        self.report_dir = Path(report_dir)
        requested = [f.lower().strip() for f in (formats or self.SUPPORTED_FORMATS)]
        self.formats = [f for f in requested if f in self.SUPPORTED_FORMATS] or ["md", "json", "csv"]

    def generate(
        self,
        summary: RunSummary,
        clean_records: List[CleanRecord],
        issues: List[ValidationIssue],
    ) -> Dict[str, str]:
        """Generate configured report files for a run and attach paths to summary."""
        self.report_dir.mkdir(parents=True, exist_ok=True)
        base_name = f"run_{summary.run_id}"
        paths: Dict[str, str] = {}

        if "json" in self.formats:
            json_path = self.report_dir / f"{base_name}.json"
            payload = {
                "summary": asdict(summary),
                "clean_records": [asdict(r) for r in clean_records],
                "validation_issues": [asdict(i) for i in issues],
            }
            json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            paths["json"] = str(json_path)

        if "csv" in self.formats:
            csv_path = self.report_dir / f"{base_name}.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow(
                    [
                        "run_id",
                        "source",
                        "entity_id",
                        "metric_name",
                        "value",
                        "unit",
                        "observed_at",
                        "is_flagged",
                        "flag_reason",
                    ]
                )
                for rec in clean_records:
                    writer.writerow(
                        [
                            summary.run_id,
                            rec.source,
                            rec.entity_id,
                            rec.metric_name,
                            rec.value,
                            rec.unit,
                            rec.observed_at,
                            rec.is_flagged,
                            rec.flag_reason or "",
                        ]
                    )
            paths["csv"] = str(csv_path)

        if "md" in self.formats:
            md_path = self.report_dir / f"{base_name}.md"
            md_path.write_text(self._render_markdown(summary, clean_records, issues), encoding="utf-8")
            paths["markdown"] = str(md_path)

        summary.report_paths = paths
        return paths

    @staticmethod
    def _render_markdown(
        summary: RunSummary,
        clean_records: List[CleanRecord],
        issues: List[ValidationIssue],
    ) -> str:
        lines = [
            f"# Ingestion Run Report: `{summary.run_id}`",
            "",
            f"- **Status:** {summary.status}",
            f"- **Mode:** {summary.mode}",
            f"- **Started (UTC):** {summary.started_at}",
            f"- **Finished (UTC):** {summary.finished_at}",
            f"- **Sources Requested:** {', '.join(summary.sources_requested) or 'none'}",
            f"- **Sources Succeeded:** {', '.join(summary.sources_succeeded) or 'none'}",
            f"- **Sources Failed:** {', '.join(summary.sources_failed) or 'none'}",
            f"- **Fallback/Mock Used:** {', '.join(summary.sources_fallback) or 'none'}",
            "",
            "## Run Metrics",
            "",
            "| Metric | Count |",
            "| --- | ---: |",
            f"| Records Fetched | {summary.records_fetched} |",
            f"| Records Valid | {summary.records_valid} |",
            f"| Records Upserted | {summary.records_upserted} |",
            f"| Invalid Dropped | {summary.records_invalid} |",
            f"| Duplicates Dropped | {summary.duplicates_dropped} |",
            f"| Flagged Records | {summary.records_flagged} |",
            f"| Invalid Ratio | {summary.invalid_ratio:.1%} |",
            "",
        ]

        if summary.alerts:
            lines.extend(["## Notifications & Alerts", ""])
            for alert in summary.alerts:
                lines.append(f"- **[{alert.severity}] {alert.title}:** {alert.message}")
            lines.append("")

        if clean_records:
            preview = clean_records[:15]
            lines.extend(
                [
                    f"## Clean Records Preview ({len(preview)} of {len(clean_records)})",
                    "",
                    "| Source | Entity | Metric | Value | Unit | Observed At (UTC) |",
                    "| --- | --- | --- | ---: | --- | --- |",
                ]
            )
            for rec in preview:
                lines.append(
                    f"| {rec.source} | {rec.entity_id} | {rec.metric_name} | "
                    f"{rec.value} | {rec.unit} | {rec.observed_at} |"
                )
            lines.append("")

        if issues:
            lines.extend(
                [
                    "## Validation Issues",
                    "",
                    "| Source | Entity | Metric | Type | Action | Message |",
                    "| --- | --- | --- | --- | --- | --- |",
                ]
            )
            for issue in issues:
                lines.append(
                    f"| {issue.source} | {issue.entity_id} | {issue.metric_name} | "
                    f"{issue.issue_type} | {issue.action} | {issue.message} |"
                )
            lines.append("")

        return "\n".join(lines)
