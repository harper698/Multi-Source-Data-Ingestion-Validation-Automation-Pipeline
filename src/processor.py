"""Record normalization, validation, deduplication, and structured issue logging."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from src.models import CleanRecord, RawRecord, ValidationIssue

logger = logging.getLogger("pipeline.processor")


@dataclass
class ProcessingResult:
    """Output of normalizing, validating, and deduplicating a batch of raw records."""

    clean_records: List[CleanRecord] = field(default_factory=list)
    issues: List[ValidationIssue] = field(default_factory=list)
    duplicates_dropped: int = 0
    invalid_dropped: int = 0
    flagged_count: int = 0


def normalize_timestamp(value: Any) -> str:
    """Normalize epoch seconds/ms, datetime objects, or ISO strings to ISO-8601 UTC."""
    if value is None or isinstance(value, bool):
        raise ValueError("Timestamp is missing or boolean")

    dt: Optional[datetime] = None

    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            raise ValueError(f"Non-finite numeric timestamp: {value}")
        ts = value / 1000.0 if abs(value) > 1e11 else float(value)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    elif isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("Empty timestamp string")
        try:
            num = float(text)
            ts = num / 1000.0 if abs(num) > 1e11 else num
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        except ValueError:
            iso_candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
            dt = datetime.fromisoformat(iso_candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
    else:
        raise ValueError(f"Unsupported timestamp type: {type(value).__name__}")

    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_float(value: Any) -> float:
    """Parse and validate a finite numeric value."""
    if value is None or isinstance(value, bool):
        raise ValueError(f"Invalid numeric value: {value!r}")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Cannot convert {value!r} to float") from exc
    if math.isnan(numeric) or math.isinf(numeric):
        raise ValueError(f"Non-finite numeric value: {value!r}")
    return numeric


class RecordProcessor:
    """Normalizes fields/timestamps, filters invalid records, and deduplicates batches."""

    def __init__(self, validation_config: Optional[Dict[str, Any]] = None) -> None:
        cfg = validation_config or {}
        self.drop_duplicates: bool = bool(cfg.get("drop_duplicates", True))
        self.drop_invalid: bool = bool(cfg.get("drop_invalid", True))
        self.allow_future_seconds: int = int(cfg.get("allow_future_seconds", 300))
        self.metric_bounds: Dict[str, Dict[str, float]] = cfg.get("metric_bounds", {})

    def process_batch(self, raw_records: List[RawRecord]) -> ProcessingResult:
        """Validate, normalize, and deduplicate a batch of raw records."""
        result = ProcessingResult()
        seen_keys: Set[Tuple[str, str, str, str]] = set()
        now_utc = datetime.now(timezone.utc)
        max_future_dt = now_utc + timedelta(seconds=self.allow_future_seconds)

        for raw in raw_records:
            source = str(raw.source).strip() if raw.source is not None else ""
            entity_id = str(raw.entity_id).strip().lower() if raw.entity_id is not None else ""
            metric_name = str(raw.metric_name).strip().lower() if raw.metric_name is not None else ""

            raw_payload = {
                "source": raw.source,
                "entity_id": raw.entity_id,
                "metric_name": raw.metric_name,
                "value": raw.value,
                "observed_at": raw.observed_at,
                "unit": raw.unit,
            }

            if not source or not entity_id or not metric_name:
                self._record_issue(
                    result,
                    ValidationIssue(
                        source=source or "unknown",
                        entity_id=entity_id or "unknown",
                        metric_name=metric_name or "unknown",
                        issue_type="missing_field",
                        message="Missing required identifier (source, entity_id, or metric_name)",
                        action="dropped",
                        raw_payload=raw_payload,
                    ),
                )
                result.invalid_dropped += 1
                continue

            if raw.value is None or (isinstance(raw.value, str) and not raw.value.strip()):
                self._record_issue(
                    result,
                    ValidationIssue(
                        source=source,
                        entity_id=entity_id,
                        metric_name=metric_name,
                        issue_type="missing_value",
                        message="Metric value is missing or empty",
                        action="dropped",
                        raw_payload=raw_payload,
                    ),
                )
                result.invalid_dropped += 1
                continue

            try:
                numeric_value = normalize_float(raw.value)
            except ValueError as exc:
                self._record_issue(
                    result,
                    ValidationIssue(
                        source=source,
                        entity_id=entity_id,
                        metric_name=metric_name,
                        issue_type="invalid_type",
                        message=str(exc),
                        action="dropped",
                        raw_payload=raw_payload,
                    ),
                )
                result.invalid_dropped += 1
                continue

            try:
                observed_at = normalize_timestamp(raw.observed_at)
            except ValueError as exc:
                self._record_issue(
                    result,
                    ValidationIssue(
                        source=source,
                        entity_id=entity_id,
                        metric_name=metric_name,
                        issue_type="invalid_timestamp",
                        message=str(exc),
                        action="dropped",
                        raw_payload=raw_payload,
                    ),
                )
                result.invalid_dropped += 1
                continue

            observed_dt = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
            is_flagged = False
            flag_reasons: List[str] = []

            if observed_dt > max_future_dt:
                msg = f"Timestamp {observed_at} exceeds allowed future skew ({self.allow_future_seconds}s)"
                action = "dropped" if self.drop_invalid else "flagged"
                self._record_issue(
                    result,
                    ValidationIssue(
                        source=source,
                        entity_id=entity_id,
                        metric_name=metric_name,
                        issue_type="future_timestamp",
                        message=msg,
                        action=action,
                        raw_payload=raw_payload,
                    ),
                )
                if self.drop_invalid:
                    result.invalid_dropped += 1
                    continue
                is_flagged = True
                flag_reasons.append(msg)

            bounds = self.metric_bounds.get(metric_name)
            if bounds:
                min_val = bounds.get("min")
                max_val = bounds.get("max")
                out_of_bounds = (min_val is not None and numeric_value < float(min_val)) or (
                    max_val is not None and numeric_value > float(max_val)
                )
                if out_of_bounds:
                    msg = (
                        f"Value {numeric_value} outside allowed bounds "
                        f"[{min_val}, {max_val}] for {metric_name}"
                    )
                    action = "dropped" if self.drop_invalid else "flagged"
                    self._record_issue(
                        result,
                        ValidationIssue(
                            source=source,
                            entity_id=entity_id,
                            metric_name=metric_name,
                            issue_type="out_of_bounds",
                            message=msg,
                            action=action,
                            raw_payload=raw_payload,
                        ),
                    )
                    if self.drop_invalid:
                        result.invalid_dropped += 1
                        continue
                    is_flagged = True
                    flag_reasons.append(msg)

            natural_key = (source, entity_id, metric_name, observed_at)
            if natural_key in seen_keys:
                msg = f"Duplicate record for key {natural_key}"
                action = "dropped" if self.drop_duplicates else "flagged"
                self._record_issue(
                    result,
                    ValidationIssue(
                        source=source,
                        entity_id=entity_id,
                        metric_name=metric_name,
                        issue_type="duplicate",
                        message=msg,
                        action=action,
                        raw_payload=raw_payload,
                    ),
                )
                if self.drop_duplicates:
                    result.duplicates_dropped += 1
                    continue
                is_flagged = True
                flag_reasons.append("duplicate")
            else:
                seen_keys.add(natural_key)

            if is_flagged:
                result.flagged_count += 1

            result.clean_records.append(
                CleanRecord(
                    source=source,
                    entity_id=entity_id,
                    metric_name=metric_name,
                    value=numeric_value,
                    observed_at=observed_at,
                    unit=str(raw.unit or "").strip(),
                    metadata=dict(raw.metadata) if raw.metadata else {},
                    is_flagged=is_flagged,
                    flag_reason="; ".join(flag_reasons) if flag_reasons else None,
                )
            )

        logger.info(
            "Processing complete: fetched=%d, valid=%d, invalid_dropped=%d, duplicates_dropped=%d, flagged=%d",
            len(raw_records),
            len(result.clean_records),
            result.invalid_dropped,
            result.duplicates_dropped,
            result.flagged_count,
        )
        return result

    @staticmethod
    def _record_issue(result: ProcessingResult, issue: ValidationIssue) -> None:
        result.issues.append(issue)
        logger.warning(
            "Validation [%s] (%s) %s/%s/%s: %s",
            issue.issue_type,
            issue.action,
            issue.source,
            issue.entity_id,
            issue.metric_name,
            issue.message,
        )
