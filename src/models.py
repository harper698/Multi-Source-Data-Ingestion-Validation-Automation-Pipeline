"""Domain data models shared across connectors, validation, storage, and alerting."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class RawRecord:
    """Unvalidated record emitted by a source connector."""

    source: str
    entity_id: Any
    metric_name: Any
    value: Any
    observed_at: Any
    unit: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CleanRecord:
    """Normalized and validated record ready for persistence."""

    source: str
    entity_id: str
    metric_name: str
    value: float
    observed_at: str  # ISO-8601 UTC string (YYYY-MM-DDTHH:MM:SSZ)
    unit: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_flagged: bool = False
    flag_reason: Optional[str] = None

    @property
    def natural_key(self) -> tuple[str, str, str, str]:
        """Natural deduplication & upsert key."""
        return (self.source, self.entity_id, self.metric_name, self.observed_at)


@dataclass
class ValidationIssue:
    """Structured record of a validation failure, flag, or dropped duplicate."""

    source: str
    entity_id: str
    metric_name: str
    issue_type: str  # e.g., "missing_field", "invalid_type", "out_of_bounds", "duplicate", "future_timestamp"
    message: str
    action: str  # "dropped" or "flagged"
    raw_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceExecutionResult:
    """Outcome of fetching a single data source."""

    source: str
    success: bool
    used_fallback: bool = False
    is_live: bool = False
    records: List[RawRecord] = field(default_factory=list)
    error: Optional[str] = None
    duration_ms: int = 0


@dataclass
class AlertEvent:
    """Notification event emitted on anomalies or run completion."""

    severity: str  # "INFO", "WARNING", "CRITICAL"
    title: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RunSummary:
    """End-to-end metrics and status for a single pipeline execution."""

    run_id: str
    mode: str
    status: str  # "SUCCESS", "PARTIAL_SUCCESS", "FAILED"
    started_at: str
    finished_at: str
    sources_requested: List[str] = field(default_factory=list)
    sources_succeeded: List[str] = field(default_factory=list)
    sources_failed: List[str] = field(default_factory=list)
    sources_fallback: List[str] = field(default_factory=list)
    records_fetched: int = 0
    records_valid: int = 0
    records_upserted: int = 0
    records_invalid: int = 0
    records_flagged: int = 0
    duplicates_dropped: int = 0
    alerts: List[AlertEvent] = field(default_factory=list)
    error_message: Optional[str] = None
    report_paths: Dict[str, str] = field(default_factory=dict)

    @property
    def invalid_ratio(self) -> float:
        if self.records_fetched == 0:
            return 0.0
        return self.records_invalid / self.records_fetched
