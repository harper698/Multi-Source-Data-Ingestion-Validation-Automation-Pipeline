"""Generic config-driven HTTP/JSON/REST connector."""

from __future__ import annotations

from datetime import datetime, timezone
import ssl
from typing import Any, Dict, List, Optional

from src.connectors.base import BaseConnector
from src.models import RawRecord
from src.processor import normalize_timestamp


def _build_ssl_context() -> ssl.SSLContext:
    """Create an SSL context using certifi's CA bundle when installed."""
    try:
        import certifi  # type: ignore

        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def _extract_path(obj: Any, path: Optional[str]) -> Any:
    """Resolve a dot-separated path (or 'literal:<value>') against a JSON dict/list."""
    if path is None:
        return None
    path_str = str(path).strip()
    if not path_str:
        return obj
    if path_str.startswith("literal:"):
        return path_str[len("literal:") :]

    current = obj
    for part in path_str.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if current is None:
            return None
    return current


class HttpJsonConnector(BaseConnector):
    """Config-driven HTTP/JSON/REST connector requiring zero custom Python per source."""

    source_name = "http_json"

    def _get_ssl_context(self, url: str) -> Optional[ssl.SSLContext]:
        """Use certifi CA bundle (with stdlib fallback) for HTTPS requests."""
        if url.lower().startswith("https://"):
            return _build_ssl_context()
        return None

    def fetch_live(self, since: Optional[str] = None) -> List[RawRecord]:
        endpoint = str(self.source_config.get("endpoint", "")).strip()
        if not endpoint:
            raise ValueError(f"[{self.source_name}] Missing required 'endpoint' in config")

        method = str(self.source_config.get("method", "GET")).upper()
        params = dict(self.source_config.get("params") or {})
        headers = dict(self.source_config.get("headers") or {})
        body = self.source_config.get("body")

        since_param = self.source_config.get("since_param")
        if since and since_param:
            params[str(since_param)] = since

        payload = self._http_request_json(
            url=endpoint,
            method=method,
            params=params,
            headers=headers,
            body=body,
        )
        return self._map_payload(payload, since=since)

    def _map_payload(self, payload: Any, since: Optional[str] = None) -> List[RawRecord]:
        records_path = str(self.source_config.get("records_path", "") or "").strip()
        root = _extract_path(payload, records_path) if records_path else payload

        items: List[Any]
        if isinstance(root, list):
            items = root
        elif isinstance(root, dict):
            items = [root]
        else:
            return []

        mapping: Dict[str, Any] = self.source_config.get("field_mapping") or {}
        filter_by_since = bool(self.source_config.get("filter_by_since", False))
        since_normalized: Optional[str] = None
        if since and filter_by_since:
            try:
                since_normalized = normalize_timestamp(since)
            except ValueError:
                since_normalized = None

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        records: List[RawRecord] = []

        for idx, item in enumerate(items):
            entity_id = _extract_path(item, mapping.get("entity_id")) or f"item_{idx}"
            observed_at = _extract_path(item, mapping.get("observed_at")) or now_iso

            if since_normalized is not None:
                try:
                    if normalize_timestamp(observed_at) <= since_normalized:
                        continue
                except ValueError:
                    # Let the processor handle invalid timestamps rather than silently dropping
                    pass

            multi_metrics = mapping.get("metrics")
            if isinstance(multi_metrics, list) and multi_metrics:
                for metric_spec in multi_metrics:
                    m_name = metric_spec.get("name")
                    m_val = _extract_path(item, metric_spec.get("path"))
                    m_unit = str(metric_spec.get("unit", ""))
                    records.append(
                        RawRecord(
                            source=self.source_name,
                            entity_id=entity_id,
                            metric_name=m_name,
                            value=m_val,
                            observed_at=observed_at,
                            unit=m_unit,
                            metadata={"connector": "http_json", "provider": "live", "since": since},
                        )
                    )
            else:
                metric_name = _extract_path(item, mapping.get("metric_name")) or "value"
                value = _extract_path(item, mapping.get("value"))
                unit = str(_extract_path(item, mapping.get("unit")) or "")
                records.append(
                    RawRecord(
                        source=self.source_name,
                        entity_id=entity_id,
                        metric_name=metric_name,
                        value=value,
                        observed_at=observed_at,
                        unit=unit,
                        metadata={"connector": "http_json", "since": since},
                    )
                )

        return records

    def fetch_fallback(
        self,
        since: Optional[str] = None,
        inject_anomalies: bool = False,
    ) -> List[RawRecord]:
        observed_iso = (
            datetime.now(timezone.utc).replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        configured = self.source_config.get("fallback_records") or []
        records: List[RawRecord] = []

        for entry in configured:
            records.append(
                RawRecord(
                    source=self.source_name,
                    entity_id=entry.get("entity_id", "default_entity"),
                    metric_name=entry.get("metric_name", "value"),
                    value=entry.get("value"),
                    observed_at=entry.get("observed_at") or observed_iso,
                    unit=str(entry.get("unit", "")),
                    metadata={"connector": "http_json", "provider": "fallback", "since": since},
                )
            )

        if inject_anomalies and records:
            records.append(records[0])  # duplicate

        return records
