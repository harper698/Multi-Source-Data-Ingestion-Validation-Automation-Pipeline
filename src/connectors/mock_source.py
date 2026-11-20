"""Built-in offline mock/sample connector for immediate testing without network calls."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.connectors.base import BaseConnector
from src.models import RawRecord


class MockSampleConnector(BaseConnector):
    """Generates deterministic sample telemetry records from YAML config."""

    source_name = "mock_sample"
    is_mock_source = True

    def fetch_live(self, since: Optional[str] = None) -> List[RawRecord]:
        # Mock connector is inherently offline; live mode delegates directly to sample generation.
        return self.fetch_fallback(since=since, inject_anomalies=False)

    def fetch_fallback(
        self,
        since: Optional[str] = None,
        inject_anomalies: bool = False,
    ) -> List[RawRecord]:
        entities: List[str] = list(
            self.source_config.get("entities") or ["sensor_alpha", "sensor_beta"]
        )
        metrics_cfg: List[Dict[str, Any]] = list(
            self.source_config.get("metrics")
            or [
                {"name": "temperature_c", "baseline": 22.0, "unit": "C"},
                {"name": "humidity_pct", "baseline": 50.0, "unit": "pct"},
            ]
        )

        observed_iso = (
            datetime.now(timezone.utc).replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        records: List[RawRecord] = []
        for idx, entity in enumerate(entities):
            for m_idx, spec in enumerate(metrics_cfg):
                name = str(spec.get("name", f"metric_{m_idx}"))
                baseline = float(spec.get("baseline", 10.0))
                unit = str(spec.get("unit", ""))
                value = round(baseline + idx * 1.25 + m_idx * 0.5, 2)
                records.append(
                    RawRecord(
                        source=self.source_name,
                        entity_id=entity,
                        metric_name=name,
                        value=value,
                        observed_at=observed_iso,
                        unit=unit,
                        metadata={"connector": "mock", "since": since},
                    )
                )

        if inject_anomalies and records:
            # 1. Exact duplicate
            records.append(records[0])
            # 2. Out-of-bounds temperature
            records.append(
                RawRecord(
                    source=self.source_name,
                    entity_id=entities[0],
                    metric_name="temperature_c",
                    value=999.9,
                    observed_at=observed_iso,
                    unit="C",
                    metadata={"connector": "mock", "injected": "out_of_bounds"},
                )
            )
            # 3. Missing value
            records.append(
                RawRecord(
                    source=self.source_name,
                    entity_id=entities[0],
                    metric_name="humidity_pct",
                    value=None,
                    observed_at=observed_iso,
                    unit="pct",
                    metadata={"connector": "mock", "injected": "missing_value"},
                )
            )
            # 4. Non-numeric value
            records.append(
                RawRecord(
                    source=self.source_name,
                    entity_id=entities[0],
                    metric_name="latency_ms",
                    value="timeout_err",
                    observed_at=observed_iso,
                    unit="ms",
                    metadata={"connector": "mock", "injected": "non_numeric"},
                )
            )

        return records
