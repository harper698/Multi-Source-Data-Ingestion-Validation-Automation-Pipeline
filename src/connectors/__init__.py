"""Connector registry and config-driven factory."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Type

from src.connectors.base import BaseConnector
from src.connectors.http_json import HttpJsonConnector
from src.connectors.mock_source import MockSampleConnector

CONNECTOR_TYPES: Dict[str, Type[BaseConnector]] = {
    "mock": MockSampleConnector,
    "http_json": HttpJsonConnector,
    "rest_json": HttpJsonConnector,
}


def build_connectors(
    config: Dict[str, Any],
    selected_sources: Optional[List[str]] = None,
) -> List[BaseConnector]:
    """Instantiate enabled connectors from YAML configuration without editing code."""
    sources_cfg = config.get("sources", {})
    http_cfg = config.get("http", {})

    if not isinstance(sources_cfg, dict):
        raise ValueError("Configuration 'sources' section must be a mapping of source_name -> config")

    if selected_sources:
        unknown = [s for s in selected_sources if s not in sources_cfg]
        if unknown:
            raise ValueError(
                f"Unknown source(s): {', '.join(unknown)}. "
                f"Configured sources: {', '.join(sorted(sources_cfg))}"
            )

    connectors: List[BaseConnector] = []
    for name, source_cfg in sources_cfg.items():
        if not isinstance(source_cfg, dict):
            continue
        if selected_sources and name not in selected_sources:
            continue
        if not source_cfg.get("enabled", True):
            continue

        connector_type = str(source_cfg.get("type", "http_json")).strip().lower()
        cls = CONNECTOR_TYPES.get(connector_type)
        if cls is None:
            raise ValueError(
                f"Source '{name}' references unknown connector type '{connector_type}'. "
                f"Supported types: {', '.join(sorted(CONNECTOR_TYPES))}"
            )

        connectors.append(
            cls(source_config=source_cfg, http_config=http_cfg, source_name=name)
        )

    return connectors


__all__ = [
    "BaseConnector",
    "CONNECTOR_TYPES",
    "HttpJsonConnector",
    "MockSampleConnector",
    "build_connectors",
]
