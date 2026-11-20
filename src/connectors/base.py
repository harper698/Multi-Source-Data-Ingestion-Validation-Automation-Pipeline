"""Base connector interface with per-source timeouts, retry/backoff, error isolation, and fallback."""

from __future__ import annotations

import json
import logging
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.models import RawRecord, SourceExecutionResult

logger = logging.getLogger("pipeline.connectors")


class BaseConnector(ABC):
    """Abstract base class for all data source connectors."""

    source_name: str = "base"
    is_mock_source: bool = False

    def __init__(
        self,
        source_config: Dict[str, Any],
        http_config: Optional[Dict[str, Any]] = None,
        source_name: Optional[str] = None,
    ) -> None:
        self.source_config = source_config or {}
        if source_name:
            self.source_name = source_name

        http_cfg = http_config or {}
        # Allow per-source overrides of global HTTP defaults
        self.timeout_seconds: float = float(
            self.source_config.get("timeout_seconds", http_cfg.get("timeout_seconds", 10))
        )
        self.max_retries: int = int(
            self.source_config.get("max_retries", http_cfg.get("max_retries", 3))
        )
        self.backoff_factor: float = float(
            self.source_config.get("backoff_factor", http_cfg.get("backoff_factor", 1.5))
        )
        self.fallback_to_mock: bool = bool(
            self.source_config.get("fallback_to_mock", http_cfg.get("fallback_to_mock", True))
        )

    @abstractmethod
    def fetch_live(self, since: Optional[str] = None) -> List[RawRecord]:
        """Fetch records from the live external data source."""

    @abstractmethod
    def fetch_fallback(
        self,
        since: Optional[str] = None,
        inject_anomalies: bool = False,
    ) -> List[RawRecord]:
        """Return sample/mock or configured fallback records."""

    def fetch_with_resilience(
        self,
        since: Optional[str] = None,
        mock: bool = False,
        inject_anomalies: bool = False,
    ) -> SourceExecutionResult:
        """Execute connector with retries, timeout, error isolation, and optional fallback."""
        start_ts = time.monotonic()

        if mock or self.is_mock_source:
            try:
                records = self.fetch_fallback(since=since, inject_anomalies=inject_anomalies)
                duration_ms = int((time.monotonic() - start_ts) * 1000)
                logger.info(
                    "[%s] Offline/mock mode: generated %d record(s) in %dms",
                    self.source_name,
                    len(records),
                    duration_ms,
                )
                return SourceExecutionResult(
                    source=self.source_name,
                    success=True,
                    used_fallback=True,
                    is_live=False,
                    records=records,
                    duration_ms=duration_ms,
                )
            except Exception as exc:  # noqa: BLE001
                duration_ms = int((time.monotonic() - start_ts) * 1000)
                logger.error("[%s] Mock/fallback fetch failed: %s", self.source_name, exc)
                return SourceExecutionResult(
                    source=self.source_name,
                    success=False,
                    used_fallback=True,
                    is_live=False,
                    records=[],
                    error=str(exc),
                    duration_ms=duration_ms,
                )

        last_error: Optional[Exception] = None
        attempts = max(1, self.max_retries)

        for attempt in range(1, attempts + 1):
            try:
                records = self.fetch_live(since=since)
                duration_ms = int((time.monotonic() - start_ts) * 1000)
                logger.info(
                    "[%s] Live fetch succeeded on attempt %d/%d (%d record(s), %dms)",
                    self.source_name,
                    attempt,
                    attempts,
                    len(records),
                    duration_ms,
                )
                return SourceExecutionResult(
                    source=self.source_name,
                    success=True,
                    used_fallback=False,
                    is_live=True,
                    records=records,
                    duration_ms=duration_ms,
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning(
                    "[%s] Attempt %d/%d failed: %s",
                    self.source_name,
                    attempt,
                    attempts,
                    exc,
                )
                if attempt < attempts:
                    sleep_seconds = self.backoff_factor ** (attempt - 1)
                    time.sleep(sleep_seconds)

        error_msg = str(last_error) if last_error else "Unknown connector error"
        if self.fallback_to_mock:
            try:
                logger.warning(
                    "[%s] Live fetch failed after %d attempt(s) (%s). Falling back to sample data.",
                    self.source_name,
                    attempts,
                    error_msg,
                )
                fallback_records = self.fetch_fallback(
                    since=since, inject_anomalies=inject_anomalies
                )
                duration_ms = int((time.monotonic() - start_ts) * 1000)
                return SourceExecutionResult(
                    source=self.source_name,
                    success=True,
                    used_fallback=True,
                    is_live=False,
                    records=fallback_records,
                    error=f"Live fetch failed ({error_msg}); used fallback data",
                    duration_ms=duration_ms,
                )
            except Exception as fb_exc:  # noqa: BLE001
                error_msg = f"{error_msg}; fallback also failed: {fb_exc}"

        duration_ms = int((time.monotonic() - start_ts) * 1000)
        logger.error("[%s] Live fetch failed permanently: %s", self.source_name, error_msg)
        return SourceExecutionResult(
            source=self.source_name,
            success=False,
            used_fallback=False,
            is_live=False,
            records=[],
            error=error_msg,
            duration_ms=duration_ms,
        )

    def _get_ssl_context(self, url: str) -> Optional[ssl.SSLContext]:
        """Hook for subclasses to supply a custom SSL context for HTTPS requests."""
        return None

    def _http_request_json(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Perform an HTTP request with configured timeout and decode JSON."""
        full_url = url
        if params:
            query_string = urllib.parse.urlencode(params)
            separator = "&" if "?" in url else "?"
            full_url = f"{url}{separator}{query_string}"

        req_headers = {
            "Accept": "application/json",
            "User-Agent": "GenericIngestionPipeline/0.2",
        }
        if headers:
            req_headers.update({k: str(v) for k, v in headers.items() if v is not None})

        data_bytes: Optional[bytes] = None
        if body is not None:
            data_bytes = json.dumps(body).encode("utf-8")
            req_headers.setdefault("Content-Type", "application/json")

        request = urllib.request.Request(
            full_url,
            data=data_bytes,
            headers=req_headers,
            method=method.upper(),
        )
        ssl_context = self._get_ssl_context(full_url)
        try:
            with urllib.request.urlopen(
                request,
                timeout=self.timeout_seconds,
                context=ssl_context,
            ) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                payload = response.read().decode(charset)
                return json.loads(payload)
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code} from {url}: {exc.reason}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error contacting {url}: {exc.reason}") from exc
        except TimeoutError as exc:
            raise RuntimeError(f"Request timed out after {self.timeout_seconds}s: {url}") from exc

    def _http_get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Convenience wrapper for HTTP GET JSON requests."""
        return self._http_request_json(url=url, method="GET", params=params, headers=headers)
