"""Unit and integration tests for connectors, processor, storage, notifier, reporter, and pipeline."""

from __future__ import annotations

import csv
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock as unittest_mock

import main as cli_main
from src.config import load_config
from src.connectors.base import BaseConnector
from src.connectors.http_json import HttpJsonConnector
from src.models import RawRecord
from src.pipeline import IngestionPipeline
from src.processor import RecordProcessor, normalize_timestamp

CONFIG_PATH = "config/pipeline.example.yaml"


class _FlakyConnector(BaseConnector):
    source_name = "flaky_source"

    def __init__(self, fail_times: int, fallback_to_mock: bool = True) -> None:
        super().__init__(
            source_config={},
            http_config={
                "max_retries": 3,
                "backoff_factor": 0.01,
                "timeout_seconds": 1,
                "fallback_to_mock": fallback_to_mock,
            },
        )
        self.fail_times = fail_times
        self.calls = 0

    def fetch_live(self, since: Optional[str] = None) -> List[RawRecord]:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError(f"Simulated failure #{self.calls}")
        return [
            RawRecord(
                source=self.source_name,
                entity_id="item1",
                metric_name="price_usd",
                value=10.5,
                observed_at="2026-01-01T00:00:00Z",
                unit="USD",
            )
        ]

    def fetch_fallback(
        self, since: Optional[str] = None, inject_anomalies: bool = False
    ) -> List[RawRecord]:
        return [
            RawRecord(
                source=self.source_name,
                entity_id="item1",
                metric_name="price_usd",
                value=9.99,
                observed_at="2026-01-01T00:00:00Z",
                unit="USD",
            )
        ]


class TestGenericPipeline(unittest.TestCase):
    def test_config_loads_and_interpolates_env(self) -> None:
        os.environ["DATABASE_PATH"] = "data/custom_test.db"
        try:
            cfg = load_config(CONFIG_PATH, env_path=None)
            self.assertEqual(cfg["pipeline"]["database_path"], "data/custom_test.db")
            self.assertIn("sample_telemetry", cfg["sources"])
            self.assertIn("open_meteo_nyc", cfg["sources"])
            self.assertIn("coingecko_btc", cfg["sources"])
            self.assertEqual(
                len(cfg["sources"]["open_meteo_nyc"]["field_mapping"]["metrics"]), 3
            )
            self.assertEqual(len(cfg["sources"]["open_meteo_nyc"]["fallback_records"]), 3)
        finally:
            os.environ.pop("DATABASE_PATH", None)

    def test_rest_json_connector_dot_path_mapping(self) -> None:
        cfg = {
            "endpoint": "https://example.test/api",
            "records_path": "data.items",
            "field_mapping": {
                "entity_id": "meta.id",
                "observed_at": "timestamp",
                "metrics": [
                    {"name": "cpu_pct", "path": "stats.cpu", "unit": "pct"},
                    {"name": "latency_ms", "path": "stats.latency", "unit": "ms"},
                ],
            },
        }
        connector = HttpJsonConnector(source_config=cfg, source_name="custom_rest")
        payload = {
            "data": {
                "items": [
                    {
                        "meta": {"id": "node-1"},
                        "timestamp": "2026-01-01T12:00:00Z",
                        "stats": {"cpu": 42.5, "latency": 18.2},
                    }
                ]
            }
        }
        records = connector._map_payload(payload)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].source, "custom_rest")
        self.assertEqual(records[0].entity_id, "node-1")
        self.assertEqual(records[0].metric_name, "cpu_pct")
        self.assertEqual(records[0].value, 42.5)
        self.assertEqual(records[1].metric_name, "latency_ms")
        self.assertEqual(records[1].value, 18.2)

    def test_connector_retry_and_fallback(self) -> None:
        conn_recover = _FlakyConnector(fail_times=2, fallback_to_mock=True)
        res = conn_recover.fetch_with_resilience(mock=False)
        self.assertTrue(res.success)
        self.assertFalse(res.used_fallback)
        self.assertEqual(conn_recover.calls, 3)

        conn_fallback = _FlakyConnector(fail_times=5, fallback_to_mock=True)
        res_fb = conn_fallback.fetch_with_resilience(mock=False)
        self.assertTrue(res_fb.success)
        self.assertTrue(res_fb.used_fallback)
        self.assertEqual(res_fb.records[0].value, 9.99)

        conn_fail = _FlakyConnector(fail_times=5, fallback_to_mock=False)
        res_fail = conn_fail.fetch_with_resilience(mock=False)
        self.assertFalse(res_fail.success)
        self.assertEqual(len(res_fail.records), 0)

    def test_processor_normalization_dedup_and_bounds(self) -> None:
        processor = RecordProcessor(
            {
                "drop_duplicates": True,
                "drop_invalid": True,
                "metric_bounds": {"price_usd": {"min": 0.01, "max": 1000000}},
            }
        )
        raw = [
            RawRecord("crypto_market", "Bitcoin ", "PRICE_USD", "50000.5", 1735689600, "USD"),
            RawRecord("crypto_market", "bitcoin", "price_usd", 50000.5, "2025-01-01T00:00:00Z", "USD"),
            RawRecord("crypto_market", "bitcoin", "price_usd", -5.0, "2025-01-01T00:01:00Z", "USD"),
            RawRecord("crypto_market", "ethereum", "price_usd", None, "2025-01-01T00:01:00Z", "USD"),
            RawRecord("crypto_market", "solana", "price_usd", "bad", "2025-01-01T00:01:00Z", "USD"),
        ]
        res = processor.process_batch(raw)
        self.assertEqual(len(res.clean_records), 1)
        self.assertEqual(res.clean_records[0].observed_at, "2025-01-01T00:00:00Z")
        self.assertEqual(res.duplicates_dropped, 1)
        self.assertEqual(res.invalid_dropped, 3)
        self.assertEqual(len(res.issues), 4)

    def test_timestamp_normalization_variants(self) -> None:
        self.assertEqual(normalize_timestamp(1735689600), "2025-01-01T00:00:00Z")
        self.assertEqual(normalize_timestamp(1735689600000), "2025-01-01T00:00:00Z")
        self.assertEqual(normalize_timestamp("2025-01-01T00:00"), "2025-01-01T00:00:00Z")
        self.assertEqual(normalize_timestamp("2025-01-01T02:00:00+02:00"), "2025-01-01T00:00:00Z")

    def test_end_to_end_reports_webhook_fallback_and_idempotency(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_pipeline.db"
            report_dir = Path(tmpdir) / "reports"
            fallback_log = Path(tmpdir) / "webhook_fallback.jsonl"

            cfg = load_config(CONFIG_PATH, env_path=None)
            cfg["notifications"]["console_enabled"] = False
            cfg["notifications"]["webhook_enabled"] = True
            cfg["notifications"]["webhook_url"] = ""
            cfg["notifications"]["fallback_log_path"] = str(fallback_log)

            pipeline = IngestionPipeline(
                config=cfg,
                db_path_override=str(db_path),
                report_dir_override=str(report_dir),
            )

            # 1. Full offline run
            summary1 = pipeline.run(mode="full", mock=True, inject_anomalies=False)
            self.assertEqual(summary1.status, "SUCCESS")
            # sample_telemetry (9) + open_meteo_nyc (3) + coingecko_btc (2) = 14
            self.assertEqual(summary1.records_fetched, 14)
            self.assertEqual(summary1.records_valid, 14)
            self.assertEqual(pipeline.storage.get_record_count(), 14)

            # Verify Markdown, JSON, and CSV reports were generated
            self.assertTrue(Path(summary1.report_paths["markdown"]).is_file())
            self.assertTrue(Path(summary1.report_paths["json"]).is_file())
            self.assertTrue(Path(summary1.report_paths["csv"]).is_file())

            with Path(summary1.report_paths["csv"]).open("r", encoding="utf-8") as fh:
                csv_rows = list(csv.DictReader(fh))
            self.assertEqual(len(csv_rows), 14)

            # Verify webhook fallback log recorded the payload
            self.assertTrue(fallback_log.is_file())
            fallback_entries = [
                json.loads(line) for line in fallback_log.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(fallback_entries), 1)
            self.assertEqual(fallback_entries[0]["fallback_reason"], "unconfigured_webhook_url")

            # 2. Incremental run with injected anomalies
            summary2 = pipeline.run(mode="incremental", mock=True, inject_anomalies=True)
            self.assertEqual(summary2.status, "SUCCESS")
            self.assertGreaterEqual(summary2.records_invalid, 3)
            self.assertGreaterEqual(summary2.duplicates_dropped, 1)
            alert_titles = [a.title for a in summary2.alerts]
            self.assertIn("High Invalid Record Ratio", alert_titles)

            # 3. Dry-run should not add a new row to ingestion_runs
            runs_before = len(pipeline.storage.get_recent_runs(limit=10))
            pipeline.run(mode="incremental", mock=True, dry_run=True)
            runs_after = len(pipeline.storage.get_recent_runs(limit=10))
            self.assertEqual(runs_before, runs_after)

    def test_http_json_since_param_and_client_filter(self) -> None:
        cfg = {
            "endpoint": "https://example.test/events",
            "since_param": "updated_after",
            "filter_by_since": True,
            "records_path": "items",
            "field_mapping": {
                "entity_id": "id",
                "metric_name": "literal:count",
                "value": "val",
                "observed_at": "ts",
            },
        }
        connector = HttpJsonConnector(source_config=cfg, source_name="events_api")
        captured_params: Dict[str, Any] = {}

        def _fake_http(
            url: str,
            method: str = "GET",
            params: Optional[Dict[str, Any]] = None,
            headers: Optional[Dict[str, str]] = None,
            body: Optional[Dict[str, Any]] = None,
        ) -> Dict[str, Any]:
            captured_params.update(params or {})
            return {
                "items": [
                    {"id": "old", "val": 1, "ts": "2026-01-01T00:00:00Z"},
                    {"id": "new", "val": 2, "ts": "2026-01-01T01:00:00Z"},
                ]
            }

        with unittest_mock.patch.object(connector, "_http_request_json", side_effect=_fake_http):
            records = connector.fetch_live(since="2026-01-01T00:00:00Z")

        self.assertEqual(captured_params.get("updated_after"), "2026-01-01T00:00:00Z")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].entity_id, "new")

    def test_http_json_ssl_context_uses_certifi_and_falls_back(self) -> None:
        connector = HttpJsonConnector(
            source_config={
                "endpoint": "https://example.test/api",
                "field_mapping": {
                    "entity_id": "literal:e1",
                    "metric_name": "literal:m1",
                    "value": "val",
                    "observed_at": "ts",
                },
            },
            source_name="https_src",
        )

        # 1. When certifi is available, ssl.create_default_context(cafile=certifi.where()) is used
        fake_certifi = types.SimpleNamespace(where=lambda: "/fake/certifi/cacert.pem")
        sentinel_ctx = object()

        class _FakeResponse:
            headers = types.SimpleNamespace(get_content_charset=lambda: "utf-8")

            def read(self) -> bytes:
                return b'{"val": 12.5, "ts": "2026-01-01T00:00:00Z"}'

            def __enter__(self) -> "_FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

        with unittest_mock.patch.dict(sys.modules, {"certifi": fake_certifi}), \
            unittest_mock.patch(
                "src.connectors.http_json.ssl.create_default_context",
                return_value=sentinel_ctx,
            ) as mock_ssl_ctx, \
            unittest_mock.patch(
                "src.connectors.base.urllib.request.urlopen",
                return_value=_FakeResponse(),
            ) as mock_urlopen:
            records = connector.fetch_live()

        mock_ssl_ctx.assert_called_once_with(cafile="/fake/certifi/cacert.pem")
        self.assertIs(mock_urlopen.call_args.kwargs.get("context"), sentinel_ctx)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].value, 12.5)

        # 2. Plain HTTP URLs do not create an SSL context
        self.assertIsNone(connector._get_ssl_context("http://example.test/api"))

        # 3. When certifi is unavailable, falls back to ssl.create_default_context()
        fallback_ctx = object()
        with unittest_mock.patch.dict(sys.modules, {"certifi": None}), \
            unittest_mock.patch(
                "src.connectors.http_json.ssl.create_default_context",
                return_value=fallback_ctx,
            ) as mock_fallback_ctx:
            ctx = connector._get_ssl_context("https://example.test/api")

        mock_fallback_ctx.assert_called_once_with()
        self.assertIs(ctx, fallback_ctx)

    def test_sync_state_advances_on_success_and_holds_on_failure_or_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "sync_test.db"
            report_dir = Path(tmpdir) / "reports"
            cfg = load_config(CONFIG_PATH, env_path=None)
            cfg["notifications"]["console_enabled"] = False
            cfg["notifications"]["webhook_enabled"] = False
            cfg["sources"] = {
                "cursor_src": {
                    "enabled": True,
                    "type": "http_json",
                    "endpoint": "https://example.test/metrics",
                    "fallback_to_mock": False,
                    "max_retries": 1,
                }
            }

            pipeline = IngestionPipeline(
                config=cfg,
                db_path_override=str(db_path),
                report_dir_override=str(report_dir),
            )

            seen_since: List[Optional[str]] = []

            class _StubConnector(BaseConnector):
                source_name = "cursor_src"
                mode_behavior = "batch1"

                def fetch_live(self, since: Optional[str] = None) -> List[RawRecord]:
                    seen_since.append(since)
                    if self.mode_behavior == "batch1":
                        return [
                            RawRecord("cursor_src", "e1", "temperature_c", 20.0, "2026-01-01T10:00:00Z", "C"),
                            RawRecord("cursor_src", "e2", "temperature_c", 21.0, "2026-01-01T10:05:00Z", "C"),
                        ]
                    if self.mode_behavior == "batch2":
                        return [
                            RawRecord("cursor_src", "e1", "temperature_c", 22.0, "2026-01-01T10:15:00Z", "C"),
                        ]
                    if self.mode_behavior == "empty":
                        return []
                    if self.mode_behavior == "all_invalid":
                        return [
                            RawRecord("cursor_src", "e1", "temperature_c", "not_a_number", "2026-01-01T10:30:00Z", "C"),
                        ]
                    raise RuntimeError("Simulated upstream outage")

                def fetch_fallback(
                    self, since: Optional[str] = None, inject_anomalies: bool = False
                ) -> List[RawRecord]:
                    return []

            def _make_stub(_cfg: Dict[str, Any], selected_sources: Optional[List[str]] = None) -> List[BaseConnector]:
                return [_StubConnector(source_config=cfg["sources"]["cursor_src"], source_name="cursor_src")]

            with unittest_mock.patch("src.pipeline.build_connectors", side_effect=_make_stub):
                # 1. Initial incremental run -> no cursor yet, persists max observed_at (10:05)
                _StubConnector.mode_behavior = "batch1"
                s1 = pipeline.run(mode="incremental", mock=False)
                self.assertEqual(s1.status, "SUCCESS")
                self.assertEqual(seen_since[-1], None)
                state1 = pipeline.storage.get_sync_state("cursor_src")
                self.assertIsNotNone(state1)
                assert state1 is not None
                self.assertEqual(state1["last_record_ts"], "2026-01-01T10:05:00Z")
                self.assertEqual(state1["last_run_id"], s1.run_id)

                # 2. Failed run -> passes saved cursor 10:05, must NOT advance cursor
                _StubConnector.mode_behavior = "fail"
                s_fail = pipeline.run(mode="incremental", mock=False)
                self.assertEqual(s_fail.status, "FAILED")
                self.assertEqual(seen_since[-1], "2026-01-01T10:05:00Z")
                state_after_fail = pipeline.storage.get_sync_state("cursor_src")
                assert state_after_fail is not None
                self.assertEqual(state_after_fail["last_record_ts"], "2026-01-01T10:05:00Z")
                self.assertEqual(state_after_fail["last_run_id"], s1.run_id)

                # 3. Empty run -> must NOT advance cursor
                _StubConnector.mode_behavior = "empty"
                s_empty = pipeline.run(mode="incremental", mock=False)
                self.assertEqual(s_empty.status, "SUCCESS")
                state_after_empty = pipeline.storage.get_sync_state("cursor_src")
                assert state_after_empty is not None
                self.assertEqual(state_after_empty["last_record_ts"], "2026-01-01T10:05:00Z")
                self.assertEqual(state_after_empty["last_run_id"], s1.run_id)

                # 4. All-invalid run -> must NOT advance cursor
                _StubConnector.mode_behavior = "all_invalid"
                pipeline.run(mode="incremental", mock=False)
                state_after_invalid = pipeline.storage.get_sync_state("cursor_src")
                assert state_after_invalid is not None
                self.assertEqual(state_after_invalid["last_record_ts"], "2026-01-01T10:05:00Z")

                # 5. Successful incremental run with newer data -> advances cursor to 10:15
                _StubConnector.mode_behavior = "batch2"
                s2 = pipeline.run(mode="incremental", mock=False)
                self.assertEqual(s2.status, "SUCCESS")
                self.assertEqual(seen_since[-1], "2026-01-01T10:05:00Z")
                state2 = pipeline.storage.get_sync_state("cursor_src")
                assert state2 is not None
                self.assertEqual(state2["last_record_ts"], "2026-01-01T10:15:00Z")
                self.assertEqual(state2["last_run_id"], s2.run_id)

                # 6. Full mode -> bypasses saved cursor (since=None)
                _StubConnector.mode_behavior = "batch1"
                s_full = pipeline.run(mode="full", mock=False)
                self.assertEqual(s_full.status, "SUCCESS")
                self.assertIsNone(seen_since[-1])
                # Cursor must not move backwards from 10:15 to 10:05
                state_after_full = pipeline.storage.get_sync_state("cursor_src")
                assert state_after_full is not None
                self.assertEqual(state_after_full["last_record_ts"], "2026-01-01T10:15:00Z")

    def test_reset_state_and_interval_cli(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "cli_state.db"
            report_dir = Path(tmpdir) / "reports"

            # Populate sync_state via an offline run
            rc = cli_main.main(
                [
                    "--config",
                    CONFIG_PATH,
                    "--mode",
                    "incremental",
                    "--offline",
                    "--db",
                    str(db_path),
                    "--report-dir",
                    str(report_dir),
                ]
            )
            self.assertEqual(rc, 0)

            cfg = load_config(CONFIG_PATH, env_path=None)
            pipeline = IngestionPipeline(config=cfg, db_path_override=str(db_path), report_dir_override=str(report_dir))
            # Offline/mock runs must NOT advance sync_state
            self.assertEqual(pipeline.storage.get_all_sync_states(), [])

            # Seed sync_state cursors as if previous live runs succeeded
            run_id = pipeline.storage.get_recent_runs(limit=1)[0]["run_id"]
            for src_name in ("sample_telemetry", "open_meteo_nyc", "coingecko_btc"):
                pipeline.storage.update_sync_state(
                    source_name=src_name,
                    last_record_ts="2026-01-01T00:00:00Z",
                    last_success_at="2026-01-01T00:01:00Z",
                    last_run_id=run_id,
                )
            self.assertEqual(len(pipeline.storage.get_all_sync_states()), 3)

            # Scoped --reset-state clears only sample_telemetry
            rc_reset_one = cli_main.main(
                [
                    "--config",
                    CONFIG_PATH,
                    "--reset-state",
                    "--source",
                    "sample_telemetry",
                    "--db",
                    str(db_path),
                ]
            )
            self.assertEqual(rc_reset_one, 0)
            self.assertIsNone(pipeline.storage.get_sync_state("sample_telemetry"))
            self.assertIsNotNone(pipeline.storage.get_sync_state("open_meteo_nyc"))

            # Unscoped --reset-state clears remaining cursors
            rc_reset_all = cli_main.main(
                [
                    "--config",
                    CONFIG_PATH,
                    "--reset-state",
                    "--db",
                    str(db_path),
                ]
            )
            self.assertEqual(rc_reset_all, 0)
            self.assertEqual(pipeline.storage.get_all_sync_states(), [])

            # Verify --interval with --max-runs runs cleanly twice
            rc_interval = cli_main.main(
                [
                    "--config",
                    CONFIG_PATH,
                    "--mode",
                    "incremental",
                    "--offline",
                    "--source",
                    "sample_telemetry",
                    "--interval",
                    "0.05",
                    "--max-runs",
                    "2",
                    "--db",
                    str(db_path),
                    "--report-dir",
                    str(report_dir),
                ]
            )
            self.assertEqual(rc_interval, 0)
            # 1 initial run + 2 interval runs = 3 total runs
            self.assertEqual(len(pipeline.storage.get_recent_runs(limit=10)), 3)

    def test_cursor_not_advanced_on_fallback_or_mock_and_advances_on_live_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "fallback_cursor.db"
            report_dir = Path(tmpdir) / "reports"
            cfg = load_config(CONFIG_PATH, env_path=None)
            cfg["notifications"]["console_enabled"] = False
            cfg["notifications"]["webhook_enabled"] = False
            cfg["http"]["max_retries"] = 1
            cfg["http"]["backoff_factor"] = 0.01
            cfg["http"]["fallback_to_mock"] = True
            cfg["sources"] = {
                "live_http_src": {
                    "enabled": True,
                    "type": "http_json",
                    "endpoint": "https://example.test/telemetry",
                    "since_param": "since",
                    "records_path": "items",
                    "field_mapping": {
                        "entity_id": "id",
                        "metric_name": "literal:temperature_c",
                        "value": "temp",
                        "observed_at": "ts",
                        "unit": "literal:C",
                    },
                    "fallback_records": [
                        {
                            "entity_id": "node_1",
                            "metric_name": "temperature_c",
                            "value": 15.0,
                            "observed_at": "2026-01-01T11:00:00Z",
                            "unit": "C",
                        }
                    ],
                }
            }

            pipeline = IngestionPipeline(
                config=cfg,
                db_path_override=str(db_path),
                report_dir_override=str(report_dir),
            )

            seen_params: List[Dict[str, Any]] = []
            http_behavior = {"mode": "live_1"}

            def _fake_http(
                _self: BaseConnector,
                url: str,
                method: str = "GET",
                params: Optional[Dict[str, Any]] = None,
                headers: Optional[Dict[str, str]] = None,
                body: Optional[Dict[str, Any]] = None,
            ) -> Dict[str, Any]:
                seen_params.append(dict(params or {}))
                if http_behavior["mode"] == "live_1":
                    return {"items": [{"id": "node_1", "temp": 20.5, "ts": "2026-01-01T10:00:00Z"}]}
                if http_behavior["mode"] == "fail":
                    raise RuntimeError("Simulated HTTPS outage")
                return {"items": [{"id": "node_1", "temp": 23.0, "ts": "2026-01-01T12:00:00Z"}]}

            with unittest_mock.patch.object(BaseConnector, "_http_request_json", new=_fake_http):
                # 1. Live fetch succeeds -> upserts record and advances cursor to 10:00
                s1 = pipeline.run(mode="incremental", mock=False)
                self.assertEqual(s1.status, "SUCCESS")
                self.assertEqual(s1.sources_fallback, [])
                self.assertEqual(pipeline.storage.get_record_count("live_http_src"), 1)
                state1 = pipeline.storage.get_sync_state("live_http_src")
                assert state1 is not None
                self.assertEqual(state1["last_record_ts"], "2026-01-01T10:00:00Z")
                self.assertEqual(state1["last_run_id"], s1.run_id)

                # 2. Live fetch fails and falls back to fallback_records (11:00):
                #    records are still validated & upserted, but cursor MUST stay at 10:00
                http_behavior["mode"] = "fail"
                s2 = pipeline.run(mode="incremental", mock=False)
                self.assertEqual(s2.status, "SUCCESS")
                self.assertEqual(s2.sources_fallback, ["live_http_src"])
                self.assertEqual(s2.records_upserted, 1)
                self.assertEqual(pipeline.storage.get_record_count("live_http_src"), 2)
                state2 = pipeline.storage.get_sync_state("live_http_src")
                assert state2 is not None
                self.assertEqual(state2["last_record_ts"], "2026-01-01T10:00:00Z")
                self.assertEqual(state2["last_run_id"], s1.run_id)

                # 3. Offline/mock run -> cursor MUST still stay at 10:00
                s3 = pipeline.run(mode="incremental", mock=True)
                self.assertEqual(s3.status, "SUCCESS")
                self.assertEqual(s3.sources_fallback, ["live_http_src"])
                state3 = pipeline.storage.get_sync_state("live_http_src")
                assert state3 is not None
                self.assertEqual(state3["last_record_ts"], "2026-01-01T10:00:00Z")
                self.assertEqual(state3["last_run_id"], s1.run_id)

                # 4. Live fetch recovers (12:00) -> receives preserved cursor 10:00 and advances to 12:00
                http_behavior["mode"] = "live_2"
                s4 = pipeline.run(mode="incremental", mock=False)
                self.assertEqual(s4.status, "SUCCESS")
                self.assertEqual(s4.sources_fallback, [])
                self.assertEqual(seen_params[-1].get("since"), "2026-01-01T10:00:00Z")
                self.assertEqual(pipeline.storage.get_record_count("live_http_src"), 3)
                state4 = pipeline.storage.get_sync_state("live_http_src")
                assert state4 is not None
                self.assertEqual(state4["last_record_ts"], "2026-01-01T12:00:00Z")
                self.assertEqual(state4["last_run_id"], s4.run_id)


if __name__ == "__main__":
    unittest.main()
