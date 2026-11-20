# Generic Multi-Source Ingestion & Webhook/Report Automation Workflow

A clean, config-driven Python 3.9+ automation pipeline for multi-source data collection, normalization/deduplication, idempotent SQLite persistence, Webhook notifications (with local fallback logging), and Markdown/JSON/CSV summary reporting.

Runs out of the box using only the Python standard library (optional `PyYAML` and `pytest` listed in `requirements.txt`).

## Project Structure

```
├── config/
│   └── pipeline.example.yaml    # Config-driven source definitions, bounds, reports & webhook thresholds
├── deploy/
│   ├── crontab.example          # Sample cron schedule for incremental & full syncs
│   └── com.pipeline.ingestion.plist # Sample macOS launchd LaunchAgent template
├── .env.example                 # Secrets & environment override template
├── main.py                      # Config-driven CLI entry point
├── src/
│   ├── config.py                # YAML + .env loader with ${VAR:-default} interpolation
│   ├── models.py                # Shared dataclasses (RawRecord, CleanRecord, RunSummary, AlertEvent)
│   ├── logging_setup.py         # Console + rotating file logger
│   ├── connectors/
│   │   ├── base.py              # BaseConnector ABC (timeouts, retry/backoff, error isolation, fallback)
│   │   ├── http_json.py         # Generic config-driven HTTP/JSON/REST connector
│   │   └── mock_source.py       # Built-in offline mock/sample connector
│   ├── processor.py             # Timestamp/field normalization, bounds validation & deduplication
│   ├── storage.py               # SQLite schema, indexes, idempotent upserts & ingestion_runs history
│   ├── notifier.py              # Threshold evaluation & Webhook delivery (with JSONL local fallback)
│   ├── reporter.py              # Per-run summary report generator (Markdown, JSON, CSV)
│   └── pipeline.py              # End-to-end orchestrator with per-source error isolation
└── tests/
    └── test_pipeline.py         # Unit & integration test suite
```

## Quick Start

1. **(Optional) Copy config & environment templates:**
   ```bash
   cp config/pipeline.example.yaml config/pipeline.yaml
   cp .env.example .env
   ```

2. **List configured sources:**
   ```bash
   python3 main.py --list-sources
   ```

3. **Run immediately offline with sample/mock data (generates MD, JSON & CSV reports):**
   ```bash
   python3 main.py --mode full --offline
   ```

4. **Test validation, deduplication & Webhook/alert triggers with injected anomalies:**
   ```bash
   python3 main.py --mode incremental --offline --inject-anomalies
   ```

5. **Dry-run (no SQLite writes, no live webhook POSTs):**
   ```bash
   python3 main.py --mode incremental --offline --dry-run
   ```

6. **Inspect SQLite run history:**
   ```bash
   python3 main.py --show-history
   ```

7. **Run tests:**
   ```bash
   python3 -m unittest discover -s tests -v
   ```

## Adding a New HTTP/JSON Source Without Editing Code

Add a new block under `sources:` in `config/pipeline.yaml` with `type: "http_json"`:

```yaml
sources:
  my_service_metrics:
    enabled: true
    type: "http_json"
    endpoint: "https://api.example.com/v1/metrics"
    method: "GET"
    since_param: "updated_after"
    headers:
      Authorization: "Bearer ${REST_API_TOKEN:-}"
    records_path: "data.items"
    field_mapping:
      entity_id: "service_name"
      observed_at: "timestamp"
      metrics:
        - name: "latency_ms"
          path: "metrics.p95_ms"
          unit: "ms"
```

## Incremental Sync State & Scheduling

- **Persistent Cursors (`sync_state`):** Each source's latest valid `observed_at` timestamp (`last_record_ts`), `last_success_at`, and `last_run_id` are stored in SQLite (`sync_state` table).
  - In `--mode incremental`, the saved cursor is loaded before fetching and passed via `since_param` (and/or filtered client-side when `filter_by_since: true`) in `HttpJsonConnector`.
  - Cursors advance **only** when a source succeeds and yields valid upserted records; failed or empty runs leave the cursor untouched.
  - In `--mode full`, saved cursors are bypassed while idempotent deduplication and `ON CONFLICT` upserts prevent duplicates.
- **Resetting State:**
  ```bash
  # Reset cursor for a single source
  python3 main.py --reset-state --source open_meteo_nyc

  # Reset cursors for all sources
  python3 main.py --reset-state
  ```
- **Interval / Background Worker Mode:**
  ```bash
  # Run every 300 seconds with clean SIGINT / SIGTERM shutdown
  python3 main.py --mode incremental --interval 300
  ```
- **Scheduler Templates (`deploy/`):**
  - Cron: [deploy/crontab.example](deploy/crontab.example)
  - macOS `launchd`: [deploy/com.pipeline.ingestion.plist](deploy/com.pipeline.ingestion.plist)

## CLI Flags

| Flag | Description |
| --- | --- |
| `--config PATH` | Path to YAML config (defaults to `config/pipeline.yaml` or `config/pipeline.example.yaml`) |
| `--mode {full,incremental}` | Full sync or incremental sync using saved per-source `sync_state` cursor |
| `--source` / `--sources NAME ...` | Run (or reset) only a subset of configured sources |
| `--offline` / `--mock` | Use offline sample/fallback data instead of live HTTP calls |
| `--inject-anomalies` | Inject duplicate/invalid records in offline mode to exercise alerts |
| `--dry-run` | Skip SQLite writes, cursor updates, and live webhook POSTs |
| `--reset-state` | Clear saved `sync_state` cursors (optionally scoped with `--source`) and exit |
| `--interval SECONDS` | Run repeatedly every `N` seconds with clean `SIGINT`/`SIGTERM` handling |
| `--max-runs N` | Optional iteration cap when using `--interval` |
| `--formats {md,json,csv} ...` | Choose report formats to generate |
| `--db PATH` | Override SQLite database path |
| `--report-dir PATH` | Override report output directory |
| `--list-sources` | Print configured sources and exit |
| `--show-history` | Print recent `ingestion_runs` and saved `sync_state` cursors and exit |
