---
name: ticker-db-store
description: Store Yahoo Finance ticker metadata and historical prices into a reusable SQLite schema and run quick validation queries. Use when a user asks to ingest one or more tickers into SQLite, update existing ticker rows, bootstrap a multi-ticker market database, or verify that data landed correctly with a small SQL check.
---

# Ticker Db Store

## Overview

Ingest ticker data with:
- `scripts/fetch_tickers_to_sqlite.py` (yfinance-based batch loader).

Use upsert semantics so re-running updates existing data instead of duplicating rows.

## Workflow

1. Run ingestion for tickers from JSON:
```bash
python scripts/fetch_tickers_to_sqlite.py --tickers-file ticker_names.json --db stock_data.db --period max
```
2. Repeat with additional ticker symbols as needed.

## Scripts

- `scripts/fetch_tickers_to_sqlite.py`
Purpose: fetch tickers from `ticker_names.json`, then upsert metadata + history into `ticker_info` and `ticker_prices`.

## Notes

- Keep table design multi-ticker by using `ticker` as part of the primary key in `ticker_prices`.
- Prefer `ON CONFLICT ... DO UPDATE` so repeated runs stay idempotent.
- Use `--period` values supported by yfinance (for example `1y`, `5y`, `max`).
