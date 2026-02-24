---
name: ticker-db-store
description: Store Yahoo Finance ticker metadata and historical prices into a reusable SQLite schema and run quick validation queries. Use when a user asks to ingest one or more tickers into SQLite, update existing ticker rows, bootstrap a multi-ticker market database, or verify that data landed correctly with a small SQL check.
---

# Ticker Db Store

## Overview

Ingest ticker data with `scripts/add_ticker_to_db.py`, then verify load quality with `scripts/check_ticker_in_db.py`. Use upsert semantics so re-running updates existing data instead of duplicating rows.

## Workflow

1. Run ingestion for a ticker:
```bash
python scripts/add_ticker_to_db.py --ticker ICOLCAP.CL --db stock_data.db --period max
```
2. Run a quick SQL check:
```bash
python scripts/check_ticker_in_db.py --ticker ICOLCAP.CL --db stock_data.db
```
3. Repeat with additional ticker symbols as needed.

## Scripts

- `scripts/add_ticker_to_db.py`
Purpose: fetch `Ticker.info` and OHLCV history from Yahoo Finance, then upsert into `ticker_info` and `ticker_prices`.

- `scripts/check_ticker_in_db.py`
Purpose: run a small SQL summary for one ticker (row count, min/max date, latest close).

## Notes

- Keep table design multi-ticker by using `ticker` as part of the primary key in `ticker_prices`.
- Prefer `ON CONFLICT ... DO UPDATE` so repeated runs stay idempotent.
- Use `--period` values supported by yfinance (for example `1y`, `5y`, `max`).
