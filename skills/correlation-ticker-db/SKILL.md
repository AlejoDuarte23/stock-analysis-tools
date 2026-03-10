---
name: correlation-ticker-db
description: Fetch and store the correlation ticker basket (ICOLCAP.CL, USDCOP=X, BZ=F, ^GSPC, ^VIX, ^TNX) into a dedicated SQLite database. Use when a user asks to refresh or bootstrap the covariables DB for correlation work.
---

# Correlation Ticker DB

## Purpose

Load the fixed 6-series basket used for COLCAP macro-correlation analysis into `covariables_data.db`.

## Workflow

1. Ensure the series config exists at `covariables_series.json`.
2. Run ingestion:

```bash
python fetch_covariables_to_sqlite.py \
  --series-file covariables_series.json \
  --db covariables_data.db \
  --start-date 2018-01-01
```

3. Optional refresh mode:

```bash
python fetch_covariables_to_sqlite.py \
  --series-file covariables_series.json \
  --db covariables_data.db \
  --period max
```

## Script

- `scripts/fetch_covariables_to_sqlite.py`
Purpose: thin wrapper that runs project script `fetch_covariables_to_sqlite.py`.

## Notes

- Table schema is multi-ticker (`ticker_info`, `ticker_prices`) with upsert behavior.
- Re-running updates existing rows and appends new dates.
- Keep this skill focused on DB ingestion only.
