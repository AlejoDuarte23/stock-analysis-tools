#!/usr/bin/env python3
"""Fetch correlation series from Yahoo Finance and store them in a separate SQLite DB."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

import yfinance as yf


def create_tables(conn: sqlite3.Connection) -> None:
    """Create tables that can store multiple tickers over time."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ticker_info (
            ticker TEXT PRIMARY KEY,
            short_name TEXT,
            long_name TEXT,
            quote_type TEXT,
            currency TEXT,
            exchange TEXT,
            market_cap REAL,
            sector TEXT,
            industry TEXT,
            country TEXT,
            website TEXT,
            raw_info_json TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ticker_prices (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume REAL,
            dividends REAL,
            stock_splits REAL,
            PRIMARY KEY (ticker, date),
            FOREIGN KEY (ticker) REFERENCES ticker_info (ticker)
        )
        """
    )


def upsert_ticker_info(conn: sqlite3.Connection, ticker: str, info: dict[str, Any]) -> None:
    """Insert or update ticker metadata."""
    conn.execute(
        """
        INSERT INTO ticker_info (
            ticker, short_name, long_name, quote_type, currency, exchange, market_cap,
            sector, industry, country, website, raw_info_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(ticker) DO UPDATE SET
            short_name = excluded.short_name,
            long_name = excluded.long_name,
            quote_type = excluded.quote_type,
            currency = excluded.currency,
            exchange = excluded.exchange,
            market_cap = excluded.market_cap,
            sector = excluded.sector,
            industry = excluded.industry,
            country = excluded.country,
            website = excluded.website,
            raw_info_json = excluded.raw_info_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            ticker,
            info.get("shortName"),
            info.get("longName"),
            info.get("quoteType"),
            info.get("currency"),
            info.get("exchange"),
            info.get("marketCap"),
            info.get("sector"),
            info.get("industry"),
            info.get("country"),
            info.get("website"),
            json.dumps(info, ensure_ascii=True),
        ),
    )


def upsert_price_history(
    conn: sqlite3.Connection,
    ticker: str,
    period: str,
    start_date: str | None,
    end_date: str | None,
) -> int:
    """Fetch and insert historical OHLCV data for a ticker."""
    history_kwargs: dict[str, Any] = {"auto_adjust": False, "actions": True}
    if start_date or end_date:
        if start_date:
            history_kwargs["start"] = start_date
        if end_date:
            history_kwargs["end"] = end_date
    else:
        history_kwargs["period"] = period

    history = yf.Ticker(ticker).history(**history_kwargs)
    if history.empty:
        return 0

    history = history.reset_index()
    date_column = history.columns[0]
    history[date_column] = history[date_column].dt.date.astype(str)

    rows: list[tuple[Any, ...]] = []
    for _, row in history.iterrows():
        rows.append(
            (
                ticker,
                row[date_column],
                row.get("Open"),
                row.get("High"),
                row.get("Low"),
                row.get("Close"),
                row.get("Adj Close"),
                row.get("Volume"),
                row.get("Dividends"),
                row.get("Stock Splits"),
            )
        )

    conn.executemany(
        """
        INSERT INTO ticker_prices (
            ticker, date, open, high, low, close, adj_close, volume, dividends, stock_splits
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, date) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            adj_close = excluded.adj_close,
            volume = excluded.volume,
            dividends = excluded.dividends,
            stock_splits = excluded.stock_splits
        """,
        rows,
    )
    return len(rows)


def fetch_info(ticker: str) -> dict[str, Any]:
    """Fetch ticker metadata from Yahoo Finance."""
    info = yf.Ticker(ticker).info
    if not info:
        raise RuntimeError(f"No metadata returned for ticker: {ticker}")
    return info


def load_series(series_file: Path) -> dict[str, str]:
    """Load correlation series as {friendly_name: ticker_symbol}."""
    with series_file.open() as f:
        data = json.load(f)

    if isinstance(data, dict) and "series" in data and isinstance(data["series"], dict):
        raw_series = data["series"]
    else:
        raise ValueError(
            f"Unexpected format in {series_file}. Expected "
            '{"series": {"COLCAP": "ICOLCAP.CL", ...}}.'
        )

    series: dict[str, str] = {}
    for name, ticker in raw_series.items():
        if not isinstance(name, str) or not isinstance(ticker, str):
            raise ValueError("All series names and ticker values must be strings.")
        series[name] = ticker

    if not series:
        raise ValueError(f"No series found in {series_file}.")
    return series


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch correlation series from Yahoo Finance and store into SQLite."
    )
    parser.add_argument(
        "--series-file",
        default="covariables_series.json",
        help="Path to JSON file with {series: {friendly_name: ticker}}",
    )
    parser.add_argument(
        "--db",
        default="covariables_data.db",
        help="Path to SQLite database file (default: covariables_data.db)",
    )
    parser.add_argument(
        "--period",
        default="max",
        help="History period for yfinance when --start-date/--end-date are not used (default: max)",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional start date (YYYY-MM-DD). If set, overrides --period behavior.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional end date (YYYY-MM-DD).",
    )
    args = parser.parse_args()

    series_file = Path(args.series_file)
    if not series_file.exists():
        raise FileNotFoundError(f"Series file not found: {series_file}")

    series = load_series(series_file)
    total = len(series)
    print(f"Loaded {total} series from {series_file}")

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    failed: list[str] = []

    with sqlite3.connect(db_path) as conn:
        create_tables(conn)

        for i, (name, ticker) in enumerate(series.items(), start=1):
            print(f"\n[{i}/{total}] Fetching {name} ({ticker})...")
            try:
                info = fetch_info(ticker)
                upsert_ticker_info(conn, ticker, info)
                count = upsert_price_history(
                    conn=conn,
                    ticker=ticker,
                    period=args.period,
                    start_date=args.start_date,
                    end_date=args.end_date,
                )
                conn.commit()
                print(f"  Saved ticker info and {count} price rows for {name} ({ticker})")
            except Exception as exc:
                print(f"  Failed to fetch {name} ({ticker}): {exc}")
                failed.append(f"{name}:{ticker}")

    print(f"\nDone. Database: {db_path}")
    if failed:
        print(f"Failed series ({len(failed)}): {failed}")
    else:
        print("All correlation series fetched successfully.")


if __name__ == "__main__":
    main()
