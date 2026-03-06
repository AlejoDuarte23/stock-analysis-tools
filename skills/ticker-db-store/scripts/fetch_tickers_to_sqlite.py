#!/usr/bin/env python3
"""Fetch ticker data from Yahoo Finance and store it in SQLite for all tickers in ticker_names.json."""

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


def upsert_price_history(conn: sqlite3.Connection, ticker: str, period: str = "max") -> int:
    """Fetch and insert historical OHLCV data for a ticker."""
    history = yf.Ticker(ticker).history(period=period, auto_adjust=False, actions=True)
    if history.empty:
        return 0

    # Flatten index to plain date strings for SQLite primary key stability.
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


def load_tickers(tickers_file: Path) -> list[str]:
    """Load ticker symbols from a JSON file.

    Supports both a flat array  ["A", "B", ...]
    and the dict form         {"ticker_names": ["A", "B", ...]}.
    """
    with tickers_file.open() as f:
        data = json.load(f)

    if isinstance(data, list):
        tickers = data
    elif isinstance(data, dict) and "ticker_names" in data:
        tickers = data["ticker_names"]
    else:
        raise ValueError(
            f"Unexpected format in {tickers_file}. "
            'Expected a JSON array or {{"ticker_names": [...]}}.'
        )

    if not all(isinstance(t, str) for t in tickers):
        raise ValueError(f"All ticker entries in {tickers_file} must be strings.")

    return tickers


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch tickers sequentially from Yahoo Finance and store metadata + history in SQLite."
    )
    parser.add_argument(
        "--tickers-file",
        default="ticker_names.json",
        help="Path to JSON file with list of ticker symbols (default: ticker_names.json)",
    )
    parser.add_argument(
        "--db",
        default="stock_data.db",
        help="Path to SQLite database file (default: stock_data.db)",
    )
    parser.add_argument(
        "--period",
        default="max",
        help="History period for yfinance (default: max)",
    )
    args = parser.parse_args()

    tickers_file = Path(args.tickers_file)
    if not tickers_file.exists():
        raise FileNotFoundError(f"Tickers file not found: {tickers_file}")

    tickers = load_tickers(tickers_file)
    total = len(tickers)
    print(f"Loaded {total} ticker(s) from {tickers_file}")

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    failed: list[str] = []

    with sqlite3.connect(db_path) as conn:
        create_tables(conn)

        for i, ticker in enumerate(tickers, start=1):
            print(f"\n[{i}/{total}] Fetching {ticker}...")
            try:
                info = fetch_info(ticker)
                upsert_ticker_info(conn, ticker, info)
                count = upsert_price_history(conn, ticker, period=args.period)
                conn.commit()
                print(f"  ✓ Saved ticker info and {count} price rows for {ticker}")
            except Exception as exc:
                print(f"  ✗ Failed to fetch {ticker}: {exc}")
                failed.append(ticker)

    print(f"\nDone. Database: {db_path}")
    if failed:
        print(f"Failed tickers ({len(failed)}): {failed}")
    else:
        print("All tickers fetched successfully.")


if __name__ == "__main__":
    main()
