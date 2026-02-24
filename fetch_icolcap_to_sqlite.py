#!/usr/bin/env python3
"""Fetch ICOLCAP.CL data from Yahoo Finance and store it in SQLite."""

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch a ticker from Yahoo Finance and store metadata + history in SQLite."
    )
    parser.add_argument(
        "--ticker",
        default="ICOLCAP.CL",
        help="Ticker symbol to fetch (default: ICOLCAP.CL)",
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

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        create_tables(conn)
        info = fetch_info(args.ticker)
        upsert_ticker_info(conn, args.ticker, info)
        count = upsert_price_history(conn, args.ticker, period=args.period)
        conn.commit()

    print(f"Saved ticker info and {count} price rows for {args.ticker} into {db_path}")


if __name__ == "__main__":
    main()
