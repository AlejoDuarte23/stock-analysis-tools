#!/usr/bin/env python3
"""Fetch one ticker from Yahoo Finance and upsert it into SQLite."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

import yfinance as yf


def create_tables(conn: sqlite3.Connection) -> None:
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


def fetch_ticker_info(ticker: str) -> dict[str, Any]:
    info = yf.Ticker(ticker).info
    if not info:
        raise RuntimeError(f"No ticker metadata returned for {ticker}")
    return info


def upsert_ticker_info(conn: sqlite3.Connection, ticker: str, info: dict[str, Any]) -> None:
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


def upsert_price_history(conn: sqlite3.Connection, ticker: str, period: str) -> int:
    history = yf.Ticker(ticker).history(period=period, auto_adjust=False, actions=True)
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Insert or update one ticker in SQLite.")
    parser.add_argument("--ticker", required=True, help="Ticker symbol, for example ICOLCAP.CL")
    parser.add_argument("--db", default="stock_data.db", help="SQLite file path")
    parser.add_argument("--period", default="max", help="yfinance history period")
    args = parser.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        create_tables(conn)
        info = fetch_ticker_info(args.ticker)
        upsert_ticker_info(conn, args.ticker, info)
        inserted = upsert_price_history(conn, args.ticker, args.period)
        conn.commit()

    print(f"Upserted {args.ticker}: {inserted} price rows into {db_path}")


if __name__ == "__main__":
    main()
