from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd


DEFAULT_DB_CANDIDATES = (
    Path("stock_data.db"),
    Path("../stock_data.db"),
)


def resolve_db_path(db: str | Path | None = None) -> Path:
    if db is not None:
        return Path(db)

    for candidate in DEFAULT_DB_CANDIDATES:
        if candidate.exists():
            return candidate
    return DEFAULT_DB_CANDIDATES[0]


def normalize_ticker(ticker: str) -> str:
    ticker = ticker.strip().upper()
    return ticker if ticker.endswith(".CL") else f"{ticker}.CL"


def ticker_label(ticker: str) -> str:
    return ticker.replace(".CL", "")


def human_number(value: float | int | None) -> str:
    if pd.isna(value):
        return "-"

    value = float(value)
    abs_value = abs(value)
    if abs_value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:,.2f}T"
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:,.2f}B"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:,.2f}M"
    return f"{value:,.2f}"


def format_percent(value: float | int | None, decimals: int = 2) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):+.{decimals}%}"


def format_number(value: float | int | None, decimals: int = 2) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):,.{decimals}f}"


def print_table(frame: pd.DataFrame) -> None:
    if frame.empty:
        print("No rows found.")
        return
    print(frame.fillna("-").to_string(index=False))


def load_price_history(db_path: Path) -> pd.DataFrame:
    query = """
        SELECT
            p.ticker,
            p.date,
            p.open,
            p.high,
            p.low,
            p.close,
            COALESCE(p.adj_close, p.close) AS price,
            p.volume,
            i.short_name,
            i.long_name,
            i.exchange,
            i.currency
        FROM ticker_prices AS p
        LEFT JOIN ticker_info AS i
            ON i.ticker = p.ticker
        ORDER BY p.ticker, p.date
    """
    with sqlite3.connect(db_path) as conn:
        frame = pd.read_sql_query(query, conn)

    frame["date"] = pd.to_datetime(frame["date"])
    numeric_cols = ["open", "high", "low", "close", "price", "volume"]
    frame[numeric_cols] = frame[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return frame


def load_price_snapshot(db_path: Path) -> pd.DataFrame:
    query = """
        SELECT
            ticker,
            date,
            COALESCE(adj_close, close) AS adj_close,
            close
        FROM ticker_prices
        ORDER BY ticker, date
    """
    with sqlite3.connect(db_path) as conn:
        frame = pd.read_sql_query(query, conn)

    frame["date"] = pd.to_datetime(frame["date"])
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame.dropna(subset=["adj_close"])


def load_fundamentals_base(db_path: Path) -> pd.DataFrame:
    query = """
        SELECT
            ticker,
            short_name,
            long_name,
            sector,
            industry,
            currency,
            exchange,
            market_cap,
            raw_info_json,
            updated_at
        FROM ticker_info
        WHERE raw_info_json IS NOT NULL
        ORDER BY ticker
    """
    with sqlite3.connect(db_path) as conn:
        base = pd.read_sql_query(query, conn)

    rows = []
    for row in base.itertuples(index=False):
        raw = json.loads(row.raw_info_json) if row.raw_info_json else {}
        rows.append(
            {
                "ticker": row.ticker,
                "symbol": ticker_label(row.ticker),
                "name": row.long_name or row.short_name or row.ticker,
                "sector": row.sector,
                "industry": row.industry,
                "currency": row.currency,
                "exchange": row.exchange,
                "market_cap_table": row.market_cap,
                "raw_info": raw,
                "updated_at": row.updated_at,
            }
        )

    return pd.DataFrame(rows)
