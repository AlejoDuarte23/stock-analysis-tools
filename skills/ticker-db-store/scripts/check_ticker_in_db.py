#!/usr/bin/env python3
"""Run a small SQL check for one ticker in SQLite."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Quick SQL check for a ticker in SQLite.")
    parser.add_argument("--ticker", required=True, help="Ticker symbol, for example ICOLCAP.CL")
    parser.add_argument("--db", default="stock_data.db", help="SQLite file path")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                p1.ticker,
                COUNT(*) AS row_count,
                MIN(date) AS first_date,
                MAX(date) AS last_date,
                (
                    SELECT close
                    FROM ticker_prices p2
                    WHERE p2.ticker = p1.ticker
                    ORDER BY date DESC
                    LIMIT 1
                ) AS latest_close
            FROM ticker_prices p1
            WHERE ticker = ?
            GROUP BY ticker
            """,
            (args.ticker,),
        ).fetchone()

    if row is None or row[1] == 0:
        print(f"No price rows found for {args.ticker}")
        return

    print(f"Ticker:      {row[0]}")
    print(f"Rows:        {row[1]}")
    print(f"First date:  {row[2]}")
    print(f"Last date:   {row[3]}")
    print(f"Latest close:{row[4]}")


if __name__ == "__main__":
    main()
