from __future__ import annotations

import argparse

import pandas as pd

from cli_tools.common import (
    format_number,
    load_price_snapshot,
    print_table,
    resolve_db_path,
    ticker_label,
)


def get_anchor_price(series: pd.Series, anchor_date: pd.Timestamp):
    history = series.loc[series.index <= anchor_date].dropna()
    if history.empty:
        return pd.NA
    return history.iloc[-1]


def trailing_change(series: pd.Series, latest_date: pd.Timestamp, anchor_date: pd.Timestamp):
    latest_price = get_anchor_price(series, latest_date)
    anchor_price = get_anchor_price(series, anchor_date)
    if pd.isna(latest_price) or pd.isna(anchor_price) or anchor_price == 0:
        return pd.NA
    return latest_price / anchor_price - 1


def build_snapshot_table(price_table: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ticker in price_table.columns:
        series = price_table[ticker].dropna()
        if series.empty:
            continue

        latest_date = series.index.max()
        previous_date = series.index[series.index < latest_date].max() if (series.index < latest_date).any() else pd.NaT
        month_anchor = latest_date - pd.DateOffset(months=1)
        quarter_anchor = latest_date - pd.DateOffset(months=3)
        year_anchor = latest_date - pd.DateOffset(years=1)
        ytd_anchor = pd.Timestamp(year=latest_date.year, month=1, day=1)

        rows.append(
            {
                "ticker": ticker_label(ticker),
                "as_of": latest_date.date().isoformat(),
                "last_price": series.loc[latest_date],
                "day_vs_day": trailing_change(series, latest_date, previous_date),
                "week_vs_week": trailing_change(series, latest_date, latest_date - pd.Timedelta(days=7)),
                "month_vs_month": trailing_change(series, latest_date, month_anchor),
                "quarter_vs_quarter": trailing_change(series, latest_date, quarter_anchor),
                "year_vs_year": trailing_change(series, latest_date, year_anchor),
                "ytd": trailing_change(series, latest_date, ytd_anchor),
            }
        )

    return pd.DataFrame(rows)


def format_snapshot(frame: pd.DataFrame) -> pd.DataFrame:
    view = frame.copy()
    view["last_price"] = view["last_price"].map(format_number)
    for column in [
        "day_vs_day",
        "week_vs_week",
        "month_vs_month",
        "quarter_vs_quarter",
        "year_vs_year",
        "ytd",
    ]:
        if column in view.columns:
            view[column] = view[column].map(lambda value: "-" if pd.isna(value) else f"{value:+.2%}")
    return view


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Relative price change CLI based on the market relative moves notebook.")
    parser.add_argument("--db", default=None, help="Path to SQLite database. Defaults to stock_data.db.")
    parser.add_argument(
        "--sort-by",
        choices=["day_vs_day", "week_vs_week", "month_vs_month", "quarter_vs_quarter", "year_vs_year", "ytd"],
        default="week_vs_week",
        help="Metric used to sort the report.",
    )
    parser.add_argument("--limit", type=int, default=20, help="Number of rows to print.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    prices_long = load_price_snapshot(db_path)
    price_table = prices_long.pivot_table(index="date", columns="ticker", values="adj_close", aggfunc="last").sort_index()
    snapshot = build_snapshot_table(price_table)
    latest_date = pd.to_datetime(snapshot["as_of"]).max()
    selected = snapshot.sort_values(args.sort_by, ascending=False).head(args.limit)

    print(f"Database: {db_path}")
    print(f"Latest trading date in DB: {latest_date.date()}")
    print(f"Tickers included: {len(snapshot)}")
    print()
    print_table(format_snapshot(selected))


if __name__ == "__main__":
    main()
