from __future__ import annotations

import argparse

import pandas as pd
import yfinance as yf

from cli_tools.common import (
    format_number,
    format_percent,
    human_number,
    load_fundamentals_base,
    normalize_ticker,
    print_table,
    resolve_db_path,
)


FIELDS = {
    "dividendYield": "dividend_yield",
    "fiveYearAvgDividendYield": "avg_dividend_yield_5y",
    "returnOnEquity": "roe",
    "bookValue": "book_value",
    "priceToBook": "price_to_book",
    "trailingEps": "eps_trailing",
    "forwardEps": "eps_forward",
    "trailingPE": "pe_trailing",
    "marketCap": "market_cap",
    "enterpriseValue": "enterprise_value",
    "profitMargins": "profit_margin",
    "revenueGrowth": "revenue_growth",
    "earningsGrowth": "earnings_growth",
    "financialCurrency": "financial_currency",
    "currentPrice": "current_price",
}

EXCLUDE_VALUATION_COLS = {"NUTRESA.CL"}


def fetch_fx_rate(from_ccy: str, to_ccy: str) -> float | None:
    if from_ccy == to_ccy:
        return 1.0
    try:
        ticker = yf.Ticker(f"{from_ccy}{to_ccy}=X")
        history = ticker.history(period="1d")
        if not history.empty:
            return float(history["Close"].iloc[-1])
    except Exception:
        return None
    return None


def load_fundamentals(db_path) -> pd.DataFrame:
    base = load_fundamentals_base(db_path)
    rows = []
    for row in base.itertuples(index=False):
        record = {
            "ticker": row.ticker,
            "symbol": row.symbol,
            "name": row.name,
            "sector": row.sector,
            "industry": row.industry,
            "currency": row.currency,
            "exchange": row.exchange,
            "updated_at": row.updated_at,
            "market_cap_table": row.market_cap_table,
        }
        for source_key, target_key in FIELDS.items():
            record[target_key] = row.raw_info.get(source_key)
        rows.append(record)

    frame = pd.DataFrame(rows)
    numeric_cols = [
        "dividend_yield",
        "avg_dividend_yield_5y",
        "roe",
        "book_value",
        "price_to_book",
        "eps_trailing",
        "eps_forward",
        "pe_trailing",
        "market_cap",
        "market_cap_table",
        "enterprise_value",
        "profit_margin",
        "revenue_growth",
        "earnings_growth",
        "current_price",
    ]
    for column in numeric_cols:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    for column in ("dividend_yield", "avg_dividend_yield_5y"):
        frame[column] = frame[column] / 100

    frame["market_cap"] = frame["market_cap"].fillna(frame["market_cap_table"])
    frame["updated_at"] = pd.to_datetime(frame["updated_at"], errors="coerce")
    frame = frame.drop(columns=["market_cap_table"])

    frame["currency_mismatch"] = frame["financial_currency"].notna() & (frame["financial_currency"] != frame["currency"])
    mismatched_pairs = (
        frame.loc[frame["currency_mismatch"], ["financial_currency", "currency"]]
        .drop_duplicates()
        .itertuples(index=False)
    )
    fx_cache: dict[tuple[str, str], float | None] = {}
    for financial_currency, quote_currency in mismatched_pairs:
        fx_cache[(financial_currency, quote_currency)] = fetch_fx_rate(financial_currency, quote_currency)

    for (financial_currency, quote_currency), rate in fx_cache.items():
        if rate is None:
            continue
        mask = (
            frame["currency_mismatch"]
            & (frame["financial_currency"] == financial_currency)
            & (frame["currency"] == quote_currency)
        )
        for column in ("book_value", "eps_trailing", "eps_forward"):
            frame.loc[mask, column] = frame.loc[mask, column] * rate
        has_price = frame["current_price"].notna() & frame["book_value"].notna()
        recompute = mask & has_price
        frame.loc[recompute, "price_to_book"] = frame.loc[recompute, "current_price"] / frame.loc[recompute, "book_value"]
        frame.loc[mask & ~has_price, "price_to_book"] = pd.NA

    exclude_mask = frame["ticker"].isin(EXCLUDE_VALUATION_COLS)
    frame.loc[exclude_mask, ["roe", "book_value", "price_to_book"]] = pd.NA
    return frame.drop(columns=["current_price"])


def format_snapshot(frame: pd.DataFrame) -> pd.DataFrame:
    view = frame.copy()
    percent_cols = ["dividend_yield", "avg_dividend_yield_5y", "roe", "profit_margin", "revenue_growth", "earnings_growth"]
    number_cols = ["book_value", "eps_trailing", "eps_forward", "price_to_book", "pe_trailing"]
    big_number_cols = ["market_cap", "enterprise_value"]

    for column in percent_cols:
        if column in view.columns:
            view[column] = view[column].map(format_percent)
    for column in number_cols:
        if column in view.columns:
            view[column] = view[column].map(format_number)
    for column in big_number_cols:
        if column in view.columns:
            view[column] = view[column].map(human_number)
    if "updated_at" in view.columns:
        view["updated_at"] = pd.to_datetime(view["updated_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    return view


def build_detail_view(row: pd.Series) -> pd.DataFrame:
    items = [
        ("Ticker", row["ticker"]),
        ("Name", row["name"]),
        ("Sector", row["sector"]),
        ("Industry", row["industry"]),
        ("Dividend yield", format_percent(row["dividend_yield"])),
        ("5Y avg dividend yield", format_percent(row["avg_dividend_yield_5y"])),
        ("ROE", format_percent(row["roe"])),
        ("Book value", format_number(row["book_value"])),
        ("Price to book", format_number(row["price_to_book"])),
        ("Trailing EPS", format_number(row["eps_trailing"])),
        ("Forward EPS", format_number(row["eps_forward"])),
        ("Trailing PE", format_number(row["pe_trailing"])),
        ("Profit margin", format_percent(row["profit_margin"])),
        ("Revenue growth", format_percent(row["revenue_growth"])),
        ("Earnings growth", format_percent(row["earnings_growth"])),
        ("Market cap", human_number(row["market_cap"])),
        ("Enterprise value", human_number(row["enterprise_value"])),
        ("Currency", row["currency"] or "-"),
        ("Exchange", row["exchange"] or "-"),
        (
            "Updated at",
            pd.to_datetime(row["updated_at"], errors="coerce").strftime("%Y-%m-%d %H:%M:%S")
            if pd.notna(row["updated_at"])
            else "-",
        ),
    ]
    return pd.DataFrame(items, columns=["metric", "value"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fundamentals helper CLI based on the fundamentals snapshot notebook.")
    parser.add_argument("--db", default=None, help="Path to SQLite database. Defaults to stock_data.db.")
    parser.add_argument("--ticker", help="Show a single ticker fundamentals summary.")
    parser.add_argument(
        "--ranking",
        choices=["dividend", "roe", "price-to-book", "eps", "snapshot"],
        default="snapshot",
        help="Choose the ranking or snapshot view to print.",
    )
    parser.add_argument("--limit", type=int, default=10, help="Number of rows to print.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    fundamentals = load_fundamentals(db_path)

    if args.ticker:
        ticker = normalize_ticker(args.ticker)
        selected = fundamentals.loc[fundamentals["ticker"] == ticker]
        if selected.empty:
            raise SystemExit(f"Ticker not found in database: {ticker}")
        print_table(build_detail_view(selected.iloc[0]))
        return

    if args.ranking == "dividend":
        selected = fundamentals[["symbol", "name", "sector", "dividend_yield", "market_cap"]].dropna(subset=["dividend_yield"])
        selected = selected.sort_values("dividend_yield", ascending=False).head(args.limit)
    elif args.ranking == "roe":
        selected = fundamentals[["symbol", "name", "sector", "roe", "market_cap"]].dropna(subset=["roe"])
        selected = selected.sort_values("roe", ascending=False).head(args.limit)
    elif args.ranking == "price-to-book":
        selected = fundamentals[["symbol", "name", "sector", "price_to_book", "market_cap"]].dropna(subset=["price_to_book"])
        selected = selected.sort_values("price_to_book", ascending=True).head(args.limit)
    elif args.ranking == "eps":
        selected = fundamentals[["symbol", "name", "sector", "eps_trailing", "market_cap"]].dropna(subset=["eps_trailing"])
        selected = selected.sort_values("eps_trailing", ascending=False).head(args.limit)
    else:
        selected = fundamentals[
            [
                "symbol",
                "name",
                "sector",
                "dividend_yield",
                "roe",
                "book_value",
                "price_to_book",
                "eps_trailing",
                "eps_forward",
                "pe_trailing",
                "profit_margin",
                "revenue_growth",
                "earnings_growth",
                "market_cap",
                "enterprise_value",
            ]
        ].sort_values(["roe", "dividend_yield"], ascending=[False, False]).head(args.limit)

    print(f"Database: {db_path}")
    print(f"Rows available: {len(fundamentals)}")
    print()
    print_table(format_snapshot(selected))


if __name__ == "__main__":
    main()
