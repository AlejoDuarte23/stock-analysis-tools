from __future__ import annotations

import argparse

import pandas as pd

from cli_tools.common import (
    format_number,
    format_percent,
    load_price_history,
    normalize_ticker,
    print_table,
    resolve_db_path,
)


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))

    flat_mask = (avg_gain == 0) & (avg_loss == 0)
    zero_loss_mask = avg_loss == 0
    rsi = rsi.mask(flat_mask, 50)
    rsi = rsi.mask(zero_loss_mask & ~flat_mask, 100)
    return rsi


def compute_ticker_signal(stock: pd.DataFrame) -> pd.Series:
    stock = stock.sort_values("date").copy()
    price = stock["price"]

    stock["ema_12"] = price.ewm(span=12, adjust=False).mean()
    stock["ema_20"] = price.ewm(span=20, adjust=False).mean()
    stock["ema_26"] = price.ewm(span=26, adjust=False).mean()
    stock["ema_50"] = price.ewm(span=50, adjust=False).mean()

    stock["macd_line"] = stock["ema_12"] - stock["ema_26"]
    stock["macd_signal"] = stock["macd_line"].ewm(span=9, adjust=False).mean()
    stock["macd_hist"] = stock["macd_line"] - stock["macd_signal"]
    stock["rsi_14"] = compute_rsi(price, 14)

    stock["bb_mid"] = price.rolling(20).mean()
    bb_std = price.rolling(20).std()
    stock["bb_upper"] = stock["bb_mid"] + (2 * bb_std)
    stock["bb_lower"] = stock["bb_mid"] - (2 * bb_std)
    stock["bb_width"] = stock["bb_upper"] - stock["bb_lower"]
    stock["bb_pct_b"] = (price - stock["bb_lower"]) / stock["bb_width"].replace(0, pd.NA)
    stock["daily_return"] = price.pct_change()

    latest = stock.iloc[-1]
    previous = stock.iloc[-2] if len(stock) > 1 else latest

    pivot = (previous["high"] + previous["low"] + previous["close"]) / 3
    r1 = (2 * pivot) - previous["low"]
    s1 = (2 * pivot) - previous["high"]
    r2 = pivot + (previous["high"] - previous["low"])
    s2 = pivot - (previous["high"] - previous["low"])
    r3 = previous["high"] + 2 * (pivot - previous["low"])
    s3 = previous["low"] - 2 * (previous["high"] - pivot)

    pivot_bias = "above pivot" if latest["price"] >= pivot else "below pivot"
    if pd.notna(s1) and latest["price"] <= s1:
        pivot_zone = "at support"
    elif pd.notna(r1) and latest["price"] >= r1:
        pivot_zone = "at resistance"
    else:
        pivot_zone = "inside range"

    rsi_state = "oversold" if latest["rsi_14"] < 30 else "overbought" if latest["rsi_14"] > 70 else "neutral"
    macd_state = "bullish" if latest["macd_line"] > latest["macd_signal"] else "bearish"
    if latest["macd_hist"] > previous["macd_hist"] and macd_state == "bullish":
        macd_state = "improving bullish"
    elif latest["macd_hist"] < previous["macd_hist"] and macd_state == "bearish":
        macd_state = "weakening bearish"

    bollinger_state = (
        "below lower band"
        if latest["price"] <= latest["bb_lower"]
        else "above upper band"
        if latest["price"] >= latest["bb_upper"]
        else "inside bands"
    )

    return pd.Series(
        {
            "ticker": latest["ticker"],
            "name": latest["long_name"] or latest["short_name"] or latest["ticker"],
            "date": latest["date"].date().isoformat(),
            "price": latest["price"],
            "daily_return_pct": latest["daily_return"],
            "pivot": pivot,
            "pivot_s1": s1,
            "pivot_s2": s2,
            "pivot_s3": s3,
            "pivot_r1": r1,
            "pivot_r2": r2,
            "pivot_r3": r3,
            "distance_to_pivot_pct": (latest["price"] / pivot) - 1 if pivot else pd.NA,
            "pivot_bias": pivot_bias,
            "pivot_zone": pivot_zone,
            "rsi_14": latest["rsi_14"],
            "rsi_state": rsi_state,
            "macd_line": latest["macd_line"],
            "macd_signal": latest["macd_signal"],
            "macd_hist": latest["macd_hist"],
            "macd_state": macd_state,
            "bb_mid": latest["bb_mid"],
            "bb_upper": latest["bb_upper"],
            "bb_lower": latest["bb_lower"],
            "bb_pct_b": latest["bb_pct_b"],
            "bollinger_state": bollinger_state,
            "ema_20": latest["ema_20"],
            "ema_50": latest["ema_50"],
            "exchange": latest["exchange"],
            "currency": latest["currency"],
        }
    )


def build_summary_table(summary: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("Ticker", summary["ticker"]),
            ("Name", summary["name"]),
            ("Date", summary["date"]),
            ("Price", format_number(summary["price"])),
            ("Daily return", format_percent(summary["daily_return_pct"])),
            ("Distance to pivot", format_percent(summary["distance_to_pivot_pct"])),
            ("Pivot bias", summary["pivot_bias"]),
            ("Pivot zone", summary["pivot_zone"]),
            ("RSI 14", "-" if pd.isna(summary["rsi_14"]) else f"{summary['rsi_14']:.1f}"),
            ("RSI state", summary["rsi_state"]),
            ("MACD hist", "-" if pd.isna(summary["macd_hist"]) else f"{summary['macd_hist']:.3f}"),
            ("MACD state", summary["macd_state"]),
            ("Bollinger state", summary["bollinger_state"]),
            ("Percent B", "-" if pd.isna(summary["bb_pct_b"]) else f"{summary['bb_pct_b']:.2f}"),
            ("EMA 20", format_number(summary["ema_20"])),
            ("EMA 50", format_number(summary["ema_50"])),
            ("Exchange", summary["exchange"] or "-"),
            ("Currency", summary["currency"] or "-"),
        ],
        columns=["metric", "value"],
    )


def build_levels_table(summary: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("S3", summary["pivot_s3"]),
            ("S2", summary["pivot_s2"]),
            ("S1", summary["pivot_s1"]),
            ("Pivot", summary["pivot"]),
            ("R1", summary["pivot_r1"]),
            ("R2", summary["pivot_r2"]),
            ("R3", summary["pivot_r3"]),
            ("BB Lower", summary["bb_lower"]),
            ("BB Mid", summary["bb_mid"]),
            ("BB Upper", summary["bb_upper"]),
        ],
        columns=["level", "value"],
    ).assign(value=lambda frame: frame["value"].map(format_number))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pivot, Bollinger, RSI, and MACD helper for a single ticker.")
    parser.add_argument("ticker", help="Ticker symbol, with or without .CL suffix.")
    parser.add_argument("--db", default=None, help="Path to SQLite database. Defaults to stock_data.db.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    ticker = normalize_ticker(args.ticker)

    prices = load_price_history(db_path)
    stock = prices.loc[prices["ticker"] == ticker]
    if stock.empty:
        raise SystemExit(f"Ticker not found in database: {ticker}")

    summary = compute_ticker_signal(stock)
    print(f"Database: {db_path}")
    print()
    print_table(build_summary_table(summary))
    print()
    print_table(build_levels_table(summary))


if __name__ == "__main__":
    main()
