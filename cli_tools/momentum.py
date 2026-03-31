from __future__ import annotations

import argparse

import pandas as pd

from cli_tools.common import (
    format_number,
    normalize_ticker,
    print_table,
    resolve_db_path,
    load_price_history,
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


def compute_indicators(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    latest_rows = []
    history_map: dict[str, pd.DataFrame] = {}

    for ticker, ticker_frame in frame.groupby("ticker", sort=True):
        stock = ticker_frame.sort_values("date").copy()
        price = stock["price"]

        stock["ema_12"] = price.ewm(span=12, adjust=False).mean()
        stock["ema_20"] = price.ewm(span=20, adjust=False).mean()
        stock["ema_26"] = price.ewm(span=26, adjust=False).mean()
        stock["ema_50"] = price.ewm(span=50, adjust=False).mean()
        stock["ema_100"] = price.ewm(span=100, adjust=False).mean()

        stock["macd_line"] = stock["ema_12"] - stock["ema_26"]
        stock["macd_signal"] = stock["macd_line"].ewm(span=9, adjust=False).mean()
        stock["macd_hist"] = stock["macd_line"] - stock["macd_signal"]

        stock["rsi_14"] = compute_rsi(price, 14)
        stock["rsi_7"] = compute_rsi(price, 7)
        stock["rsi_21"] = compute_rsi(price, 21)

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

        pivot_bias = "above pivot" if latest["price"] >= pivot else "below pivot"
        if pd.notna(s1) and latest["price"] <= s1:
            pivot_zone = "at support"
        elif pd.notna(r1) and latest["price"] >= r1:
            pivot_zone = "at resistance"
        else:
            pivot_zone = "inside range"

        macd_state = "bullish" if latest["macd_line"] > latest["macd_signal"] else "bearish"
        if latest["macd_hist"] > previous["macd_hist"] and macd_state == "bullish":
            macd_state = "improving bullish"
        elif latest["macd_hist"] < previous["macd_hist"] and macd_state == "bearish":
            macd_state = "weakening bearish"

        rsi_state = "oversold" if latest["rsi_14"] < 30 else "overbought" if latest["rsi_14"] > 70 else "neutral"
        bollinger_state = (
            "below lower band"
            if latest["price"] <= latest["bb_lower"]
            else "above upper band"
            if latest["price"] >= latest["bb_upper"]
            else "inside bands"
        )

        momentum_score = (
            int(latest["macd_line"] > latest["macd_signal"])
            + int(latest["macd_hist"] > 0)
            + int(latest["macd_hist"] > previous["macd_hist"])
            + int(latest["price"] > latest["ema_20"])
            + int(latest["price"] > latest["ema_50"])
            + int(50 <= latest["rsi_14"] <= 70)
            + int(latest["price"] > pivot)
        )

        latest_rows.append(
            {
                "ticker": ticker,
                "name": latest["long_name"] or latest["short_name"] or ticker,
                "date": latest["date"].date().isoformat(),
                "price": latest["price"],
                "daily_return_pct": latest["daily_return"],
                "rsi_14": latest["rsi_14"],
                "rsi_7": latest["rsi_7"],
                "rsi_21": latest["rsi_21"],
                "rsi_state": rsi_state,
                "macd_line": latest["macd_line"],
                "macd_signal": latest["macd_signal"],
                "macd_hist": latest["macd_hist"],
                "macd_state": macd_state,
                "momentum_score": momentum_score,
                "bb_pct_b": latest["bb_pct_b"],
                "bollinger_state": bollinger_state,
                "pivot": pivot,
                "pivot_bias": pivot_bias,
                "pivot_zone": pivot_zone,
                "pivot_s1": s1,
                "pivot_s2": s2,
                "pivot_r1": r1,
                "pivot_r2": r2,
                "distance_to_pivot_pct": (latest["price"] / pivot) - 1 if pivot else pd.NA,
                "exchange": latest["exchange"],
                "currency": latest["currency"],
            }
        )
        history_map[ticker] = stock

    latest_df = pd.DataFrame(latest_rows).sort_values(
        ["momentum_score", "macd_hist", "daily_return_pct"],
        ascending=[False, False, False],
    )
    return latest_df, history_map


def build_view(frame: pd.DataFrame) -> pd.DataFrame:
    view = frame.loc[
        :,
        [
            "ticker",
            "date",
            "price",
            "daily_return_pct",
            "rsi_14",
            "rsi_state",
            "macd_state",
            "macd_hist",
            "momentum_score",
            "bb_pct_b",
            "pivot_bias",
            "pivot_zone",
            "distance_to_pivot_pct",
        ],
    ].copy()
    view["price"] = view["price"].map(format_number)
    view["daily_return_pct"] = view["daily_return_pct"].map(lambda value: "-" if pd.isna(value) else f"{value:+.2%}")
    view["rsi_14"] = view["rsi_14"].map(lambda value: "-" if pd.isna(value) else f"{value:.1f}")
    view["macd_hist"] = view["macd_hist"].map(lambda value: "-" if pd.isna(value) else f"{value:.3f}")
    view["bb_pct_b"] = view["bb_pct_b"].map(lambda value: "-" if pd.isna(value) else f"{value:.2f}")
    view["distance_to_pivot_pct"] = view["distance_to_pivot_pct"].map(
        lambda value: "-" if pd.isna(value) else f"{value:+.2%}"
    )
    return view


def select_top_bottom(
    frame: pd.DataFrame,
    *,
    metric: str,
    limit: int,
    ascending: bool,
    tie_breakers: list[str] | None = None,
) -> pd.DataFrame:
    sort_columns = [metric, *(tie_breakers or [])]
    sort_directions = [ascending, *([ascending] * len(tie_breakers or []))]
    return frame.sort_values(sort_columns, ascending=sort_directions).head(limit)


def print_ticker_detail(summary: pd.Series) -> None:
    detail = pd.DataFrame(
        [
            ("Ticker", summary["ticker"]),
            ("Date", summary["date"]),
            ("Price", format_number(summary["price"])),
            ("RSI 14", "-" if pd.isna(summary["rsi_14"]) else f"{summary['rsi_14']:.1f}"),
            ("RSI state", summary["rsi_state"]),
            ("MACD hist", "-" if pd.isna(summary["macd_hist"]) else f"{summary['macd_hist']:.3f}"),
            ("MACD state", summary["macd_state"]),
            ("Momentum score", str(int(summary["momentum_score"]))),
            ("Bollinger", summary["bollinger_state"]),
            ("Pivot bias", summary["pivot_bias"]),
            ("Pivot zone", summary["pivot_zone"]),
            ("Distance to pivot", "-" if pd.isna(summary["distance_to_pivot_pct"]) else f"{summary['distance_to_pivot_pct']:+.2%}"),
            ("Exchange", summary["exchange"] or "-"),
            ("Currency", summary["currency"] or "-"),
        ],
        columns=["metric", "value"],
    )
    pivots = pd.DataFrame(
        [
            ("S2", summary["pivot_s2"]),
            ("S1", summary["pivot_s1"]),
            ("Pivot", summary["pivot"]),
            ("R1", summary["pivot_r1"]),
            ("R2", summary["pivot_r2"]),
        ],
        columns=["level", "value"],
    )
    pivots["value"] = pivots["value"].map(format_number)
    print_table(detail)
    print()
    print_table(pivots)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Momentum and oversold CLI based on the stock momentum notebook.")
    parser.add_argument("--db", default=None, help="Path to SQLite database. Defaults to stock_data.db.")
    parser.add_argument(
        "--view",
        choices=[
            "all",
            "leaders",
            "oversold",
            "overbought",
            "momentum-top",
            "momentum-bottom",
            "rsi-top",
            "rsi-bottom",
        ],
        default="leaders",
        help="Which momentum slice to print.",
    )
    parser.add_argument("--limit", type=int, default=10, help="Number of rows to print for list views.")
    parser.add_argument("--ticker", help="Print a single ticker detail view instead of a list.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    prices = load_price_history(db_path)
    analysis, _ = compute_indicators(prices)

    if args.ticker:
        ticker = normalize_ticker(args.ticker)
        match = analysis.loc[analysis["ticker"] == ticker]
        if match.empty:
            raise SystemExit(f"Ticker not found in database: {ticker}")
        print_ticker_detail(match.iloc[0])
        return

    selected = analysis
    if args.view == "leaders":
        selected = analysis.head(args.limit)
    elif args.view == "oversold":
        selected = analysis.loc[analysis["rsi_state"] == "oversold"].sort_values("rsi_14").head(args.limit)
    elif args.view == "overbought":
        selected = analysis.loc[analysis["rsi_state"] == "overbought"].sort_values("rsi_14", ascending=False).head(args.limit)
    elif args.view == "momentum-top":
        selected = select_top_bottom(
            analysis,
            metric="momentum_score",
            limit=args.limit,
            ascending=False,
            tie_breakers=["macd_hist", "daily_return_pct"],
        )
    elif args.view == "momentum-bottom":
        selected = select_top_bottom(
            analysis,
            metric="momentum_score",
            limit=args.limit,
            ascending=True,
            tie_breakers=["macd_hist", "daily_return_pct"],
        )
    elif args.view == "rsi-top":
        selected = select_top_bottom(
            analysis,
            metric="rsi_14",
            limit=args.limit,
            ascending=False,
            tie_breakers=["daily_return_pct"],
        )
    elif args.view == "rsi-bottom":
        selected = select_top_bottom(
            analysis,
            metric="rsi_14",
            limit=args.limit,
            ascending=True,
            tie_breakers=["daily_return_pct"],
        )
    elif args.view == "all":
        selected = analysis.head(args.limit)

    print(f"Database: {db_path}")
    print(f"Rows analyzed: {len(analysis)}")
    print()
    print_table(build_view(selected))


if __name__ == "__main__":
    main()
