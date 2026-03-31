# CLI Tools

This repository includes notebook-derived CLI helpers under `cli_tools/`.

Use them with `uv run`:

- `uv run stock-momentum --view leaders --limit 10`
- `uv run stock-momentum --view momentum-top --limit 5`
- `uv run stock-momentum --view momentum-bottom --limit 5`
- `uv run stock-momentum --view rsi-top --limit 5`
- `uv run stock-momentum --view rsi-bottom --limit 5`
- `uv run stock-momentum --view oversold`
- `uv run stock-momentum --ticker ECOPETROL`
- `uv run stock-fundamentals --ranking snapshot --limit 15`
- `uv run stock-fundamentals --ranking dividend`
- `uv run stock-fundamentals --ticker GEB`
- `uv run stock-relative-moves --sort-by week_vs_week --limit 20`
- `uv run stock-relative-moves --sort-by year_vs_year`
- `uv run stock-pivots PROMIGAS`

All commands default to `stock_data.db`. Override with `--db path/to/db.sqlite` when needed.
