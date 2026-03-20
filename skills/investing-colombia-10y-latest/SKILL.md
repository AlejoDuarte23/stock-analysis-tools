---
name: investing-colombia-10y-latest
description: Fetch the latest historical row from Investing.com for Colombia 10-year bond yield using a Playwright script. Use when the user asks for the newest value/date from the historical table on the Colombia 10Y page.
---

# Investing Colombia 10Y Latest

## Use This Skill For

- Latest historical row from Colombia 10-year bond page.
- Latest `Fecha`, `Último`, `Apertura`, `Máximo`, `Mínimo`, `% var.` values.

Target page:
`https://es.investing.com/rates-bonds/colombia-10-year-bond-yield-historical-data`

## Workflow

1. Install dependencies in this skill folder:

```bash
npm install
```

2. Fetch latest row:

```bash
npm run fetch
```

3. Return JSON output from the script:

```json
{
  "fecha": "06.03.2026",
  "ultimo": "13,425",
  "apertura": "13,400",
  "maximo": "13,469",
  "minimo": "13,400",
  "variacion": "+0,94%"
}
```

## Selector Rules

- Do not rely on hashed CSS classes.
- Find the table by header text (`Fecha`, `Último`).
- Use first row in `tbody` as latest historical row.
- Accept cookie banner if visible.

## Script

- `scripts/fetch_latest_colombia_10y.ts`
Purpose: open page, accept cookies, find historical table, parse first row, print JSON.
