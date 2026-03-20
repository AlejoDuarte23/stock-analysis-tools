import { chromium, type Page } from "playwright";

type HistoricalRow = {
  fecha: string;
  ultimo: string;
  apertura: string;
  maximo: string;
  minimo: string;
  variacion: string;
};

async function clickIfVisible(selectors: string[], page: Page): Promise<void> {
  for (const selector of selectors) {
    const locator = page.locator(selector).first();
    try {
      if (await locator.isVisible({ timeout: 2000 })) {
        await locator.click({ timeout: 2000 });
        return;
      }
    } catch {
      // Ignore and try next selector.
    }
  }
}

async function getLatestHistoricalRow(): Promise<HistoricalRow> {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    locale: "es-ES",
    userAgent:
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  });

  try {
    await page.goto(
      "https://es.investing.com/rates-bonds/colombia-10-year-bond-yield-historical-data",
      { waitUntil: "domcontentloaded", timeout: 60000 },
    );

    await clickIfVisible(
      [
        'button:has-text("Aceptar")',
        'button:has-text("Acepto")',
        'button:has-text("Accept")',
        "#onetrust-accept-btn-handler",
        '[data-testid="banner-accept-button"]',
      ],
      page,
    );

    await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => {});

    const candidateTables = page.locator("table");
    const count = await candidateTables.count();

    for (let i = 0; i < count; i++) {
      const table = candidateTables.nth(i);
      const headerText = await table.locator("thead").innerText().catch(() => "");
      if (headerText.includes("Fecha") && headerText.includes("Último")) {
        const firstRow = table.locator("tbody tr").first();
        await firstRow.waitFor({ state: "visible", timeout: 15000 });

        const cells = (await firstRow.locator("td").allInnerTexts()).map((x) => x.trim());
        if (cells.length >= 6) {
          return {
            fecha: cells[0],
            ultimo: cells[1],
            apertura: cells[2],
            maximo: cells[3],
            minimo: cells[4],
            variacion: cells[5],
          };
        }
      }
    }

    throw new Error("Historical data table not found or first row did not contain 6 cells.");
  } finally {
    await browser.close();
  }
}

async function main(): Promise<void> {
  const row = await getLatestHistoricalRow();
  process.stdout.write(`${JSON.stringify(row, null, 2)}\n`);
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`Failed to fetch latest historical row: ${message}\n`);
  process.exit(1);
});
