import { chromium } from 'playwright';
import { writeFileSync, mkdirSync } from 'fs';
import path from 'path';

const TZ = 'Asia/Kolkata';
function fmt(d){
  return new Intl.DateTimeFormat('en-GB', { timeZone: TZ, day:'2-digit', month:'2-digit', year:'numeric' })
    .format(d).replace(/\//g,'-'); // dd-mm-yyyy
}

const now  = new Date();
const to   = new Date(now); to.setDate(to.getDate()-1);
const from = new Date(now); from.setDate(from.getDate()-30);

const FROM = fmt(from);
const TO   = fmt(to);

const PAGE_URL = 'https://www.nseindia.com/companies-listing/corporate-filings-insider-trading';
const CSV_URL  = `https://www.nseindia.com/api/corporates-pit?index=equities&from_date=${FROM}&to_date=${TO}&csv=true`;

const OUT_DIR = 'data';
const LATEST  = path.join(OUT_DIR, 'CF-Insider-Trading-equities-latest.csv');
const DATED   = path.join(OUT_DIR, `CF-Insider-Trading-equities-${FROM}-to-${TO}.csv`);

(async () => {
  // --- hardened launch to avoid HTTP/2 protocol errors on CI ---
  const browser = await chromium.launch({
    headless: true,
    args: [
      '--disable-http2',                          // <— key fix
      '--no-sandbox',
      '--disable-gpu',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--disable-features=IsolateOrigins,site-per-process'
    ]
  });

  const ctx = await browser.newContext({
    ignoreHTTPSErrors: true,
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    viewport: { width: 1366, height: 768 },
    locale: 'en-US',
    extraHTTPHeaders: {
      'Accept-Language': 'en-US,en;q=0.9',
      'sec-ch-ua': '"Not/A)Brand";v="99", "Chromium";v="126", "Google Chrome";v="126"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"'
    }
  });

  const page = await ctx.newPage();

  // mask automation
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });

  // --- warm-up & navigation with fallbacks ---
  try {
    // try insider page directly (skip homepage that errored)
    await page.goto(PAGE_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
  } catch (e) {
    console.log('Direct goto insider failed, trying homepage then insider…', String(e));
    try {
      await page.goto('https://www.nseindia.com/', { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(1200);
      await page.goto(PAGE_URL, { waitUntil: 'domcontentloaded', timeout: 60000 });
    } catch (e2) {
      console.log('Both gotos failed, bailing early with clear error.');
      throw e2;
    }
  }

  await page.waitForTimeout(1500);

  // --- API fetch in page context (uses real cookies). If blocked, we click the button. ---
  try {
    const csvText = await page.evaluate(async (url) => {
      const resp = await fetch(url, {
        headers: {
          'Accept': 'text/csv,*/*;q=0.1',
          'x-requested-with': 'XMLHttpRequest'
        },
        credentials: 'include'
      });
      if (!resp.ok) throw new Error('HTTP '+resp.status);
      return await resp.text();
    }, CSV_URL);

    if (!/^<!doctype|<html/i.test(csvText)) {
      mkdirSync(OUT_DIR, { recursive: true });
      writeFileSync(LATEST, csvText);
      writeFileSync(DATED,  csvText);
      console.log('Saved via API:', DATED);
      await browser.close();
      process.exit(0);
    }
  } catch (e) {
    console.log('API fetch blocked, falling back to button click…', String(e));
  }

  // Fallback: click "Download (.csv)"
  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: 30000 }),
    page.getByText('Download (.csv)').click()
  ]);
  const stream = await download.createReadStream();
  const chunks = [];
  for await (const ch of stream) chunks.push(ch);
  const buf = Buffer.concat(chunks);

  mkdirSync(OUT_DIR, { recursive: true });
  writeFileSync(LATEST, buf);
  writeFileSync(DATED,  buf);
  console.log('Saved via button:', DATED);

  await browser.close();
})();
