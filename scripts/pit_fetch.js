// scripts/pit_fetch.js
// Uses real Chromium via Playwright in GitHub Actions.
// Plan: warm up site → try API fetch with in-page cookies → if blocked, click "Download (.csv)" and save.

import { chromium } from 'playwright';
import { writeFileSync, mkdirSync } from 'fs';
import path from 'path';

const TZ = 'Asia/Kolkata';
function fmt(d){
  return new Intl.DateTimeFormat('en-GB', { timeZone: TZ, day:'2-digit', month:'2-digit', year:'numeric' })
    .format(d).replace(/\//g,'-'); // dd-mm-yyyy
}

// Rolling window: FROM = today-30, TO = yesterday (IST)
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
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    viewport: { width: 1366, height: 768 },
    locale: 'en-US'
  });
  const page = await ctx.newPage();

  // Warm-up: homepage then insider page (sets cookies/anti-bot tokens)
  await page.goto('https://www.nseindia.com/', { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(1000);
  await page.goto(PAGE_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(1500);

  // Try API fetch inside the page context (sends cookies)
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
    console.log('API fetch blocked, falling back to button click...', String(e));
  }

  // Fallback: click "Download (.csv)" and capture the download
  // The button has visible text "Download (.csv)"
  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: 30000 }),
    page.getByText('Download (.csv)').click()
  ]);
  const csvPath = await download.path(); // temp path in runner
  const csvBuf  = await download.createReadStream();

  // Read the stream fully to a buffer
  let chunks = [];
  for await (const chunk of csvBuf) chunks.push(chunk);
  const fileData = Buffer.concat(chunks);

  mkdirSync(OUT_DIR, { recursive: true });
  writeFileSync(LATEST, fileData);
  writeFileSync(DATED,  fileData);
  console.log('Saved via button:', DATED);

  await browser.close();
})();
