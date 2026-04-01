# AutoTrader Bot

Autonomous trading bot — Twelve Data + Claude AI + E*Trade execution.

## Deploy to Vercel

1. Push this folder to a GitHub repo
2. Import the repo at vercel.com/new
3. Deploy — no build step needed (static HTML)
4. Open the deployed URL on your phone
5. Safari: tap Share → Add to Home Screen
6. Chrome: tap menu → Add to Home Screen

## Files

- `index.html` — full bot app (PWA)
- `manifest.json` — enables Add to Home Screen
- `sw.js` — service worker (offline shell)
- `vercel.json` — headers config
- `icon-192.png` / `icon-512.png` — app icons (replace with real PNGs)

## First Run

1. Open the app → tap CONFIG
2. Add your Twelve Data API key (twelvedata.com)
3. Add your Anthropic key for Claude grading (optional)
4. Add E*Trade OAuth tokens when ready for live trading
5. Keep Sandbox mode ON until you've verified behavior
6. Tap START BOT

## Live Orders

E*Trade requires OAuth 1.0a HMAC-SHA1 signing.
Deploy a small backend proxy (Node/Python) alongside this app
and uncomment the placeOrder() fetch call in index.html.

Recommended: Vercel Serverless Function at /api/etrade/order
that handles signing server-side so your secrets stay safe.
