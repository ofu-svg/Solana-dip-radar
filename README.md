# Solana Dip Radar 🚨

A personal Solana DEX scanner that looks for tokens trading **80%, 95%, or 99% below their observed historical high** and sends Telegram alerts.

## What this first version does

Every scan it:
1. Pulls Solana **new pools**, **top pools by 24h volume**, and **trending pools** from GeckoTerminal.
2. Filters out pools with very low liquidity/volume/transactions.
3. Pulls daily OHLCV for each candidate.
4. Finds the highest daily high available in that history.
5. Calculates:

`drawdown = (1 - current_price / observed_high) × 100`

6. Sends a Telegram message when a candidate crosses 80%, 95%, or 99% drawdown.
7. Saves observations in SQLite so the scanner remembers what it has already seen.

## Important limitation

The scanner says **"observed high"**, not guaranteed true ATH.

GeckoTerminal's public API provides OHLCV and pool data, but historical coverage and public rate limits are finite. A token can therefore have a true ATH that is older/higher than the history returned to the scanner.

For a stronger version, connect a CoinGecko API plan and use its onchain/historical coverage. CoinGecko currently documents broad onchain coverage and historical DEX data. See the official docs:
- https://api.geckoterminal.com/docs/
- https://docs.coingecko.com/

## Install on a computer/VPS

```bash
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows:
# .venv\Scripts\activate

pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` and add your Telegram bot token and chat ID.

Run one scan:

```bash
python solana_dip_radar.py
```

If Telegram is not configured, alerts are printed in the terminal instead.

## Create the Telegram bot

1. Open Telegram.
2. Search for **@BotFather**.
3. Create a new bot with `/newbot`.
4. Copy the bot token into `.env`.
5. Send any message to your new bot.
6. Get your chat ID using:

```text
https://api.telegram.org/botYOUR_TOKEN/getUpdates
```

Find `"chat":{"id": ...}` and put that number into `TELEGRAM_CHAT_ID`.

Telegram's official Bot API uses HTTPS requests to `https://api.telegram.org/bot<TOKEN>/METHOD_NAME` and `sendMessage` accepts a chat ID and message text.

## Run automatically

### Linux/VPS cron — every 15 minutes

Open cron:

```bash
crontab -e
```

Add:

```cron
*/15 * * * * cd /path/to/solana_dip_radar && /path/to/solana_dip_radar/.venv/bin/python solana_dip_radar.py >> radar.log 2>&1
```

### Android

The easiest reliable setup is **Termux** plus a device/server that stays online. Install Python in Termux, copy the folder, configure `.env`, and schedule the script with Termux:Boot/Termux:API or run it from a small always-on VPS.

For an always-on alert system, a cheap VPS is usually more reliable than leaving an Android phone awake.

## Recommended starting filters for your scout strategy

These are intentionally conservative starting values, not trading recommendations:

- Liquidity: **≥ $5,000**
- 24h volume: **≥ $500**
- 24h transactions: **≥ 10**
- Pool age: **≥ 24 hours**
- Alerts: **80%, 95%, 99% below observed high**

The liquidity filter matters because your FRAG experience showed that a displayed price/market cap can look attractive while a small $0.50 sale is difficult to execute.

## Next upgrades

The next version can add:
- true historical ATH using deeper historical data;
- holder count/top-holder concentration;
- mint/freeze authority checks;
- buy/sell counts;
- distance from ATL;
- a **"$0.20 scout execution"** liquidity test;
- Telegram buttons linking directly to the token;
- a daily shortlist instead of every alert;
- scoring based on liquidity, volume, age, concentration, and drawdown;
- separate 80/95/99 alert channels.
