#!/usr/bin/env python3
"""
Solana Dip Radar
Scans Solana DEX pools via GeckoTerminal's public API, estimates drawdown
from the observed historical high, applies liquidity/volume/security filters,
and sends qualifying alerts to Telegram.

IMPORTANT:
- This is a research/alert tool, not a trading bot.
- "ATH" is the highest high found in the OHLCV history returned for the pool.
  The public GeckoTerminal API has finite historical depth, so an observed ATH
  can be lower than the token's true all-time high.
- Run this repeatedly (e.g. every 10-15 minutes on a VPS/PC/Android Termux).
"""

from __future__ import annotations
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("GT_BASE_URL", "https://api.geckoterminal.com/api/v2")
NETWORK = "solana"
DB_PATH = os.getenv("DB_PATH", "dip_radar.sqlite3")

# Scanner controls
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY_USD", "300"))
MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H_USD", "30"))
MIN_TX_24H = int(os.getenv("MIN_TX_24H", "10"))
MIN_POOL_AGE_HOURS = float(os.getenv("MIN_POOL_AGE_HOURS", "24"))
MAX_POOL_AGE_DAYS = float(os.getenv("MAX_POOL_AGE_DAYS", "3650"))
MAX_CANDIDATES_PER_RUN = int(os.getenv("MAX_CANDIDATES_PER_RUN", "30"))
PAGES_TOP = int(os.getenv("PAGES_TOP", "2"))
PAGES_NEW = int(os.getenv("PAGES_NEW", "2"))

# Alert thresholds: percentage below observed high
THRESHOLDS = [80.0, 95.0, 99.0]

# To avoid repeating the same alert every run:
ALERT_COOLDOWN_HOURS = float(os.getenv("ALERT_COOLDOWN_HOURS", "24"))

# Optional stricter filters
REQUIRE_SOL_QUOTE = os.getenv("REQUIRE_SOL_QUOTE", "false").lower() == "true"
MIN_HOLDERS = int(os.getenv("MIN_HOLDERS", "0"))  # public endpoint may not expose it
MAX_FDV = float(os.getenv("MAX_FDV_USD", "0"))     # 0 = disabled

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

HEADERS = {
    "accept": "application/json;version=20230203",
    "user-agent": "SolanaDipRadar/1.0 (personal research tool)",
}

session = requests.Session()
session.headers.update(HEADERS)


def now_ts() -> int:
    return int(time.time())


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pools (
            pool_address TEXT PRIMARY KEY,
            token_address TEXT,
            symbol TEXT,
            name TEXT,
            dex TEXT,
            first_seen INTEGER,
            last_seen INTEGER,
            observed_ath REAL,
            observed_ath_ts INTEGER,
            last_price REAL,
            last_drawdown REAL,
            last_liquidity REAL,
            last_volume_24h REAL,
            last_alert_threshold REAL,
            last_alert_ts INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER,
            pool_address TEXT,
            token_address TEXT,
            symbol TEXT,
            price REAL,
            observed_ath REAL,
            drawdown REAL,
            liquidity REAL,
            volume_24h REAL,
            result TEXT
        )
    """)
    conn.commit()
    return conn


def get_json(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"

    # Keep requests spaced out so we don't hammer the public API.
    time.sleep(1.5)

    for attempt in range(4):
        try:
            r = session.get(
                url,
                params=params or {},
                timeout=30
            )

            if r.status_code == 429:
                # Never trust a zero Retry-After value.
                wait = min(5.0 * (attempt + 1), 20.0)

                print(
                    f"[WARN] GeckoTerminal rate limit (429). "
                    f"Waiting {wait:.1f}s before retry {attempt + 1}/4..."
                )

                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.RequestException as e:
            if attempt >= 3:
                raise

            wait = min(5.0 * (attempt + 1), 20.0)

            print(
                f"[WARN] API request failed: {e}. "
                f"Retrying in {wait:.1f}s..."
            )

            time.sleep(wait)

    raise RuntimeError("GeckoTerminal API request failed after retries.")

    for attempt in range(4):
        try:
            r = session.get(url, params=params, timeout=30)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")

                if retry_after:
                    try:
                        wait = min(float(retry_after), 30)
                    except ValueError:
                        wait = 5.0
                else:
                    wait = min(5.0 * (attempt + 1), 20.0)

                print(
                    f"[WARN] GeckoTerminal rate limit (429). "
                    f"Waiting {wait:.1f}s before retry {attempt + 1}/4..."
                )
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.RequestException as e:
            if attempt >= 3:
                raise

            wait = min(3.0 * (attempt + 1), 12.0)
            print(
                f"[WARN] API request failed: {e}. "
                f"Retrying in {wait:.1f}s..."
            )
            time.sleep(wait)

    raise RuntimeError("GeckoTerminal API request failed after retries.")


def get_pool_list(path: str, pages: int) -> list[dict[str, Any]]:
    out = []
    for page in range(1, pages + 1):
        try:
            payload = get_json(path, {"page": page, "include": "base_token,quote_token,dex"})
            out.extend(payload.get("data", []))
        except Exception as e:
            print(f"[WARN] pool list page {page}: {e}")
            break
    return out


def attrs(item: dict[str, Any]) -> dict[str, Any]:
    return item.get("attributes", {}) or {}


def relationships(item: dict[str, Any]) -> dict[str, Any]:
    return item.get("relationships", {}) or {}


def included_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        x.get("id"): x
        for x in payload.get("included", [])
        if x.get("id")
    }


def parse_pool(item: dict[str, Any]) -> dict[str, Any]:
    a = attrs(item)
    rel = relationships(item)
    pool_id = item.get("id", "")
    address = a.get("address") or pool_id.split("_", 1)[-1]

    # The API's pool response may include base/quote relationships, while
    # metadata is sometimes in "included". We keep the IDs for a later lookup.
    base_id = ((rel.get("base_token") or {}).get("data") or {}).get("id")
    quote_id = ((rel.get("quote_token") or {}).get("data") or {}).get("id")

    tx = a.get("transactions") or {}
    vol = a.get("volume_usd") or {}

    return {
        "pool_address": address,
        "pool_id": pool_id,
        "name": a.get("name") or "",
        "symbol": "",
        "token_address": base_id.split("_", 1)[1] if base_id and "_" in base_id else None,
        "base_id": base_id,
        "quote_id": quote_id,
        "dex_id": ((rel.get("dex") or {}).get("data") or {}).get("id"),
        "price": safe_float(a.get("base_token_price_usd")),
        "liquidity": safe_float(a.get("reserve_in_usd")),
        "fdv": safe_float(a.get("fdv_usd")),
        "volume_24h": safe_float(vol.get("h24")),
        "tx_24h": tx_count(tx, "h24"),
        "pool_created_at": parse_time(a.get("pool_created_at")),
        "price_change_h24": safe_float((a.get("price_change_percentage") or {}).get("h24")),
    }


def tx_count(tx: dict[str, Any], key: str) -> int:
    obj = tx.get(key) or {}
    if isinstance(obj, dict):
        return int(obj.get("buys", 0) or 0) + int(obj.get("sells", 0) or 0)
    return int(obj or 0)


def safe_float(x: Any) -> float | None:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def parse_time(x: Any) -> int | None:
    if not x:
        return None
    try:
        return int(datetime.fromisoformat(str(x).replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def get_token_data(token_address: str) -> dict[str, Any]:
    return get_json(f"/networks/{NETWORK}/tokens/{token_address}", {})

def get_token_info(token_address: str) -> dict[str, Any]:
    return get_json(f"/networks/{NETWORK}/tokens/{token_address}", {})


def get_ohlcv(pool_address: str) -> list[list[Any]]:
    # Daily candles are enough for a dip scanner and use far fewer rows.
    payload = get_json(
        f"/networks/{NETWORK}/pools/{quote(pool_address, safe='')}/ohlcv/day",
        {"aggregate": 1, "limit": 1000, "currency": "usd", "token": "base"},
    )
    return ((payload.get("data") or {}).get("attributes") or {}).get("ohlcv_list", [])

def get_recent_ohlcv(pool_address: str) -> list[list[Any]]:
    payload = get_json(
        f"/networks/{NETWORK}/pools/{quote(pool_address, safe='')}/ohlcv/hour",
        {"aggregate": 1, "limit": 12, "currency": "usd", "token": "base"},
    )
    return ((payload.get("data") or {}).get("attributes") or {}).get("ohlcv_list", [])


def get_token_symbol_and_security(token_address: str) -> tuple[str, dict[str, Any]]:
    try:
        payload = get_token_info(token_address)
        a = ((payload.get("data") or {}).get("attributes") or {})
        symbol = a.get("symbol") or ""
        return symbol, a
    except Exception as e:
        print(f"[WARN] token info {token_address[:8]}...: {e}")
        return "", {}


def calculate_ath(candles: list[list[Any]]) -> tuple[float | None, int | None]:
    high = None
    high_ts = None
    for row in candles:
        # GeckoTerminal OHLCV rows are [timestamp, open, high, low, close, volume]
        if len(row) < 3:
            continue
        ts = int(row[0])
        h = safe_float(row[2])
        if h is not None and (high is None or h > high):
            high, high_ts = h, ts
    return high, high_ts


def drawdown_percent(price: float, ath: float) -> float:
    if ath <= 0:
        return 0.0
    return max(0.0, (1.0 - price / ath) * 100.0)


def threshold_for(drawdown: float) -> float | None:
    hits = [x for x in THRESHOLDS if drawdown >= x]
    return max(hits) if hits else None


def should_alert(conn: sqlite3.Connection, pool_address: str, threshold: float) -> bool:
    row = conn.execute(
        "SELECT last_alert_threshold,last_alert_ts FROM pools WHERE pool_address=?",
        (pool_address,),
    ).fetchone()
    if not row:
        return True
    last_thr, last_ts = row
    if last_thr is None or last_ts is None:
        return True
    # Alert again only if crossing a higher threshold, or after cooldown.
    if threshold > float(last_thr):
        return True
    return (now_ts() - int(last_ts)) >= ALERT_COOLDOWN_HOURS * 3600


def update_pool(
    conn: sqlite3.Connection,
    p: dict[str, Any],
    symbol: str,
    ath: float,
    ath_ts: int | None,
    dd: float,
    alert_threshold: float | None,
    alert_sent: bool,
):
    ts = now_ts()
    row = conn.execute(
        "SELECT observed_ath,observed_ath_ts FROM pools WHERE pool_address=?",
        (p["pool_address"],),
    ).fetchone()
    old_ath = float(row[0]) if row and row[0] is not None else 0.0
    old_ts = int(row[1]) if row and row[1] is not None else None

    best_ath = max(old_ath, ath)
    best_ts = old_ts if old_ath >= ath else ath_ts

    last_alert_threshold = alert_threshold if alert_sent else (
        conn.execute(
            "SELECT last_alert_threshold FROM pools WHERE pool_address=?",
            (p["pool_address"],),
        ).fetchone() or [None]
    )[0]
    last_alert_ts = ts if alert_sent else (
        conn.execute(
            "SELECT last_alert_ts FROM pools WHERE pool_address=?",
            (p["pool_address"],),
        ).fetchone() or [None]
    )[0]

    conn.execute("""
        INSERT INTO pools(
            pool_address,token_address,symbol,name,dex,first_seen,last_seen,
            observed_ath,observed_ath_ts,last_price,last_drawdown,
            last_liquidity,last_volume_24h,last_alert_threshold,last_alert_ts
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(pool_address) DO UPDATE SET
            token_address=excluded.token_address,
            symbol=excluded.symbol,
            name=excluded.name,
            dex=excluded.dex,
            last_seen=excluded.last_seen,
            observed_ath=excluded.observed_ath,
            observed_ath_ts=excluded.observed_ath_ts,
            last_price=excluded.last_price,
            last_drawdown=excluded.last_drawdown,
            last_liquidity=excluded.last_liquidity,
            last_volume_24h=excluded.last_volume_24h,
            last_alert_threshold=excluded.last_alert_threshold,
            last_alert_ts=excluded.last_alert_ts
    """, (
        p["pool_address"], p["token_address"], symbol, p["name"], p["dex_id"],
        ts, ts, best_ath, best_ts, p["price"], dd, p["liquidity"],
        p["volume_24h"], last_alert_threshold, last_alert_ts
    ))
    conn.execute("""
        INSERT INTO scans(
            ts,pool_address,token_address,symbol,price,observed_ath,drawdown,
            liquidity,volume_24h,result
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
    """, (
        ts, p["pool_address"], p["token_address"], symbol, p["price"], best_ath,
        dd, p["liquidity"], p["volume_24h"], "ALERT" if alert_sent else "WATCH"
    ))
    conn.commit()


def fmt_usd(x: float | None) -> str:
    if x is None:
        return "n/a"
    if x >= 1000:
        return f"${x:,.0f}"
    if x >= 1:
        return f"${x:,.2f}"
    return f"${x:.8f}".rstrip("0").rstrip(".")


def build_message(p: dict[str, Any], symbol: str, ath: float, dd: float,
                  threshold: float, security: dict[str, Any]) -> str:
    name = symbol or p["name"].split(" / ")[0] or "Unknown"
    age = "n/a"
    if p.get("pool_created_at"):
        days = max(0, (now_ts() - p["pool_created_at"]) / 86400)
        age = f"{days:.1f}d"

    # Security metadata fields differ by API version; show only if present.
    mint = security.get("mint_authority")
    freeze = security.get("freeze_authority")
    mint_s = "unknown" if mint is None else ("YES" if mint else "NO")
    freeze_s = "unknown" if freeze is None else ("YES" if freeze else "NO")

    return (
        "🚨 SOLANA DIP RADAR\n\n"
        f"Token: {name}\n"
        f"Mint: `{p['token_address']}`\n"
        f"Pool: `{p['pool_address']}`\n"
        f"DEX: {p.get('dex_id') or 'unknown'}\n\n"
        f"Price: {fmt_usd(p['price'])}\n"
        f"Observed high: {fmt_usd(ath)}\n"
        f"Drawdown: ↓ {dd:.2f}%\n"
        f"Threshold: {threshold:.0f}%+\n\n"
        f"Liquidity: {fmt_usd(p['liquidity'])}\n"
        f"24h volume: {fmt_usd(p['volume_24h'])}\n"
        f"24h tx: {p['tx_24h']}\n"
        f"Pool age: {age}\n"
        f"Mint authority: {mint_s}\n"
        f"Freeze authority: {freeze_s}\n\n"
        "⚠️ Research alert only. A deep drawdown does not imply recovery, "
        "and displayed liquidity may not equal the amount you can sell without slippage."
    )


def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("\n[ALERT — Telegram not configured]\n" + message)
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        },
        timeout=20,
    )
    if not r.ok:
        print(f"[WARN] Telegram error: {r.status_code} {r.text[:300]}")
        return False
    return True


def candidate_pools() -> list[dict[str, Any]]:
    # New pools + high-volume pools + trending pools gives a mix of "random"
    # discovery and established tokens.
    items = []
    items += get_pool_list(f"/networks/{NETWORK}/new_pools", PAGES_NEW)
    items += get_pool_list(f"/networks/{NETWORK}/pools?sort=h24_volume_usd_desc", PAGES_TOP)
    try:
        items += get_pool_list(f"/networks/{NETWORK}/trending_pools", 1)
    except Exception as e:
        print(f"[WARN] trending: {e}")

    seen = set()
    out = []
    for item in items:
        p = parse_pool(item)
        if not p["pool_address"] or p["pool_address"] in seen:
            continue
        seen.add(p["pool_address"])
        out.append(p)

    # Prefer pools that have enough liquidity/volume to make a $0.20-$2 scout
    # more realistically executable, while still allowing a configurable floor.
    out.sort(key=lambda x: (
        x["liquidity"] or 0,
        x["volume_24h"] or 0
    ), reverse=True)
    return out[:MAX_CANDIDATES_PER_RUN]


def passes_filters(p: dict[str, Any]) -> tuple[bool, str]:
    if p["price"] is None or p["price"] <= 0:
        return False, "no price"
    if (p["liquidity"] or 0) < MIN_LIQUIDITY:
        return False, "low liquidity"
    if (p["volume_24h"] or 0) < MIN_VOLUME_24H:
        return False, "low volume"
    if p["tx_24h"] < MIN_TX_24H:
        return False, "low tx"
    created = p.get("pool_created_at")
    if created:
        age_h = (now_ts() - created) / 3600
        if age_h < MIN_POOL_AGE_HOURS:
            return False, "too new"
        if age_h > MAX_POOL_AGE_DAYS * 24:
            return False, "too old"
    if MAX_FDV and (p["fdv"] or 0) > MAX_FDV:
        return False, "fdv too high"
    if REQUIRE_SOL_QUOTE:
        # Common SOL mint suffix in relationship IDs.
        q = p.get("quote_id") or ""
        if "So11111111111111111111111111111111111111112" not in q:
            return False, "not SOL quote"
    return True, ""


def scan():
    conn = init_db()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] scanning Solana...")
    pools = candidate_pools()
    print(f"Candidates: {len(pools)}")

    for i, p in enumerate(pools, 1):
        ok, why = passes_filters(p)
        if not ok:
            print(f"[{i}] skip {p['pool_address'][:8]}... {why}")
            continue

        try:
            candles = get_ohlcv(p["pool_address"])

        if len(recent_candles) >= 6:
            old_price = safe_float(recent_candles[-1][4])
            recent_price = safe_float(recent_candles[0][4])
            if old_price and recent_price and recent_price > old_price * 1.05:
                continue

        ath, ath_ts = calculate_ath(candles)

            dd = drawdown_percent(p["price"], ath)
            threshold = threshold_for(dd)

            symbol, security = get_token_symbol_and_security(p["token_address"]) if p["token_address"] else ("", {})
            alert = threshold is not None and should_alert(conn, p["pool_address"], threshold)

            update_pool(conn, p, symbol, ath, ath_ts, dd, threshold, alert)

            print(
                f"[{i}] {symbol or p['name'][:20]:20} "
                f"price={fmt_usd(p['price']):>12} "
                f"dd={dd:6.2f}% liq={fmt_usd(p['liquidity']):>10} "
                f"vol={fmt_usd(p['volume_24h']):>10}"
            )

            if alert:
                msg = build_message(p, symbol, ath, dd, threshold, security)
                sent = send_telegram(msg)
                # Even if Telegram isn't configured, don't repeatedly print the
                # same alert every scan: mark it as alerted after displaying it.
                if not sent and TELEGRAM_BOT_TOKEN:
                    print("[WARN] alert could not be sent.")
        except Exception as e:
            print(f"[WARN] {p['pool_address'][:8]}... {e}")

        # Be gentle with the public API.
        time.sleep(0.8)

    conn.close()


if __name__ == "__main__":
    scan()
