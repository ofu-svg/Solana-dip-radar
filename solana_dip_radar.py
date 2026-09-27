#!/usr/bin/env python3
"""
SOLANA DIP RADAR v4 — 48H / 20 VERIFIED POOLS MAX

Rules:
- Minimum pool age: 48 hours.
- No maximum age.
- Maximum of 20 fully verified pools are analyzed.
- Paginate through all Solana pools available from GeckoTerminal.
- Dip alerts: -30% or worse only.
- Pump alerts: strictly above +100% only.
- Dev/team holding must be <= 5%.
- Mint authority must be revoked (not mintable).
- Metaplex metadata must be immutable.
- If a safety check cannot be verified, reject the token.
"""

from __future__ import annotations

import base64
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("GT_BASE_URL", "https://api.geckoterminal.com/api/v2")
NETWORK = "solana"

SOLANA_RPC_URL = os.getenv(
    "SOLANA_RPC_URL",
    "https://api.mainnet-beta.solana.com",
).strip()

# =========================
# USER SAFETY FILTERS
# =========================

MAX_DEV_HOLDING_PERCENT = float(
    os.getenv("MAX_DEV_HOLDING_PERCENT", "5")
)

REQUIRE_MINT_AUTHORITY_REVOKED = (
    os.getenv("REQUIRE_MINT_AUTHORITY_REVOKED", "true").lower() == "true"
)

REQUIRE_METADATA_IMMUTABLE = (
    os.getenv("REQUIRE_METADATA_IMMUTABLE", "true").lower() == "true"
)

# Pool age: minimum 48 hours. NO maximum age.
MIN_POOL_AGE_DAYS = float(os.getenv("MIN_POOL_AGE_DAYS", "2"))

# Only selected real Solana DEX pool venues.
# Phantom is a wallet/trading interface and DEXTools is an analytics platform,
# so neither is treated as a pool venue here.
SOLANA_DEX_IDS = [
    "raydium",
    "raydium-clmm",
    "raydium-launchlab",
    "meteora",
    "meteora-dbc",
    "orca",
    "pumpswap",
]

# Discovery is deliberately bounded to reduce GeckoTerminal 429s.
DEX_PAGES_PER_SOURCE = int(os.getenv("DEX_PAGES_PER_SOURCE", "3"))
MAX_VERIFIED_POOLS = 20  # HARD CAP: never fully analyze more than 20 verified pools

# Keep these modest so obvious dust pools are ignored.
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY_USD", "300"))
MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H_USD", "30"))
MIN_TX_24H = int(os.getenv("MIN_TX_24H", "3"))  # At least 3 transactions in 24h

# Alert thresholds
MIN_DIP_ALERT = 30.0
PUMP_1H = 100.0001

DEEP_DIP_24H = 80.0
EXTREME_DIP_24H = 95.0
ULTRA_DIP_24H = 99.0

REQUEST_INTERVAL = float(os.getenv("REQUEST_INTERVAL", "6.5"))
MAX_RETRIES = 3
ALERT_COOLDOWN_HOURS = float(os.getenv("ALERT_COOLDOWN_HOURS", "12"))

DB_PATH = os.getenv("DB_PATH", "solana_dip_radar_v3.sqlite3")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

HEADERS = {
    "accept": "application/json;version=20230203",
    "user-agent": "SolanaDipRadar/3.0",
}

session = requests.Session()
session.headers.update(HEADERS)

solana_session = requests.Session()
solana_session.headers.update({
    "accept": "application/json",
    "content-type": "application/json",
    "user-agent": "SolanaDipRadar/3.0",
})

_last_gt_request = 0.0


# =========================
# HELPERS
# =========================

def now_ts() -> int:
    return int(time.time())


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_time(value: Any) -> int | None:
    if not value:
        return None
    try:
        return int(
            datetime.fromisoformat(
                str(value).replace("Z", "+00:00")
            ).timestamp()
        )
    except Exception:
        return None


def fmt_usd(value: float | None) -> str:
    if value is None:
        return "n/a"
    if value >= 1000:
        return f"${value:,.0f}"
    if value >= 1:
        return f"${value:,.2f}"
    if value >= 0.01:
        return f"${value:.6f}".rstrip("0").rstrip(".")
    return f"${value:.10f}".rstrip("0").rstrip(".")


def pct_change(new_price: float, old_price: float) -> float | None:
    if new_price is None or old_price is None or old_price <= 0:
        return None
    return ((new_price - old_price) / old_price) * 100.0


def format_age(created_at: int | None) -> str:
    if not created_at:
        return "unknown"
    seconds = max(0, now_ts() - created_at)
    if seconds < 3600:
        return f"{seconds / 60:.0f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def format_duration(seconds: int) -> str:
    minutes = max(1, int(round(seconds / 60)))
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    rem = minutes % 60
    if hours < 24:
        return f"{hours}h" if not rem else f"{hours}h {rem}m"
    return f"{hours / 24:.1f}d"


# =========================
# SOLANA RPC / SAFETY
# =========================


def solana_rpc(method: str, params: list[Any]) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    max_retries = 5

    for attempt in range(max_retries):
        try:
            response = solana_session.post(
                SOLANA_RPC_URL,
                json=payload,
                timeout=30,
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")

                try:
                    wait_time = float(retry_after)
                except (TypeError, ValueError):
                    wait_time = min(30, 2 ** attempt)

                print(
                    f"[WARN] Solana RPC rate limit (429). "
                    f"Waiting {wait_time:.1f}s before retry "
                    f"({attempt + 1}/{max_retries})..."
                )

                time.sleep(wait_time)
                continue

            response.raise_for_status()

            data = response.json()

            if data.get("error"):
                raise RuntimeError(
                    f"Solana RPC {method}: {data['error']}"
                )

            return data.get("result")

        except requests.RequestException as exc:
            if attempt >= max_retries - 1:
                raise

            wait_time = min(30, 2 ** attempt)

            print(
                f"[WARN] Solana RPC request failed: {exc}. "
                f"Retrying in {wait_time}s..."
            )

            time.sleep(wait_time)

    raise RuntimeError(
        f"Solana RPC {method} failed after {max_retries} attempts"
    )


def get_mint_info(mint: str) -> dict[str, Any]:
    result = solana_rpc(
        "getAccountInfo",
        [
            mint,
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
    )

    value = (result or {}).get("value")
    if not value:
        raise RuntimeError("Mint account not found")

    parsed = value.get("data", {}).get("parsed", {})
    info = parsed.get("info", {})

    if parsed.get("type") != "mint":
        raise RuntimeError("Address is not a parsed SPL mint")

    supply_raw = int(info.get("supply", 0))
    decimals = int(info.get("decimals", 0))

    return {
        "mint_authority": info.get("mintAuthority"),
        "freeze_authority": info.get("freezeAuthority"),
        "supply_raw": supply_raw,
        "decimals": decimals,
        "supply": supply_raw / (10 ** decimals) if decimals >= 0 else 0,
    }


def get_token_accounts_for_owner(owner: str, mint: str) -> int:
    result = solana_rpc(
        "getTokenAccountsByOwner",
        [
            owner,
            {"mint": mint},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
    )

    total_raw = 0

    for item in (result or {}).get("value", []):
        parsed = (
            item.get("account", {})
            .get("data", {})
            .get("parsed", {})
        )
        amount = (
            parsed.get("info", {})
            .get("tokenAmount", {})
            .get("amount")
        )
        try:
            total_raw += int(amount or 0)
        except (TypeError, ValueError):
            pass

    return total_raw


def base58_encode(data: bytes) -> str:
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = int.from_bytes(data, "big")

    if number == 0:
        return "1" if data else ""

    chars = []
    while number:
        number, remainder = divmod(number, 58)
        chars.append(alphabet[remainder])

    leading_zeroes = 0
    for byte in data:
        if byte != 0:
            break
        leading_zeroes += 1

    return "1" * leading_zeroes + "".join(reversed(chars))


def metadata_pda(mint: str) -> str:
    from solders.pubkey import Pubkey

    metadata_program = Pubkey.from_string(
        "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
    )
    mint_pubkey = Pubkey.from_string(mint)

    pda, _ = Pubkey.find_program_address(
        [
            b"metadata",
            bytes(metadata_program),
            bytes(mint_pubkey),
        ],
        metadata_program,
    )
    return str(pda)


def read_metadata(mint: str) -> tuple[str | None, bool | None, list[str]]:
    """
    Returns:
      update_authority, is_mutable, creator_addresses

    Fail closed if metadata cannot be decoded.
    """
    try:
        pda = metadata_pda(mint)

        result = solana_rpc(
            "getAccountInfo",
            [
                pda,
                {"encoding": "base64", "commitment": "confirmed"},
            ],
        )

        value = (result or {}).get("value")
        if not value:
            return None, None, []

        data = value.get("data")
        if not isinstance(data, list) or not data:
            return None, None, []

        raw = base64.b64decode(data[0])
        offset = 0

        if len(raw) < 65:
            return None, None, []

        offset += 1  # key

        update_authority = base58_encode(raw[offset:offset + 32])
        offset += 32

        offset += 32  # mint

        def read_borsh_string(buf: bytes, pos: int):
            if pos + 4 > len(buf):
                raise ValueError("truncated string length")
            length = int.from_bytes(buf[pos:pos + 4], "little")
            pos += 4
            if pos + length > len(buf):
                raise ValueError("truncated string")
            value = buf[pos:pos + length]
            pos += length
            return value, pos

        _, offset = read_borsh_string(raw, offset)  # name
        _, offset = read_borsh_string(raw, offset)  # symbol
        _, offset = read_borsh_string(raw, offset)  # uri

        offset += 2  # seller fee

        if offset >= len(raw):
            return None, None, []

        creators_option = raw[offset]
        offset += 1

        creators = []

        if creators_option == 1:
            if offset + 4 > len(raw):
                return None, None, []
            count = int.from_bytes(raw[offset:offset + 4], "little")
            offset += 4

            for _ in range(count):
                if offset + 35 > len(raw):
                    return None, None, []
                creators.append(base58_encode(raw[offset:offset + 32]))
                offset += 35  # pubkey + verified + share

        # collection Option<Collection>
        if offset >= len(raw):
            return None, None, []
        collection_option = raw[offset]
        offset += 1

        if collection_option == 1:
            offset += 33  # verified + key

        # uses Option<Uses>
        if offset >= len(raw):
            return None, None, []
        uses_option = raw[offset]
        offset += 1

        if uses_option == 1:
            offset += 17  # use_method + remaining + total

        if offset >= len(raw):
            return None, None, []

        is_mutable = bool(raw[offset])

        return update_authority, is_mutable, creators

    except Exception:
        return None, None, []


def get_dev_holding_percent(
    mint: str,
    mint_info: dict[str, Any],
    update_authority: str | None,
    creator_addresses: list[str],
) -> float | None:

    supply_raw = int(mint_info.get("supply_raw", 0))
    if supply_raw <= 0:
        return None

    owners = []

    for owner in creator_addresses:
        if owner and owner not in owners:
            owners.append(owner)

    if update_authority and update_authority not in owners:
        owners.append(update_authority)

    if not owners:
        return None

    total_raw = 0

    for owner in owners:
        try:
            total_raw += get_token_accounts_for_owner(owner, mint)
        except Exception:
            return None

    return total_raw / supply_raw * 100.0


def safety_check_token(mint: str) -> tuple[bool, dict[str, Any], str]:
    details: dict[str, Any] = {}

    try:
        mint_info = get_mint_info(mint)

        details["mint_authority"] = mint_info.get("mint_authority")
        details["freeze_authority"] = mint_info.get("freeze_authority")
        details["supply"] = mint_info.get("supply")

        if REQUIRE_MINT_AUTHORITY_REVOKED:
            if mint_info.get("mint_authority") is not None:
                return False, details, "mintable (mint authority active)"

        update_authority, is_mutable, creators = read_metadata(mint)

        details["update_authority"] = update_authority
        details["metadata_mutable"] = is_mutable
        details["creator_addresses"] = creators

        if REQUIRE_METADATA_IMMUTABLE:
            if is_mutable is not False:
                return False, details, "metadata mutable or unverified"

        dev_pct = get_dev_holding_percent(
            mint,
            mint_info,
            update_authority,
            creators,
        )

        details["dev_holding_percent"] = dev_pct

        if dev_pct is None:
            return False, details, "dev holdings unverified"

        if dev_pct > MAX_DEV_HOLDING_PERCENT:
            return (
                False,
                details,
                f"dev holds {dev_pct:.2f}% > {MAX_DEV_HOLDING_PERCENT:.2f}%",
            )

        return True, details, "passed"

    except Exception as exc:
        return False, details, f"safety check error: {exc}"


# =========================
# DATABASE
# =========================

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pools (
            pool_address TEXT PRIMARY KEY,
            token_address TEXT,
            symbol TEXT,
            pool_name TEXT,
            source_type TEXT,
            first_seen INTEGER,
            last_seen INTEGER,
            last_price REAL,
            last_15m REAL,
            last_1h REAL,
            last_24h_high REAL,
            last_24h_drawdown REAL,
            last_alert_key TEXT,
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
            source_type TEXT,
            price REAL,
            change_15m REAL,
            change_1h REAL,
            change_24h_high_drawdown REAL,
            liquidity REAL,
            volume_24h REAL,
            tx_24h INTEGER,
            pool_age_hours REAL,
            result TEXT
        )
    """)

    conn.commit()
    return conn


def previous_alert(conn, pool_address: str):
    row = conn.execute(
        """
        SELECT last_alert_key, last_alert_ts
        FROM pools
        WHERE pool_address = ?
        """,
        (pool_address,),
    ).fetchone()

    return (row[0], row[1]) if row else (None, None)


def alert_allowed(conn, pool_address: str, alert_key: str) -> bool:
    old_key, old_ts = previous_alert(conn, pool_address)

    if not old_key or not old_ts:
        return True

    if old_key != alert_key:
        return True

    return now_ts() - int(old_ts) >= ALERT_COOLDOWN_HOURS * 3600


# =========================
# GECKOTERMINAL
# =========================

def get_json(path: str, params: dict[str, Any] | None = None):
    global _last_gt_request

    url = f"{BASE_URL}{path}"

    for attempt in range(MAX_RETRIES):
        wait = REQUEST_INTERVAL - (
            time.monotonic() - _last_gt_request
        )

        if wait > 0:
            time.sleep(wait)

        try:
            _last_gt_request = time.monotonic()

            response = session.get(
                url,
                params=params or {},
                timeout=30,
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    retry_wait = float(retry_after)
                except (TypeError, ValueError):
                    retry_wait = 15.0

                retry_wait = max(10.0, min(retry_wait, 60.0))

                print(
                    f"[WARN] GeckoTerminal 429. "
                    f"Waiting {retry_wait:.0f}s..."
                )
                time.sleep(retry_wait)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException as exc:
            if attempt == MAX_RETRIES - 1:
                raise

            retry_wait = 10.0 * (attempt + 1)
            print(
                f"[WARN] API error: {exc}. "
                f"Retrying in {retry_wait:.0f}s..."
            )
            time.sleep(retry_wait)

    raise RuntimeError("GeckoTerminal request failed")


def tx_count(transactions: dict[str, Any], key: str) -> int:
    value = transactions.get(key) or {}

    if isinstance(value, dict):
        return (
            int(value.get("buys", 0) or 0)
            + int(value.get("sells", 0) or 0)
        )

    try:
        return int(value or 0)
    except Exception:
        return 0


def parse_pool(item: dict[str, Any], source_type: str) -> dict[str, Any]:
    attributes = item.get("attributes") or {}
    relationships = item.get("relationships") or {}

    pool_id = item.get("id", "")

    address = (
        attributes.get("address")
        or pool_id.split("_", 1)[-1]
    )

    base_data = (
        relationships.get("base_token", {}).get("data") or {}
    )

    quote_data = (
        relationships.get("quote_token", {}).get("data") or {}
    )

    dex_data = (
        relationships.get("dex", {}).get("data") or {}
    )

    base_id = base_data.get("id") or ""
    quote_id = quote_data.get("id") or ""

    token_address = (
        base_id.split("_", 1)[1]
        if "_" in base_id
        else None
    )

    tx = attributes.get("transactions") or {}
    volume = attributes.get("volume_usd") or {}
    changes = attributes.get("price_change_percentage") or {}

    pool_name = attributes.get("name") or ""
    symbol = pool_name.split(" / ")[0].strip()

    return {
        "pool_address": address,
        "token_address": token_address,
        "pool_name": pool_name,
        "symbol": symbol,
        "source_type": source_type,
        "dex": dex_data.get("id"),
        "quote_id": quote_id,
        "price": safe_float(attributes.get("base_token_price_usd")),
        "liquidity": safe_float(attributes.get("reserve_in_usd")),
        "volume_24h": safe_float(volume.get("h24")),
        "tx_24h": tx_count(tx, "h24"),
        "pool_created_at": parse_time(attributes.get("pool_created_at")),
        "change_24h": safe_float(changes.get("h24")),
        "fdv": safe_float(attributes.get("fdv_usd")),
    }


def discover_all_pools(
    endpoint: str,
    params: dict[str, Any],
    source_type: str,
) -> list[dict[str, Any]]:

    results = []
    page = 1

    while True:
        if MAX_POOL_PAGES > 0 and page > MAX_POOL_PAGES:
            print(
                f"[INFO] {source_type}: stopped at configured "
                f"MAX_POOL_PAGES={MAX_POOL_PAGES}"
            )
            break

        query = dict(params)
        query["page"] = page

        try:
            payload = get_json(endpoint, query)
        except Exception as exc:
            print(
                f"[WARN] {source_type}: discovery stopped at "
                f"page {page}: {exc}"
            )
            break

        data = payload.get("data", []) or []

        if not data:
            print(
                f"[INFO] {source_type}: finished at page {page - 1}"
            )
            break

        for item in data:
            results.append(parse_pool(item, source_type))

        print(
            f"[INFO] {source_type}: page {page} "
            f"({len(data)} pools)"
        )

        # GeckoTerminal commonly returns a partial final page.
        if len(data) < 20:
            print(
                f"[INFO] {source_type}: final partial page reached"
            )
            break

        page += 1

    return results


def discover_candidates() -> list[dict[str, Any]]:
    """Discover pools only from selected Solana DEX venues.

    We deliberately do NOT crawl the entire Solana network.
    Pools older than 7 days are allowed; only pools younger than 48 hours
    are rejected later during the hard age filter.
    """
    print(
        "[INFO] Scanning selected Solana DEX pool sources only: "
        + ", ".join(SOLANA_DEX_IDS)
    )
    print(
        f"[INFO] Max {DEX_PAGES_PER_SOURCE} page(s) per DEX; "
        "no all-Solana pool crawl; NO MAXIMUM AGE."
    )

    seen = set()
    selected: list[dict[str, Any]] = []

    for dex_id in SOLANA_DEX_IDS:
        endpoint = f"/networks/{NETWORK}/dexes/{dex_id}/pools"
        params = {
            "include": "base_token,quote_token,dex",
            "sort": "h24_volume_usd_desc",
        }

        print(f"[INFO] DEX: {dex_id}")

        for page in range(1, DEX_PAGES_PER_SOURCE + 1):
            try:
                payload = get_json(
                    endpoint,
                    {**params, "page": page},
                )
            except Exception as exc:
                print(
                    f"[WARN] {dex_id}: stopped at page {page}: {exc}"
                )
                break

            data = payload.get("data", []) or []
            if not data:
                break

            added = 0
            for item in data:
                parsed = parse_pool(item, dex_id.upper())
                address = parsed["pool_address"]
                if address and address not in seen:
                    seen.add(address)
                    selected.append(parsed)
                    added += 1

            print(
                f"[INFO] {dex_id}: page {page} "
                f"({len(data)} pools, {added} new)"
            )

            if len(data) < 20:
                break

    print(
        f"[INFO] Selected DEX pool candidates: {len(selected)} "
        f"(from {len(SOLANA_DEX_IDS)} DEX sources)"
    )
    return selected


# =========================
# OHLCV
# =========================

def get_15m_candles(pool_address: str):
    payload = get_json(
        f"/networks/{NETWORK}/pools/"
        f"{quote(pool_address, safe='')}/ohlcv/minute",
        {
            "aggregate": 15,
            "limit": 97,
            "currency": "usd",
            "token": "base",
        },
    )

    raw = (
        (payload.get("data") or {})
        .get("attributes", {})
        .get("ohlcv_list", [])
    )

    rows = []

    for row in raw:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue

        try:
            timestamp = int(float(row[0]))
        except Exception:
            continue

        close = safe_float(row[4])

        if close and close > 0:
            rows.append((timestamp, close))

    rows.sort(key=lambda x: x[0])
    return rows


def nearest_change(
    rows,
    target_seconds: int,
    min_seconds: int,
    max_seconds: int,
):
    if len(rows) < 2:
        return None

    latest_ts, latest_price = rows[-1]
    choices = []

    for timestamp, old_price in rows[:-1]:
        elapsed = latest_ts - timestamp

        if not min_seconds <= elapsed <= max_seconds:
            continue

        change = pct_change(latest_price, old_price)

        if change is None:
            continue

        choices.append(
            (
                abs(elapsed - target_seconds),
                change,
                elapsed,
            )
        )

    if not choices:
        return None

    choices.sort(key=lambda x: x[0])
    _, change, elapsed = choices[0]

    return change, elapsed


def best_down(rows, min_seconds: int, max_seconds: int):
    if len(rows) < 2:
        return None

    latest_ts, latest_price = rows[-1]
    best = None

    for timestamp, old_price in rows[:-1]:
        elapsed = latest_ts - timestamp

        if not min_seconds <= elapsed <= max_seconds:
            continue

        change = pct_change(latest_price, old_price)

        if change is None:
            continue

        if best is None or change < best[0]:
            best = (change, elapsed)

    return best


def best_up(rows, min_seconds: int, max_seconds: int):
    if len(rows) < 2:
        return None

    latest_ts, latest_price = rows[-1]
    best = None

    for timestamp, old_price in rows[:-1]:
        elapsed = latest_ts - timestamp

        if not min_seconds <= elapsed <= max_seconds:
            continue

        change = pct_change(latest_price, old_price)

        if change is None:
            continue

        if best is None or change > best[0]:
            best = (change, elapsed)

    return best


def observed_24h_high(rows):
    if not rows:
        return None, None

    latest_ts = rows[-1][0]

    recent = [
        x for x in rows
        if latest_ts - x[0] <= 86400
    ]

    if not recent:
        recent = rows

    high = max(price for _, price in recent)
    high_ts = max(
        ts for ts, price in recent
        if price == high
    )

    return high, high_ts


# =========================
# ALERT LOGIC
# =========================

def build_alert_key(
    change_15m,
    change_1h,
    down_24h,
    drawdown_24h,
    up_1h,
):
    levels = []

    # ONLY -30% OR WORSE.
    if change_15m is not None and change_15m <= -MIN_DIP_ALERT:
        levels.append("DIP_15M_30")

    if change_1h is not None and change_1h <= -MIN_DIP_ALERT:
        levels.append("DIP_1H_30")

    if down_24h is not None and down_24h <= -MIN_DIP_ALERT:
        levels.append("DIP_24H_30")

    if drawdown_24h is not None:
        if drawdown_24h >= ULTRA_DIP_24H:
            levels.append("DIP_99")
        elif drawdown_24h >= EXTREME_DIP_24H:
            levels.append("DIP_95")
        elif drawdown_24h >= DEEP_DIP_24H:
            levels.append("DIP_80")
        elif drawdown_24h >= MIN_DIP_ALERT:
            levels.append("DIP_30")

    # ONLY STRICTLY ABOVE +100%.
    if up_1h is not None and up_1h > PUMP_1H:
        levels.append("PUMP100+")

    return "+".join(levels) if levels else None


# =========================
# TELEGRAM
# =========================

def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("\n[TELEGRAM NOT CONFIGURED]\n" + message)
        return False

    url = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    try:
        response = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "disable_web_page_preview": True,
            },
            timeout=20,
        )

        if not response.ok:
            print(
                f"[WARN] Telegram: "
                f"{response.status_code} "
                f"{response.text[:300]}"
            )
            return False

        return True

    except requests.RequestException as exc:
        print(f"[WARN] Telegram request failed: {exc}")
        return False


# =========================
# DATABASE WRITE
# =========================

def save_scan(
    conn,
    p,
    change_15m,
    change_1h,
    drawdown_24h,
    result,
):
    created = p.get("pool_created_at")
    age_hours = None

    if created:
        age_hours = max(0, (now_ts() - created) / 3600)

    conn.execute(
        """
        INSERT INTO scans(
            ts, pool_address, token_address, symbol, source_type,
            price, change_15m, change_1h, change_24h_high_drawdown,
            liquidity, volume_24h, tx_24h, pool_age_hours, result
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_ts(),
            p["pool_address"],
            p["token_address"],
            p["symbol"],
            p["source_type"],
            p["price"],
            change_15m,
            change_1h,
            drawdown_24h,
            p["liquidity"],
            p["volume_24h"],
            p["tx_24h"],
            age_hours,
            result,
        ),
    )

    conn.commit()


def update_pool(
    conn,
    p,
    change_15m,
    change_1h,
    high_24h,
    drawdown_24h,
    alert_key,
    alert_sent,
):
    old_key, old_ts = previous_alert(
        conn,
        p["pool_address"],
    )

    last_key = alert_key if alert_sent else old_key
    last_ts = now_ts() if alert_sent else old_ts

    conn.execute(
        """
        INSERT INTO pools(
            pool_address, token_address, symbol, pool_name, source_type,
            first_seen, last_seen, last_price, last_15m, last_1h,
            last_24h_high, last_24h_drawdown, last_alert_key, last_alert_ts
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(pool_address) DO UPDATE SET
            token_address=excluded.token_address,
            symbol=excluded.symbol,
            pool_name=excluded.pool_name,
            source_type=excluded.source_type,
            last_seen=excluded.last_seen,
            last_price=excluded.last_price,
            last_15m=excluded.last_15m,
            last_1h=excluded.last_1h,
            last_24h_high=excluded.last_24h_high,
            last_24h_drawdown=excluded.last_24h_drawdown,
            last_alert_key=excluded.last_alert_key,
            last_alert_ts=excluded.last_alert_ts
        """,
        (
            p["pool_address"],
            p["token_address"],
            p["symbol"],
            p["pool_name"],
            p["source_type"],
            now_ts(),
            now_ts(),
            p["price"],
            change_15m,
            change_1h,
            high_24h,
            drawdown_24h,
            last_key,
            last_ts,
        ),
    )

    conn.commit()


# =========================
# TELEGRAM MESSAGE
# =========================

def alert_message(
    p,
    change_15m,
    change_1h,
    down_24h,
    drawdown_24h,
    high_24h,
    up_1h,
    alert_key,
):
    title = "🚨 SOLANA DIP RADAR"

    if "PUMP100+" in alert_key and "DIP_" not in alert_key:
        title = "🚀 SOLANA PUMP ALERT"

    lines = [
        title,
        "",
        f"Token: {p['symbol'] or 'UNKNOWN'}",
        f"Type: {p['source_type']}",
        f"Pool age: {format_age(p['pool_created_at'])}",
        f"Price: {fmt_usd(p['price'])}",
        "",
    ]

    if change_15m is not None:
        lines.append(f"15m: {change_15m:+.2f}%")

    if change_1h is not None:
        lines.append(f"1h: {change_1h:+.2f}%")

    if down_24h:
        lines.append(
            f"Best 1h-24h drop: "
            f"{down_24h[0]:+.2f}% "
            f"in {format_duration(down_24h[1])}"
        )

    if high_24h:
        lines.append(f"24h high: {fmt_usd(high_24h)}")

    if drawdown_24h is not None:
        lines.append(
            f"Below 24h high: {drawdown_24h:.2f}%"
        )

    if up_1h:
        lines.append(
            f"Best 1h-24h rise: "
            f"{up_1h[0]:+.2f}% "
            f"in {format_duration(up_1h[1])}"
        )

    lines += [
        "",
        f"Liquidity: {fmt_usd(p['liquidity'])}",
        f"24h volume: {fmt_usd(p['volume_24h'])}",
        f"24h transactions: {p['tx_24h']}",
        f"DEX: {p['dex'] or 'unknown'}",
        f"Dev holdings: {p.get('dev_holding_percent', 0):.2f}% (MAX 5%)",
        "Metadata mutable: NO",
        "Mint authority: REVOKED",
        f"Signal: {alert_key}",
        "",
        f"Mint: {p['token_address'] or 'unknown'}",
        f"Pool: {p['pool_address']}",
        "",
        "⚠️ Research alert only. Deep drops can be caused by scams, "
        "rug pulls, liquidity removal or other risks.",
    ]

    return "\n".join(lines)


# =========================
# MAIN
# =========================

def scan():
    conn = init_db()

    started = datetime.now(
        timezone.utc
    ).astimezone().isoformat(
        timespec="seconds"
    )

    print("")
    print("=" * 72)
    print("SOLANA DIP RADAR v5")
    print(f"Started: {started}")
    print("SOLANA POOLS: SELECTED DEX SOURCES ONLY / MAX 20 VERIFIED POOLS")
    print("MINIMUM POOL AGE: >= 48 HOURS")
    print("NO MAXIMUM AGE")
    print("MAX VERIFIED POOLS: 20")
    print(f"MINIMUM 24H TRANSACTIONS: {MIN_TX_24H}")
    print("DEV HOLDING FILTER: <= 5% REQUIRED")
    print("MINT AUTHORITY: MUST BE REVOKED")
    print("METADATA: MUST BE IMMUTABLE")
    print("MINIMUM DIP ALERT: -30%")
    print("PUMP ALERT: > +100% ONLY")
    print("DIPS: -30% OR WORSE ONLY")
    print("=" * 72)

    candidates = discover_candidates()

    print(f"\nPools discovered: {len(candidates)}")

    source_counts = {}
    for p in candidates:
        source = p["source_type"]
        source_counts[source] = source_counts.get(source, 0) + 1

    print(
        "Sources: "
        + " | ".join(
            f"{k}: {v}"
            for k, v in sorted(source_counts.items())
        )
    )

    print(
        f"API spacing: {REQUEST_INTERVAL:.1f}s "
        "(deliberately slow to reduce 429 errors)"
    )
    print("")

    verified_count = 0

    for index, p in enumerate(candidates, start=1):
        # HARD STOP: after 20 pools have passed all filters and have
        # usable OHLCV data, do not fully analyze another pool.
        if verified_count >= MAX_VERIFIED_POOLS:
            print(f"\n[INFO] 20 verified pools reached. Stopping full scan.")
            break
        name = p["symbol"] or p["pool_name"] or "UNKNOWN"

        try:
            # HARD 48-HOUR AGE FILTER.
            created_at = p.get("pool_created_at")

            if not created_at:
                print(
                    f"[{index:04d}] {name} | "
                    f"{p['source_type']} | age unknown | FILTERED"
                )
                continue

            age_days = (now_ts() - created_at) / 86400.0

            if age_days < MIN_POOL_AGE_DAYS:
                print(
                    f"[{index:04d}] {name} | "
                    f"{p['source_type']} | too new "
                    f"({age_days * 24:.1f}h < 48h)"
                )
                continue

            if p["price"] is None:
                print(
                    f"[{index:04d}] {name} | NO PRICE"
                )
                continue

            if (p["liquidity"] or 0) < MIN_LIQUIDITY:
                print(
                    f"[{index:04d}] {name} | "
                    f"low liquidity {fmt_usd(p['liquidity'])}"
                )
                continue

            if (p["volume_24h"] or 0) < MIN_VOLUME_24H:
                print(
                    f"[{index:04d}] {name} | "
                    f"low 24h volume {fmt_usd(p['volume_24h'])}"
                )
                continue

            if p["tx_24h"] < MIN_TX_24H:
                print(
                    f"[{index:04d}] {name} | "
                    f"low transactions {p['tx_24h']}"
                )
                continue

            if not p["token_address"]:
                print(
                    f"[{index:04d}] {name} | "
                    "missing token address | FILTERED"
                )
                continue

            # HARD SAFETY FILTERS BEFORE OHLCV.
            safety_ok, safety_details, safety_reason = (
                safety_check_token(p["token_address"])
            )

            if not safety_ok:
                print(
                    f"[{index:04d}] FILTERED {name} | "
                    f"{safety_reason}"
                )
                save_scan(
                    conn,
                    p,
                    None,
                    None,
                    None,
                    f"FILTERED: {safety_reason}",
                )
                continue

            p["dev_holding_percent"] = (
                safety_details["dev_holding_percent"]
            )
            p["metadata_mutable"] = (
                safety_details["metadata_mutable"]
            )
            p["mint_authority"] = (
                safety_details["mint_authority"]
            )

            rows = get_15m_candles(p["pool_address"])

            if len(rows) < 5:
                print(
                    f"[{index:04d}] {name} | "
                    "not enough candles"
                )
                continue

            verified_count += 1
            print(
                f"[VERIFIED {verified_count:02d}/{MAX_VERIFIED_POOLS}] "
                f"{name} | {p['source_type']} | "
                f"age {format_age(p['pool_created_at'])} | "
                f"tx24h {p['tx_24h']} | "
                f"liq {fmt_usd(p['liquidity'])}"
            )

            latest_price = rows[-1][1]
            p["price"] = latest_price

            change_15m_data = nearest_change(
                rows, 15 * 60, 10 * 60, 25 * 60
            )

            change_1h_data = nearest_change(
                rows, 60 * 60, 45 * 60, 90 * 60
            )

            down_24h = best_down(
                rows, 60 * 60, 24 * 60 * 60
            )

            up_1h = best_up(
                rows, 60 * 60, 24 * 60 * 60
            )

            change_15m = (
                change_15m_data[0]
                if change_15m_data else None
            )

            change_1h = (
                change_1h_data[0]
                if change_1h_data else None
            )

            high_24h, _ = observed_24h_high(rows)

            drawdown_24h = None

            if high_24h and high_24h > 0:
                drawdown_24h = max(
                    0.0,
                    (1.0 - latest_price / high_24h) * 100.0,
                )

            up_value = up_1h[0] if up_1h else None

            alert_key = build_alert_key(
                change_15m,
                change_1h,
                down_24h[0] if down_24h else None,
                drawdown_24h,
                up_value,
            )

            # SILENT for ordinary movements.
            if not alert_key:
                save_scan(
                    conn,
                    p,
                    change_15m,
                    change_1h,
                    drawdown_24h,
                    "NO_ALERT",
                )

                update_pool(
                    conn,
                    p,
                    change_15m,
                    change_1h,
                    high_24h,
                    drawdown_24h,
                    None,
                    False,
                )
                continue

            print(
                f"[{index:04d}] 🚨 ALERT | "
                f"{name} / {p['source_type']} | "
                f"15m "
                f"{change_15m:+.2f}%"
                if change_15m is not None
                else
                f"[{index:04d}] 🚨 ALERT | "
                f"{name} / {p['source_type']} | 15m n/a",
                end="",
            )

            if change_1h is not None:
                print(
                    f" | 1h {change_1h:+.2f}%",
                    end="",
                )

            if drawdown_24h is not None:
                print(
                    f" | 24hHigh↓ {drawdown_24h:.2f}%",
                    end="",
                )

            print()

            if not alert_allowed(
                conn,
                p["pool_address"],
                alert_key,
            ):
                print(
                    f"       ↳ {alert_key} "
                    "already alerted recently"
                )

                update_pool(
                    conn,
                    p,
                    change_15m,
                    change_1h,
                    high_24h,
                    drawdown_24h,
                    alert_key,
                    False,
                )
                continue

            message = alert_message(
                p,
                change_15m,
                change_1h,
                down_24h,
                drawdown_24h,
                high_24h,
                up_1h,
                alert_key,
            )

            sent = send_telegram(message)

            update_pool(
                conn,
                p,
                change_15m,
                change_1h,
                high_24h,
                drawdown_24h,
                alert_key,
                sent,
            )

            if sent:
                print(
                    f"       🚨 ALERT SENT: {alert_key}"
                )
            else:
                print(
                    f"       ⚠️ Signal found: {alert_key}"
                )

        except Exception as exc:
            print(
                f"[{index:04d}] ERROR {name}: "
                f"{type(exc).__name__}: {exc}"
            )

    print(
        f"\nVerified pools fully analyzed: {verified_count}/{MAX_VERIFIED_POOLS}"
    )

    conn.close()

    print("")
    print("=" * 72)
    print("SCAN COMPLETE")
    print("=" * 72)


if __name__ == "__main__":
    scan()
