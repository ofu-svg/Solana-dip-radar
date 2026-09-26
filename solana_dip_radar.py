#!/usr/bin/env python3
"""
SOLANA DIP RADAR v2
- Scans BOTH new and established Solana pools.
- Only scans tokens/pools at least 7 days old; no maximum age.
- Prints real 15m and 1h percentage changes.
- Prints price, liquidity, 24h volume, transactions, pool age and 24h-high drawdown.
- Uses a slow request gate to stay below GeckoTerminal's public rate limit.
- Sends Telegram alerts for major crashes / deep dips / extreme pumps.

GeckoTerminal public API is rate-limited, so this version intentionally runs
more slowly than the old script. That is deliberate: fewer 429 errors and
more reliable numbers.
"""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv

# Requires: requests, python-dotenv, solders

load_dotenv()

BASE_URL = os.getenv(
    "GT_BASE_URL",
    "https://api.geckoterminal.com/api/v2",
)
NETWORK = "solana"

# Solana RPC used for mint-authority and metadata/developer checks.
# You can replace this with a Helius/QuickNode/other RPC URL in .env
# if the public endpoint is rate-limited.
SOLANA_RPC_URL = os.getenv(
    "SOLANA_RPC_URL",
    "https://api.mainnet-beta.solana.com",
).strip()

# Safety filters requested:
MAX_DEV_HOLDING_PERCENT = float(
    os.getenv("MAX_DEV_HOLDING_PERCENT", "5")
)
REQUIRE_MINT_AUTHORITY_REVOKED = (
    os.getenv("REQUIRE_MINT_AUTHORITY_REVOKED", "true").lower()
    == "true"
)
REQUIRE_METADATA_IMMUTABLE = (
    os.getenv("REQUIRE_METADATA_IMMUTABLE", "true").lower()
    == "true"
)

# Use a fresh DB by default so the old broken schema cannot interfere.
DB_PATH = os.getenv("DB_PATH", "solana_dip_radar_v2.sqlite3")

# -------- SCANNER SETTINGS --------
TOTAL_CANDIDATES = int(os.getenv("TOTAL_CANDIDATES", "30"))
NEW_CANDIDATES = int(os.getenv("NEW_CANDIDATES", "15"))
ESTABLISHED_CANDIDATES = TOTAL_CANDIDATES - NEW_CANDIDATES

MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY_USD", "300"))
MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H_USD", "30"))
MIN_TX_24H = int(os.getenv("MIN_TX_24H", "10"))
MIN_POOL_AGE_DAYS = float(os.getenv("MIN_POOL_AGE_DAYS", "7"))

# IMPORTANT: only pools at least 7 days old are scanned.
# Very new tokens are intentionally excluded.
# There is NO maximum-age filter: older established tokens remain eligible.

# GeckoTerminal public API is about 10 calls/minute.
# 6.5 sec between calls keeps us under that limit.
REQUEST_INTERVAL = float(os.getenv("REQUEST_INTERVAL", "6.5"))
MAX_RETRIES = 3

# Alert thresholds
CRASH_15M = max(30.0, float(os.getenv("CRASH_15M", "30")))
CRASH_1H = max(30.0, float(os.getenv("CRASH_1H", "30")))
CRASH_24H = max(30.0, float(os.getenv("CRASH_24H", "45")))
DEEP_DIP_24H = float(os.getenv("DEEP_DIP_24H", "80"))
EXTREME_DIP_24H = float(os.getenv("EXTREME_DIP_24H", "95"))
ULTRA_DIP_24H = float(os.getenv("ULTRA_DIP_24H", "99"))
MIN_DIP_ALERT = 30.0  # Never report a dip smaller than -30%.
PUMP_1H = max(100.0001, float(os.getenv("PUMP_1H", "100.0001")))  # Pump alert: >100%

ALERT_COOLDOWN_HOURS = float(
    os.getenv("ALERT_COOLDOWN_HOURS", "12")
)

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN", ""
).strip()
TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID", ""
).strip()

HEADERS = {
    "accept": "application/json;version=20230203",
    "user-agent": "SolanaDipRadar/2.0",
}

session = requests.Session()
session.headers.update(HEADERS)

_last_gt_request = 0.0


# ============================================================
# BASIC HELPERS
# ============================================================

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
    if (
        new_price is None
        or old_price is None
        or old_price <= 0
    ):
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


# ============================================================
# SOLANA SAFETY / AUTHORITY CHECKS
# ============================================================

# These checks are intentionally conservative:
# if the RPC cannot verify a safety property, the token is rejected
# rather than being shown as "safe".

solana_session = requests.Session()
solana_session.headers.update({
    "accept": "application/json",
    "content-type": "application/json",
    "user-agent": "SolanaDipRadar/2.0",
})


def solana_rpc(method: str, params: list[Any]) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    response = solana_session.post(
        SOLANA_RPC_URL,
        json=payload,
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()

    if data.get("error"):
        raise RuntimeError(
            f"Solana RPC {method}: {data['error']}"
        )

    return data.get("result")


def get_mint_info(mint: str) -> dict[str, Any]:
    result = solana_rpc(
        "getAccountInfo",
        [
            mint,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
            },
        ],
    )

    value = (result or {}).get("value")

    if not value:
        raise RuntimeError("Mint account not found")

    parsed = (
        value.get("data", {})
        .get("parsed", {})
    )

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
        "supply": (
            supply_raw / (10 ** decimals)
            if decimals >= 0
            else 0
        ),
    }


def get_token_accounts_for_owner(
    owner: str,
    mint: str,
) -> int:
    result = solana_rpc(
        "getTokenAccountsByOwner",
        [
            owner,
            {
                "mint": mint,
            },
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
            },
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


def metadata_pda(mint: str) -> str:
    """
    Derive the Metaplex Metadata PDA.
    Requires solders (included in the requirements below).
    """
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


def read_metadata_update_authority_and_mutability(
    mint: str,
) -> tuple[str | None, bool | None, list[str]]:
    """
    Read Metaplex metadata.

    Returns:
      (update_authority, is_mutable, creator_addresses)

    If the metadata account cannot be decoded, returns (None, None, []).
    """
    try:
        pda = metadata_pda(mint)

        result = solana_rpc(
            "getAccountInfo",
            [
                pda,
                {
                    "encoding": "base64",
                    "commitment": "confirmed",
                },
            ],
        )

        value = (result or {}).get("value")

        if not value:
            return None, None, []

        data = value.get("data")

        if (
            not isinstance(data, list)
            or len(data) < 1
        ):
            return None, None, []

        import base64
        raw = base64.b64decode(data[0])

        # Metaplex Metadata account layout:
        # key(1) + update_authority(32) + mint(32) +
        # name(4+32) + symbol(4+10) + uri(4+200) +
        # seller_fee(2) + creators option...
        #
        # isMutable sits after the creators/collection/uses fields,
        # so a full borsh decoder is preferable. We use solders/
        # borsh-construct when available; otherwise fail closed.
        try:
            from borsh_construct import (
                CStruct,
                U8,
                U16,
                U32,
                Bool,
                Bytes,
                Option,
                Vec,
            )

            # A compact parser is implemented below instead of depending
            # on a fragile fixed offset because creators are optional.
            offset = 0

            # key
            offset += 1

            update_authority = (
                base58_encode(raw[offset:offset + 32])
            )
            offset += 32

            # mint
            offset += 32

            def read_borsh_string(buf, pos):
                length = int.from_bytes(
                    buf[pos:pos + 4],
                    "little",
                )
                pos += 4
                value = buf[pos:pos + length]
                pos += length
                return value, pos

            _, offset = read_borsh_string(raw, offset)  # name
            _, offset = read_borsh_string(raw, offset)  # symbol
            _, offset = read_borsh_string(raw, offset)  # uri

            # seller fee basis points
            offset += 2

            # creators Option<Vec<Creator>>
            creators_option = raw[offset]
            offset += 1

            creator_addresses = []

            if creators_option == 1:
                creator_count = int.from_bytes(
                    raw[offset:offset + 4],
                    "little",
                )
                offset += 4

                # Creator = address(32) + verified(1) + share(1)
                for _ in range(creator_count):
                    creator_bytes = raw[offset:offset + 32]
                    offset += 32
                    creator_addresses.append(
                        base58_encode(creator_bytes)
                    )

                    # verified + share
                    offset += 2

            # collection Option<Collection>
            collection_option = raw[offset]
            offset += 1

            if collection_option == 1:
                # key/discriminator + collection pubkey
                offset += 1 + 32

            # uses Option<Uses>
            uses_option = raw[offset]
            offset += 1

            if uses_option == 1:
                # use_method(1) + remaining(8) + total(8)
                offset += 1 + 8 + 8

            # is_mutable
            is_mutable = bool(raw[offset])

            return update_authority, is_mutable, creator_addresses

        except Exception:
            return None, None, []

    except Exception:
        return None, None, []


def base58_encode(data: bytes) -> str:
    alphabet = (
        "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    )

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

    return (
        "1" * leading_zeroes
        + "".join(reversed(chars))
    )


def get_dev_holding_percent(
    mint: str,
    mint_info: dict[str, Any],
    update_authority: str | None,
    creator_addresses: list[str] | None,
) -> float | None:
    """
    Conservative developer/team holding estimate.

    We inspect the Metaplex creator addresses and the metadata update
    authority, then sum any token balance held directly by those wallets.

    Only tokens that pass the configured creator/update-authority
    holding threshold are allowed through this filter.
    """
    supply_raw = int(
        mint_info.get("supply_raw", 0)
    )

    if supply_raw <= 0:
        return None

    owners = []

    for owner in (creator_addresses or []):
        if owner and owner not in owners:
            owners.append(owner)

    if (
        update_authority
        and update_authority not in owners
    ):
        owners.append(update_authority)

    if not owners:
        return None

    total_raw = 0

    for owner in owners:
        try:
            total_raw += get_token_accounts_for_owner(
                owner,
                mint,
            )
        except Exception:
            # If any creator balance cannot be checked, fail closed.
            return None

    return (
        total_raw
        / supply_raw
        * 100.0
    )


def safety_check_token(
    mint: str,
) -> tuple[bool, dict[str, Any], str]:
    """
    Returns:
      (passes, details, reason)

    Fail closed:
    - mint authority must be revoked
    - metadata must be immutable
    - dev/update-authority holdings must be <=5%
    - unknown verification => reject
    """
    details = {}

    try:
        mint_info = get_mint_info(mint)
        details["mint_authority"] = mint_info.get(
            "mint_authority"
        )
        details["freeze_authority"] = mint_info.get(
            "freeze_authority"
        )
        details["supply"] = mint_info.get("supply")

        if REQUIRE_MINT_AUTHORITY_REVOKED:
            if mint_info.get("mint_authority") is not None:
                return (
                    False,
                    details,
                    "mintable (mint authority active)",
                )

        update_authority, is_mutable, creator_addresses = (
            read_metadata_update_authority_and_mutability(
                mint
            )
        )

        details["update_authority"] = update_authority
        details["metadata_mutable"] = is_mutable
        details["creator_addresses"] = creator_addresses

        if REQUIRE_METADATA_IMMUTABLE:
            if is_mutable is not False:
                return (
                    False,
                    details,
                    "metadata mutable or unverified",
                )

        dev_pct = get_dev_holding_percent(
            mint,
            mint_info,
            update_authority,
            creator_addresses,
        )

        details["dev_holding_percent"] = dev_pct

        if dev_pct is None:
            return (
                False,
                details,
                "dev holdings unverified",
            )

        if dev_pct > MAX_DEV_HOLDING_PERCENT:
            return (
                False,
                details,
                f"dev holds {dev_pct:.2f}% > "
                f"{MAX_DEV_HOLDING_PERCENT:.2f}%",
            )

        return True, details, "passed"

    except Exception as exc:
        return (
            False,
            details,
            f"safety check error: {exc}",
        )


# ============================================================
# DATABASE
# ============================================================

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


def previous_alert(
    conn: sqlite3.Connection,
    pool_address: str,
) -> tuple[str | None, int | None]:
    row = conn.execute(
        """
        SELECT last_alert_key, last_alert_ts
        FROM pools
        WHERE pool_address = ?
        """,
        (pool_address,),
    ).fetchone()

    if not row:
        return None, None

    return row[0], row[1]


def alert_allowed(
    conn: sqlite3.Connection,
    pool_address: str,
    alert_key: str,
) -> bool:
    old_key, old_ts = previous_alert(conn, pool_address)

    if not old_key or not old_ts:
        return True

    if old_key != alert_key:
        return True

    return (
        now_ts() - int(old_ts)
        >= ALERT_COOLDOWN_HOURS * 3600
    )


# ============================================================
# GECKOTERMINAL REQUESTS
# ============================================================

def get_json(
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:

    global _last_gt_request

    url = f"{BASE_URL}{path}"

    for attempt in range(MAX_RETRIES):

        # Hard request gate.
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
                retry_after = response.headers.get(
                    "Retry-After"
                )

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

    raise RuntimeError(
        "GeckoTerminal request failed."
    )


# ============================================================
# POOL DISCOVERY
# ============================================================

def pool_list(
    endpoint: str,
    params: dict[str, Any] | None = None,
    pages: int = 1,
) -> list[dict[str, Any]]:

    results = []

    for page in range(1, pages + 1):

        query = dict(params or {})
        query["page"] = page

        try:
            payload = get_json(
                endpoint,
                query,
            )

            results.extend(
                payload.get("data", []) or []
            )

        except Exception as exc:
            print(
                f"[WARN] Pool list {endpoint} "
                f"page {page}: {exc}"
            )
            break

    return results


def parse_pool(
    item: dict[str, Any],
    source_type: str,
) -> dict[str, Any]:

    attributes = (
        item.get("attributes") or {}
    )

    relationships = (
        item.get("relationships") or {}
    )

    pool_id = item.get("id", "")

    address = (
        attributes.get("address")
        or pool_id.split("_", 1)[-1]
    )

    base_data = (
        relationships.get("base_token", {})
        .get("data")
        or {}
    )

    quote_data = (
        relationships.get("quote_token", {})
        .get("data")
        or {}
    )

    dex_data = (
        relationships.get("dex", {})
        .get("data")
        or {}
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
    changes = (
        attributes.get("price_change_percentage")
        or {}
    )

    pool_name = (
        attributes.get("name")
        or ""
    )

    # Pool names are normally "TOKEN / SOL" or
    # "TOKEN / USDC". This gives us a symbol without
    # making another token-info request.
    symbol = pool_name.split(" / ")[0].strip()

    return {
        "pool_address": address,
        "token_address": token_address,
        "pool_name": pool_name,
        "symbol": symbol,
        "source_type": source_type,
        "dex": dex_data.get("id"),
        "quote_id": quote_id,
        "price": safe_float(
            attributes.get("base_token_price_usd")
        ),
        "liquidity": safe_float(
            attributes.get("reserve_in_usd")
        ),
        "volume_24h": safe_float(
            volume.get("h24")
        ),
        "tx_24h": tx_count(tx, "h24"),
        "pool_created_at": parse_time(
            attributes.get("pool_created_at")
        ),
        "change_24h": safe_float(
            changes.get("h24")
        ),
        "fdv": safe_float(
            attributes.get("fdv_usd")
        ),
    }


def tx_count(
    transactions: dict[str, Any],
    key: str,
) -> int:

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


def discover_candidates() -> list[dict[str, Any]]:

    print("[INFO] Finding NEW pools...")

    new_items = pool_list(
        f"/networks/{NETWORK}/new_pools",
        {"include": "base_token,quote_token,dex"},
        pages=2,
    )

    print("[INFO] Finding ESTABLISHED pools...")

    established_items = pool_list(
        f"/networks/{NETWORK}/pools",
        {
            "include": "base_token,quote_token,dex",
            "sort": "h24_volume_usd_desc",
        },
        pages=2,
    )

    print("[INFO] Finding TRENDING pools...")

    trending_items = pool_list(
        f"/networks/{NETWORK}/trending_pools",
        {"include": "base_token,quote_token,dex"},
        pages=1,
    )

    new_pools = [
        parse_pool(x, "NEW")
        for x in new_items
    ]

    established_pools = [
        parse_pool(x, "ESTABLISHED")
        for x in (
            established_items
            + trending_items
        )
    ]

    # Deduplicate by pool address.
    def dedupe(items):
        seen = set()
        result = []

        for item in items:
            address = item["pool_address"]

            if not address or address in seen:
                continue

            seen.add(address)
            result.append(item)

        return result

    new_pools = dedupe(new_pools)
    established_pools = dedupe(established_pools)

    # Prefer pools with actual liquidity/volume.
    new_pools.sort(
        key=lambda p: (
            p["liquidity"] or 0,
            p["volume_24h"] or 0,
        ),
        reverse=True,
    )

    established_pools.sort(
        key=lambda p: (
            p["volume_24h"] or 0,
            p["liquidity"] or 0,
        ),
        reverse=True,
    )

    selected = (
        new_pools[:NEW_CANDIDATES]
        + established_pools[:ESTABLISHED_CANDIDATES]
    )

    # Final deduplication in case a pool appears in both lists.
    selected = dedupe(selected)

    return selected[:TOTAL_CANDIDATES]


# ============================================================
# OHLCV / NUMBERS
# ============================================================

def get_15m_candles(
    pool_address: str,
) -> list[tuple[int, float]]:

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
        if not isinstance(row, (list, tuple)):
            continue

        if len(row) < 5:
            continue

        try:
            timestamp = int(float(row[0]))
        except Exception:
            continue

        close = safe_float(row[4])

        if close and close > 0:
            rows.append(
                (timestamp, close)
            )

    rows.sort(key=lambda x: x[0])

    return rows


def nearest_change(
    rows: list[tuple[int, float]],
    target_seconds: int,
    min_seconds: int,
    max_seconds: int,
) -> tuple[float, int] | None:

    if len(rows) < 2:
        return None

    latest_ts, latest_price = rows[-1]

    choices = []

    for timestamp, old_price in rows[:-1]:

        elapsed = latest_ts - timestamp

        if not (
            min_seconds
            <= elapsed
            <= max_seconds
        ):
            continue

        change = pct_change(
            latest_price,
            old_price,
        )

        if change is None:
            continue

        choices.append(
            (
                abs(
                    elapsed - target_seconds
                ),
                change,
                elapsed,
            )
        )

    if not choices:
        return None

    choices.sort(key=lambda x: x[0])

    _, change, elapsed = choices[0]

    return change, elapsed


def best_down(
    rows: list[tuple[int, float]],
    min_seconds: int,
    max_seconds: int,
) -> tuple[float, int] | None:

    if len(rows) < 2:
        return None

    latest_ts, latest_price = rows[-1]

    best = None

    for timestamp, old_price in rows[:-1]:

        elapsed = latest_ts - timestamp

        if not (
            min_seconds
            <= elapsed
            <= max_seconds
        ):
            continue

        change = pct_change(
            latest_price,
            old_price,
        )

        if change is None:
            continue

        if best is None or change < best[0]:
            best = (change, elapsed)

    return best


def best_up(
    rows: list[tuple[int, float]],
    min_seconds: int,
    max_seconds: int,
) -> tuple[float, int] | None:

    if len(rows) < 2:
        return None

    latest_ts, latest_price = rows[-1]

    best = None

    for timestamp, old_price in rows[:-1]:

        elapsed = latest_ts - timestamp

        if not (
            min_seconds
            <= elapsed
            <= max_seconds
        ):
            continue

        change = pct_change(
            latest_price,
            old_price,
        )

        if change is None:
            continue

        if best is None or change > best[0]:
            best = (change, elapsed)

    return best


def observed_24h_high(
    rows: list[tuple[int, float]],
) -> tuple[float | None, int | None]:

    if not rows:
        return None, None

    latest_ts = rows[-1][0]

    recent = [
        x for x in rows
        if latest_ts - x[0] <= 86400
    ]

    if not recent:
        recent = rows

    high = max(
        price for _, price in recent
    )

    high_ts = max(
        ts for ts, price in recent
        if price == high
    )

    return high, high_ts


# ============================================================
# ALERT LOGIC
# ============================================================

def build_alert_key(
    change_15m: float | None,
    change_1h: float | None,
    down_24h: float | None,
    drawdown_24h: float | None,
    up_1h: float | None,
) -> str | None:
    """
    ALERT RULES:

    DIP:
      - Only -30% or worse is an alert.
      - Smaller drops (-5%, -15%, -20%, -29.99%) are ignored.

    PUMP:
      - Only strictly above +100% is an alert.
      - +100.00% exactly is not a pump alert.
    """

    levels = []

    # ---------------- DIP ALERTS ----------------
    if (
        change_15m is not None
        and change_15m <= -MIN_DIP_ALERT
    ):
        levels.append("DIP_15M_30")

    if (
        change_1h is not None
        and change_1h <= -MIN_DIP_ALERT
    ):
        levels.append("DIP_1H_30")

    if (
        down_24h is not None
        and down_24h <= -MIN_DIP_ALERT
    ):
        levels.append("DIP_24H_30")

    if (
        drawdown_24h is not None
        and drawdown_24h >= ULTRA_DIP_24H
    ):
        levels.append("DIP_99")
    elif (
        drawdown_24h is not None
        and drawdown_24h >= EXTREME_DIP_24H
    ):
        levels.append("DIP_95")
    elif (
        drawdown_24h is not None
        and drawdown_24h >= DEEP_DIP_24H
    ):
        levels.append("DIP_80")
    elif (
        drawdown_24h is not None
        and drawdown_24h >= MIN_DIP_ALERT
    ):
        levels.append("DIP_30")

    # ---------------- PUMP ALERT ----------------
    # Strictly greater than +100%.
    if (
        up_1h is not None
        and up_1h > PUMP_1H
    ):
        levels.append("PUMP100+")

    return "+".join(levels) if levels else None


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message: str) -> bool:

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(
            "\n[TELEGRAM NOT CONFIGURED]\n"
            + message
        )
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
        print(
            f"[WARN] Telegram request failed: {exc}"
        )
        return False


# ============================================================
# DATABASE WRITE
# ============================================================

def save_scan(
    conn: sqlite3.Connection,
    p: dict[str, Any],
    change_15m: float | None,
    change_1h: float | None,
    drawdown_24h: float | None,
    result: str,
) -> None:

    created = p.get("pool_created_at")

    age_hours = None

    if created:
        age_hours = max(
            0,
            (now_ts() - created) / 3600,
        )

    conn.execute(
        """
        INSERT INTO scans(
            ts,
            pool_address,
            token_address,
            symbol,
            source_type,
            price,
            change_15m,
            change_1h,
            change_24h_high_drawdown,
            liquidity,
            volume_24h,
            tx_24h,
            pool_age_hours,
            result
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
    conn: sqlite3.Connection,
    p: dict[str, Any],
    change_15m: float | None,
    change_1h: float | None,
    high_24h: float | None,
    drawdown_24h: float | None,
    alert_key: str | None,
    alert_sent: bool,
) -> None:

    old_key, old_ts = previous_alert(
        conn,
        p["pool_address"],
    )

    last_key = (
        alert_key if alert_sent else old_key
    )

    last_ts = (
        now_ts() if alert_sent else old_ts
    )

    conn.execute(
        """
        INSERT INTO pools(
            pool_address,
            token_address,
            symbol,
            pool_name,
            source_type,
            first_seen,
            last_seen,
            last_price,
            last_15m,
            last_1h,
            last_24h_high,
            last_24h_drawdown,
            last_alert_key,
            last_alert_ts
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


# ============================================================
# TELEGRAM MESSAGE
# ============================================================

def alert_message(
    p: dict[str, Any],
    change_15m: float | None,
    change_1h: float | None,
    down_24h: tuple[float, int] | None,
    drawdown_24h: float | None,
    high_24h: float | None,
    up_1h: tuple[float, int] | None,
    alert_key: str,
) -> str:

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
        lines.append(
            f"15m: {change_15m:+.2f}%"
        )

    if change_1h is not None:
        lines.append(
            f"1h: {change_1h:+.2f}%"
        )

    if down_24h:
        lines.append(
            f"Best 1h-24h drop: "
            f"{down_24h[0]:+.2f}% "
            f"in {format_duration(down_24h[1])}"
        )

    if high_24h:
        lines.append(
            f"24h high: {fmt_usd(high_24h)}"
        )

    if drawdown_24h is not None:
        lines.append(
            f"Below 24h high: "
            f"{drawdown_24h:.2f}%"
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


# ============================================================
# MAIN SCAN
# ============================================================

def scan() -> None:

    conn = init_db()

    started = datetime.now(
        timezone.utc
    ).astimezone().isoformat(
        timespec="seconds"
    )

    print("")
    print("=" * 64)
    print("SOLANA DIP RADAR v2")
    print(f"Started: {started}")
    print("NEW TOKENS: DISCOVERED, BUT < 7 DAYS EXCLUDED")
    print("ESTABLISHED TOKENS: ON")
    print("DEV HOLDING FILTER: API DATA REQUIRED")
    print(f"TOKEN AGE FILTER: >= {MIN_POOL_AGE_DAYS:.0f} DAYS")
    print("MINIMUM DIP ALERT: -30%")
    print("PUMP ALERT: > +100% ONLY")
    print("DIPS: -30% OR WORSE ONLY")
    print("=" * 64)

    candidates = discover_candidates()

    print(
        f"\nCandidates selected: "
        f"{len(candidates)}"
    )

    new_count = sum(
        1 for p in candidates
        if p["source_type"] == "NEW"
    )

    established_count = len(candidates) - new_count

    print(
        f"NEW: {new_count} | "
        f"ESTABLISHED: {established_count}"
    )

    print(
        f"API spacing: {REQUEST_INTERVAL:.1f}s "
        f"(deliberately slow to reduce 429 errors)"
    )
    print("")

    for index, p in enumerate(
        candidates,
        start=1,
    ):

        name = (
            p["symbol"]
            or p["pool_name"]
            or "UNKNOWN"
        )

        try:
            # AGE FILTER:
            # Only scan pools/tokens that are at least 7 days old.
            # There is deliberately no maximum age.
            created_at = p.get("pool_created_at")

            if created_at:
                age_days = (
                    now_ts() - created_at
                ) / 86400.0

                if age_days < MIN_POOL_AGE_DAYS:
                    print(
                        f"[{index:02d}] {name} "
                        f"| {p['source_type']} "
                        f"| too new "
                        f"({age_days:.1f}d < "
                        f"{MIN_POOL_AGE_DAYS:.0f}d)"
                    )
                    continue
            else:
                # If the API cannot tell us the pool age,
                # don't assume it is old enough.
                print(
                    f"[{index:02d}] {name} "
                    f"| {p['source_type']} "
                    f"| age unknown"
                )
                continue

            # Basic market-quality filter.
            if p["price"] is None:
                print(
                    f"[{index:02d}] {name} "
                    f"| {p['source_type']} "
                    f"| NO PRICE"
                )
                continue

            if (
                p["liquidity"] or 0
            ) < MIN_LIQUIDITY:
                print(
                    f"[{index:02d}] {name} "
                    f"| {p['source_type']} "
                    f"| low liquidity "
                    f"{fmt_usd(p['liquidity'])}"
                )
                continue

            if (
                p["volume_24h"] or 0
            ) < MIN_VOLUME_24H:
                print(
                    f"[{index:02d}] {name} "
                    f"| {p['source_type']} "
                    f"| low 24h volume "
                    f"{fmt_usd(p['volume_24h'])}"
                )
                continue

            if p["tx_24h"] < MIN_TX_24H:
                print(
                    f"[{index:02d}] {name} "
                    f"| {p['source_type']} "
                    f"| low transactions "
                    f"{p['tx_24h']}"
                )
                continue

            # -----------------------------------------------------
            # HARD SAFETY FILTERS
            # -----------------------------------------------------
            # Do this before expensive OHLCV work. If the token is
            # mintable, metadata-mutable, or dev holdings exceed 5%,
            # it is not shown and cannot alert.
            safety_ok, safety_details, safety_reason = (
                safety_check_token(
                    p["token_address"]
                )
            )

            if not safety_ok:
                print(
                    f"[{index:02d}] FILTERED {name} "
                    f"| {safety_reason}"
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

            rows = get_15m_candles(
                p["pool_address"]
            )

            if len(rows) < 5:
                print(
                    f"[{index:02d}] {name} "
                    f"| {p['source_type']} "
                    f"| not enough candles"
                )
                continue

            latest_price = rows[-1][1]

            # Use the actual latest OHLCV close when available.
            p["price"] = latest_price

            change_15m_data = nearest_change(
                rows,
                15 * 60,
                10 * 60,
                25 * 60,
            )

            change_1h_data = nearest_change(
                rows,
                60 * 60,
                45 * 60,
                90 * 60,
            )

            down_24h = best_down(
                rows,
                60 * 60,
                24 * 60 * 60,
            )

            up_1h = best_up(
                rows,
                60 * 60,
                24 * 60 * 60,
            )

            change_15m = (
                change_15m_data[0]
                if change_15m_data
                else None
            )

            change_1h = (
                change_1h_data[0]
                if change_1h_data
                else None
            )

            high_24h, _ = observed_24h_high(
                rows
            )

            drawdown_24h = None

            if high_24h and high_24h > 0:
                drawdown_24h = max(
                    0.0,
                    (
                        1.0
                        - latest_price / high_24h
                    ) * 100.0,
                )

            up_value = (
                up_1h[0]
                if up_1h
                else None
            )

            alert_key = build_alert_key(
                change_15m,
                change_1h,
                (
                    down_24h[0]
                    if down_24h
                    else None
                ),
                drawdown_24h,
                up_value,
            )

            # IMPORTANT:
            # Do NOT print ordinary movements such as -5%, -15%,
            # -20% or -29.99%. The user only wants >=30% dips.
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

            # Only qualifying >=30% downward signals reach this point.
            print(
                f"[{index:02d}] 🚨 DIP FOUND | "
                f"{name} / {p['source_type']} | "
                f"Price {fmt_usd(latest_price)} | "
                f"15m "
                f"{change_15m:+.2f}%"
                if change_15m is not None
                else
                f"[{index:02d}] 🚨 DIP FOUND | "
                f"{name} / {p['source_type']} | "
                f"Price {fmt_usd(latest_price)} | "
                f"15m n/a",
                end="",
            )

            if change_1h is not None:
                print(
                    f" | 1h {change_1h:+.2f}%",
                    end="",
                )
            else:
                print(" | 1h n/a", end="")

            print(
                f" | 24hHigh↓ "
                f"{drawdown_24h:.2f}%"
                if drawdown_24h is not None
                else " | 24hHigh↓ n/a"
            )

            if not alert_key:
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

            if not alert_allowed(
                conn,
                p["pool_address"],
                alert_key,
            ):
                print(
                    f"       ↳ {alert_key} "
                    f"already alerted recently"
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
                    f"       🚨 ALERT SENT: "
                    f"{alert_key}"
                )
            else:
                print(
                    f"       ⚠️ Signal found: "
                    f"{alert_key}"
                )

        except Exception as exc:
            # One bad token must never kill the whole scan.
            print(
                f"[{index:02d}] ERROR {name}: "
                f"{type(exc).__name__}: {exc}"
            )

    conn.close()

    print("")
    print("=" * 64)
    print("SCAN COMPLETE")
    print("=" * 64)


if __name__ == "__main__":
    scan()
