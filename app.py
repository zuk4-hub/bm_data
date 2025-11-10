# app.py — Bet Masterson (Bot + FastAPI + Enforcer + Reminders + SLS + Branding + Aforismos + Scheduler + Cakto Invite)
import os, json, time, asyncio, hmac, hashlib, random
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set, DefaultDict
from collections import defaultdict
from urllib.parse import urlencode

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
import uvicorn

from aiogram.types import ChatJoinRequest
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiohttp import ClientSession
from dateutil import tz
import html
from dateutil import parser, tz
from datetime import datetime
from datetime import datetime, date, timezone, timedelta


import math




# -------------------- ENV --------------------
BOT_TOKEN        = os.getenv("BOT_TOKEN", "").strip()
GROUP_ID         = int(os.getenv("GROUP_ID", "0"))
AUTHORIZED       = {int(x) for x in os.getenv("AUTHORIZED_USERS","").replace(" ","").split(",") if x}
ODDS_FILE        = os.getenv("ODDS_FILE", "/data/odds_hoje.json").strip()
GITHUB_RAW_BASE  = os.getenv("GITHUB_RAW_BASE", "https://raw.githubusercontent.com/zuk4-hub/bm_data/main/").strip()
ODDS_AMANHA_FILE  = os.environ.get("ODDS_AMANHA_FILE", "/data/odds_amanha.json")
AFORISMOS_FILE    = os.environ.get("AFORISMOS_FILE", "/data/aforismos.json")
PUBLISHED_DB_FILE = os.environ.get("PUBLISHED_DB_FILE", "/data/published.json")
PUBLISHED_LOG     = os.getenv("PUBLISHED_LOG", "/data/published_log.json").strip()

ODDS_URL = os.getenv("ODDS_URL", "").strip() #DEIXAR -> Motivo: a função load_odds_generic() verifica if ODDS_URL: — se a variável não existe no código, dá NameError quando o scheduler chama.
# --- CONFIGURAÇÃO DE FONTES (cole na parte de config do app.py) ---
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/zuk4-hub/bm_data/main"
ODDS_HOJE_URL   = f"{GITHUB_RAW_BASE}/odds_hoje.json"
ODDS_AMANHA_URL = f"{GITHUB_RAW_BASE}/odds_amanha.json"
AGENDA_URL      = f"{GITHUB_RAW_BASE}/agenda_editorial.json"
# ---------------------------------------------------------------


MIN_LEAD_MIN = int(os.environ.get("MIN_LEAD_MIN", "25"))
RESERVE_CUTOFF_HOUR        = int(os.environ.get("RESERVE_CUTOFF_HOUR", "15"))
RESERVE_SLS_THRESHOLD      = float(os.environ.get("RESERVE_SLS_THRESHOLD", "75"))
RESERVE_EXPIRY_RELEASE_MIN = int(os.environ.get("RESERVE_EXPIRY_RELEASE_MIN", "120"))
COMBOS_TYPES_ORDER = os.environ.get("COMBOS_TYPES_ORDER", "duplo,triplo,multi")


MIN_PROB         = float(os.getenv("MIN_PROB", "0.0"))   # ex.: 0.70 para 70%
MIN_EV           = float(os.getenv("MIN_EV", "0.0"))     # ex.: 5.0 para 5%
CAKTO_SECRET     = os.getenv("CAKTO_SECRET", "").strip()          # token na query (?token=)
CAKTO_SECRET_KEY = os.getenv("CAKTO_SECRET_KEY", "").strip()      # opcional: HMAC header X-Cakto-Signature
CHECKOUT_URL     = os.getenv("CHECKOUT_URL", "").strip()
REF_PARAM        = os.getenv("REF_PARAM", "ref").strip() or "ref"
TZ_NAME          = os.getenv("TZ", "America/Sao_Paulo")
PORT             = int(os.getenv("PORT", "8000"))  # Render injeta $PORT
AFORISMOS_FILE   = os.getenv("AFORISMOS_FILE", "/data/aforismos.json").strip()
INVITES_PATH     = os.getenv("INVITES_PATH", "/data/invites_map.json").strip()
GITHUB_TOKEN     = os.getenv("GITHUB_TOKEN", "").strip()
FETCH_MIN_INTERVAL = int(os.getenv("FETCH_MIN_INTERVAL", "120"))



# SLS pesos (0..1). Resultado final em 0..100.
SLS_WP           = float(os.getenv("SLS_WP", "0.6"))  # peso probabilidade real
SLS_WE           = float(os.getenv("SLS_WE", "0.4"))  # peso EV (%)

REMINDER_INTERVAL_SEC = int(os.getenv("REMINDER_INTERVAL_SEC", "1800"))  # 30 min

# Scheduler/editorial
MODE                       = os.getenv("MODE", "auto").strip().lower()      # 'editorial' | 'auto'
AGENDA_JSON_PATH           = os.getenv("AGENDA_JSON_PATH", "/data/agenda_editorial.json").strip()
AGENDA_URL                 = os.getenv("AGENDA_URL", "").strip()  # opcional: para apontar uma URL específica se quiser

AUTO_SCHEDULING_DEFAULT    = os.getenv("AUTO_SCHEDULING_DEFAULT", "true").lower() == "true"
ENABLE_FALLBACK_SELECTION  = os.getenv("ENABLE_FALLBACK_SELECTION", "true").lower() == "true"
MAX_PUBLICATIONS_PER_DAY   = int(os.getenv("MAX_PUBLICATIONS_PER_DAY", "100"))
HOURLY_MAX = int(os.getenv("MAX_PUBLICATIONS_PER_HOUR", "3"))  # no máx 3/h
MINUTES_BETWEEN_REPOST     = int(os.getenv("MINUTES_BETWEEN_REPOST", "240"))

if not BOT_TOKEN or not GROUP_ID:
    raise RuntimeError("Defina BOT_TOKEN e GROUP_ID (-100...) no Environment.")

# -------------------- BOT CORE --------------------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher()

# -------------------- STORAGE (/data) --------------------
DATA_DIR  = Path("/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
SUBS_PATH = DATA_DIR / "subs.json"
LOG_PATH  = DATA_DIR / "cakto_events.json"
AF_USED   = DATA_DIR / "aforismos_used.json"
PUBLISHED_LOG = DATA_DIR / "published_log.json"
INVITES_MAP = Path(INVITES_PATH)  # { invite_link: {"allowed_uid": int, "expire": ts, "created_at": ts} }

def _now() -> int:
     return int(time.time())

def subs_set(data: Dict[str, Any]) -> None:
     _safe_save(SUBS_PATH, data)

# ---- invites map helpers ----
def invites_get() -> Dict[str, Any]:
    return _safe_load(INVITES_MAP, {})

def invites_set(data: Dict[str, Any]) -> None:
    _safe_save(INVITES_MAP, data)

# --------------------------

def _safe_load(path: Path, default):
    try:
        if not path.exists():
            return default
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _safe_save(path: Path, data) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

# === PATCH: loader blindado + normalizador de estrutura ===
def _safe_load_json_any(path: Path | str, fallback: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback

def _normalize_pick_from_generic(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        mand = item.get("mandante") or item.get("home") or (item.get("match") or {}).get("home") or ""
        vist = item.get("visitante") or item.get("away") or (item.get("match") or {}).get("away") or ""
        mercado = item.get("mercado") or item.get("market") or ""
        selecao = item.get("selecao") or item.get("selection") or item.get("pick") or ""
        odd = float(item.get("odd_mercado") or item.get("odd") or item.get("market_odds") or 0)
        fair = float(item.get("fair_odd") or item.get("fair") or 0)
        prob = float(item.get("prob_real") or item.get("prob") or item.get("probability") or 0)
        ev = float(item.get("ev") or item.get("ev_percent") or item.get("EV_percent") or 0)
        hora = (
            item.get("hora") or item.get("hora_local") or item.get("date_local")
            or item.get("hora_utc") or item.get("kickoff") or item.get("date_GMT")
            or (item.get("match") or {}).get("kickoff") or ""
        )
        if not mand or not vist or odd <= 0 or prob <= 0:
            return None
        return {
            "mandante": mand, "visitante": vist,
            "mercado": mercado, "selecao": selecao,
            "odd_mercado": odd, "fair_odd": fair,
            "prob_real": prob, "ev": ev,
            "hora": hora,
        }
    except Exception:
        return None

def _normalize_combo_from_generic(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        legs = item.get("legs") or item.get("pernas") or item.get("selections") or []
        if not isinstance(legs, list) or not legs:
            return None
        pr = float(item.get("prob_real_combo") or item.get("prob") or item.get("probability") or 0)
        fair = float(item.get("fair_combo") or item.get("fair") or 0)
        oddc = float(item.get("odd_combo") or item.get("market_odds") or item.get("odd") or 0)
        evc = float(item.get("ev_combo") or item.get("ev") or item.get("ev_percent") or 0)
        title = item.get("titulo") or item.get("title") or item.get("name") or "Múltipla"
        if pr <= 0 or oddc <= 0:
            return None
        return {
            "legs": legs,
            "prob_real_combo": pr,
            "fair_combo": fair,
            "odd_combo": oddc,
            "ev_combo": evc,
            "titulo": title,
        }
    except Exception:
        return None

def _normalize_data_to_internal(data: Any) -> Dict[str, Any]:
    picks, combos = [], []
    if isinstance(data, dict):
        if isinstance(data.get("picks"), list) or isinstance(data.get("combos"), list):
            for p in (data.get("picks") or []):
                np = _normalize_pick_from_generic(p) or p
                if isinstance(np, dict): picks.append(np)
            for c in (data.get("combos") or []):
                nc = _normalize_combo_from_generic(c) or c
                if isinstance(nc, dict): combos.append(nc)
            return {"picks": picks, "combos": combos}
        bets = data.get("bets") or data.get("apostas") or None
        if isinstance(bets, list):
            for b in bets:
                typ = (b.get("type") or b.get("tipo") or "").lower()
                if typ in ("single","singles","aposta_simples"):
                    np = _normalize_pick_from_generic(b)
                    if np: picks.append(np)
                elif typ in ("combo","duplo","dupla","triplo","multipla","múltipla","combination"):
                    nc = _normalize_combo_from_generic(b)
                    if nc: combos.append(nc)
            return {"picks": picks, "combos": combos}
        for _, v in data.items():
            if isinstance(v, list):
                for it in v:
                    if not isinstance(it, dict): continue
                    if "legs" in it or "pernas" in it or "selections" in it:
                        nc = _normalize_combo_from_generic(it)
                        if nc: combos.append(nc)
                    else:
                        np = _normalize_pick_from_generic(it)
                        if np: picks.append(np)
        return {"picks": picks, "combos": combos}
    if isinstance(data, list):
        for it in data:
            if not isinstance(it, dict): continue
            if "legs" in it or "pernas" in it or "selections" in it:
                nc = _normalize_combo_from_generic(it)
                if nc: combos.append(nc)
            else:
                np = _normalize_pick_from_generic(it)
                if np: picks.append(np)
        return {"picks": picks, "combos": combos}
    return {"picks": [], "combos": []}
# === END PATCH ===

def subs_get() -> Dict[str, Any]:
    return _safe_load(SUBS_PATH, {})

def subs_set(data: Dict[str, Any]) -> None:
    _safe_save(SUBS_PATH, data)

# === PATCH: helpers de envio seguro/escape ===
def _escape_html(s: str) -> str:
    return html.escape(str(s), quote=False)

async def _send_long(m, text: str, parse_mode: str = "HTML", chunk_size: int = 3800):
    buf = []
    total = 0
    for ln in text.split("\n"):
        if total + len(ln) + 1 > chunk_size:
            await m.answer("\n".join(buf), parse_mode=parse_mode)
            buf, total = [ln], len(ln) + 1
        else:
            buf.append(ln)
            total += len(ln) + 1
    if buf:
        await m.answer("\n".join(buf), parse_mode=parse_mode)
# === END PATCH ===

def _fmt_hhmm_to_hour(hhmm: str) -> int:
    # "03h15" -> 3
    try:
        return int(hhmm.split('h', 1)[0])
    except Exception:
        return -1

def _is_corujao_hhmm(hhmm: str) -> bool:
    # Corujão: 00:00–06:59 (UTC-3)
    h = _fmt_hhmm_to_hour(hhmm)
    return 0 <= h < 7

def extract_sections_from_odds(obj: dict, *, detach_corujao_from_main: bool = True):
    """
    Se o arquivo tem 'corujao': {'picks': [...], 'combos': [...]}, usa isso.
    Caso não tenha, deriva Corujão pela janela 00:00–06:59 local.

    detach_corujao_from_main = True:
       remove os picks/combos de Corujão do feed geral (evita duplicar na timeline).
    """
    picks_all = list(obj.get("picks", []))
    combos_all = list(obj.get("combos", []))

    coru = obj.get("corujao") or {}
    coru_p = list(coru.get("picks", []) or [])
    coru_c = list(coru.get("combos", []) or [])

    if not coru_p and not coru_c:
        # Deriva a partir do horário (retrocompatibilidade)
        coru_p = [p for p in picks_all if _is_corujao_hhmm(str(p.get("hora", "")))]
        # Combos: Corujão se TODAS as pernas estiverem no intervalo
        def _combo_is_corujao(c):
            legs = c.get("pernas", [])
            if not legs:
                # alguns intra-game trazem contexto no 'contexto' (uma vez só)
                # nesses casos, olhe a hora do combo se existir
                hh = str(c.get("hora", ""))
                return _is_corujao_hhmm(hh) if hh else False
            return all(_is_corujao_hhmm(str(l.get("hora", ""))) for l in legs)
        coru_c = [c for c in combos_all if _combo_is_corujao(c)]

    if detach_corujao_from_main:
        # Remove do feed geral o que está no corujão
        def _mk_key_pick(p):
            return (p.get("pais"), p.get("campeonato"), p.get("data"),
                    p.get("hora"), p.get("mandante"), p.get("visitante"),
                    p.get("mercado"), p.get("selecao"))
        coru_keys = {_mk_key_pick(p) for p in coru_p}
        picks_all = [p for p in picks_all if _mk_key_pick(p) not in coru_keys]

        def _mk_key_combo(c):
            legs = c.get("pernas") or []
            return tuple(
                (l.get("pais"), l.get("campeonato"), l.get("data"), l.get("hora"),
                 l.get("mandante"), l.get("visitante"), l.get("mercado"), l.get("selecao"))
                for l in legs
            )
        coru_ckeys = {_mk_key_combo(c) for c in coru_c}
        combos_all = [c for c in combos_all if _mk_key_combo(c) not in coru_ckeys]

    return {
        "picks_all": picks_all,
        "combos_all": combos_all,
        "corujao": {
            "picks": coru_p,
            "combos": coru_c
        }
    }


# ==== Assinaturas ====
def upsert_sub(user_id: str, status: str, expires_at: int = 0, plan: str = "") -> None:
    data = subs_get()
    prev = data.get(user_id, {})
    same_exp = (int(prev.get("expires_at") or 0) == int(expires_at or 0))
    notified_3d = prev.get("notified_3d") if same_exp else False
    notified_0d = prev.get("notified_0d") if same_exp else False

    data[user_id] = {
        "telegram_id": user_id,
        "status": status,
        "expires_at": int(expires_at or 0),
        "plan": plan,
        "updated_at": _now(),
        "notified_3d": bool(notified_3d),
        "notified_0d": bool(notified_0d),
    }
    subs_set(data)

def sub_is_active(user_id: int) -> bool:
    h = subs_get().get(str(user_id), {})
    if not h or (h.get("status","").lower() != "active"):
        return False
    exp = int(h.get("expires_at") or 0)
    return (exp == 0) or (_now() < exp)

def is_admin(uid: int) -> bool:
    return uid in AUTHORIZED if AUTHORIZED else False

# -------------------- Data/hora + Branding --------------------
def _tz_offset_text(dtl: datetime) -> str:
    off = dtl.utcoffset() or timedelta(0)
    total_min = int(off.total_seconds() // 60)
    sign = "-" if total_min < 0 else "+"
    hrs = abs(total_min) // 60
    return f"UTC {sign}{hrs}"

def as_local(s: str) -> str:
    dtl = _parse_any_dt_local(s)
    if not dtl:
        return s or "—"
    return dtl.strftime("%Y-%m-%d %H:%M %Z")


def _parse_any_dt_local(s: str):
    """
    Aceita:
      - ISO 'Z' → 2025-11-07T16:00:00Z
      - ISO com offset → 2025-11-07T13:00:00-03:00
      - "YYYY-MM-DD HH:MM"
    Retorna aware em TZ_NAME.
    """
    if not s:
        return None
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        else:
            try:
                dt = datetime.fromisoformat(s)
            except Exception:
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
        tz_sp = tz.gettz(TZ_NAME)
        # Se dt veio naïve, assume que já é local e marca TZ
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz_sp)
        return dt.astimezone(tz_sp)
    except Exception:
        return None


def _pick_time_str(p: Dict[str, Any]) -> str:
    """
    Retorna UMA string de horário em ISO 'aware' (com timezone) no fuso TZ_NAME.
    Suporta:
      - já vir como ISO (com Z ou offset) em qualquer campo conhecido;
      - par (data='dd-mm-yyyy', hora='HHhMM' ou 'HH:MM') vindo do odds_hoje/amanha;
      - alguns alias ('kickoff','date_GMT','date_local').
    """
    tz_sp = tz.gettz(TZ_NAME)

    # 1) Se já veio em campos ISO (Z ou com offset), devolvemos como está.
    for k in ("hora_utc", "hora_iso", "kickoff", "date_GMT", "date_local", "hora"):
        v = (p.get(k) or "").strip()
        if not v:
            continue
        # ISO com Z ou offset?
        try:
            if v.endswith("Z"):
                dt = datetime.fromisoformat(v.replace("Z","+00:00"))
                return dt.astimezone(tz_sp).isoformat()
            else:
                # tenta ISO com offset
                dt = datetime.fromisoformat(v)
                return dt.astimezone(tz_sp).isoformat()
        except Exception:
            pass

    # 2) Se houver 'data' (dd-mm-yyyy) + 'hora' ("11h30" ou "11:30"), monta local
    d = (p.get("data") or "").strip()
    h = (p.get("hora") or "").strip()
    if d and h:
        # normaliza "11h30" -> "11:30"
        if "h" in h and ":" not in h:
            h = h.replace("h", ":")
        # aceita "11:30" ou "11:30:00"
        try:
            # dd-mm-yyyy
            dd, mm, yy = d.split("-")
            Y, M, D = int(yy), int(mm), int(dd)
            hh, mm_ = h.split(":")[:2]
            HH, MM = int(hh), int(mm_)
            dt_local = datetime(Y, M, D, HH, MM, 0, tzinfo=tz_sp)
            return dt_local.isoformat()  # com -03:00
        except Exception:
            pass

    # 3) Por fim, se aparecer algo como "Nov 07 2025 - 3:00am"
    for k in ("hora_utc","kickoff","date_GMT"):
        v = (p.get(k) or "").strip()
        if not v:
            continue
        # tentativa simples desse formato
        # ex.: "Nov 07 2025 - 3:00am"
        try:
            v2 = v.replace("-", "").strip()
            dt = datetime.strptime(v2, "%b %d %Y %I:%M%p")  # naïve
            # vem em UTC → anexa UTC e converte para local
            dt = dt.replace(tzinfo=timezone.utc).astimezone(tz_sp)
            return dt.isoformat()
        except Exception:
            continue

    return ""  # não conseguiu inferir

def _game_id_from_pick(p: Dict[str, Any]) -> str:
    # Usa mandante, visitante e horário (normalizado) para ID estável de jogo
    key = f"{p.get('mandante','')}|{p.get('visitante','')}|{_pick_time_str(p)}"
    return hashlib.md5(key.encode('utf-8')).hexdigest()[:10]


def _dt_key_or_now(hora: str):
    dt = _parse_any_dt_local(hora)
    return dt or datetime.now(tz.gettz(TZ_NAME))

def _time_ok_lead(hora_str: str, now_local: datetime, min_lead_min: int) -> bool:
    dtl = _parse_any_dt_local(hora_str)
    if not dtl:
        return False
    lead = (dtl - now_local).total_seconds() / 60.0
    return lead >= float(min_lead_min)

def _local_date_of_dt(dt_obj: datetime):
    return dt_obj.astimezone(tz.gettz(TZ_NAME)).date()

BRAND_LINE = (
    "|<i>Mathematics, ethics and the beautiful game</i>|"
    "@betmasteron"
)

# ---------- PUBLICADOS (persistência) ----------

def _pub_today():
    return datetime.now(tz.gettz(TZ_NAME)).date()


def _ensure_dir_of(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _is_today_local_from_pick(p: Dict[str, Any]) -> bool:
    try:
        tz_sp = tz.gettz(TZ_NAME)
        today = datetime.now(tz_sp).date()
        dtl = _parse_any_dt_local(_pick_time_str(p))
        return bool(dtl and dtl.date() == today)
    except Exception:
        return False

def _kick_date_local_from_pick(p: Dict[str, Any]) -> Optional[datetime.date]:
    dtl = _parse_any_dt_local(_pick_time_str(p))
    return dtl.date() if dtl else None

def _kick_date_local_from_combo(c: Dict[str, Any]) -> Optional[datetime.date]:
    dt = _earliest_leg_kickoff(c)
    return dt.date() if dt else None

def _pick_signature(p: Dict[str, Any]) -> str:
    # assinatura estável do pick
    parts = [
        p.get("pais",""), p.get("campeonato",""),
        p.get("mandante",""), p.get("visitante",""),
        p.get("mercado",""),  p.get("selecao",""),
        f"{p.get('odd_mercado','')}", f"{p.get('fair_odd','')}",
        _pick_time_str(p) or ""
    ]
    raw = "|".join(map(str, parts))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]

def _combo_signature(c: Dict[str, Any]) -> str:
    # hash das legs + odds + horário base
    legs = c.get("legs", []) or []
    base = "|".join(legs) + f"|{c.get('odd_combo','')}|{c.get('fair_combo','')}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()[:16]

def _load_published() -> Dict[str, Any]:
    try:
        with open(PUBLISHED_DB_FILE, "r", encoding="utf-8") as f:
            db = json.load(f)
        if not isinstance(db, dict): return {}
        db.setdefault("picks", {})
        db.setdefault("combos", {})
        return db
    except Exception:
        return {"picks": {}, "combos": {}}

def _save_published(db: Dict[str, Any]) -> None:
    _ensure_dir_of(PUBLISHED_DB_FILE)
    tmp = PUBLISHED_DB_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PUBLISHED_DB_FILE)

def _purge_published(db: Dict[str, Any]) -> None:
    # mantém só hoje e amanhã (datas locais dos jogos)
    tz_sp = tz.gettz(TZ_NAME)
    today = datetime.now(tz_sp).date()
    tomorrow = today + timedelta(days=1)
    keep = {today.isoformat(), tomorrow.isoformat()}

    for k in ("picks", "combos"):
        by_date = db.get(k, {})
        drop = [d for d in by_date.keys() if d not in keep]
        for d in drop:
            by_date.pop(d, None)

def already_published_pick(p: Dict[str, Any]) -> bool:
    d = _kick_date_local_from_pick(p)
    if not d: return False
    sig = _pick_signature(p)
    db  = _load_published()
    return bool(db.get("picks", {}).get(d.isoformat(), {}).get(sig))

def already_published_combo(c: Dict[str, Any]) -> bool:
    d = _kick_date_local_from_combo(c)
    if not d: return False
    sig = _combo_signature(c)
    db  = _load_published()
    return bool(db.get("combos", {}).get(d.isoformat(), {}).get(sig))

def mark_published_pick(p: Dict[str, Any]) -> None:
    d = _kick_date_local_from_pick(p)
    if not d: return
    sig = _pick_signature(p)
    gid = _game_id_from_pick(p)
    db  = _load_published()
    db.setdefault("picks", {}).setdefault(d.isoformat(), {})[sig] = {
        "ts": datetime.utcnow().isoformat()+"Z",
        "gid": gid
    }
    _purge_published(db)
    _save_published(db)

@dp.message(Command("post_pick"))
async def post_pick(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    args = (m.text or "").split()
    if len(args) < 2:
        return await m.answer("Uso: /post_pick <GAME_ID>")

    target = args[1].strip()
    data = await load_odds_generic()
    # normaliza estrutura e campos antes de qualquer uso
    try:
        data = normalize_odds(data)
    except Exception:
        pass

    picks = data.get("picks", []) or []

    # escolhe o melhor SLS daquele jogo
    best = None
    best_sls = -1
    for p in picks:
        gid = _game_id_from_pick(p)
        if gid != target:
            continue
        pr = _f(p.get("prob_real", 0.0))
        ev = _f(p.get("ev", 0.0))
        sls = sls_score(pr, ev)
        if sls > best_sls:
            best, best_sls = p, sls

    if not best:
        return await m.answer("GAME_ID não encontrado nos picks de hoje/amanhã.")

    # checa lead mínimo
    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    if not _time_ok_lead(_pick_time_str(best), now_l, MIN_LEAD_MIN):
        return await m.answer("Kickoff muito próximo (menos que lead mínimo).")

    try:
        await bot.send_message(GROUP_ID, fmt_pick(best))
        mark_published_pick(best)
        return await m.answer("✅ Pick publicado.")
    except Exception as e:
        return await m.answer(f"Falhou ao publicar: {e}")

def _combo_hash(c: Dict[str, Any]) -> str:
    legs_s = "|".join(c.get("legs", []))
    return hashlib.md5(legs_s.encode("utf-8")).hexdigest()[:12]

@dp.message(Command("post_combo"))
async def post_combo(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    args = (m.text or "").split(maxsplit=1)
    key = args[1].strip() if len(args) >= 2 else None

    data = await load_odds_generic()
    # normaliza estrutura e campos antes de qualquer uso
    try:
        data = normalize_odds(data)
    except Exception:
        pass

    combos = data.get("combos", []) or []
    cand = None

    def _sls_c(c):
        return sls_score(_f(c.get("prob_real_combo",0.0)), _f(c.get("ev_combo",0.0)))

    if key:
        # tenta por hash
        for c in combos:
            if _combo_hash(c) == key:
                cand = c; break
        # tenta por índice
        if cand is None and key.isdigit():
            i = int(key) - 1
            if 0 <= i < len(combos):
                cand = combos[i]
        if not cand:
            return await m.answer("Combo não encontrado.")
    else:
        # sem argumento: pega o melhor SLS disponível (lead ok / não publicado)
        tz_sp = tz.gettz(TZ_NAME)
        now_l = datetime.now(tz_sp)
        elegiveis = []
        for c in combos:
            ek = _earliest_leg_kickoff(c)
            if not ek or (ek - now_l).total_seconds()/60.0 < MIN_LEAD_MIN:
                continue
            if already_published_combo(c):
                continue
            elegiveis.append(c)
        if not elegiveis:
            return await m.answer("Sem múltiplas elegíveis agora.")
        cand = sorted(elegiveis, key=_sls_c, reverse=True)[0]

    # lead ok (proteção final)
    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    ek = _earliest_leg_kickoff(cand)
    if not ek or (ek - now_l).total_seconds()/60.0 < MIN_LEAD_MIN:
        return await m.answer("Kickoff muito próximo (combo).")

    try:
        await bot.send_message(GROUP_ID, _fmt_combo_msg(cand))
        mark_published_combo(cand)
        return await m.answer("✅ Combo publicado.")
    except Exception as e:
        return await m.answer(f"Falhou ao publicar: {e}")


def mark_published_combo(c: Dict[str, Any]) -> None:
    d = _kick_date_local_from_combo(c)
    if not d: return
    sig = _combo_signature(c)
    db  = _load_published()
    db.setdefault("combos", {}).setdefault(d.isoformat(), {})[sig] = {
        "ts": datetime.utcnow().isoformat()+"Z",
        "legs": c.get("legs", [])
    }
    _purge_published(db)
    _save_published(db)
# ---------- FIM PUBLICADOS ----------

# === NUM PARSER ROBUSTO (aceita '@2.10', '2,10', '85%', '  1.50 ') ===
def _f(v, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        # remove marcas comuns
        if s.startswith("@"):
            s = s[1:]
        s = s.replace("%", "").replace(" ", "")
        # vírgula decimal -> ponto
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return float(default)

# -------------------- SLS + Badges --------------------
def sls_score(prob_real: float, ev: float) -> float:
    p = max(0.0, min(1.0, float(prob_real)))
    e = max(0.0, float(ev))
    e_norm = max(0.0, min(1.0, e / 30.0))
    score = (SLS_WP * p + SLS_WE * e_norm) * 100.0
    return round(score, 1)

def primary_badges(prob: float, ev: float) -> str:
    b = []
    if prob >= 0.80:
        b.append("🎯")
    if ev >= 5.0:
        b.append("⚡")
    return " ".join(b) + (" " if b else "")

def right_badge_sls(sls: float) -> str:
    return "  💎" if sls >= 90.0 else ""

# -------------------- Aforismos --------------------
def _hash_id(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()[:16]

def _af_fallback_lists() -> Dict[str, List[Dict[str, Any]]]:
    return {
        "cynical": [{"id": "c1", "html": "— <i>The house smiles when you chase rain with a sieve.</i>"}],
        "neutral": [{"id": "n1", "html": "— <i>Fair odds are the grammar of honesty.</i>"}],
        "hopeful": [{"id": "h1", "html": "— <i>When probability is patient, profit often remembers your name.</i>"}],
    }

def _bucket_tone_stable(text: str) -> str:
    h = int(hashlib.sha1(text.encode("utf-8","ignore")).hexdigest(), 16)
    r = h % 3
    return ["cynical","neutral","hopeful"][r]

def _load_aforismos_lists() -> Dict[str, List[Dict[str, Any]]]:
    """
    Aceita:
      A) {"entries":[{"html":..., "id":..., "tone":...}, ...]}
      B) {"cynical":[str|{html}], "neutral":[...], "hopeful":[...]}
    """
    raw = _safe_load(Path(AFORISMOS_FILE), None)
    out = {"cynical": [], "neutral": [], "hopeful": []}

    if isinstance(raw, dict) and "entries" in raw and isinstance(raw["entries"], list):
        for ent in raw["entries"]:
            if not isinstance(ent, dict): 
                continue
            html = str(ent.get("html","")).strip()
            if not html:
                continue
            _id = (str(ent.get("id")).strip() if ent.get("id") else _hash_id(html))
            tone = (str(ent.get("tone") or "").strip().lower()) or _bucket_tone_stable(html)
            tone = tone if tone in out else _bucket_tone_stable(html)
            out[tone].append({"id": _id, "html": html})

    elif isinstance(raw, dict):
        for k in ("cynical","neutral","hopeful"):
            vals = raw.get(k, [])
            if isinstance(vals, list):
                for v in vals:
                    html = str(v.get("html","")).strip() if isinstance(v, dict) else str(v).strip()
                    if not html:
                        continue
                    out[k].append({"id": _hash_id(html), "html": html})

    if not any(out.values()):
        out = _af_fallback_lists()
    return out

def _pick_aforismo_for_sls(sls: float) -> str:
    pools = _load_aforismos_lists()
    if sls >= 80:
        cat = "hopeful"
    elif sls >= 60:
        cat = "neutral"
    else:
        cat = "cynical"

    used = _safe_load(AF_USED, {})  # {id: ts}
    horizon = _now() - 7*24*3600

    candidates = [e for e in pools[cat] if int(used.get(e["id"],0)) < horizon] or pools[cat]
    ent = random.choice(candidates)
    used[ent["id"]] = _now()
    _safe_save(AF_USED, used)
    return ent["html"]

# -------------------- ODDS loader + filtros --------------------

def pass_filter(p: Dict[str, Any], min_prob: float, min_ev: float) -> bool:
    try:
        return _f(p.get("prob_real",0.0)) >= min_prob and _f(p.get("ev",0.0)) > min_ev
    except Exception:
        return False

# -------------------- Formatação do card --------------------
def fmt_pick(p: Dict[str, Any], *, add_debug_line: Optional[str] = None) -> str:
    # números
    prob = _f(p.get("prob_real", 0.0))
    ev   = _f(p.get("ev", 0.0))
    roi  = p.get("roi", None)

    # odd mercado (@) — aceita já com @ ou sem
    odd_raw = p.get("odd_mercado")
    odd_num = _f(odd_raw or 0.0)
    odd_str = f"@{odd_num:.2f}" if odd_num > 0 else (str(odd_raw) if odd_raw else "—")

    # odd justa (1/prob)
    odd_justa = (1.0/prob) if prob > 0 else 0.0
    odd_justa_str = f"@{odd_justa:.3f}" if odd_justa > 0 else "—"

    # SLS e badges
    sls  = sls_score(prob, ev)
    left = primary_badges(prob, ev)
    right = right_badge_sls(sls)

    # mercado/seleção PT-BR (com rótulos)
    mercado_pt = translate_market(p.get("mercado") or p.get("market") or "")
    selecao_pt = (p.get("selecao") or p.get("selection") or p.get("pick") or "")
    selecao_pt = (selecao_pt
        .replace("1st Half", "1º Tempo")
        .replace("2nd Half", "2º Tempo")
        .replace("Over", "Mais de")
        .replace("Under", "Menos de")
        .replace("Goals", "gols")
        .replace("BTTS Yes", "Ambos Marcam — Sim")
        .replace("BTTS No", "Ambos Marcam — Não")
    )

    # data/hora no padrão do prompt (DD-MM-YYYY / HHhMM)
    data_str, hora_str = format_date_hour_from_utc_str(
        p.get("hora_utc") or p.get("hora") or p.get("kickoff") or p.get("date_GMT") or _pick_time_str(p)
    )
    when_line = f"🕒 {data_str or '—'} {hora_str or '—'} ({TZ_NAME})"

    linhas = [
        BRAND_LINE,
        "",
        f"🏆 {p.get('campeonato','—')} · {p.get('pais','—')}",
        when_line,
        f"⚽ {p.get('mandante','?')} vs {p.get('visitante','?')}",
        "",
        f"{left}<b>Mercado:</b> {mercado_pt}{right}",
        f"<b>Seleção:</b> {selecao_pt}",
        "",
        f"• Prob. real: <b>{prob*100:.1f}%</b>",
        f"• Odd justa: <b>{odd_justa_str}</b>  |  Odd mercado: <b>{odd_str}</b>",
        "• EV: <b>{:.1f}%</b>{}".format(ev, (f"  |  ROI: <b>{_f(roi, 0.0):.1f}%</b>" if roi is not None else "")),
        "",
        f"Comentário: {p.get('nota','—')}",
        "",
        _pick_aforismo_for_sls(sls),
    ]

    if add_debug_line:
        linhas.append(f"\n<code>{add_debug_line}</code>")

    return "\n".join(linhas)

async def publish_picks(chat_id: int, picks: List[Dict[str, Any]], admin_dm: Optional[int] = None):
    if not picks:
        await bot.send_message(chat_id, "🔎 Nenhuma entrada encontrada com os filtros atuais.")
        return
    for p in picks:
        await bot.send_message(chat_id, fmt_pick(p))
        if admin_dm and is_admin(admin_dm):
            prob = _f(p.get("prob_real",0.0))
            ev   = _f(p.get("ev",0.0))
            sls  = sls_score(prob, ev)
            dbg = f"[DEBUG] prob={prob:.4f} ev%={ev:.2f} sls={sls:.2f}"
            try:
                await bot.send_message(admin_dm, fmt_pick(p, add_debug_line=dbg))
            except Exception:
                pass
        await asyncio.sleep(0.7)

# -------------------- CHECKOUT helpers --------------------
def build_checkout_url(ref: int | None = None) -> str:
    if not CHECKOUT_URL:
        return "https://app.cakto.com.br/"
    if ref:
        sep = "&" if "?" in CHECKOUT_URL else "?"
        return f"{CHECKOUT_URL}{sep}{urlencode({REF_PARAM: str(ref)})}"
    return CHECKOUT_URL

def set_trial_active(user_id: int, days: int = 30, plan_label: str = "trial"):
    seconds = max(1, int(days)) * 86400
    expires = _now() + seconds
    upsert_sub(str(user_id), status="active", expires_at=expires, plan=plan_label)
    return expires

# -------------------- BOT COMMANDS --------------------
async def _require_private(m: types.Message) -> bool:
    if m.chat.type != "private":
        await m.answer("Este comando só pode ser usado no privado.")
        return False
    if not is_admin(m.from_user.id):
        await m.answer("🚫 Acesso restrito.")
        return False
    return True

@dp.message(Command("start"))
async def start_cmd(m: types.Message):
    await m.answer("🤖 Bot Bet Masterson online. Use /help para ver comandos.")

@dp.message(Command("help"))
async def help_cmd(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("👋 Este bot publica cards no canal. (Comandos administrativos restritos.)")

    # se chamado no grupo, avisa e manda a ajuda no privado
    if m.chat.type != "private":
        await m.answer("📥 Enviando a ajuda completa no privado (DM).")
        try:
            await bot.send_message(m.from_user.id, "🛠️ Comandos admin:\n" + "\n".join([
                "/which_source — mostra fontes e paths",
                "/ls_data — lista arquivos em /data",
                "/fetch_update — força baixar odds_hoje/amanha e agenda",
                "/games_today — lista jogos (IDs) de hoje",
                "/games_tomorrow — lista jogos (IDs) de amanhã",
                "/post_pick <GAME_ID> — publica 1 pick do jogo (se vazio, escolhe melhor SLS)",
                "/post_combo — publica 1 combo segundo regra do slot",
                "/post_coruja — publica card Corujão (00:00–07:00)",
                "/pub_show_today — mostra publicados hoje",
                "/pub_reset_today — zera marcações de hoje",
                "/diag_time — mostra relógios local/UTC",
                "/diag_odds — estatísticas dos JSON de odds",
                "/diag_slots — valida agenda_editorial contra os dados",
                "/help — mostra esta ajuda"
            ]))
        except Exception:
            pass
        return

    # aqui é DM: defina 'lines' e envie em chunks
    lines = [
        "🛠️ Comandos admin:",
        "/which_source — mostra fontes e paths",
        "/ls_data — lista arquivos em /data",
        "/fetch_update — força baixar odds_hoje/amanha e agenda",
        "/games_today — lista jogos (IDs) de hoje",
        "/games_tomorrow — lista jogos (IDs) de amanhã",
        "/post_pick <GAME_ID> — publica 1 pick do jogo (se vazio, escolhe melhor SLS)",
        "/post_combo — publica 1 combo segundo regra do slot",
        "/post_coruja — publica card Corujão (00:30–07:00)",
        "/pub_show_today — mostra publicados hoje",
        "/pub_reset_today — zera marcações de hoje",
        "/diag_time — mostra relógios local/UTC",
        "/diag_odds — estatísticas dos JSON de odds",
        "/diag_slots — valida agenda_editorial contra os dados",
        "/help — mostra esta ajuda"
    ]
    chunk = []; char_sum = 0
    for ln in lines:
        if char_sum + len(ln) + 1 > 3500:
            await m.answer("\n".join(chunk))
            chunk = [ln]; char_sum = len(ln) + 1
        else:
            chunk.append(ln); char_sum += len(ln) + 1
    if chunk:
        await m.answer("\n".join(chunk))

    
@dp.message(Command("ping"))
async def ping_cmd(m: types.Message):
    await m.answer("pong ✅")

@dp.message(Command("whoami"))
async def whoami_cmd(m: types.Message):
    await m.answer(f"user.id = <code>{m.from_user.id}</code> | @{m.from_user.username or '—'}")

@dp.message(Command("gid"))
async def gid_cmd(m: types.Message):
    await m.answer(f"chat.id = <code>{m.chat.id}</code> | type = <code>{m.chat.type}</code>")

@dp.message(Command("post_here"))
async def post_here(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    demo = {
        "campeonato":"Brasileirão Série A","pais":"Brasil","hora_utc":"2025-10-31T19:00:00Z",
        "mandante":"Flamengo","visitante":"Palmeiras","mercado":"Over 1.5 Goals",
        "prob_real":0.84,"fair_odd":1.19,"odd_mercado":1.35,"ev":13.0,"roi":9.6,"nota":"Linha conservadora; xG alto recente"
    }
    await bot.send_message(m.chat.id, fmt_pick(demo))
    await m.answer("✅ Publicado aqui mesmo.")

@dp.message(Command("post_today"))
async def post_today(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    try:
        await bot.send_message(GROUP_ID, "✅ Publicação de teste no grupo configurado.")
        await m.answer(f"✅ Publicado no grupo (GROUP_ID={GROUP_ID}).")
    except Exception as e:
        await m.answer(f"❌ Erro ao publicar no grupo.\n<code>{e}</code>")

@dp.message(Command("post_from_file"))
async def post_from_file(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    data = await load_odds_generic()
    # normaliza estrutura e campos antes de qualquer uso
    try:
        data = normalize_odds(data)
    except Exception:
        pass

    picks = [x for x in data["picks"] if pass_filter(x, MIN_PROB, MIN_EV)]
    picks.sort(key=lambda x: (sls_score(float(x.get("prob_real",0)), float(x.get("ev",0))), float(x.get("ev",0))), reverse=True)
    await publish_picks(GROUP_ID, picks, admin_dm=m.from_user.id)
    await m.answer(f"✅ Publicado {len(picks)} entradas.")

@dp.message(Command("post_combos"))
async def post_combos(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    data = await load_odds_generic()
    # normaliza estrutura e campos antes de qualquer uso
    try:
        data = normalize_odds(data)
    except Exception:
        pass

    combos = data.get("combos", [])
    if not combos:
        return await m.answer("❌ Nenhuma múltipla encontrada no arquivo.")

    def combo_sls(c):
        pr  = _f(c.get("prob_real_combo", 0.0))
        evc = _f(c.get("ev_combo", 0.0))
        return sls_score(pr, evc)

    combos.sort(key=combo_sls, reverse=True)

    for c in combos:
        pr   = _f(c.get("prob_real_combo", 0.0))
        evc  = _f(c.get("ev_combo", 0.0))
        sls_c = sls_score(pr, evc)
        left = primary_badges(pr, evc)
        right = right_badge_sls(sls_c)
        legs = "\n".join([f"• {leg}" for leg in c.get("legs", [])]) or "—"
        msg = (
            f"{BRAND_LINE}\n"
            f"{left}<b>{c.get('titulo','Múltipla')}</b>{right}\n\n"
            f"• Prob. real (combo): <b>{pr*100:.1f}%</b>\n"
            f"• Fair (combo): <b>{_f(c.get('fair_combo',0.0)):.2f}</b>  |  Odd mercado (combo): <b>{_f(c.get('odd_combo',0.0)):.2f}</b>\n"
            f"• EV (combo): <b>{evc:.1f}%</b>\n\n"
            f"{legs}\n\n"
            f"{_pick_aforismo_for_sls(sls_c)}"
        )
        await bot.send_message(GROUP_ID, msg)
        if is_admin(m.from_user.id):
            dbg = f"[DEBUG] prob_combo={pr:.4f} ev%={evc:.2f} sls={sls_c:.2f}"
            try:
                await bot.send_message(m.from_user.id, msg + f"\n\n<code>{dbg}</code>")
            except Exception:
                pass
        await asyncio.sleep(0.7)
    await m.answer(f"✅ Publicadas {len(combos)} múltiplas.")

# ---- Status de assinatura (público) ----
@dp.message(Command("status_sub"))
async def status_sub(m: types.Message):
    h = subs_get().get(str(m.from_user.id))
    if not h:
        return await m.answer("❌ Nenhuma assinatura encontrada.")
    exp = int(h.get("expires_at") or 0)
    if exp:
        exp_txt = datetime.utcfromtimestamp(exp).astimezone(tz.gettz(TZ_NAME)).strftime("%d/%m/%Y %H:%M")
        exp_txt += f" {TZ_NAME}"
    else:
        exp_txt = "—"
    await m.answer(f"👤 Assinatura: <b>{h.get('status','—')}</b>\nExpira: <b>{exp_txt}</b>")

# ---- Convites / Pagamento ----
@dp.message(Command("status_user"))
async def cmd_status_user(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    parts = (m.text or "").split()
    uid = None
    if len(parts) >= 2:
        try:
            uid = int(parts[1])
        except Exception:
            return await m.answer("Uso: /status_user <telegram_id>")
    else:
        uid = m.from_user.id

    subs = subs_get().get(str(uid)) or {}
    sub_status = (subs.get("status") or "—").lower()
    exp = int(subs.get("expires_at") or 0)
    exp_txt = "—"
    if exp:
        try:
            exp_txt = datetime.utcfromtimestamp(exp).astimezone(tz.gettz(TZ_NAME)).strftime("%d/%m/%Y %H:%M") + f" {TZ_NAME}"
        except Exception:
            pass

    channel_status = await _get_member_status(uid)

    msg = (
        f"👤 <b>User</b>: <code>{uid}</code>\n"
        f"🔐 <b>Assinatura</b>: <b>{sub_status}</b>\n"
        f"🗓️ <b>Expira</b>: <b>{exp_txt}</b>\n"
        f"📡 <b>Canal</b>: <b>{channel_status}</b>"
    )
    await m.answer(msg)

# ---- Convites / Pagamento ----
@dp.message(Command("join"))
async def join_cmd(m: types.Message):
    url = build_checkout_url()
    await m.answer(
        "Para entrar no grupo como assinante, conclua o pagamento aqui:\n"
        f"{url}\n\n"
        "Após a confirmação, o acesso é liberado automaticamente."
    )

@dp.message(Command("refer"))
async def refer_cmd(m: types.Message):
    if not sub_is_active(m.from_user.id):
        return await m.answer(
            "Você ainda não é assinante ativo.\n"
            f"Assine aqui: {build_checkout_url()}"
        )
    url = build_checkout_url(ref=m.from_user.id)
    await m.answer(
        "🔗 Seu link de indicação (checkout):\n"
        f"{url}\n\n"
        "Envie ao seu amigo. Ao concluir o pagamento, ele recebe acesso."
    )

@dp.message(Command("grant_trial"))
async def grant_trial_cmd(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    parts = m.text.strip().split()
    if len(parts) < 2:
        return await m.answer("Uso: /grant_trial <telegram_id> [dias=30]")

    try:
        target = int(parts[1])
    except Exception:
        return await m.answer("ID inválido.")

    days = 30
    if len(parts) >= 3:
        try:
            days = max(1, int(parts[2]))
        except Exception:
            pass

    exp = set_trial_active(target, days=days, plan_label="trial")
    try:
        expire_inv = _now() + 2*60*60
        invite = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            name=f"Trial {target}",
            expire_date=expire_inv,
            member_limit=1,
            creates_join_request=True
        )
        inv = invites_get()
        inv[invite.invite_link] = {
            "allowed_uid": int(target),
            "expire": int(expire_inv),
            "created_at": _now()
        }
        invites_set(inv)

        exp_txt = datetime.utcfromtimestamp(exp).astimezone(tz.gettz(TZ_NAME)).strftime("%d/%m/%Y %H:%M") + " " + TZ_NAME
        await m.answer(
            f"✅ Trial concedido a <code>{target}</code> por {days} dias.\n"
            f"Expira em: <b>{exp_txt}</b>\n"
            f"Convite (2h, 1 uso):\n{invite.invite_link}"
        )

        try:
            await bot.send_message(
                target,
                "🎟️ Você recebeu um TRIAL para o grupo Bet Masterson.\n"
                f"Use este link nas próximas 2 horas (1 uso):\n{invite.invite_link}"
            )
        except Exception:
            pass

    except Exception as e:
        await m.answer(f"❌ Erro ao criar convite trial.\n<code>{e}</code>")
        
# ---- Novo: reemitir convite (assinante ativo) ----
@dp.message(Command("enforce_now"))
async def cmd_enforce_now(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    await enforce_once()
    await m.answer("Enforcer executado agora. ✅")

@dp.message(Command("sub_set"))
async def cmd_sub_set(m: types.Message):
    """
    Uso: /sub_set <telegram_id> <status> [expires]
      - status: active | expired | cancelled
      - expires: timestamp (UTC) OU "+<dias>" (ex.: +30)
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    parts = (m.text or "").split()
    if len(parts) < 3:
        return await m.answer("Uso: /sub_set <telegram_id> <status> [expires_ts|+dias] [plan]")
    try:
        uid = int(parts[1])
    except Exception:
        return await m.answer("telegram_id inválido.")
    status = parts[2].lower().strip()
    exp = 0
    plan = parts[4] if len(parts) >= 5 else "manual"
    if len(parts) >= 4:
        arg = parts[3].strip()
        if arg.startswith("+"):
            try:
                days = max(1, int(arg[1:]))
                exp = _now() + days*86400
            except Exception:
                exp = 0
        else:
            try:
                exp = int(arg)
            except Exception:
                exp = 0
    upsert_sub(str(uid), status, exp, plan)
    await m.answer(f"Assinatura atualizada: id={uid} status={status} expires_at={exp}")
    # ação imediata se marcamos como expirado/cancelado
    if status != "active":
        await enforce_once()

# ---- Novo: reemitir convite (assinante ativo) ----
@dp.message(Command("entrar"))
async def cmd_entrar(m: types.Message):
    if m.chat.type != "private":
        return  # só DM
    uid = m.from_user.id
    if not sub_is_active(uid):
        return await m.answer("Sua assinatura não está ativa. Use /join para assinar.")
    if await is_in_channel(uid):
        return await m.answer("Você já está no canal ✅. Se precisar de ajuda, mande /help.")
    await on_payment_confirmed(uid)
    await m.answer("Enviei um novo link de acesso acima. ✅")

# -------------------- CANAL MEMBERSHIP HELPERS --------------------
async def _get_member_status(user_id: int) -> str:
    """Retorna o status do usuário no canal: 'creator', 'administrator', 'member', 'restricted', 'left', 'kicked' ou 'unknown'."""
    try:
        m = await bot.get_chat_member(GROUP_ID, int(user_id))
        return str(m.status)
    except Exception:
        return "unknown"

async def is_in_channel(user_id: int) -> bool:
    status = await _get_member_status(user_id)
    return status in {"creator", "administrator", "member"}

async def is_channel_admin(user_id: int) -> bool:
    status = await _get_member_status(user_id)
    return status in {"creator", "administrator"}

# --------- Handler do join request com validação de assinatura e dono do link
@dp.chat_join_request()
async def handle_chat_join_request(req: ChatJoinRequest):
    try:
        uid = int(req.from_user.id)
        uname = f"@{req.from_user.username}" if req.from_user.username else (req.from_user.first_name or str(uid))
        chat_id = req.chat.id
        link_obj = getattr(req, "invite_link", None)
        link_str = getattr(link_obj, "invite_link", None)

        # 1) assinatura precisa estar ativa
        if not sub_is_active(uid):
            try:
                await bot.decline_chat_join_request(chat_id, uid)
            except Exception:
                pass
            try:
                await bot.send_message(uid, "❌ Sua assinatura não está ativa. Use /join para assinar.")
            except Exception:
                pass

            # logs + trilha + alerta admin
            print(f"[JOIN DECLINED - INACTIVE] id={uid} username={uname} chat={chat_id}")
            save_event({
                "event": "join_request_declined_inactive",
                "user_id": uid,
                "username": uname,
                "chat_id": chat_id,
                "invite_link": link_str,
            })
            try:
                await notify_admins(f"[JOIN DECLINED] {uname} ({uid}) — assinatura inativa")
            except Exception:
                pass
            return

        # 2) se houver mapeamento link→uid, o link só vale para o dono
        invmap = invites_get()
        if link_str and link_str in invmap:
            allowed = int(invmap[link_str].get("allowed_uid") or 0)
            if allowed and allowed != uid:
                try:
                    await bot.decline_chat_join_request(chat_id, uid)
                except Exception:
                    pass
                try:
                    await bot.send_message(uid, "❌ Este link pertence a outra conta. Use /entrar para gerar o seu.")
                except Exception:
                    pass

                # logs + trilha + alerta admin
                print(f"[JOIN DECLINED - WRONG LINK] id={uid} username={uname} link_owner={allowed}")
                save_event({
                    "event": "join_request_declined_wrong_link",
                    "user_id": uid,
                    "username": uname,
                    "chat_id": chat_id,
                    "invite_link": link_str,
                    "link_owner": allowed,
                })
                try:
                    await notify_admins(f"[JOIN DECLINED] {uname} ({uid}) tentou usar link de {allowed}")
                except Exception:
                    pass
                return

        # 3) aprovado
        await bot.approve_chat_join_request(chat_id, uid)
        try:
            await bot.send_message(uid, "✅ Acesso aprovado ao canal. Bem-vindo!")
        except Exception:
            pass

        # logs + trilha + alerta admin
        print(f"[JOIN APPROVED] id={uid} username={uname} chat={chat_id}")
        save_event({
            "event": "join_request_approved",
            "user_id": uid,
            "username": uname,
            "chat_id": chat_id,
            "invite_link": link_str,
        })
        try:
            await notify_admins(f"[JOIN APPROVED] {uname} ({uid})")
        except Exception:
            pass

    except Exception as e:
        print("JOIN_REQUEST_HANDLER_ERROR:", repr(e))

# -------------------- ENFORCER + REMINDERS --------------------
async def enforce_once():
    try:
        subs = subs_get()
        now  = _now()
        for uid, h in list(subs.items()):
            status = (h.get("status","").lower())
            exp    = int(h.get("expires_at") or 0)
            # ignorar admins/owner do canal
            try:
                if await is_channel_admin(int(uid)):
                    continue
            except Exception:
                pass
            # apenas agir se assinatura não ativa ou expirada
            if status != "active" or (exp and exp <= now):
                try:
                    member_status = await _get_member_status(int(uid))
                    if member_status not in {"member", "restricted"}:
                        continue
                    await bot.ban_chat_member(GROUP_ID, int(uid))
                    await bot.unban_chat_member(GROUP_ID, int(uid))
                except Exception as e:
                    print("ENFORCE_KICK_ERROR:", uid, repr(e))
    except Exception as e:
        print("ENFORCE_ONCE_ERROR:", repr(e))

async def enforce_loop():
    while True:
        try:
            await enforce_once()
            await asyncio.sleep(900)
        except Exception as e:
            print("ENFORCE_LOOP_ERROR:", repr(e))
            await asyncio.sleep(30)

def _as_dt_local(ts: int) -> str:
    try:
        return datetime.utcfromtimestamp(ts).astimezone(tz.gettz(TZ_NAME)).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return "-"

def _days_left(expires_at: int) -> int:
    if not expires_at:
        return 99999
    delta = max(0, expires_at - _now())
    return (delta + 86399) // 86400

async def _dm(uid: int, text: str):
    try:
        await bot.send_message(uid, text)
    except Exception as e:
        print("DM_ERROR", uid, repr(e))

async def reminder_loop():
    while True:
        try:
            subs = subs_get()
            changed = False
            for uid, h in list(subs.items()):
                try:
                    uid_int = int(uid)
                except:
                    continue
                status = (h.get("status", "").lower())
                exp    = int(h.get("expires_at") or 0)
                if status != "active" or not exp:
                    continue

                dleft = _days_left(exp)
                if dleft == 3 and not h.get("notified_3d"):
                    msg = (
                        "⏰ <b>Lembrete de renovação</b>\n"
                        f"Sua assinatura vence em 3 dias (até <b>{_as_dt_local(exp)} {TZ_NAME}</b>)."
                    )
                    await _dm(uid_int, msg)
                    h["notified_3d"] = True
                    changed = True

                if dleft == 0 and not h.get("notified_0d"):
                    msg = (
                        "⏰ <b>Último dia de assinatura</b>\n"
                        f"Sua assinatura expira hoje (<b>{_as_dt_local(exp)} {TZ_NAME}</b>)."
                    )
                    await _dm(uid_int, msg)
                    h["notified_0d"] = True
                    changed = True

                subs[uid] = h

            if changed:
                subs_set(subs)
            await asyncio.sleep(REMINDER_INTERVAL_SEC)
        except Exception as e:
            print("REMINDER_LOOP_ERROR:", repr(e))
            await asyncio.sleep(60)

# -------- GitHub fetch para manter /data sincronizado --------
import urllib.request, time, urllib.error

GITHUB_RAW_BASE  = os.environ.get("GITHUB_RAW_BASE", "").strip()
ODDS_HOJE_URL    = os.environ.get("ODDS_HOJE_URL", "").strip()
ODDS_AMANHA_URL  = os.environ.get("ODDS_AMANHA_URL", "").strip()

_FETCH_MIN_INTERVAL = int(os.environ.get("FETCH_MIN_INTERVAL", "120"))  # seg
_last_fetch_ts = 0

_FETCH_MIN_INTERVAL = FETCH_MIN_INTERVAL

def _download_to(local_path: str, url: str) -> None:
    if not url or not local_path:
        print(f"[FETCH][SKIP] url/local vazio ({url=} {local_path=})")
        return
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"[FETCH][TRY] {url} -> {local_path}")
        urllib.request.urlretrieve(url, local_path)
        st = os.stat(local_path)
        print(f"[FETCH][OK ] {local_path} ({st.st_size} bytes)")
    except urllib.error.HTTPError as e:
        print(f"[FETCH][HTTP] {url} -> {e.code} {e.reason}")
    except Exception as e:
        print(f"[FETCH][ERR]  {url} -> {e}")

def ensure_data_files(force: bool = False) -> None:
    """
    Baixa/atualiza /data/odds_hoje.json e /data/odds_amanha.json.
    Prioriza ODDS_HOJE_URL/ODDS_AMANHA_URL; se ausentes, usa GITHUB_RAW_BASE.
    """
    global _last_fetch_ts
    now = time.time()
    if (not force) and (now - _last_fetch_ts < _FETCH_MIN_INTERVAL):
        return

    hoje_url    = ODDS_HOJE_URL
    amanha_url  = ODDS_AMANHA_URL
    if not hoje_url and GITHUB_RAW_BASE:
        hoje_url   = GITHUB_RAW_BASE.rstrip("/") + "/odds_hoje.json"
    if not amanha_url and GITHUB_RAW_BASE:
        amanha_url = GITHUB_RAW_BASE.rstrip("/") + "/odds_amanha.json"

    print(f"[FETCH][CFG] hoje_url={hoje_url} | amanha_url={amanha_url}")
    print(f"[FETCH][CFG] ODDS_FILE={ODDS_FILE} | ODDS_AMANHA_FILE={ODDS_AMANHA_FILE}")

    # odds hoje
    if hoje_url:
        _download_to(ODDS_FILE, hoje_url)

    # odds amanhã
    if amanha_url and ODDS_AMANHA_FILE:
        _download_to(ODDS_AMANHA_FILE, amanha_url)

    # agenda editorial
    agenda_url = AGENDA_URL or (GITHUB_RAW_BASE.rstrip("/") + "/agenda_editorial.json" if GITHUB_RAW_BASE else "")
    if agenda_url and AGENDA_JSON_PATH:
        _download_to(AGENDA_JSON_PATH, agenda_url)

    _last_fetch_ts = now
# -------- fim GitHub fetch --------

# -------------------- SCHEDULER (AUTO/EDITORIAL) --------------------
def _key_pub(dt_utc_iso: str, ref_hash: str) -> str:
    return f"{dt_utc_iso}#{ref_hash}"

def _to_utc_iso(date_local: str, time_local: str) -> str:
    tz_sp = tz.gettz(TZ_NAME)
    y, m, d = [int(x) for x in date_local.split("-")]
    h, M = [int(x) for x in time_local.split(":")]
    dt_local = datetime(y, m, d, h, M, 0, tzinfo=tz_sp)
    return dt_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _published_get():
    return _safe_load(PUBLISHED_LOG, {})

def _published_set(d):
    _safe_save(PUBLISHED_LOG, d)

def _hour_key(dt_utc: datetime) -> str:
    # chave por hora UTC p/ anti-flood
    return dt_utc.strftime("%Y-%m-%dT%H:00Z")

def _inc_hour_count(dt_utc: datetime) -> None:
    db = _published_get()
    hk = _hour_key(dt_utc)
    c = int(db.get("_hour_count", {}).get(hk, 0))
    db.setdefault("_hour_count", {})[hk] = c + 1
    _published_set(db)

def _hour_count(dt_utc: datetime) -> int:
    db = _published_get()
    hk = _hour_key(dt_utc)
    return int(db.get("_hour_count", {}).get(hk, 0))


# === PATCH: seleção por regra com variabilidade e “não colapsar jogo” ===
async def _select_by_rule(data: Dict[str, Any], rule: Dict[str, Any], now_local: Optional[datetime] = None):
    """
    Seleciona itens respeitando:
      - filtros MIN_PROB/MIN_EV (para singles),
      - RESERVA de SLS>=RESERVE_SLS_THRESHOLD até RESERVE_CUTOFF_HOUR (salvo lead < MIN_LEAD_MIN),
      - diversidade POR CATEGORIA no slot (valores vêm do ENV):
          • pick:   até PICK_PER_MATCH_PER_SLOT por JOGO no slot
          • inter:  até INTER_PER_MATCH_PER_SLOT por JOGO no slot
          • intra:  até INTRA_PER_MATCH_PER_SLOT por JOGO no slot
      - alternância de tipos inter via COMBOS_TYPES_ORDER ("duplo,triplo,multi").
    """
    tz_sp = tz.gettz(TZ_NAME)
    now_local = now_local or datetime.now(tz_sp)

    secs = rule.get("sections", [])
    max_cards = int(rule.get("max_cards", 3))

    # limites por jogo (ENV; defaults = 1)
    PICK_PER_MATCH_PER_SLOT  = int(os.environ.get("PICK_PER_MATCH_PER_SLOT", 1))
    INTER_PER_MATCH_PER_SLOT = int(os.environ.get("INTER_PER_MATCH_PER_SLOT", 1))
    INTRA_PER_MATCH_PER_SLOT = int(os.environ.get("INTRA_PER_MATCH_PER_SLOT", 1))

    # Alternância (INTER)
    types_order = [t.strip() for t in os.environ.get("COMBOS_TYPES_ORDER", "duplo,triplo,multi").split(",") if t.strip()]

    # Coletores
    raw_singles: List[tuple] = []  # ("pick", payload, sls, game_id)
    raw_inter:   List[tuple] = []  # ("combo", payload, sls, set(game_ids))
    raw_intra:   List[tuple] = []  # ("combo", payload, sls, single_game_id)

    # -------- Singles
    if "singles" in secs:
        for p in (data.get("picks", []) or []):
            if not pass_filter(p, MIN_PROB, MIN_EV):
                continue
            pr = _f(p.get("prob_real", 0.0))
            ev = _f(p.get("ev", 0.0))
            sls = sls_score(pr, ev)
            kick = _pick_time_str(p)

            RESERVE_ENABLED = os.getenv("RESERVE_ENABLED", "false").lower() == "true"

            # RESERVA (segurar diamantes) — só se habilitada
            if RESERVE_ENABLED and (sls >= RESERVE_SLS_THRESHOLD) and (now_local.hour < RESERVE_CUTOFF_HOUR):
                if _time_ok_lead(kick, now_local, MIN_LEAD_MIN):
                    continue

            gid = _game_id_from_pick(p)
            raw_singles.append(("pick", p, sls, gid))

    # -------- Combos
    def _combo_kind_and_games(c: Dict[str, Any]) -> Tuple[str, Set[str]]:
        games: Set[str] = set()
        legs = c.get("legs", []) or []
        for leg in legs:
            if isinstance(leg, dict):
                mm = leg.get("mandante") or leg.get("home") or ""
                vv = leg.get("visitante") or leg.get("away") or ""
                hh = leg.get("hora_utc") or leg.get("hora") or leg.get("kickoff") or ""
                key = f"{mm}|{vv}|{hh}"
                games.add(hashlib.md5(key.encode("utf-8")).hexdigest()[:10])
            else:
                # perna em texto "Time A vs Time B — ..."
                head = str(leg).split(" — ", 1)[0].strip()
                games.add(hashlib.md5(head.encode("utf-8")).hexdigest()[:10])
        kind = "intra" if len(games) == 1 else "inter"
        return kind, games

    if any(s in secs for s in ("doubles","trebles","multiples","intra_game_combos","inter_game_multiples","combos")):
        for c in (data.get("combos", []) or []):
            pr  = _f(c.get("prob_real_combo", 0.0))
            evc = _f(c.get("ev_combo", 0.0))
            sls = sls_score(pr, evc)
            kind, games = _combo_kind_and_games(c)

            # checa lead (pela perna mais cedo)
            ek = _earliest_leg_kickoff(c)
            if not ek or (ek - now_local).total_seconds()/60.0 < MIN_LEAD_MIN:
                continue

            if kind == "intra":
                raw_intra.append(("combo", c, sls, next(iter(games)) if games else ""))
            else:
                raw_inter.append(("combo", c, sls, games))

    # Ordenação por SLS
    raw_singles.sort(key=lambda x: x[2], reverse=True)
    raw_inter.sort(key=lambda x: x[2], reverse=True)
    raw_intra.sort(key=lambda x: x[2], reverse=True)

    # -------- Merge com diversidade por JOGO
    out: List[tuple] = []
    pick_by_game:  Dict[str,int] = defaultdict(int)
    inter_by_game: Dict[str,int] = defaultdict(int)
    intra_by_game: Dict[str,int] = defaultdict(int)

    # 1) Singles
    if "singles" in secs:
        for kind, payload, sls, gid in raw_singles:
            if len(out) >= max_cards: break
            if pick_by_game[gid] >= PICK_PER_MATCH_PER_SLOT: continue
            if not _time_ok_lead(_pick_time_str(payload), now_local, MIN_LEAD_MIN): continue
            if already_published_pick(payload): continue
            out.append((kind, payload, sls))
            pick_by_game[gid] += 1

    # 2) Intra (se permitido)
    if len(out) < max_cards and any(s in secs for s in ("intra_game_combos","combos")):
        for kind, payload, sls, gid in raw_intra:
            if len(out) >= max_cards: break
            if intra_by_game[gid] >= INTRA_PER_MATCH_PER_SLOT: continue
            if already_published_combo(payload): continue
            out.append(("combo", payload, sls))
            intra_by_game[gid] += 1

    # 3) Inter (alternância de tipo: duplo → triplo → multi)
    if len(out) < max_cards and any(s in secs for s in ("doubles","trebles","multiples","inter_game_multiples","combos")):
        buckets: Dict[str, List[tuple]] = {"duplo":[], "triplo":[], "multi":[]}
        for kind, payload, sls, games in raw_inter:
            nlegs = len(payload.get("legs", []) or []) or len(games)
            if nlegs == 2:   buckets["duplo"].append(("combo", payload, sls, games))
            elif nlegs == 3: buckets["triplo"].append(("combo", payload, sls, games))
            else:            buckets["multi"].append(("combo", payload, sls, games))

        for t in types_order:
            if len(out) >= max_cards: break
            for kind, payload, sls, games in buckets.get(t, []):
                if len(out) >= max_cards: break
                # limitação por jogo nos INTER
                if any(inter_by_game[g] >= INTER_PER_MATCH_PER_SLOT for g in games): continue
                if already_published_combo(payload): continue
                out.append(("combo", payload, sls))
                for g in games:
                    inter_by_game[g] += 1

    return out[:max_cards]
# === END PATCH ===


def _fmt_combo_msg(c: Dict[str, Any]) -> str:
    pr  = _f(c.get("prob_real_combo", 0.0))
    evc = _f(c.get("ev_combo", 0.0))
    sls_c = sls_score(pr, evc)
    left = primary_badges(pr, evc)
    right = right_badge_sls(sls_c)
    legs = "\n".join([f"• {leg}" for leg in c.get("legs", [])]) or "—"
    return (
        f"{BRAND_LINE}\n"
        f"{left}<b>{c.get('titulo','Múltipla')}</b>{right}\n\n"
        f"• Prob. real (combo): <b>{pr*100:.1f}%</b>\n"
        f"• Fair (combo): <b>{_f(c.get('fair_combo',0.0)):.2f}</b>  |  Odd mercado (combo): <b>{_f(c.get('odd_combo',0.0)):.2f}</b>\n"
        f"• EV (combo): <b>{evc:.1f}%</b>\n\n"
        f"{legs}\n\n"
        f"{_pick_aforismo_for_sls(sls_c)}"
    )



# === PATCH: carregadores com normalização ===
def _load_odds_from_path(path: str) -> Dict[str, Any]:
    """
    Loader blindado para os JSONs de odds (retorna sempre um dict com chaves 'picks' e 'combos').
    - Se o arquivo não existir ou estiver corrompido, retorna {'picks': [], 'combos': []}
    - Se o arquivo JSON for uma lista de picks (legado), normaliza para {'picks': [...], 'combos': []}
    - Loga erros silenciosamente retornando dict vazio com listas internas.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        # arquivo não existe ou JSON inválido
        print(f"[LOAD_ODDS] erro lendo {path}: {repr(e)}")
        return {"picks": [], "combos": []}

    # Normalizar estruturas possíveis:
    # - se o JSON já for um dict com 'picks'/'combos' -> ok
    if isinstance(data, dict):
        # garantir chaves
        if "picks" not in data:
            data["picks"] = []
        if "combos" not in data:
            data["combos"] = []
        return data

    # - se for uma lista (ex: apenas picks) -> transformamos
    if isinstance(data, list):
        return {"picks": data, "combos": []}

    # Qualquer outro formato -> devolver dict vazio padronizado
    return {"picks": [], "combos": []}

from pathlib import Path

DATA_DIR = Path("/data")  # ajuste se o seu caminho for outro

def _safe_load(path: Path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            import json
            return json.load(f)
    except Exception:
        return default

def _header_target_date(obj) -> str | None:
    # Prioridade 1: odds_file_header.target_date_local
    h = (obj or {}).get("odds_file_header") or {}
    td = (h.get("target_date_local") or "").strip()
    if td:
        return td
    # Prioridade 2: meta.target_date_local (compat.)
    m = (obj or {}).get("meta") or {}
    td2 = (m.get("target_date_local") or "").strip()
    return td2 or None

def _header_is_corujao_ready(obj) -> bool:
    h = (obj or {}).get("odds_file_header") or {}
    return bool(h.get("corujao_ready", False))

def _list_odds_files() -> list[Path]:
    # varre todos os odds*.json (ex.: odds.json, odds2.json, odds_2025-11-10.json, odds-qualquercoisa.json...)
    return sorted(DATA_DIR.glob("odds*.json"))

def _pick_best_for_date(files: list[Path], target_date_iso: str) -> dict | None:
    """
    Seleciona o arquivo cujo header aponta para 'target_date_iso' (YYYY-MM-DD).
    Critérios:
      1) match exato do target_date_local
      2) se houver ≥1 candidato, preferir aquele com corujao_ready=True quando for após meia-noite
      3) em empate, escolher o mais novo por mtime
      4) fallback: se nenhum bate a data, pegar o mais novo por mtime
    """
    import os, time
    candidates = []
    for p in files:
        obj = _safe_load(p, None)
        if not obj:
            continue
        td = _header_target_date(obj)
        if td == target_date_iso:
            candidates.append((p, obj, _header_is_corujao_ready(obj), os.path.getmtime(p)))
    if candidates:
        # preferir corujao_ready True, depois mtime mais recente
        candidates.sort(key=lambda x: (not x[2], x[3]), reverse=True)
        return candidates[0][1]

    # fallback: arquivo mais novo
    newest = None
    newest_mtime = -1
    for p in files:
        try:
            mt = p.stat().st_mtime
            if mt > newest_mtime:
                newest, newest_mtime = p, mt
        except Exception:
            continue
    return _safe_load(newest, None) if newest else None

async def load_odds_by_date(d) -> dict:
    """
    Carrega o odds*.json que tiver header target_date_local == d (timezone SP).
    """
    tz_sp = tz.gettz(TZ_NAME)
    # d já deve ser date(). Garantimos ISO YYYY-MM-DD:
    target_iso = d.strftime("%Y-%m-%d")
    files = _list_odds_files()
    data = _pick_best_for_date(files, target_iso)
    return data or {"picks": [], "combos": [], "corujao": {"picks": [], "combos": []}}

async def load_odds_generic() -> dict:
    """
    Mantém a API existente, mas agora:
    - decide a data-alvo pelo TZ São Paulo
    - carrega via cabeçalho e não por nome do arquivo
    """
    tz_sp = tz.gettz(TZ_NAME)
    today = datetime.now(tz_sp).date()
    return await load_odds_by_date(today)

async def load_odds_hoje() -> dict:
    # compat: “hoje” vira hoje pelo cabeçalho
    tz_sp = tz.gettz(TZ_NAME)
    today = datetime.now(tz_sp).date()
    return await load_odds_by_date(today)

async def load_odds_amanha() -> dict:
    # compat: “amanhã” vira hoje+1 pelo cabeçalho
    tz_sp = tz.gettz(TZ_NAME)
    tomorrow = (datetime.now(tz_sp).date() + timedelta(days=1))
    return await load_odds_by_date(tomorrow)



async def load_odds_hoje() -> Dict[str, Any]:
    return _load_odds_from_path(ODDS_FILE)


async def load_odds_amanha() -> Dict[str, Any]:
    if not ODDS_AMANHA_FILE:
        return {"picks": [], "combos": []}
    return _load_odds_from_path(ODDS_AMANHA_FILE)


# =========================
# QUAL ODDS USAR (exclusivo pelo DIA REAL dos jogos)
# =========================

async def load_odds_generic() -> Dict[str, Any]:
    """
    Escolhe EXCLUSIVAMENTE o arquivo cujo primeiro jogo é HOJE (data local),
    NUNCA mescla. Regras:
      - Se odds_hoje tiver data == hoje → usa odds_hoje.
      - Senão, se odds_amanha tiver data == hoje → usa odds_amanha.
      - Senão → fallback seguro: odds_hoje.
    """
    tz_sp = tz.gettz(TZ_NAME)
    today = datetime.now(tz_sp).date()

    d1 = await load_odds_hoje()
    d2 = await load_odds_amanha()

    # Se o JSON só trouxer hora_utc, forçamos a suposição:
    # - odds_hoje → assume 'today'
    # - odds_amanha → assume 'today + 1'
    date1 = _infer_list_date_local(d1, assume_date=today)
    date2 = _infer_list_date_local(d2, assume_date=(today + timedelta(days=1)))

    if date1 == today:
        return d1
    if date2 == today:
        return d2

    # fallback (nenhum bateu com hoje)
    return d1

# === END PATCH ===

TZ_NAME = "America/Sao_Paulo"
tz_sp = tz.gettz(TZ_NAME)

# mapeamento breve para mercados em português (com fallback)
MARKET_MAP = {
    "Over": "Mais de",
    "Under": "Menos de",
    "Over 2.5 Goals": "Gols — Mais de 2.5 gols",
    "Under 2.5 Goals": "Gols — Menos de 2.5 gols",
    "BTTS Yes": "Ambos Marcam — Sim",
    "BTTS No": "Ambos Marcam — Não",
    "1st Half Over 0.5": "1º Tempo – Mais de 0.5 gol(s)",
    "1st Half Over 1.5": "1º Tempo – Mais de 1.5 gol(s)",
    "1st Half Over 1.5": "1º Tempo – Mais de 1.5 gol(s)",
    # padrões genéricos (usamos quando não há mapeamento literal)
}

def translate_market(m):
    if m in MARKET_MAP:
        return MARKET_MAP[m]
    # tentativa de tradução por padrões simples
    m2 = m.replace("Goals", "gols").replace("Over", "Mais de").replace("Under", "Menos de")
    m2 = m2.replace("1st Half", "1º Tempo").replace("2nd Half", "2º Tempo")
    m2 = m2.replace("BTTS", "Ambos Marcam").replace("Yes", "Sim").replace("No", "Não")
    # remover repetições estranhas
    return m2

def format_date_hour_from_utc_str(hora_utc_str):
    if not hora_utc_str:
        return None, None
    try:
        dt = parser.isoparse(hora_utc_str)
    except Exception:
        # às vezes o campo já está em formato sem TZ — tentar parser genérico
        try:
            dt = parser.parse(hora_utc_str)
        except Exception:
            return None, None
    # converter para fuso Brasil
    dt_local = dt.astimezone(tz_sp)
    data_str = dt_local.strftime("%d-%m-%Y")        # DD-MM-YYYY conforme PROMPT :contentReference[oaicite:3]{index=3}
    hora_str = dt_local.strftime("%Hh%M")          # HHhMM conforme PROMPT :contentReference[oaicite:4]{index=4}
    return data_str, hora_str

def calc_sls(prob_real, ev_percent):
    # prompt: SLS = 0.6 * prob_real + 0.4 * EV  (EV internal percent) :contentReference[oaicite:5]{index=5}
    # nosso EV no JSON está em %, ex 52.75 => transformamos para decimal (0.5275)
    ev_norm = (ev_percent or 0) / 100.0
    prob = prob_real or 0.0
    sls = 0.6 * prob + 0.4 * ev_norm
    return sls

def safe_round_odd(x, ndigits=2):
    try:
        return round(float(x), ndigits)
    except Exception:
        return x

def normalize_odds(data):
    """
    Recebe o dict carregado do odds_*.json e:
    - adiciona data/hora no padrão DD-MM-YYYY e HHhMM
    - traduz mercados e selecoes para PT-BR
    - calcula odd_justa = 1/prob_real (exibida com '@')
    - preserva odd_mercado exibindo com '@' se necessário
    - calcula SLS e adiciona 'sls' campo
    - ordena picks por sls desc
    - normaliza combos (cada perna)
    """
    picks = data.get("picks", []) or []
    combos = data.get("combos", []) or []

    normalized_picks = []
    for p in picks:
        # campos originais possíveis: hora_utc, prob_real, fair_odd, odd_mercado, ev, roi
        prob = float(p.get("prob_real") or 0.0)
        ev = float(p.get("ev") or p.get("EV") or 0.0)
        # data/hora
        data_str, hora_str = format_date_hour_from_utc_str(p.get("hora_utc") or p.get("hora"))
        # mercado / selecao traduzidos
        mercado_pt = translate_market(p.get("mercado") or p.get("market") or "")
        selecao_pt = p.get("selecao") or p.get("selection") or ""
        # normalizar seleção: se seleção em inglês 'Over 2.5' -> 'Mais de 2.5'
        selecao_pt = selecao_pt.replace("Over", "Mais de").replace("Under", "Menos de")
        selecao_pt = selecao_pt.replace("BTTS Yes", "Sim").replace("BTTS No", "Não")
        # odd justa
        odd_justa_val = None
        if prob > 0:
            odd_justa_val = 1.0 / prob
        # format strings
        odd_mercado_raw = p.get("odd_mercado") or p.get("odd_market") or None
        odd_mercado_num = _f(odd_mercado_raw, 0.0) if odd_mercado_raw is not None else 0.0
        odd_justa_str = f"@{safe_round_odd(odd_justa_val, 3)}" if odd_justa_val else None

        sls = calc_sls(prob, ev)

        newp = dict(p)  # copia tudo pra não perder notas etc.
        # sobrescrever / adicionar campos obrigatórios do PROMPT
        if data_str: newp["data"] = data_str
        if hora_str: newp["hora"] = hora_str
        newp["mercado"] = mercado_pt
        newp["selecao"] = selecao_pt
        newp["odd_mercado"] = odd_mercado_num  # sempre número aqui
        # odd_justa no nome exigido pelo Prompt (renomear fair_odd -> odd_justa)
        newp["odd_justa"] = odd_justa_str or newp.get("fair_odd") or newp.get("fairOdd")
        # ev/roi mantidos
        newp["ev"] = ev
        newp["roi"] = float(newp.get("roi") or ev or 0.0)
        newp["sls"] = sls
        normalized_picks.append(newp)

    # ordenar por sls desc
    normalized_picks.sort(key=lambda x: x.get("sls", 0), reverse=True)

    # NORMALIZA COMBOS (cada combo pode ter 'legs' como strings ou objetos)
    normalized_combos = []
    for c in combos:
        # campos esperados: legs (strings), or pernas objects
        newc = dict(c)
        legs = c.get("legs") or c.get("pernas") or []
        normalized_legs = []
        for leg in legs:
            if isinstance(leg, str):
                # formato atual: "Al Salt vs Al Hashemeya — Over 2.5 Goals Over 2.5 @2.15"
                # tentar parse simples: split '—' e '@'
                try:
                    left, right = leg.split("—", 1) if "—" in leg else (leg, "")
                    teams = left.strip()
                    market_sel = right.strip()
                    # separar market e odd
                    if "@" in market_sel:
                        ms, oddraw = market_sel.rsplit("@", 1)
                        odd_str = f"@{safe_round_odd(oddraw.strip(),2)}"
                    else:
                        ms = market_sel
                        odd_str = None
                    market_pt = translate_market(ms.strip())
                    # tentativa de extrair mandante/visitante
                    if " vs " in teams:
                        mand, vis = [t.strip() for t in teams.split(" vs ", 1)]
                    else:
                        mand, vis = teams, ""
                    # se existir uma hora no combo principal, tentaremos reaproveitar (mas ideal é que o combo já traga data)
                    # adicionar perna normalizada
                    normalized_legs.append({
                        "mandante": mand,
                        "visitante": vis,
                        "mercado": market_pt,
                        "selecao": ms.strip(),
                        "odd_mercado": odd_str
                    })
                except Exception:
                    normalized_legs.append({"raw": leg})
            elif isinstance(leg, dict):
                # se já for dict, só formatar hora e mercado
                dd, hh = format_date_hour_from_utc_str(leg.get("hora_utc") or leg.get("hora"))
                leg["data"] = dd or leg.get("data")
                leg["hora"] = hh or leg.get("hora")
                leg["mercado"] = translate_market(leg.get("mercado") or leg.get("market") or "")
                normalized_legs.append(leg)
            else:
                normalized_legs.append({"raw": str(leg)})

        newc["pernas"] = normalized_legs
        # calcular sls agregado (média ponderada simples das pernas)
        perna_sls = []
        for pl in normalized_legs:
            pprob = pl.get("prob_real") or None
            pev = pl.get("ev") or 0.0
            if pprob is not None:
                perna_sls.append(calc_sls(float(pprob), float(pev)))
        newc["sls"] = sum(perna_sls)/len(perna_sls) if perna_sls else 0.0
        normalized_combos.append(newc)

    normalized_combos.sort(key=lambda x: x.get("sls",0), reverse=True)

    # sobrescrever no objeto
    data["picks"] = normalized_picks
    data["combos"] = normalized_combos
    return data

def _picks_for_date_from_data(data: Dict[str, Any], d) -> List[Dict[str, Any]]:
    picks = data.get("picks", []) or []
    out = []
    for p in picks:
        tstr = _pick_time_str(p)
        dtl = _parse_any_dt_local(tstr)
        if dtl and _local_date_of_dt(dtl) == d:
            out.append(p)
    return out

def _earliest_leg_kickoff(c: Dict[str, Any]):
    for k in ("hora","hora_utc","kickoff","date_GMT","date_local"):
        v = c.get(k)
        if isinstance(v, str) and v.strip():
            dt = _parse_any_dt_local(v.strip())
            if dt: return dt
    return None

def _match_keys_from_legs(c: Dict[str, Any]) -> List[str]:
    """
    Extrai chaves estáveis de partida a partir de textos de legs do tipo:
    'Time A vs Time B — Mercado Seleção @1.55'
    """
    legs = c.get("legs", []) or []
    keys = []
    for leg in legs:
        try:
            head = str(leg).split(" — ", 1)[0].strip()
            if " vs " in head:
                k = hashlib.md5(head.encode("utf-8")).hexdigest()[:10]
                keys.append(k)
        except Exception:
            pass
    return keys

def _combo_unique_match_keys(c: Dict[str, Any]) -> List[str]:
    """Conjunto (lista) de partidas únicas presentes nas legs do combo."""
    return list(dict.fromkeys(_match_keys_from_legs(c)))  # preserva ordem e remove duplicatas

def _combo_is_intra(c: Dict[str, Any]) -> bool:
    """True se todas as pernas são do MESMO jogo."""
    mks = _combo_unique_match_keys(c)
    return len(mks) == 1

def _combo_is_inter(c: Dict[str, Any]) -> bool:
    """True se o combo mistura partidas diferentes (≥2 jogos)."""
    mks = _combo_unique_match_keys(c)
    return len(mks) >= 2

def _combos_for_date_from_data(data: Dict[str, Any], d) -> List[Dict[str, Any]]:
    combos = data.get("combos", []) or []
    out = []
    for c in combos:
        dt = _earliest_leg_kickoff(c)
        if dt and _local_date_of_dt(dt) == d:
            out.append(c)
    return out

async def load_data_for_date(d) -> Dict[str, List[Dict[str, Any]]]:
    tz_sp = tz.gettz(TZ_NAME)
    today = datetime.now(tz_sp).date()
    tomorrow = today + timedelta(days=1)
    dh = await load_odds_hoje()
    da = await load_odds_amanha()

    def dedup_picks(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set(); out = []
        for it in items:
            gid = _game_id_from_pick(it)
            if gid in seen: continue
            seen.add(gid); out.append(it)
        return out

    def dedup_combos(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set(); out = []
        for it in items:
            legs_s = "|".join(it.get("legs", []))
            key    = hashlib.md5(legs_s.encode("utf-8")).hexdigest()[:12]
            if key in seen: continue
            seen.add(key); out.append(it)
        return out

    if d == today:
        picks  = _picks_for_date_from_data(dh, d) or _picks_for_date_from_data(da, d)
        combos = _combos_for_date_from_data(dh, d) or _combos_for_date_from_data(da, d)
    elif d == tomorrow:
        picks  = _picks_for_date_from_data(da, d) or _picks_for_date_from_data(dh, d)
        combos = _combos_for_date_from_data(da, d) or _combos_for_date_from_data(dh, d)
    else:
        picks  = _picks_for_date_from_data(dh, d) + _picks_for_date_from_data(da, d)
        combos = _combos_for_date_from_data(dh, d) + _combos_for_date_from_data(da, d)

    return {"picks": dedup_picks(picks), "combos": dedup_combos(combos)}


def _badge_prob(p: float) -> str:
    if p >= 0.85: return "🟩 Alta"
    if p >= 0.70: return "🟨 Média"
    return "🟥 Baixa"

def _badge_ev(ev: float) -> str:
    if ev >= 15: return "💎 EV+"
    if ev >= 5:  return "🟢 EV"
    return "⚪"

def render_many_picks_as_one_card(picks: List[Dict[str, Any]], title: str, footer_aphorism: Optional[str]) -> str:
    lines = [f"<b>{title}</b>"]
    for p in picks:
        prob = float(p.get("prob_real", 0))
        odd  = float(p.get("odd_mercado", 0))
        fair = float(p.get("fair_odd", 0))
        sel  = f"{p.get('mercado','')} — {p.get('selecao','')} @{odd:.2f}"
        lines.append(
            f"• {p.get('mandante','')} vs {p.get('visitante','')} — 🕒 {as_local(_pick_time_str(p))}"
            f"  {sel} | Prob {prob:.0%} | Fair {fair:.2f} | {_badge_prob(prob)}"
        )
    if footer_aphorism:
        lines.append(f"<i>{footer_aphorism}</i>")
        lines.append("— Bet Masterson")
    return "\n".join(lines)

def _get_night_aphorism() -> Optional[str]:
    path = AFORISMOS_FILE
    try:
        with open(path, "r", encoding="utf-8") as f:
            arr = json.load(f)
        tagged = [a for a in arr if any(t.lower() in ("madrugada", "night") for t in a.get("tags", []))]
        pool = tagged or arr
        if not pool:
            return None
        return pool[0].get("text") or None
    except Exception:
        return None

async def _collect_coruja_picks_for_date(d_local):
    data_hj = await load_odds_hoje()
    picks = _picks_for_date_from_data(data_hj, d_local)
    if not picks:
        data_am = await load_odds_amanha()
        picks = _picks_for_date_from_data(data_am, d_local)
    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    start_hm = "00:00"
    end_hm   = "07:00"
    result = []
    for p in picks:
        tstr = _pick_time_str(p); dtl = _parse_any_dt_local(tstr)
        if not dtl or dtl.date() != d_local:
            continue
        # janela da madrugada + lead
        sh, sm = map(int, start_hm.split(":"))
        eh, em = map(int, end_hm.split(":"))
        dmin = dtl.astimezone(tz_sp).replace(second=0, microsecond=0)
        if not (dmin.hour*60 + dmin.minute >= sh*60+sm and dmin.hour*60 + dmin.minute <= eh*60+em):
            continue
        if not _time_ok_lead(tstr, now_l, MIN_LEAD_MIN):
            continue
        result.append(p)
    result.sort(key=lambda x: _dt_key_or_now(_pick_time_str(x)))
    return result

async def post_coruja_card():
    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    d_local = now_l.date()
    picks = await _collect_coruja_picks_for_date(d_local)
    if not picks:
        return False
    title = "🌙🦉 Corujão — jogos até 07:00"
    aph = _get_night_aphorism()
    text = render_many_picks_as_one_card(picks, title=title, footer_aphorism=aph)
    await bot.send_message(GROUP_ID, text)
    # marcar cada pick como publicado
    for p in picks:
        mark_published_pick(p)
    return True

@dp.message(Command("post_coruja"))
async def post_coruja(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    ok = await post_coruja_card()
    if ok:
        return await m.answer("🌙 Corujão publicado.")
    return await m.answer("Sem jogos válidos para o Corujão.")

async def scheduler_loop():
    """
    Ajustes:
    - Garante que os arquivos de odds existam/estejam atualizados (ensure_data_files) sem precisar reiniciar o bot.
    - Mantém a lógica de agenda editorial (MODE=editorial) e fallback automático.
    - Corrige o envio de picks/combos: usa o 'payload' retornado por _select_by_rule, aplica controle de já-publicado,
      e respeita lead mínimo (MIN_LEAD_MIN) antes do kickoff.
    """
    daily_count = 0
    while True:
        try:
            # 🔄 baixa/atualiza os JSONs do GitHub → /data/odds_hoje.json e /data/odds_amanha.json
            ensure_data_files(force=False)

            data = await load_odds_generic()  # sua função existente (não altero)
            # normaliza estrutura e campos antes de qualquer uso
            try:
                data = normalize_odds(data)
            except Exception:
                pass

            tz_sp = tz.gettz(TZ_NAME)
            now_utc = datetime.now(timezone.utc)
            now_local = datetime.now(tz_sp)
            today_sp = now_local.strftime("%Y-%m-%d")
            # ---- Corujão 00:30 ----
            try:
                coruja_key = f"coruja#{today_sp}"
                pub = _published_get()
                dt_coruja = datetime.fromisoformat(_to_utc_iso(today_sp, "00:30").replace("Z","+00:00"))
                if (timedelta(0) <= now_utc - dt_coruja <= timedelta(minutes=10)) and not pub.get(coruja_key):
                    ran = await post_coruja_card()
                    if ran:
                        pub[coruja_key] = _now()
                        _published_set(pub)
            except Exception as _e:
                print("CORUJAO_ERROR:", repr(_e))

            # agenda editorial (se MODE=editorial)
            agenda = _safe_load(Path(AGENDA_JSON_PATH), None) if MODE == "editorial" else None
            plan = (agenda or {}).get("schedule_plan", [])

            
            if not plan:
                await asyncio.sleep(30)
                continue

            published = _published_get()
            for slot in plan:
                t_local = slot.get("time_local")
                if not t_local:
                    continue
                dt_utc_iso = _to_utc_iso(today_sp, t_local)
                dt_utc = datetime.fromisoformat(dt_utc_iso.replace("Z","+00:00"))

                # janela de disparo: publica até 5 minutos depois do horário
                if not (timedelta(0) <= now_utc - dt_utc <= timedelta(minutes=5)):
                    continue

                # seleção por refs (se houver)
                items = []
                refs = slot.get("refs") or []

                # 1) Se houver refs especiais, trate antes e pule para o próximo slot
                if refs:
                    if "coruja_card" in refs:
                        ran = await post_coruja_card()
                        if ran:
                            ref_hash = "coruja_card"
                            key = _key_pub(dt_utc_iso, ref_hash)
                            pub = _published_get()
                            pub[key] = _now()
                            _published_set(pub)
                    # nada de mistura com seleção automática quando há refs
                    continue

                # 2) Sem refs → seleção automática (se habilitado)
                if ENABLE_FALLBACK_SELECTION:
                    rule = slot.get("selection_rule") or {}
                    items = await _select_by_rule(data, rule, now_local=now_local)

                    # publica cada item retornado pelo seletor do slot
                    # publica cada item retornado...
                    for (kind, payload, _sls) in items:
                        if daily_count >= MAX_PUBLICATIONS_PER_DAY:
                            break

                        # ✅ freio por hora (3/h por padrão)
                        if _hour_count(dt_utc) >= HOURLY_MAX:
                            continue


                        # anti-repost por hash do payload (payload inteiro → hash)
                        try:
                            ref_hash = hashlib.md5(
                                json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()
                            ).hexdigest()[:10]
                        except Exception:
                            # caso payload não seja 100% serializável
                            ref_hash = hashlib.md5(str(payload).encode()).hexdigest()[:10]

                        key = _key_pub(dt_utc_iso, ref_hash)
                        last_ts = _published_get().get(key)
                        if last_ts and (_now() - int(last_ts)) < MINUTES_BETWEEN_REPOST*60:
                            continue

                        # valida lead mínimo e anti-duplicação por DIA DO JOGO
                        if kind == "pick":
                            kick = _pick_time_str(payload)
                            if not _time_ok_lead(kick, now_local, MIN_LEAD_MIN):
                                continue
                            if already_published_pick(payload):
                                continue
                            try:
                                await bot.send_message(GROUP_ID, fmt_pick(payload))
                                mark_published_pick(payload)
                                _inc_hour_count(dt_utc)
                            except Exception:
                                continue

                        elif kind == "combo":
                            ek = _earliest_leg_kickoff(payload)
                            if not ek:
                                continue
                            if (ek - now_local).total_seconds()/60.0 < MIN_LEAD_MIN:
                                continue
                            if already_published_combo(payload):
                                continue
                            try:
                                await bot.send_message(GROUP_ID, _fmt_combo_msg(payload))
                                mark_published_combo(payload)
                                _inc_hour_count(dt_utc)
                            except Exception:
                                continue

                        # marca como publicado neste slot (proteção contra flood no mesmo minuto)
                        pub = _published_get()
                        pub[key] = _now()
                        _published_set(pub)
                        daily_count += 1
                        await asyncio.sleep(0.7)


            await asyncio.sleep(30)
        except Exception as e:
            print("SCHED_LOOP_ERROR:", repr(e))
            await asyncio.sleep(30)

# -------------------- FASTAPI (WEBHOOK CAKTO) --------------------
app = FastAPI()

@app.get("/")
async def root():
    return JSONResponse({
        "status": "ok",
        "service": "Bet Masterson Bot",
        "time": datetime.now(timezone.utc).isoformat()
    })

async def notify_admins(text: str):
    for uid in AUTHORIZED:
        try:
            await bot.send_message(uid, text)
        except Exception:
            pass

def _auth_ok(request: Request, body_bytes: bytes) -> bool:
    if not CAKTO_SECRET:
        return False
    tok = request.query_params.get("token")
    if tok != CAKTO_SECRET:
        return False
    if CAKTO_SECRET_KEY:
        sig = request.headers.get("X-Cakto-Signature", "")
        expected = hmac.new(CAKTO_SECRET_KEY.encode(), body_bytes, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return False
    return True

def save_event(event_dict: Dict[str, Any]) -> None:
    try:
        content = _safe_load(LOG_PATH, [])
        if not isinstance(content, list):
            content = []
        content.append({
            "timestamp": datetime.utcnow().isoformat(),
            "event": event_dict.get('event') or event_dict.get('status') or "unknown",
            "data": event_dict
        })
        _safe_save(LOG_PATH, content)
        print(f"[Webhook] Evento registrado: {event_dict.get('event') or event_dict.get('status')}")
    except Exception as e:
        print("[Webhook] Erro ao salvar evento:", repr(e))

# ---- Novo: gerar convite ao confirmar pagamento ----
async def on_payment_confirmed(user_id: str | int):
    # Gera link com JOIN REQUEST e vincula link→uid (anti-compartilhamento)
    try:
        uid = int(user_id)
    except Exception:
        return
    expire = int((datetime.utcnow() + timedelta(hours=24)).timestamp())
    try:
        link = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            name=f"Acesso {uid}",
            expire_date=expire,
            member_limit=1,
            creates_join_request=True
        )
        inv = invites_get()
        inv[link.invite_link] = {"allowed_uid": uid, "expire": expire, "created_at": _now()}
        invites_set(inv)
        await bot.send_message(
            uid,
            "✅ Pagamento confirmado!\n"
            "Use este link para solicitar entrada (24h, 1 uso):\n"
            f"{link.invite_link}\n\n"
            "Ao clicar, seu pedido será aprovado automaticamente."
        )
    except Exception as e:
        print("INVITE_LINK_ERROR:", uid, repr(e))

@app.get("/healthz")
async def healthz():
    return PlainTextResponse("OK")

@app.post("/cakto/webhook")
async def cakto_webhook(request: Request):
    body = await request.body()
    if not _auth_ok(request, body):
        raise HTTPException(401, "unauthorized")

    try:
        payload = json.loads(body.decode("utf-8") or "{}")
    except Exception:
        raise HTTPException(400, "invalid json")

    save_event(payload)

    uid = str(payload.get("telegram_id","")).strip()
    status = str(payload.get("status","")).strip().lower()
    expires_at = int(payload.get("expires_at", 0) or 0)
    plan = str(payload.get("plan","")).strip()

    if uid and status in {"active","cancelled","expired"}:
        upsert_sub(uid, status, expires_at, plan)
        if status == "active":
            await on_payment_confirmed(uid)

    return JSONResponse({"ok": True, "user": uid or None, "status": status or None, "logged": True})

@dp.message(Command("games_today"))
async def games_today(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    # tenta no 'hoje', cai pro 'amanha' se vazio
    data = await load_odds_hoje()
    if isinstance(data, list):
        picks = data
    else:
        picks = (data or {}).get("picks", []) or []

    if not picks:
        data = await load_odds_amanha()
        if isinstance(data, list):
            picks = data
        else:
            picks = (data or {}).get("picks", []) or []

    if not picks:
        return await m.answer("Nenhum jogo/pick encontrado em ODDS_FILE/AMANHA.")

    # agrupa por jogo — apenas os de HOJE (America/Sao_Paulo)
    tz_sp = tz.gettz(TZ_NAME)
    today = datetime.now(tz_sp).date()
    bucket = {}
    for p in picks:
        tstr = _pick_time_str(p)
        dtl  = _parse_any_dt_local(tstr)
        if not _is_today_local_from_pick(p):
            continue
        gid = _game_id_from_pick(p)
        if gid not in bucket:
            bucket[gid] = {
                "hora": _pick_time_str(p),
                "pais": p.get("pais",""),
                "liga": p.get("campeonato",""),
                "home": p.get("mandante",""),
                "away": p.get("visitante",""),
                "total_picks": 0,
            }
        bucket[gid]["total_picks"] += 1
    if not bucket:
        return await m.answer("Não há jogos hoje no arquivo.")

    lines = ["📅 Jogos do dia (IDs):"]
    for gid, info in sorted(bucket.items(), key=lambda kv: _dt_key_or_now(kv[1]["hora"])):
        lines.append(
            f"<code>{gid}</code> — {info['home']} vs {info['away']} | {info['liga']} · {info['pais']} | 🕒 {as_local(info['hora'])} | picks: {info['total_picks']}"
        )

    # quebra em blocos
    chunk, s = [], 0
    for ln in lines:
        if s + len(ln) + 1 > 3800:
            await m.answer("\n".join(chunk)); chunk, s = [ln], len(ln)+1
        else:
            chunk.append(ln); s += len(ln)+1
    if chunk:
        await m.answer("\n".join(chunk))

@dp.message(Command("games_tomorrow"))
async def games_tomorrow(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    today = now_l.date()
    tomorrow = today + timedelta(days=1)

    data = await load_odds_amanha()
    if isinstance(data, list):
        picks = data
    else:
        picks = (data or {}).get("picks", []) or []

    if not picks:
        data = await load_odds_hoje()
        if isinstance(data, list):
            picks = data
        else:
            picks = (data or {}).get("picks", []) or []

    if not picks:
        return await m.answer("Nenhum jogo/pick encontrado.")

    bucket = {}
    for p in picks:
        tstr = _pick_time_str(p)
        dtl  = _parse_any_dt_local(tstr)
        if not dtl or dtl.date() != tomorrow:
            continue
        gid = _game_id_from_pick(p)
        if gid not in bucket:
            bucket[gid] = {
                "hora": tstr,
                "pais": p.get("pais",""),
                "liga": p.get("campeonato",""),
                "home": p.get("mandante",""),
                "away": p.get("visitante",""),
                "total_picks": 0,
            }
        bucket[gid]["total_picks"] += 1

    if not bucket:
        return await m.answer("Não há jogos para amanhã no arquivo.")

    lines = ["📅 Jogos de amanhã (IDs):"]
    for gid, info in sorted(bucket.items(), key=lambda kv: _dt_key_or_now(kv[1].get("hora",""))):
        lines.append(
            f"<code>{gid}</code> — {info['home']} vs {info['away']} | {info['liga']} · {info['pais']} | 🕒 {as_local(info['hora'])} | picks: {info['total_picks']}"
        )

    chunk, s = [], 0
    for ln in lines:
        if s + len(ln) + 1 > 3800:
            await m.answer("\n".join(chunk)); chunk, s = [ln], len(ln)+1
        else:
            chunk.append(ln); s += len(ln)+1
    if chunk:
        await m.answer("\n".join(chunk))

@dp.message(Command("pub_stats"))
async def pub_stats(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    db = _load_published()
    lines = ["📦 Publicados:"]
    for typ in ("picks", "combos"):
        by_date = db.get(typ, {})
        for d in sorted(by_date.keys()):
            lines.append(f"• {typ} — {d}: {len(by_date[d])}")
    await m.answer("<code>" + ("\n".join(lines) or "vazio") + "</code>")

@dp.message(Command("pub_show_today"))
async def pub_show_today(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    db = _load_published()
    today = _pub_today().isoformat()
    lines = [f"🗂️ Publicados hoje ({today}):"]
    for typ in ("picks", "combos"):
        lines.append(f"\n[{typ}]")
        by_date = db.get(typ, {})
        for sig, meta in by_date.get(today, {}).items():
            if typ == "picks":
                lines.append(f"- {sig} (gid={meta.get('gid')})")
            else:
                legs = meta.get("legs", [])
                lines.append(f"- {sig} ({len(legs)} legs)")
    await m.answer("<code>" + ("\n".join(lines)) + "</code>")

@dp.message(Command("pub_reset_today"))
async def pub_reset_today(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    db = _load_published()
    today = _pub_today().isoformat()
    for k in ("picks","combos"):
        if today in db.get(k, {}):
            db[k].pop(today, None)
    _save_published(db)
    await m.answer("♻️ Registros de hoje limpos.")

# ============== DEBUG STORAGE & FETCH (ADMIN) ==============
def _human(n: int) -> str:
    for u in ["B","KB","MB","GB","TB"]:
        if n < 1024: return f"{n:.0f} {u}"
        n /= 1024
    return f"{n:.0f} PB"

def _stat_file(path: str) -> str:
    try:
        st = os.stat(path)
        mtime = datetime.fromtimestamp(st.st_mtime, tz.gettz(TZ_NAME)).strftime("%Y-%m-%d %H:%M:%S")
        return f"{path} — {_human(st.st_size)} — mtime {mtime}"
    except FileNotFoundError:
        return f"{path} — (não encontrado)"
    except Exception as e:
        return f"{path} — erro: {e}"

@dp.message(Command("which_source"))
async def which_source(m: types.Message):
    if not is_admin(m.from_user.id): return await m.answer("🚫 Acesso restrito.")
    lines = [
        "<b>Fontes configuradas</b>",
        f"ODDS_FILE = <code>{ODDS_FILE}</code>",
        f"ODDS_AMANHA_FILE = <code>{ODDS_AMANHA_FILE}</code>",
        f"PUBLISHED_DB_FILE = <code>{PUBLISHED_DB_FILE}</code>",
        f"GITHUB_RAW_BASE = <code>{os.environ.get('GITHUB_RAW_BASE','')}</code>",
        f"ODDS_HOJE_URL = <code>{os.environ.get('ODDS_HOJE_URL','')}</code>",
        f"ODDS_AMANHA_URL = <code>{os.environ.get('ODDS_AMANHA_URL','')}</code>",
        f"AGENDA_JSON_PATH = <code>{AGENDA_JSON_PATH}</code>",
        f"AGENDA_URL = <code>{os.environ.get('AGENDA_URL','')}</code>",
    ]
    await m.answer("\n".join(lines), parse_mode="HTML")

@dp.message(Command("ls_data"))
async def ls_data(m: types.Message):
    if not is_admin(m.from_user.id): return await m.answer("🚫 Acesso restrito.")
    try:
        items = os.listdir("/data")
    except Exception as e:
        return await m.answer(f"Falha em listar /data: {e}")
    lines = ["<b>/data</b>"]
    for it in sorted(items):
        p = os.path.join("/data", it)
        lines.append(_stat_file(p))
    await m.answer("\n".join(lines), parse_mode="HTML")

@dp.message(Command("fetch_update"))
async def fetch_update(m: types.Message):
    if not is_admin(m.from_user.id): return await m.answer("🚫 Acesso restrito.")
    try:
        ensure_data_files(force=True)
    except Exception as e:
        return await m.answer(f"❌ ensure_data_files falhou: {e}")
    lines = [
        "<b>Fetch concluído</b>",
        _stat_file(ODDS_FILE),
        _stat_file(ODDS_AMANHA_FILE),
        _stat_file(PUBLISHED_DB_FILE),
    ]
    await m.answer("\n".join(lines), parse_mode="HTML")

@dp.message(Command("diag_time"))
async def diag_time(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    now_u = datetime.now(timezone.utc)
    await m.answer(
        "🕒 Diagnóstico de horário\n"
        f"• TZ: {TZ_NAME}\n"
        f"• Agora (local): {now_l.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
        f"• Agora (UTC):   {now_u.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
        f"• MIN_LEAD_MIN:  {MIN_LEAD_MIN} min\n"
        f"• MODE:          {MODE}\n"
    )

@dp.message(Command("diag_odds"))
async def diag_odds(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    data = await load_odds_generic()
    # normaliza estrutura e campos antes de qualquer uso
    try:
        data = normalize_odds(data)
    except Exception:
        pass

    picks = data.get("picks", []) or []
    combos = data.get("combos", []) or []

    # -------------------- NOVO BLOCO ROBUSTO DE HORA LOCAL --------------------
    from datetime import datetime
    from dateutil import parser as dtparser, tz as dtz

    tz_sp = dtz.gettz(TZ_NAME)

    def _parse_data_hora_local(p):
        """
        1) Preferir campos normalizados: data='DD-MM-YYYY' e hora='HHhMM'
        2) Se não houver, tentar hora_utc (ISO) -> converter para America/Sao_Paulo
        3) Por último, tentar os helpers legados (_pick_time_str/_parse_any_dt_local)
        Retorna datetime timezone-aware em tz_sp ou None.
        """
        # 1) data/hora no padrão do card (DD-MM-YYYY / HHhMM)
        d = (p.get("data") or "").strip()
        h = (p.get("hora") or "").strip()
        if d and h and "h" in h:
            try:
                # '12-11-2025' e '07h30'
                hh = int(h.split("h", 1)[0])
                mm = int(h.split("h", 1)[1] or "0")
                dt_naive = datetime.strptime(d, "%d-%m-%Y").replace(hour=hh, minute=mm)
                return dt_naive.replace(tzinfo=tz_sp)
            except Exception:
                pass

        # 2) hora_utc (ISO) -> local
        hu = p.get("hora_utc") or p.get("horaISO") or p.get("kickoff_utc")
        if hu:
            try:
                dt_utc = dtparser.isoparse(hu)
                # se vier sem tz, assumir UTC
                if dt_utc.tzinfo is None:
                    from dateutil.tz import UTC
                    dt_utc = dt_utc.replace(tzinfo=UTC)
                return dt_utc.astimezone(tz_sp)
            except Exception:
                # fallback: parse genérico
                try:
                    dt_any = dtparser.parse(hu)
                    if dt_any.tzinfo is None:
                        from dateutil.tz import UTC
                        dt_any = dt_any.replace(tzinfo=UTC)
                    return dt_any.astimezone(tz_sp)
                except Exception:
                    pass

        # 3) helpers legados do seu código (se existirem)
        try:
            if "_pick_time_str" in globals() and "_parse_any_dt_local" in globals():
                dtl = _parse_any_dt_local(_pick_time_str(p))
                if dtl:
                    return dtl.astimezone(tz_sp)
        except Exception:
            pass

        return None
    # -------------------------------------------------------------------------

    # contagem rápida por hora local
    by_hour = {}
    for p in picks:
        dtl = _parse_data_hora_local(p)
        if dtl:
            h = dtl.hour
            by_hour[h] = by_hour.get(h, 0) + 1

    by_hour_str = ", ".join(f"{h:02d}h:{c}" for h, c in sorted(by_hour.items()))

    await m.answer(
        "📊 Diagnóstico de odds\n"
        f"• picks: {len(picks)} | combos: {len(combos)}\n"
        f"• distribuição por hora (local): {by_hour_str or '—'}\n"
        f"• MIN_PROB={MIN_PROB:.2f}  MIN_EV={MIN_EV:.1f}\n"
        f"• RESERVE: SLS≥{RESERVE_SLS_THRESHOLD:.1f} até {RESERVE_CUTOFF_HOUR}:00\n"
    )

def _try_parse_iso_utc(s: str) -> Optional[datetime]:
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def _extract_dt_utc_from_pick(p: Dict[str, Any]) -> Optional[datetime]:
    for k in ("dt_utc_iso","dt_utc","kickoff_utc","kickoff_iso","utc"):
        v = p.get(k)
        if isinstance(v, str) and v.strip():
            dt = _try_parse_iso_utc(v.strip())
            if dt:
                return dt
    return None

def _extract_dt_local_from_pick(p: Dict[str, Any]) -> Optional[datetime]:
    tz_sp = tz.gettz(TZ_NAME)
    dt_utc = _extract_dt_utc_from_pick(p)
    if dt_utc:
        return dt_utc.astimezone(tz_sp)

    d_local = (p.get("data_local") or p.get("date_local") or "").strip()
    h_local = (p.get("hora_local") or "").strip()
    if d_local and h_local:
        try:
            if "-" in d_local:
                y, m, d = map(int, d_local.split("-"))
            else:
                d, m, y = map(int, d_local.split("/"))
            hh, mm = map(int, h_local.split(":"))
            return datetime(y, m, d, hh, mm, tzinfo=tz_sp)
        except Exception:
            pass

    h_utc = (p.get("hora_utc") or "").strip()
    if h_utc:
        try:
            hh, mm = map(int, h_utc.split(":"))
            # sentinela 2000-01-01 indica que só veio hora; a data será assumida fora
            return datetime(2000, 1, 1, hh, mm, tzinfo=timezone.utc).astimezone(tz_sp)
        except Exception:
            pass
    return None

def _infer_list_date_local(data: Dict[str, Any], *, assume_date: Optional[date] = None) -> Optional[date]:
    tz_sp = tz.gettz(TZ_NAME)
    picks = (data.get("picks") or [])
    if not picks:
        return None

    for p in picks:
        dt_loc = _extract_dt_local_from_pick(p)
        if dt_loc and dt_loc.year != 2000:  # 2000 → caso em que só havia hora
            return dt_loc.date()

    if assume_date:
        for p in picks:
            h_utc = (p.get("hora_utc") or "").strip()
            if h_utc:
                try:
                    hh, mm = map(int, h_utc.split(":"))
                    dt_utc = datetime(assume_date.year, assume_date.month, assume_date.day, hh, mm, tzinfo=timezone.utc)
                    _ = dt_utc.astimezone(tz_sp)  # valida
                    return assume_date
                except Exception:
                    continue
    return None

# === PATCH: /diag_slots com split e escape ===
@dp.message(Command("diag_slots"))
async def diag_slots(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    # garante dados locais atualizados (fetch já implementado no ensure_data_files)
    try:
        ensure_data_files(force=False)
    except Exception:
        pass

    # carrega agenda
    agenda = _safe_load(Path(AGENDA_JSON_PATH), None) if MODE == "editorial" else None
    plan = (agenda or {}).get("schedule_plan", [])
    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    now_u = datetime.now(timezone.utc)
    today_sp = now_l.strftime("%Y-%m-%d")

    if (not plan) and (not AUTO_SCHEDULING_DEFAULT):
        return await m.answer("❌ Sem slots para hoje (agenda vazia e fallback desabilitado).")

    used_fallback = False
    if not plan and AUTO_SCHEDULING_DEFAULT:
        used_fallback = True
        plan = [
            {"time_local":"08:00","selection_rule":{"sections":["singles"],"max_cards":2}},
            {"time_local":"10:00","selection_rule":{"sections":["doubles","intra_game_combos"],"max_cards":3}},
            {"time_local":"12:00","selection_rule":{"sections":["trebles","multiples"],"max_cards":2}},
            {"time_local":"15:00","selection_rule":{"sections":["singles","inter_game_multiples"],"max_cards":3}},
            {"time_local":"18:00","selection_rule":{"sections":["multiples","intra_game_combos"],"max_cards":4}},
            {"time_local":"20:30","selection_rule":{"sections":["doubles","trebles"],"max_cards":3}},
        ]

    data = await load_data_for_date(now_l.date())

    lines = [
        f"🧪 Diagnóstico de Agenda (slots de hoje)",
        f"• MODE: {MODE} | TZ: {TZ_NAME}",
        f"• Agora (local): {now_l.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"• Agora (UTC):   {now_u.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"• Fonte da agenda: {'AGENDA_JSON_PATH (editorial)' if MODE=='editorial' else ('fallback automático' if used_fallback else '—')}",
        ""
    ]

    for idx, slot in enumerate(plan, 1):
        t_local = slot.get("time_local", "").strip()
        rule = slot.get("selection_rule", {}) or {}
        desc = slot.get("description", "") or ""
        try:
            dt_utc_iso = _to_utc_iso(today_sp, t_local)
            dt_utc = datetime.fromisoformat(dt_utc_iso.replace("Z","+00:00"))
            delta_min = int((now_u - dt_utc).total_seconds() // 60)
            when_txt = ("⏳ falta %d min" % abs(delta_min)) if delta_min < 0 else (("🟢 janela (%d min atrás)" % delta_min) if delta_min <= 10 else ("⏱️ passou faz %d min" % delta_min))
        except Exception:
            dt_utc_iso = "—"
            when_txt = "⛔ horário inválido"

        # simula seleção
        try:
            picked = await _select_by_rule(data, rule, now_local=now_l)
            pre_count = len(picked)
            sample = []
            if picked:
                k0, pay0, sls0 = picked[0]
                if k0 == "pick":
                    sample.append(f"1º: pick | {pay0.get('mandante','?')} vs {pay0.get('visitante','?')} | SLS {sls0:.1f}")
                else:
                    legs = pay0.get("legs", []) or []
                    sample.append(f"1º: combo | {len(legs)} legs | SLS {sls0:.1f}")
        except Exception as e:
            pre_count = 0
            sample = [f"erro seleção: {repr(e)}"]

        lines.append(f"#{idx} {t_local} → {dt_utc_iso} ({when_txt})")
        lines.append(f"   desc={desc or '—'} sections={rule.get('sections', '—')} max_cards={rule.get('max_cards', '—')}")
        lines.append(f"   pré-seleção agora: {pre_count} itens" + (f"  |  {', '.join(sample)}" if sample else ""))

    # enviar em chunks para evitar 'message too long'
    chunk, char_sum = [], 0
    for ln in lines:
        if char_sum + len(ln) + 1 > 3800:
            await m.answer("\n".join(chunk))
            chunk, char_sum = [ln], len(ln) + 1
        else:
            chunk.append(ln); char_sum += len(ln) + 1
    if chunk:
        await m.answer("\n".join(chunk))

# -------------------- RUN BOTH --------------------
async def run_all():
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)

    api_task = asyncio.create_task(server.serve())
    bot_task = asyncio.create_task(dp.start_polling(bot))
    enf_task = asyncio.create_task(enforce_loop())
    notify_task = asyncio.create_task(reminder_loop())

    START_SCHEDULER = os.getenv("START_SCHEDULER", "true").lower() == "true"
    tasks = [api_task, bot_task, enf_task, notify_task]
    if START_SCHEDULER:
        tasks.append(asyncio.create_task(scheduler_loop()))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        os.environ["TZ"] = TZ_NAME
    except:
        pass
    asyncio.run(run_all())