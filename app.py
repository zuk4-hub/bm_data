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

from aiogram.types import Message
from aiogram.types import ChatJoinRequest
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiohttp import ClientSession
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats,
    BotCommandScopeDefault,
    BotCommandScopeAllChatAdministrators,   
    BotCommandScopeChatAdministrators       
)

import unicodedata  # <-- novo (para normalizar acentos)


from dateutil import tz
import html
from dateutil import parser, tz
from datetime import datetime, date, timezone, timedelta


import math




# -------------------- ENV --------------------
# --- CONFIG BÁSICA / ENV ----------------------------
BOT_TOKEN  = os.getenv("BOT_TOKEN", "").strip()
GROUP_ID   = int(os.getenv("GROUP_ID", "0"))
AUTHORIZED = {int(x) for x in os.getenv("AUTHORIZED_USERS", "").replace(" ", "").split(",") if x}

# Fuso
TZ_NAME = os.getenv("TZ", "America/Sao_Paulo")

# Caminhos locais padrão
ODDS_FILE         = os.getenv("ODDS_FILE", "/data/odds1.json").strip()           # hoje: usamos odds1.json
ODDS_AMANHA_FILE  = os.getenv("ODDS_AMANHA_FILE", "/data/odds_amanha.json").strip()
AFORISMOS_FILE    = os.getenv("AFORISMOS_FILE", "/data/aforismos.json").strip()
PUBLISHED_DB_FILE = os.getenv("PUBLISHED_DB_FILE", "/data/published.json").strip()
PUBLISHED_LOG     = os.getenv("PUBLISHED_LOG", "/data/published_log.json").strip()

#-----------------------------SUPER CARDS --------------------------
# Super games / super cards
SUPER_GAMES_GLOB       = os.getenv("SUPER_GAMES_GLOB", "/data/super_jogos-*.json").strip()
SUPER_CARD_LEAD_MIN    = int(os.getenv("SUPER_CARD_LEAD_MIN", "80"))   # minutos antes do chute inicial
SUPER_CARD_WINDOW_MIN  = int(os.getenv("SUPER_CARD_WINDOW_MIN", "10"))  # janela ± em torno das 2h
ENABLE_SUPER_GAMES_LOOP = os.getenv("ENABLE_SUPER_GAMES_LOOP", "true").lower() == "true"  #false "desliga" o loop
# estado em memória dos Super Games (usado pelo super_games_loop)

# NOVOS PARÂMETROS DE AGENDA DOS SUPERCARDS
SUPER_CARD_FIRST_HOUR       = int(os.getenv("SUPER_CARD_FIRST_HOUR", "7"))   # 07h
SUPER_CARD_FIRST_MINUTE     = int(os.getenv("SUPER_CARD_FIRST_MINUTE", "15"))# 07h15
SUPER_CARD_LAST_MIN_BEFORE  = int(os.getenv("SUPER_CARD_LAST_MIN_BEFORE", "45"))  # último card até 45' antes do KO

# Estado em memória dos Super Games (por jogo)
GLOBAL_SUPER_GAMES: Dict[str, Dict[str, Any]] = {}
#------------------------------------------------------------------------


# Histórico rotativo (em dias)
HIST_KEEP_DAYS          = int(os.getenv("HIST_KEEP_DAYS", "7"))  # --------- Histórico de JSON em /data (Render)

# Base remota (não sobrescrever depois)
GITHUB_RAW_BASE = os.getenv("GITHUB_RAW_BASE", "https://raw.githubusercontent.com/zuk4-hub/bm_data/main").rstrip("/")

# URLs remotas (defaults sensatos; podem ser override por ENV)
ODDS_HOJE_URL   = os.getenv("ODDS_HOJE_URL",   f"{GITHUB_RAW_BASE}/odds1.json").strip()
ODDS_AMANHA_URL = os.getenv("ODDS_AMANHA_URL", f"{GITHUB_RAW_BASE}/odds_amanha.json").strip()
AGENDA_URL      = os.getenv("AGENDA_URL",      f"{GITHUB_RAW_BASE}/agenda_editorial.json").strip()
AFORISMOS_URL   = os.getenv("AFORISMOS_URL",   f"{GITHUB_RAW_BASE}/aforismos.json").strip()

# Limiares e outros params
MIN_LEAD_MIN              = int(os.getenv("MIN_LEAD_MIN", "10"))
RESERVE_CUTOFF_HOUR       = int(os.getenv("RESERVE_CUTOFF_HOUR", "15"))
RESERVE_SLS_THRESHOLD     = float(os.getenv("RESERVE_SLS_THRESHOLD", "75"))
RESERVE_EXPIRY_RELEASE_MIN= int(os.getenv("RESERVE_EXPIRY_RELEASE_MIN", "120"))
COMBOS_TYPES_ORDER        = os.getenv("COMBOS_TYPES_ORDER", "duplo,triplo,multi")
MIN_PROB     = float(os.getenv("MIN_PROB", "0.60"))
MIN_EV       = float(os.getenv("MIN_EV", "0.0"))

# ---------------- Pagamentos -------------------------
CAKTO_SECRET = os.getenv("CAKTO_SECRET", "").strip()
CAKTO_SECRET_KEY = os.getenv("CAKTO_SECRET_KEY", "").strip()
CHECKOUT_URL = os.getenv("CHECKOUT_URL", "").strip()
REF_PARAM    = os.getenv("REF_PARAM", "ref").strip() or "ref"
DEBUG_TOKEN = os.getenv("DEBUG_TOKEN", "").strip()


PORT                = int(os.getenv("PORT", "8000"))  # Render injeta $PORT
INVITES_PATH        = os.getenv("INVITES_PATH", "/data/invites_map.json").strip()
GITHUB_TOKEN        = os.getenv("GITHUB_TOKEN", "").strip()
FETCH_MIN_INTERVAL  = int(os.getenv("FETCH_MIN_INTERVAL", "120"))
SLS_WP              = float(os.getenv("SLS_WP", "0.8"))
SLS_WE              = float(os.getenv("SLS_WE", "0.2"))
DIAMOND_SLS_THRESHOLD = float(os.getenv("DIAMOND_SLS_THRESHOLD", "90.0"))

REMINDER_INTERVAL_SEC = int(os.getenv("REMINDER_INTERVAL_SEC", "1800"))

# Scheduler/editorial
MODE                     = os.getenv("MODE", "editorial").strip().lower()      # 'editorial' | 'auto'
AGENDA_JSON_PATH         = os.getenv("AGENDA_JSON_PATH", "/data/agenda_editorial.json").strip()
AUTO_SCHEDULING_DEFAULT  = os.getenv("AUTO_SCHEDULING_DEFAULT", "true").lower() == "true"
ENABLE_FALLBACK_SELECTION= os.getenv("ENABLE_FALLBACK_SELECTION", "true").lower() == "true"
MAX_PUBLICATIONS_PER_DAY = int(os.getenv("MAX_PUBLICATIONS_PER_DAY", "100"))
HOURLY_MAX               = int(os.getenv("MAX_PUBLICATIONS_PER_HOUR", "3"))
MINUTES_BETWEEN_REPOST   = int(os.getenv("MINUTES_BETWEEN_REPOST", "240"))
TELEGRAM_HTML_LIMIT = 4096
TELEGRAM_SAFE_BUDGET = 3600  # margem para evitar erro (tags HTML contam no parse)


if not BOT_TOKEN or not GROUP_ID:
    raise RuntimeError("Defina BOT_TOKEN e GROUP_ID (-100...) no Environment.")
# ---------------------------------------------------

# -------------------- BOT CORE --------------------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher()

async def _setup_bot_commands():
    """
    Escopos:
      - DM (todos): comandos públicos de assinante
      - Canal (apenas administradores): comandos administrativos completos
      - Grupos (se usados): comandos administrativos (ou reduzir conforme preferir)
    """
    public_cmds = [
        BotCommand(command="help", description="Ajuda"),
        BotCommand(command="status_sub", description="Status da assinatura"),
        BotCommand(command="join", description="Assinar"),
        BotCommand(command="refer", description="Indicar amigo"),
        BotCommand(command="whoami", description="Seu ID"),
        BotCommand(command="ping", description="Ping"),
    ]

    admin_cmds = [
        BotCommand(command="help", description="Ajuda"),
        BotCommand(command="which_source", description="Mostrar fontes e paths"),
        BotCommand(command="ls_data", description="Listar /data"),
        BotCommand(command="fetch_update", description="Forçar fetch odds + agenda + aforismos"),
        BotCommand(command="games_today", description="Listar jogos de hoje (com IDs)"),
        BotCommand(command="games_tomorrow", description="Listar jogos de amanhã (com IDs)"),
        BotCommand(command="post_pick", description="Publicar 1 pick (ou melhor SLS)"),
        BotCommand(command="post_combo", description="Publicar 1 combo (regra de slot)"),
        BotCommand(command="post_combos", description="Publicar N combos do arquivo"),
        BotCommand(command="post_coruja", description="Publicar Corujão (00:00–07:00)"),
        BotCommand(command="pub_show_today", description="Mostrar publicados hoje"),
        BotCommand(command="pub_reset_today", description="Zerar publicados de hoje"),
        BotCommand(command="diag_time", description="Relógios local/UTC"),
        BotCommand(command="diag_odds", description="Stats dos JSON de odds"),
        BotCommand(command="enforce_now", description="Executar enforcer agora"),
        BotCommand(command="sub_set", description="Editar assinatura de um usuário"),
        BotCommand(command="status_user", description="Status detalhado do usuário"),
        BotCommand(command="entrar", description="Reemitir convite (assinante ativo)"),
        BotCommand(command="post_today", description="Teste: publicar no grupo"),
        BotCommand(command="post_from_file", description="Publicar picks do arquivo (limitado)"),
        BotCommand(command="supercard_preview", description="Prévia dos super cards de hoje"),

    ]

    # DM (todos usuários)
    await bot.set_my_commands(public_cmds, scope=BotCommandScopeAllPrivateChats())

    # Canal: exibir menu só para administradores do canal
    # (IMPORTANTE para você ver tudo dentro do canal)
    try:
        await bot.set_my_commands(admin_cmds, scope=BotCommandScopeChatAdministrators(chat_id=GROUP_ID))
    except Exception as e:
        print("[SETUP_CMDS][WARN] ChatAdministrators scope failed:", repr(e))

    # Grupos (se usar algum): admins
    try:
        await bot.set_my_commands(admin_cmds, scope=BotCommandScopeAllGroupChats())
    except Exception as e:
        print("[SETUP_CMDS][WARN] AllGroupChats scope failed:", repr(e))

    # Default (fallback): deixar o básico público
    try:
        await bot.set_my_commands(public_cmds, scope=BotCommandScopeDefault())
    except Exception as e:
        print("[SETUP_CMDS][WARN] Default scope failed:", repr(e))

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

def cleanup_history():
    """
    Remove automaticamente arquivos antigos de /data para:
      • odds*.json (incluindo odds1..30 e quaisquer odds_x.json manuais)
      • super_jogos-*.json

    Mantém apenas arquivos cuja data-alvo (target_date_local / target_date_local do supercard)
    esteja dentro da janela de HIST_KEEP_DAYS dias para trás.
    Para arquivos sem data clara no JSON, usa a data de modificação (mtime),
    o que limpa inclusive odds_hoje / odds_amanha antigos.
    """
    try:
        tz_sp = tz.gettz(TZ_NAME)
        today = datetime.now(tz_sp).date()
        cutoff = today - timedelta(days=max(HIST_KEEP_DAYS, 0))

        patterns = ["odds*.json", "super_jogos-*.json"]

        for pat in patterns:
            for fp in DATA_DIR.glob(pat):
                try:
                    obj = _read_json_silent(fp)
                    fdate = None

                    if isinstance(obj, dict):
                        # odds*.json
                        if pat.startswith("odds"):
                            hdr = obj.get("odds_file_header") or {}
                            meta = obj.get("meta") or {}
                            ds = (hdr.get("target_date_local") or meta.get("target_date_local") or "").strip()
                            if ds:
                                try:
                                    y, m, d = map(int, ds.split("-"))
                                    fdate = date(y, m, d)
                                except Exception:
                                    fdate = None

                        # super_jogos-*.json
                        elif pat.startswith("super_jogos"):
                            hdr = obj.get("supercard_header") or {}
                            ds = (hdr.get("target_date_local") or "").strip()
                            if ds:
                                try:
                                    y, m, d = map(int, ds.split("-"))
                                    fdate = date(y, m, d)
                                except Exception:
                                    fdate = None

                    # fallback para mtime (inclusive para odds_hoje / odds_amanha antigos)
                    if fdate is None:
                        fdate = datetime.fromtimestamp(fp.stat().st_mtime, tz_sp).date()

                    if fdate < cutoff:
                        print(f"[HIST][DEL] {fp} (data={fdate}, cutoff={cutoff})")
                        fp.unlink(missing_ok=True)

                except Exception as e:
                    print("[HIST][ERR]", fp, repr(e))
    except Exception as e:
        print("[HIST][FATAL]", repr(e))


async def _try_fetch_latest_odds() -> None:
    """
    Tenta baixar odds do dia para /data, respeitando variáveis de ambiente.
    Não levanta exceção; é best-effort.
    """
    import aiohttp, asyncio
    targets = []
    # 1) ODDS_URL explícita (se configurada)
    if ODDS_URL:
        targets.append(("odds_auto.json", ODDS_URL))
    # 2) URLs padrão do repositório
    if ODDS_HOJE_URL:
        targets.append(("odds_hoje.json", ODDS_HOJE_URL))
    # 3) Amanhã (útil quando já virou o dia)
    if ODDS_AMANHA_URL:
        targets.append(("odds_amanha.json", ODDS_AMANHA_URL))

    async with aiohttp.ClientSession() as sess:
        for fname, url in targets:
            try:
                async with sess.get(url, timeout=20) as r:
                    if r.status == 200:
                        txt = await r.text()
                        p = Path("/data") / fname
                        p.write_text(txt, encoding="utf-8")
            except Exception:
                continue  # silencioso: seguimos tentando outras fontes

async def load_odds_generic() -> Dict[str, Any]:
    """
    Escolhe odds*.json do *dia local* (TZ_NAME) por cabeçalho target_date_local.
    Se não houver, tenta baixar e reavaliar. Não usa fallback cego para arquivos antigos.
    """
    tz_sp = tz.gettz(TZ_NAME)
    today_sp = datetime.now(tz_sp).strftime("%Y-%m-%d")

    def _best_obj_for(date_iso: str) -> Optional[Dict[str, Any]]:
        best_obj, best_ga = None, ""
        for fp in Path("/data").glob("odds*.json"):
            obj = _read_json_silent(fp)
            if not isinstance(obj, dict):
                continue
            hdr = obj.get("odds_file_header") or {}
            meta = obj.get("meta") or {}
            d1 = (hdr.get("target_date_local") or meta.get("target_date_local") or "").strip()
            if d1 != date_iso:
                continue
            ga = (meta.get("generated_at") or "")
            if ga > best_ga:
                best_ga, best_obj = ga, obj
        return best_obj

    # 1) tenta local de primeira
    obj = _best_obj_for(today_sp)
    if obj:
        return obj

    # 2) tenta baixar (ODDS_URL / ODDS_HOJE_URL / ODDS_AMANHA_URL)
    await _try_fetch_latest_odds()

    # 3) reavalia local (hoje)
    obj = _best_obj_for(today_sp)
    if obj:
        return obj

    # 4) *opcional*: se for antes de 05:00, permitir ontem (para cobrir fuso/madrugadas)
    now_l = datetime.now(tz_sp)
    if now_l.hour < 5:
        yday = (now_l.date() - timedelta(days=1)).strftime("%Y-%m-%d")
        obj = _best_obj_for(yday)
        if obj:
            return obj

    # 5) por fim, vazio (não inventa e nem retrocede 2 dias sem critério)
    return {}


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



# ===== Header-based odds loader (CANÔNICO) =====
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

DATA_DIR = Path("/data")  # mantenha este caminho como raiz de dados

def _read_json_silent(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def load_odds_for_date(date_local: str) -> Optional[Dict[str, Any]]:
    """
    Procura em /data/odds*.json o arquivo cujo
    odds_file_header.target_date_local (ou meta.target_date_local) == date_local.
    Se houver múltiplos candidatos, escolhe o de 'meta.generated_at' mais recente.
    """
    candidates = []
    for fp in DATA_DIR.glob("odds*.json"):
        obj = _read_json_silent(fp)
        if not obj:
            continue
        hdr  = obj.get("odds_file_header") or {}
        meta = obj.get("meta") or {}
        td   = (hdr.get("target_date_local") or meta.get("target_date_local") or "").strip()
        if td == date_local:
            ga = (meta.get("generated_at") or "")
            candidates.append((ga, fp, obj))

    if not candidates:
        return None

    # ordena por generated_at desc (string ISO compara bem)
    candidates.sort(key=lambda t: t[0], reverse=True)
    return candidates[0][2]

async def load_odds_by_date(d) -> Dict[str, Any]:
    """
    Carrega o odds*.json do dia 'd' (date), via cabeçalho.
    """
    target_iso = d.strftime("%Y-%m-%d")
    obj = load_odds_for_date(target_iso)
    return obj or {"picks": [], "combos": [], "corujao": {"picks": [], "combos": []}}

# ---- Odds loader seguro (sempre retorna dict) ----
async def load_odds_generic() -> Dict[str, Any]:
    """
    Retorna o odds*.json ativo do dia (target_date_local == hoje em TZ_NAME).
    Fallback: odds2.json, odds_hoje.json, odds1.json (nessa ordem).
    Nunca retorna None.
    """
    tz_sp = tz.gettz(TZ_NAME)
    today_sp = datetime.now(tz_sp).strftime("%Y-%m-%d")

    # 1) Tenta pelo header target_date_local + maior generated_at
    best_obj = None
    best_ga = ""
    try:
        for fp in Path("/data").glob("odds*.json"):
            obj = _read_json_silent(fp)
            if not isinstance(obj, dict):
                continue
            hdr = obj.get("odds_file_header") or {}
            meta = obj.get("meta") or {}
            d1 = (hdr.get("target_date_local") or meta.get("target_date_local") or "").strip()
            if d1 != today_sp:
                continue
            ga = (meta.get("generated_at") or "")
            if ga > best_ga:
                best_ga = ga
                best_obj = obj
    except Exception:
        pass

    # 2) Fallbacks explícitos
    if not isinstance(best_obj, dict):
        for nm in ("odds2.json", "odds_hoje.json", "odds1.json"):
            fp = Path("/data") / nm
            if fp.exists():
                obj = _read_json_silent(fp)
                if isinstance(obj, dict):
                    best_obj = obj
                    break

    return best_obj or {}


async def load_odds_hoje() -> Dict[str, Any]:
    tz_sp = tz.gettz(TZ_NAME)
    today = datetime.now(tz_sp).date()
    return await load_odds_by_date(today)

async def load_odds_amanha() -> Dict[str, Any]:
    tz_sp = tz.gettz(TZ_NAME)
    tomorrow = (datetime.now(tz_sp).date() + timedelta(days=1))
    return await load_odds_by_date(tomorrow)
# ===== /Header-based odds loader =====


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


# =========================
# Relógios por horário (00/30)
# =========================

_CLOCK_EMOJI_MAP = {
    # 12h / 12h30
    (0, 0): "🕛",  # 12:00
    (0, 30): "🕧", # 12:30
    # 1h / 1h30
    (1, 0): "🕐",
    (1, 30): "🕜",
    # 2h / 2h30
    (2, 0): "🕑",
    (2, 30): "🕝",
    # 3h / 3h30
    (3, 0): "🕒",
    (3, 30): "🕞",
    # 4h / 4h30
    (4, 0): "🕓",
    (4, 30): "🕟",
    # 5h / 5h30
    (5, 0): "🕔",
    (5, 30): "🕠",
    # 6h / 6h30
    (6, 0): "🕕",
    (6, 30): "🕡",
    # 7h / 7h30
    (7, 0): "🕖",
    (7, 30): "🕢",
    # 8h / 8h30
    (8, 0): "🕗",
    (8, 30): "🕣",
    # 9h / 9h30
    (9, 0): "🕘",
    (9, 30): "🕤",
    # 10h / 10h30
    (10, 0): "🕙",
    (10, 30): "🕥",
    # 11h / 11h30
    (11, 0): "🕚",
    (11, 30): "🕦",
}

def _clock_emoji_for_hhmm(hhmm: str) -> str:
    """
    Recebe 'HHhMM' ou 'HH:MM' em horário LOCAL e devolve o emoji de relógio
    correspondente, sempre em hora cheia ou meia hora.

    Se os minutos não forem 00 ou 30, arredonda para o slot mais próximo.
    """
    if not hhmm:
        return "🕒"  # fallback neutro

    s = hhmm.strip().replace("h", ":")
    try:
        hh_part, mm_part = s.split(":", 1)
        h24 = int(hh_part)
        m = int(mm_part[:2])
    except Exception:
        return "🕒"

    # arredonda minutos para 00 ou 30
    if m < 15:
        m_rounded = 0
    elif m < 45:
        m_rounded = 30
    else:
        m_rounded = 0
        h24 = (h24 + 1) % 24

    # converte para relógio 12h (0 -> 12)
    h12 = h24 % 12  # 0 representa 12

    return _CLOCK_EMOJI_MAP.get((h12, m_rounded), "🕒")

# --------------------------- FUNÇÕES DISPARO SEGMENTADO DE SUPERCARDS (JOGÃO E JOGAÇO) --------

def _super_num_cards_for_game(g):
    cards = fmt_super_game_card(g)
    if isinstance(cards, list):
        return len(cards)
    return 1

def _super_first_dt(today_local):
    return datetime(
        today_local.year, today_local.month, today_local.day,
        SUPER_CARD_FIRST_HOUR,
        SUPER_CARD_FIRST_MINUTE,
        0,
        tzinfo=tz.gettz(TZ_NAME)
    )

def _super_last_dt(dt_kickoff):
    return dt_kickoff - timedelta(minutes=SUPER_CARD_LAST_MIN_BEFORE)

def _super_compute_schedule_for_game(g, today_local):
    dt_k = _super_kickoff_dt_local(g)
    if not dt_k:
        return []

    n_cards = _super_num_cards_for_game(g)
    if n_cards <= 0:
        return []

    first_dt = _super_first_dt(today_local)
    last_dt  = _super_last_dt(dt_k)

    if last_dt <= first_dt:
        return [first_dt] * n_cards

    total_seconds = (last_dt - first_dt).total_seconds()
    interval = total_seconds / max(n_cards, 1)

    result = []
    for i in range(n_cards):
        t = first_dt + timedelta(seconds=i * interval)
        result.append(t)

    return result


# ----------------- BANDEIRAS POR PAÍS -----------------

# Mapa básico país → bandeira (usar sempre chaves em minúsculo)
# Você pode ir expandindo este dicionário com base na tabela completa que mandou.
COUNTRY_FLAG_MAP = {
    # Lista principal (CLDR Short Name → emoji), tudo em minúsculo

    "ascension island": "🇦🇨",
    "andorra": "🇦🇩",
    "united arab emirates": "🇦🇪",
    "afghanistan": "🇦🇫",
    "antigua & barbuda": "🇦🇬",
    "anguilla": "🇦🇮",
    "albania": "🇦🇱",
    "armenia": "🇦🇲",
    "angola": "🇦🇴",
    "antarctica": "🇦🇶",
    "argentina": "🇦🇷",
    "american samoa": "🇦🇸",
    "austria": "🇦🇹",
    "australia": "🇦🇺",
    "aruba": "🇦🇼",
    "åland islands": "🇦🇽",
    "azerbaijan": "🇦🇿",

    "bosnia & herzegovina": "🇧🇦",
    "barbados": "🇧🇧",
    "bangladesh": "🇧🇩",
    "belgium": "🇧🇪",
    "burkina faso": "🇧🇫",
    "bulgaria": "🇧🇬",
    "bahrain": "🇧🇭",
    "burundi": "🇧🇮",
    "benin": "🇧🇯",
    "st. barthélemy": "🇧🇱",
    "bermuda": "🇧🇲",
    "brunei": "🇧🇳",
    "bolivia": "🇧🇴",
    "caribbean netherlands": "🇧🇶",
    "brazil": "🇧🇷",
    "bahamas": "🇧🇸",
    "bhutan": "🇧🇹",
    "bouvet island": "🇧🇻",
    "botswana": "🇧🇼",
    "belarus": "🇧🇾",
    "belize": "🇧🇿",

    "canada": "🇨🇦",
    "cocos (keeling) islands": "🇨🇨",
    "congo - kinshasa": "🇨🇩",
    "central african republic": "🇨🇫",
    "congo - brazzaville": "🇨🇬",
    "switzerland": "🇨🇭",
    "côte d’ivoire": "🇨🇮",
    "cook islands": "🇨🇰",
    "chile": "🇨🇱",
    "cameroon": "🇨🇲",
    "china": "🇨🇳",
    "colombia": "🇨🇴",
    "clipperton island": "🇨🇵",
    "sark": "🇨🇶",
    "costa rica": "🇨🇷",
    "cuba": "🇨🇺",
    "cape verde": "🇨🇻",
    "curaçao": "🇨🇼",
    "christmas island": "🇨🇽",
    "cyprus": "🇨🇾",
    "czechia": "🇨🇿",

    "germany": "🇩🇪",
    "diego garcia": "🇩🇬",
    "djibouti": "🇩🇯",
    "denmark": "🇩🇰",
    "dominica": "🇩🇲",
    "dominican republic": "🇩🇴",
    "algeria": "🇩🇿",

    "ceuta & melilla": "🇪🇦",
    "ecuador": "🇪🇨",
    "estonia": "🇪🇪",
    "egypt": "🇪🇬",
    "western sahara": "🇪🇭",
    "eritrea": "🇪🇷",
    "spain": "🇪🇸",
    "ethiopia": "🇪🇹",
    "european union": "🇪🇺",

    "finland": "🇫🇮",
    "fiji": "🇫🇯",
    "falkland islands": "🇫🇰",
    "micronesia": "🇫🇲",
    "faroe islands": "🇫🇴",
    "france": "🇫🇷",

    "gabon": "🇬🇦",
    "united kingdom": "🇬🇧",
    "grenada": "🇬🇩",
    "georgia": "🇬🇪",
    "french guiana": "🇬🇫",
    "guernsey": "🇬🇬",
    "ghana": "🇬🇭",
    "gibraltar": "🇬🇮",
    "greenland": "🇬🇱",
    "gambia": "🇬🇲",
    "guinea": "🇬🇳",
    "guadeloupe": "🇬🇵",
    "equatorial guinea": "🇬🇶",
    "greece": "🇬🇷",
    "south georgia & south sandwich islands": "🇬🇸",
    "guatemala": "🇬🇹",
    "guam": "🇬🇺",
    "guinea-bissau": "🇬🇼",
    "guyana": "🇬🇾",

    "hong kong sar china": "🇭🇰",
    "heard & mcdonald islands": "🇭🇲",
    "honduras": "🇭🇳",
    "croatia": "🇭🇷",
    "haiti": "🇭🇹",
    "hungary": "🇭🇺",

    "canary islands": "🇮🇨",
    "indonesia": "🇮🇩",
    "ireland": "🇮🇪",
    "israel": "🇮🇱",
    "isle of man": "🇮🇲",
    "india": "🇮🇳",
    "british indian ocean territory": "🇮🇴",
    "iraq": "🇮🇶",
    "iran": "🇮🇷",
    "iceland": "🇮🇸",
    "italy": "🇮🇹",

    "jersey": "🇯🇪",
    "jamaica": "🇯🇲",
    "jordan": "🇯🇴",
    "japan": "🇯🇵",

    "kenya": "🇰🇪",
    "kyrgyzstan": "🇰🇬",
    "cambodia": "🇰🇭",
    "kiribati": "🇰🇮",
    "comoros": "🇰🇲",
    "st. kitts & nevis": "🇰🇳",
    "north korea": "🇰🇵",
    "south korea": "🇰🇷",
    "kuwait": "🇰🇼",
    "cayman islands": "🇰🇾",
    "kazakhstan": "🇰🇿",

    "laos": "🇱🇦",
    "lebanon": "🇱🇧",
    "st. lucia": "🇱🇨",
    "liechtenstein": "🇱🇮",
    "sri lanka": "🇱🇰",
    "liberia": "🇱🇷",
    "lesotho": "🇱🇸",
    "lithuania": "🇱🇹",
    "luxembourg": "🇱🇺",
    "latvia": "🇱🇻",
    "libya": "🇱🇾",

    "morocco": "🇲🇦",
    "monaco": "🇲🇨",
    "moldova": "🇲🇩",
    "montenegro": "🇲🇪",
    "st. martin": "🇲🇫",
    "madagascar": "🇲🇬",
    "marshall islands": "🇲🇭",
    "north macedonia": "🇲🇰",
    "mali": "🇲🇱",
    "myanmar (burma)": "🇲🇲",
    "mongolia": "🇲🇳",
    "macao sar china": "🇲🇴",
    "northern mariana islands": "🇲🇵",
    "martinique": "🇲🇶",
    "mauritania": "🇲🇷",
    "montserrat": "🇲🇸",
    "malta": "🇲🇹",
    "mauritius": "🇲🇺",
    "maldives": "🇲🇻",
    "malawi": "🇲🇼",
    "mexico": "🇲🇽",
    "malaysia": "🇲🇾",
    "mozambique": "🇲🇿",

    "namibia": "🇳🇦",
    "new caledonia": "🇳🇨",
    "niger": "🇳🇪",
    "norfolk island": "🇳🇫",
    "nigeria": "🇳🇬",
    "nicaragua": "🇳🇮",
    "netherlands": "🇳🇱",
    "norway": "🇳🇴",
    "nepal": "🇳🇵",
    "nauru": "🇳🇷",
    "niue": "🇳🇺",
    "new zealand": "🇳🇿",

    "oman": "🇴🇲",

    "panama": "🇵🇦",
    "peru": "🇵🇪",
    "french polynesia": "🇵🇫",
    "papua new guinea": "🇵🇬",
    "philippines": "🇵🇭",
    "pakistan": "🇵🇰",
    "poland": "🇵🇱",
    "st. pierre & miquelon": "🇵🇲",
    "pitcairn islands": "🇵🇳",
    "puerto rico": "🇵🇷",
    "palestinian territories": "🇵🇸",
    "portugal": "🇵🇹",
    "palau": "🇵🇼",
    "paraguay": "🇵🇾",

    "qatar": "🇶🇦",

    "réunion": "🇷🇪",
    "romania": "🇷🇴",
    "serbia": "🇷🇸",
    "russia": "🇷🇺",
    "rwanda": "🇷🇼",

    "saudi arabia": "🇸🇦",
    "solomon islands": "🇸🇧",
    "seychelles": "🇸🇨",
    "sudan": "🇸🇩",
    "sweden": "🇸🇪",
    "singapore": "🇸🇬",
    "st. helena": "🇸🇭",
    "slovenia": "🇸🇮",
    "svalbard & jan mayen": "🇸🇯",
    "slovakia": "🇸🇰",
    "sierra leone": "🇸🇱",
    "san marino": "🇸🇲",
    "senegal": "🇸🇳",
    "somalia": "🇸🇴",
    "suriname": "🇸🇷",
    "south sudan": "🇸🇸",
    "são tomé & príncipe": "🇸🇹",
    "el salvador": "🇸🇻",
    "sint maarten": "🇸🇽",
    "syria": "🇸🇾",
    "eswatini": "🇸🇿",

    "tristan da cunha": "🇹🇦",
    "turks & caicos islands": "🇹🇨",
    "chad": "🇹🇩",
    "french southern territories": "🇹🇫",
    "togo": "🇹🇬",
    "thailand": "🇹🇭",
    "tajikistan": "🇹🇯",
    "tokelau": "🇹🇰",
    "timor-leste": "🇹🇱",
    "turkmenistan": "🇹🇲",
    "tunisia": "🇹🇳",
    "tonga": "🇹🇴",
    "türkiye": "🇹🇷",
    "trinidad & tobago": "🇹🇹",
    "tuvalu": "🇹🇻",
    "taiwan": "🇹🇼",
    "tanzania": "🇹🇿",

    "ukraine": "🇺🇦",
    "uganda": "🇺🇬",
    "u.s. outlying islands": "🇺🇲",
    "united nations": "🇺🇳",
    "united states": "🇺🇸",
    "uruguay": "🇺🇾",
    "uzbekistan": "🇺🇿",

    "vatican city": "🇻🇦",
    "st. vincent & grenadines": "🇻🇨",
    "venezuela": "🇻🇪",
    "british virgin islands": "🇻🇬",
    "u.s. virgin islands": "🇻🇮",
    "vietnam": "🇻🇳",
    "vanuatu": "🇻🇺",

    "wallis & futuna": "🇼🇫",
    "samoa": "🇼🇸",

    "kosovo": "🇽🇰",

    "yemen": "🇾🇪",
    "mayotte": "🇾🇹",

    "south africa": "🇿🇦",
    "zambia": "🇿🇲",
    "zimbabwe": "🇿🇼",

    # Subdivision flags
    "england": "🇬🇧",
    "scotland": "🇬🇧",
    "wales": "🇬🇧",

    # Internacional / sem bandeira nacional
    "international": "🇺🇳",
    "world": "🇺🇳",
    "europe": "🇺🇳",
    "south america": "🇺🇳",
    "north & central america": "🇺🇳",
    "asia": "🇺🇳",
    "africa": "🇺🇳",
    "oceania": "🇺🇳",
    "united nations": "🇺🇳",
}


def get_country_flag(country: Optional[str], league: Optional[str] = None) -> str:
    """
    Devolve a bandeira do país da liga.
    - Se não encontrar ou parecer competição internacional, usa 🇺🇳.
    - country e league vêm do odds.json (pais/country e campeonato/league).
    """
    if not country:
        return "🇺🇳"

    c = str(country).strip().lower()

    # Normalizações básicas em PT → EN (caso você use 'Brasil', 'Inglaterra' etc.)
    ALIAS = {
        "brasil": "brazil",
        "inglaterra": "england",
        "escócia": "scotland",
        "escocia": "scotland",
        "país de gales": "wales",
        "pais de gales": "wales",
        "eua": "united states",
        "estados unidos": "united states",
    }
    c = ALIAS.get(c, c)

    # Se tiver no mapa, retorna
    flag = COUNTRY_FLAG_MAP.get(c)
    if flag:
        return flag

    # Se parecer rótulo de confederação/continente → ONU
    if any(k in c for k in ["europe", "world", "international", "liga dos campeões", "champions league"]):
        return "🇺🇳"

    # Fallback padrão: ONU
    return "🇺🇳"



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

# ====== NOVOS HELPERS PARA GID ESTÁVEL ======

def _strip_accents(s: str) -> str:
    if not s:
        return ""
    return unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")

def _norm_team(x: str) -> str:
    return _strip_accents(x).lower().strip()

def _kick_date_local_str(p: Dict[str, Any]) -> str:
    """YYYYMMDD do kickoff em TZ São Paulo; vazio se não disponível."""
    dt = _parse_any_dt_local(_pick_time_str(p))
    return dt.strftime("%Y%m%d") if dt else ""

# ====== SUBSTITUIR A FUNÇÃO ANTIGA PELO NOVO GID ======
def _game_id_from_pick(p: Dict[str, Any]) -> str:
    """
    GID estável por JOGO (não por pick/mercado):
    md5(YYYYMMDD | mandante_norm | visitante_norm)[:10]
    """
    ymd = _kick_date_local_str(p)
    home = _norm_team(p.get("mandante"))
    away = _norm_team(p.get("visitante"))
    sig = f"{ymd}|{home}|{away}"
    return hashlib.md5(sig.encode("utf-8")).hexdigest()[:10]


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


BRAND_LINE = "\n".join([
    "|<i>Data, ethics and the beautiful game</i>|" 
    "@betmasterson"
   
])


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

def _count_published_picks_for_gid(d, gid: str) -> int:
    """
    Quantos picks deste jogo (gid) já foram publicados nessa data d (YYYY-MM-DD).
    Usa o mesmo banco de publicados do already_published_pick / mark_published_pick.
    """
    if not d or not gid:
        return 0
    db = _load_published()
    day_key = d.isoformat()
    day_picks = (db.get("picks", {}) or {}).get(day_key, {}) or {}
    count = 0
    for v in day_picks.values():
        if isinstance(v, dict) and v.get("gid") == gid:
            count += 1
    return count


def _best_pick_for_gid(picks: List[Dict[str, Any]], gid: str) -> Optional[Dict[str, Any]]:
    """Retorna o melhor pick (por SLS) dentre os picks cujo jogo gera o gid informado."""
    def _sls(p):
        pr = float(p.get("prob_real", 0) or 0)
        ev = _to_float_pct(p.get("ev", 0) or 0)
        return sls_score(pr, ev)
    candidates = []
    for p in picks:
        try:
            if _game_id_from_pick(p) == gid:
                candidates.append(p)
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=_sls, reverse=True)
    return candidates[0]

@dp.message(Command("post_pick"))
async def post_pick(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    args = (m.text or "").split(maxsplit=1)
    gid = args[1].strip() if len(args) >= 2 else None

    data = await load_odds_generic()
    try:
        data = normalize_odds(data)
    except Exception:
        pass

    picks = list(data.get("picks", []) or [])
    if not picks:
        return await m.answer("❌ Nenhum pick disponível.")

    # Se não recebeu GID, escolhe o melhor SLS com lead ok e não publicado
    if not gid:
        def _sls(p):
            pr = float(p.get("prob_real", 0) or 0)
            ev = _to_float_pct(p.get("ev", 0) or 0)
            return sls_score(pr, ev)
        tz_sp = tz.gettz(TZ_NAME)
        now_l = datetime.now(tz_sp)
        candidates = []
        for p in picks:
            t = _pick_time_str(p)
            if not _time_ok_lead(t, now_l, MIN_LEAD_MIN):
                continue
            if already_published_pick(p):
                continue
            candidates.append(p)
        if not candidates:
            return await m.answer("Sem pick elegível agora (lead ou já publicado).")
        pk = sorted(candidates, key=_sls, reverse=True)[0]
    else:
        pk = _best_pick_for_gid(picks, gid)
        if not pk:
            return await m.answer("GID não encontrado entre os picks.")
        tz_sp = tz.gettz(TZ_NAME)
        now_l = datetime.now(tz_sp)
        if not _time_ok_lead(_pick_time_str(pk), now_l, MIN_LEAD_MIN):
            return await m.answer("Kickoff muito próximo para este jogo.")
        if already_published_pick(pk):
            return await m.answer("Pick deste jogo já foi publicado hoje.")

    # saneia odds
    if isinstance(pk.get("odd_mercado"), str):
        pk["odd_mercado"] = _to_float_odd(pk["odd_mercado"])
    if isinstance(pk.get("fair_odd"), str):
        pk["fair_odd"] = _to_float_odd(pk["fair_odd"])

    try:
        await bot.send_message(GROUP_ID, fmt_pick(pk))
        mark_published_pick(pk)
        return await m.answer("✅ Pick publicado no canal.")
    except Exception as e:
        return await m.answer(f"❌ Falha ao publicar no canal.\n<code>{e}</code>")


def _combo_hash(c: Dict[str, Any]) -> str:
    legs = c.get("legs", []) or []
    legs_s = "|".join(
        json.dumps(l, ensure_ascii=False, sort_keys=True)
        for l in legs
    )
    base = f"{legs_s}|{c.get('odd_combo','')}|{c.get('fair_combo','')}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()[:12]

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
    """
    prob_real: 0..1
    ev: EV em % (ex.: 15.3 significa +15.3%)
    SLS combina probabilidade e EV normalizado.
    """
    p = max(0.0, min(1.0, float(prob_real)))
    e = max(0.0, float(ev))  # EV já em %
    # normaliza EV% em [0,1] usando 30% como teto "ideal"
    e_norm = max(0.0, min(1.0, e / 30.0))
    score = (SLS_WP * p + SLS_WE * e_norm) * 100.0
    return round(score, 1)


def primary_badges(prob: float, ev: float) -> str:
    """
    Badges principais (lado esquerdo do título):

      🎯 prob >= 70%
      💸 prob < 40%

      EV tiers (todos em %):
        
        $  5%–24.9%
        💳  25%–44.9%
        💵  45%–74.9%
        💰  >=75%
    """
    b = []

    # probabilidade
    if prob < 0.40:
        b.append("💸")
    elif prob >= 0.70:
        b.append("🎯")

    # EV em %
    if ev >= 75.0:
        b.append("💰")
    elif ev >= 45.0:
        b.append("💵")
    elif ev >= 25.0:
        b.append("💳")
    elif ev >= 5.0:
        b.append("$")

    return " ".join(b) + (" " if b else "")


def right_badge_sls(sls: float) -> str:
    """
    Badge de excelência por SLS.
    Diamante reservado para SLS >= DIAMOND_SLS_THRESHOLD (config via ENV).
    """
    return "  💎" if sls >= DIAMOND_SLS_THRESHOLD else ""


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

# --- helpers visuais / formatação ---
HR = "——————————"  # separador horizontal (use uma linha sozinha)
def _hr(): return HR

# rótulo de fuso no card
TZ_LABEL = "(UTC: -3)"
# -------------------------------------

def fmt_pick(p: Dict[str, Any], *, add_debug_line: Optional[str] = None) -> str:
    # números
    prob = _f(p.get("prob_real", 0.0))
    ev = _f(p.get("ev") or p.get("ev_percent") or p.get("EV_percent") or 0.0)


    # odd mercado (pode vir com "@")
    odd_num = _to_float_odd(p.get("odd_mercado"))
    odd_str = f"@{odd_num:.2f}" if odd_num > 0 else "—"

    # odd justa (2 casas)
    odd_justa = (1.0/prob) if prob > 0 else 0.0
    odd_justa_str = f"@{odd_justa:.2f}" if odd_justa > 0 else "—"

    # SLS e badges
    sls  = sls_score(prob, ev)
    left = primary_badges(prob, ev)
    right = right_badge_sls(sls)

    # -------------------------------------------------------------
    #  MERCADO / SELEÇÃO — NOVO PADRÃO OMNIA
    # -------------------------------------------------------------
    raw_market = str(p.get("market") or p.get("mercado") or "").strip()
    raw_sel    = str(p.get("selection") or p.get("selecao") or p.get("pick") or "").strip()

    home = p.get("mandante") or p.get("home") or "Casa"
    away = p.get("visitante") or p.get("away") or "Fora"

    # Mercado: já vem limpo do builder
    mercado_pt = translate_market(raw_market)

    # Seleção: tradução padrão para O/U, BTTS, etc.
    selecao_pt = (raw_sel
        .replace("Over", "Acima de")
        .replace("Under", "Abaixo de")
        .replace("1st Half", "1º Tempo")
        .replace("2nd Half", "2º Tempo")
        .replace("Goals", "gols")
        .replace("BTTS Yes", "Ambas Marcam — Sim")
        .replace("BTTS No",  "Ambas Marcam — Não")
    )

    m_lower = raw_market.lower()
    s_lower = raw_sel.lower()

    # -------------- 1x2 --------------
    if "1x2" in m_lower:
        if s_lower in {"home", "1", "casa", "mandante"}:
            selecao_pt = home
        elif s_lower in {"away", "2", "fora", "visitante"}:
            selecao_pt = away
        elif s_lower in {"draw", "x", "empate"}:
            selecao_pt = "Empate"

    # -------------- DNB --------------
    if "empate anula aposta" in m_lower or "dnb" in m_lower:
        if s_lower in {"home", "1", "casa", "mandante"}:
            selecao_pt = f"{home}"
        elif s_lower in {"away", "2", "fora", "visitante"}:
            selecao_pt = f"{away}"



    # data/hora DD-MM-YYYY e HHhMM
    data_str, hora_str = format_date_hour_from_utc_str(
        p.get("hora_utc") or p.get("hora") or p.get("kickoff") or p.get("date_GMT") or _pick_time_str(p)
    )
    clock_emoji = _clock_emoji_for_hhmm(hora_str or "")
    when_line = f"{clock_emoji} <b>{data_str or '—'}</b> | <b>{hora_str or '—'}</b> {TZ_LABEL}"

    # Liga / país + bandeira
    league  = p.get("campeonato") or p.get("league") or "—"
    country = p.get("pais") or p.get("country") or "—"
    flag    = get_country_flag(country, league)

    linhas = [
        BRAND_LINE,
        _hr(),
        f"🏆 {league} · {country} {flag}",
        when_line,
        f"⚽ <b>{p.get('mandante','?')}</b> vs <b>{p.get('visitante','?')}</b>",
        "",
        f"{left}Mercado: <b>{mercado_pt}</b>{right}",
        f"Seleção: <b>{selecao_pt}</b>",
        "",
        f"Prob. real: <b>{prob*100:.1f}%</b>  |  Odd justa: <b>{odd_justa_str}</b>",
        f"Odd mercado: <b>{odd_str}</b>  |  EV: <b>{ev:.1f}%</b>",
        "",
        f"🎩 <b>BM:</b> {p.get('notes_pt','—')}",
        _hr(),
        _pick_aforismo_for_sls(sls),
    ]

    if add_debug_line:
        linhas.append(f"\n<code>{add_debug_line}</code>")

    return "\n".join(linhas)

def _to_float_odd(x) -> float:
    """Converte 1.23, '1.23', '@1.23' em float 1.23. Vazio/erro -> 0.0"""
    try:
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return 0.0
        if s.startswith("@"):
            s = s[1:].strip()
        return float(s.replace(",", "."))
    except Exception:
        return 0.0

def _to_float_pct(x) -> float:
    """Converte '28.0', 28.0, '28%' em 28.0 (percent)."""
    try:
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace("%","").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


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

def fmt_super_game_card(g: Dict[str, Any], header: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    Template OFICIAL do Super Game (Jogão / Jogaço).

    Retorna UMA LISTA de cards (texto), cada card representando um bloco
    de mercados (1x2, DNB, DC, Gols FT, 1ºT, 2ºT, Escanteios, Cartões,
    Outros Mercados com Odd, Mercados Projetados).

    • Cabeçalho padrão: |Data, ethics and the beautiful game| @betmasterson
    • Ícones: 🏟 / 👑, ⚽️, 🚩, 🟨, etc.
    • Destaques: ⭐️⭐️ para melhor EV>0 do jogo, ⭐️ para EV>0
    • Aforismo no final do último card (se disponível).
    """

    # ----------------- CAMPOS BÁSICOS -----------------
    home    = g.get("home", "—")
    away    = g.get("away", "—")
    league  = g.get("league", "—")
    country = g.get("country", "—")
    flag    = get_country_flag(country, league)

    # Tier / tipo de super jogo
    tier_raw = str(g.get("super_type") or g.get("tier") or g.get("super_tier") or "").strip().lower()
    is_jogaco = bool(g.get("flag_jogaco")) or (tier_raw == "jogaco") or ("jogaço" in tier_raw)

    if is_jogaco:
        card_name = "JOGAÇO"
        title_icon = "👑"
    else:
        card_name = "JOGÃO"
        title_icon = "🏟️"

    # Data/hora local do jogo
    tz_sp = tz.gettz(TZ_NAME)
    dt    = _super_kickoff_dt_local(g) or datetime.now(tz_sp)
    ko_local = dt.strftime("%Hh%M")
    date_local = dt.strftime("%d/%m/%Y")

    # Contexto de modelo
    lam_total   = g.get("lam_total")
    lam_corners = g.get("lam_corners")
    lam_cards   = g.get("lam_cards")

    def _fmt_num(x):
        try:
            return f"{float(x):.2f}"
        except Exception:
            return "—"

    # BRAND_LINE deve existir no arquivo (ex.: "|Data, ethics and the beautiful game|\n@betmasterson")
    brand = BRAND_LINE if "BRAND_LINE" in globals() else "|Data, ethics and the beautiful game|\n@betmasterson"

    # ----------------- HELPERS DE CABEÇALHO -----------------

    def _header_full() -> List[str]:
        lines = [
            brand,
            "──────────",
            f"{title_icon} <b>{card_name}</b> — {home} vs {away}",
            f"{flag}{league} — {country}",
            f"⏰ Hoje • <b>{ko_local}</b> (UTC:-3)",
        ]
        if lam_total is not None and lam_corners is not None and lam_cards is not None:
            lines += [
                "",
                "📌 CONTEXTO",
                f"xG projetado: {_fmt_num(lam_total)} • Escanteios: {_fmt_num(lam_corners)} • Cartões: {_fmt_num(lam_cards)}",
            ]
        lines.append("──────────")
        return lines

    def _header_short() -> List[str]:
        return [
            brand,
            "──────────",
            f"{title_icon} <b>{card_name}</b> — {home} vs {away}",
            f"{flag}{league} — {country}",
            f"⏰ Hoje • <b>{ko_local}</b> (UTC:-3)",
            "──────────",
        ]

    # ----------------- SEPARAÇÃO DE MERCADOS -----------------

    markets   = list(g.get("markets") or [])
    with_odds = [m for m in markets if m.get("odds_market") not in (None, 0, 0.0, "", "0")]
    no_odds   = [m for m in markets if m.get("odds_market") in (None, 0, 0.0, "", "0")]

    # Melhor EV do jogo (para ⭐️⭐️)
    best_ev   = None
    best_prob = 0.0
    for m in with_odds:
        ev = m.get("ev_percent")
        pr = m.get("p_model")
        try:
            evf = float(ev) if ev is not None else None
        except Exception:
            evf = None
        try:
            prf = float(pr) if pr is not None else 0.0
        except Exception:
            prf = 0.0
        if evf is not None and evf > 0:
            if best_ev is None or evf > best_ev:
                best_ev   = evf
                best_prob = prf

    def _ev_badge(ev: Optional[float]) -> str:
        if ev is None:
            return ""
        try:
            evf = float(ev)
        except Exception:
            return ""
        if evf <= 0:
            return ""
        if best_ev is not None and abs(evf - best_ev) < 1e-6:
            return " ⭐️⭐️"
        return " ⭐️"

    def _fmt_prob(p: Optional[float]) -> str:
        if p is None:
            return ""
        try:
            return f"Prob: {float(p)*100:0.1f}%"
        except Exception:
            return ""

    def _fmt_odd_mkt(o: Optional[float]) -> str:
        if o is None:
            return ""
        try:
            v = float(o)
            if v <= 0:
                return ""
            return f"Odd mercado: {v:0.2f}"
        except Exception:
            return ""

    def _fmt_fair(o: Optional[float]) -> str:
        if o is None:
            return ""
        try:
            v = float(o)
            if v <= 0:
                return ""
            return f"Odd justa: {v:0.2f}"
        except Exception:
            return ""

    def _fmt_fair_short(o: Optional[float]) -> str:
        if o is None:
            return ""
        try:
            v = float(o)
            if v <= 0:
                return ""
            return f"Odd Justa: {v:0.2f}"
        except Exception:
            return ""

    def _fmt_ev(ev: Optional[float]) -> str:
        if ev is None:
            return ""
        try:
            v = float(ev)
        except Exception:
            return ""
        return f"EV: {v:+0.1f}%"

    # ----------------- AGRUPAMENTO POR FAMÍLIA -----------------

    family_sections = [
        ("1x2",          "🏆 RESULTADO FINAL"),
        ("Empate Anula", "🛡 EMPATE ANULA APOSTA"),
        ("Dupla Chance", "🎭 DUPLA CHANCE"),
        ("Gols",         "⚽️ GOLS (FT / 1ºT / 2ºT)"),
        ("Escanteios",   "🚩 ESCANTEIOS"),   # <– APENAS ESTA, igual ao odds.json
        ("Cartões",      "🟨 🟥CARTÕES"),
    ]

    used_ids: Set[int] = set()
    sections: List[Dict[str, Any]] = []

    # vamos acumular linhas específicas de BTTS (FT) e BTTS 1ºT
    btts_ft_lines: List[str] = []
    btts_ht_lines: List[str] = []

    # 1) Seções principais com odd de mercado
    for key, title in family_sections:
        fam_markets = [
            m for m in with_odds
            if key.lower() in str(m.get("market_family", "")).lower()
        ]
        if not fam_markets:
            continue

        # Caso especial: GOLS → dividir em FT / 1ºT / 2ºT
        if key.lower() == "gols":
            ft = []
            h1 = []
            h2 = []
            for m in fam_markets:
                fam_name = str(m.get("market_family", "")).lower()
                if "1º" in fam_name or "1t" in fam_name:
                    h1.append(m)
                elif "2º" in fam_name or "2t" in fam_name:
                    h2.append(m)
                else:
                    ft.append(m)

            def _build_gols_section(label: str, arr: List[Dict[str, Any]]):
                if not arr:
                    return
                lines: List[str] = []
                for mm in arr:
                    used_ids.add(id(mm))
                    sel = (mm.get("selection_pt") or "").strip() or (mm.get("market_label_pt") or "").strip() or "Seleção"
                    pr   = _fmt_prob(mm.get("p_model"))
                    omkt = _fmt_odd_mkt(mm.get("odds_market"))
                    ofair= _fmt_fair(mm.get("odd_fair"))
                    ev_t = _fmt_ev(mm.get("ev_percent"))
                    badge= _ev_badge(mm.get("ev_percent"))

                    parts = [sel]
                    for x in (pr, omkt, ofair, ev_t):
                        if x:
                            parts.append(x)
                    line = " | ".join(parts) + badge
                    lines.append(line)
                if lines:
                    if not any(s["title"] == label for s in sections):
                        sections.append({"title": label, "lines": lines})

            _build_gols_section("⚽️ GOLS FT", ft)
            _build_gols_section("⚽️ GOLS 1º TEMPO", h1)
            _build_gols_section("⚽️ GOLS 2º TEMPO", h2)
            continue  # já tratamos GOLS

        # Demais famílias (1x2, DNB, DC, ESCANTEIOS, CARTÕES)
        lines: List[str] = []
        for m in fam_markets:
            used_ids.add(id(m))
            sel  = (m.get("selection_pt") or "").strip() or (m.get("market_label_pt") or "").strip() or "Seleção"
            pr   = _fmt_prob(m.get("p_model"))
            omkt = _fmt_odd_mkt(m.get("odds_market"))
            ofair= _fmt_fair(m.get("odd_fair"))
            ev_t = _fmt_ev(m.get("ev_percent"))
            badge = _ev_badge(m.get("ev_percent"))

            parts = [sel]
            for x in (pr, omkt, ofair, ev_t):
                if x:
                    parts.append(x)
            line = " | ".join(parts) + badge
            lines.append(line)

        if lines:
            if not any(s["title"] == title for s in sections):
                sections.append({"title": title, "lines": lines})

    # 2) OUTROS MERCADOS COM ODD → aqui vamos capturar só "Ambas Marcam" FT
    others_with_odds = [m for m in with_odds if id(m) not in used_ids]
    if others_with_odds:
        for m in others_with_odds:
            raw_label = (m.get("market_label_pt") or "").strip()
            base = raw_label.split("—")[0].strip() if "—" in raw_label else raw_label
            sel  = (m.get("selection_pt") or "").strip()

            if "ambas marcam" not in base.lower() and "ambas marcam" not in str(m.get("market_family", "")).lower():
                continue

            display = sel or base or "Mercado"

            pr    = _fmt_prob(m.get("p_model"))
            omkt  = _fmt_odd_mkt(m.get("odds_market"))
            ofair = _fmt_fair(m.get("odd_fair"))
            ev_t  = _fmt_ev(m.get("ev_percent"))
            badge = _ev_badge(m.get("ev_percent"))

            parts = [display]
            for x in (pr, omkt, ofair, ev_t):
                if x:
                    parts.append(x)
            line = " | ".join(parts) + badge
            btts_ft_lines.append(line)

    # 3) MERCADOS PROJETADOS (sem odd de mercado)
    proj_lines: List[str] = []
    if no_odds:
        for m in no_odds:
            raw_label = (m.get("market_label_pt") or "").strip()
            base = raw_label.split("—")[0].strip() if "—" in raw_label else raw_label
            sel  = (m.get("selection_pt") or "").strip()

            # Separar "Ambas Marcam 1º Tempo" para ir junto do card de Ambos Marcam
            if base and "ambas marcam 1º tempo" in base.lower():
                display = f"{base} — {sel}" if sel else base
                pr    = _fmt_prob(m.get("p_model"))
                ofair = _fmt_fair_short(m.get("odd_fair"))
                parts = [display]
                for x in (pr, ofair):
                    if x:
                        parts.append(x)
                line = " | ".join(parts)
                btts_ht_lines.append(line)
                continue  # não entra em CARTÕES PROJETADOS

            # Demais projeções
            if base and sel and base.lower() in ("ambas marcam", "cartões"):
                display = sel
            elif base and sel:
                display = f"{base} — {sel}"
            elif sel:
                display = sel
            else:
                display = base or "Mercado"

            pr    = _fmt_prob(m.get("p_model"))
            ofair = _fmt_fair_short(m.get("odd_fair"))

            parts = [display]
            for x in (pr, ofair):
                if x:
                    parts.append(x)
            line = " | ".join(parts)
            proj_lines.append(line)

    # Se houver projeções (sem odd) → seção "🟨 🟥 CARTÕES PROJETADOS"
    if proj_lines:
        sections.append({
            "title": "🟨 🟥 CARTÕES PROJETADOS",
            "lines": proj_lines,
        })

    # Se houver BTTS (FT + 1ºT) → seção "📊 AMBOS MARCAM"
    btts_all_lines: List[str] = []
    if btts_ft_lines:
        btts_all_lines.extend(btts_ft_lines)
    if btts_ht_lines:
        if btts_all_lines:
            btts_all_lines.append("")  # quebra visual entre FT e 1ºT
        btts_all_lines.extend(btts_ht_lines)

    if btts_all_lines:
        sections.append({
            "title": "📊 AMBOS MARCAM",
            "lines": btts_all_lines,
        })

    if not sections:
        return ["\n".join(_header_full() + ["Nenhum mercado disponível para este jogo."])]

    # ----------------- REORDENAR SEÇÕES (ORDEM DOS CARDS) -----------------

    desired_order = [
        "🟨 🟥 CARTÕES PROJETADOS",
        "🟨 🟥CARTÕES",
        "📊 AMBOS MARCAM",
        "🚩 ESCANTEIOS",
        "⚽️ GOLS 2º TEMPO",
        "⚽️ GOLS 1º TEMPO",
        "⚽️ GOLS FT",
        "🎭 DUPLA CHANCE",
        "🛡 EMPATE ANULA APOSTA",
        "🏆 RESULTADO FINAL",
    ]

    by_title: Dict[str, Dict[str, Any]] = {s["title"]: s for s in sections}
    ordered_sections: List[Dict[str, Any]] = []

    for t in desired_order:
        if t in by_title:
            ordered_sections.append(by_title.pop(t))

    for s in sections:
        if s["title"] in by_title:
            ordered_sections.append(s)
            by_title.pop(s["title"], None)

    sections = ordered_sections

    # ----------------- AFORISMO (NO ÚLTIMO CARD) -----------------
    af_txt = ""
    try:
        if "sls_score" in globals() and "_pick_aforismo_for_sls" in globals():
            sls_val = sls_score(best_prob, best_ev) if best_ev is not None else 0.0
            af_txt = _pick_aforismo_for_sls(sls_val).strip()
    except Exception:
        af_txt = ""

    # ----------------- MONTAGEM DOS CARDS -----------------
    cards: List[str] = []
    total_sections = len(sections)

    for idx, sec in enumerate(sections):
        title = sec["title"]
        lines_section = [ln for ln in sec["lines"] if ln is not None and ln != ""]

        header_lines = _header_full() if idx == total_sections - 1 else _header_short()

        body: List[str] = []
        body.append(title)
        body.append("──────────")
        body.append("\n• • •\n".join(lines_section))
        body.append("──────────")

        if idx == total_sections - 1 and af_txt:
            body.append(af_txt)

        txt = "\n".join(header_lines + body)

        if "TELEGRAM_SAFE_LIMIT" in globals():
            limit = TELEGRAM_SAFE_LIMIT
            if len(txt) > limit:
                txt = txt[:limit - 10] + "\n…"

        cards.append(txt)

    return cards




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

from aiogram.filters import Command

@dp.message(Command("help"))
async def help_cmd(m: types.Message):
    # DEBUG
    print(f"[HELP] hit: chat={m.chat.id} type={m.chat.type} user={m.from_user.id}")

    public_help = "\n".join([
        "📗 <b>Comandos (assinante)</b>",
        "/status_sub — Status da assinatura",
        "/join — Assinar",
        "/refer — Link de indicação",
    ])

    admin_help = "\n".join([
        "🛠️ <b>Comandos (admin)</b>",
        "/which_source — mostra fontes e paths",
        "/ls_data — lista arquivos em /data",
        "/fetch_update — força baixar odds*.json + agenda + aforismos",
        "/games_today — lista jogos (IDs) de hoje",
        "/games_tomorrow — lista jogos (IDs) de amanhã",
        "/supercard_preview — prévia dos Super Cards de hoje (não publica, só mostra)",
        "/post_pick [GAME_ID] — publica 1 pick do jogo (ou melhor SLS se vazio)",
        "/post_combo [hash|índice] — publica 1 combo específico ou o melhor SLS",
        "/post_combos [N] — publica N combos elegíveis (padrão 1, anti-flood)",
        "/post_coruja — publica o card do 🌙🦉 Corujão (00:00–07:00)",
        "/pub_show_today — mostra publicados hoje",
        "/pub_reset_today — zera marcações de hoje",
        "/diag_time — relógios local/UTC",
        "/diag_odds — estatísticas dos JSON de odds",
        "/diag_slots — valida agenda_editorial.json",
        "/help — esta ajuda",
    ])

    is_admin_user = is_admin(m.from_user.id)

    # DM: responde completo (público + admin se admin)
    if m.chat.type == "private":
        text = public_help + ("\n\n" + admin_help if is_admin_user else "")
        return await m.answer(text)

    # Grupo/canal: informa que a ajuda completa vai por DM
    await m.answer("📥 Enviando a ajuda completa no privado (DM)…")
    try:
        await bot.send_message(m.from_user.id, public_help + ("\n\n" + admin_help if is_admin_user else ""))
    except Exception as e:
        print("[HELP][DM_FAIL]", repr(e))
        # fallback: mostra pelo menos a parte pública no chat onde foi chamado
        await m.answer(public_help)

@dp.message(Command("post_coruja"))
async def post_coruja_cmd(m: types.Message):
    print(f"[POST_CORUJA] hit by {m.from_user.id} in chat {m.chat.id}")
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    try:
        ok = await post_coruja_card()
        if ok:
            return await m.answer("🌙🦉 Corujão publicado manualmente.")
        return await m.answer("❌ Nenhum jogo válido no bloco Corujão do arquivo do dia.")
    except Exception as e:
        print("[POST_CORUJA][ERR]", repr(e))
        return await m.answer(f"❌ Erro ao publicar Corujão.\n<code>{e}</code>")


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
        "prob_real":0.84,"fair_odd":1.19,"odd_mercado":1.35,"ev":13.0,"roi":9.6,"notes_pt":"Linha conservadora; xG alto recente"
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

    tz_sp = tz.gettz(TZ_NAME)
    today_sp = datetime.now(tz_sp).strftime("%Y-%m-%d")
    data = load_odds_for_date(today_sp) or {}

    try:
        data = normalize_odds(data)
    except Exception:
        pass

    picks = list(data.get("picks", []) or [])
    # ordena por SLS e limita para não flodar
    def _sls(p):
        pr = float(p.get("prob_real", 0) or 0)
        ev = _to_float_pct(p.get("ev", 0) or 0)
        return sls_score(pr, ev)
    picks.sort(key=_sls, reverse=True)

    MAX_SEND = 1  # ajuste aqui o limite que você quer
    picks = picks[:MAX_SEND]

    sent = 0
    for pk in picks:
        # saneia odds com '@'
        if isinstance(pk.get("odd_mercado"), str):
            pk["odd_mercado"] = _to_float_odd(pk["odd_mercado"])
        if isinstance(pk.get("fair_odd"), str):
            pk["fair_odd"] = _to_float_odd(pk["fair_odd"])
        try:
            await bot.send_message(GROUP_ID, fmt_pick(pk))
            mark_published_pick(pk)
            sent += 1
            await asyncio.sleep(0.5)
        except Exception:
            continue
    await m.answer(f"✅ Publicado {sent} picks (limitado).")


@dp.message(Command("post_combos"))
async def post_combos(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    # /post_combos [N]
    parts = (m.text or "").split()
    N = 1
    if len(parts) >= 2:
        try:
            N = max(1, int(parts[1]))
        except Exception:
            N = 1

    data = await load_odds_generic()
    try:
        data = normalize_odds(data)
    except Exception:
        pass

    combos = list(data.get("combos", []) or [])
    if not combos:
        return await m.answer("❌ Nenhuma múltipla encontrada no arquivo.")

    def _sls_c(c):
        pr  = _f(c.get("prob_real_combo", 0.0))
        evc = _f(c.get("ev_combo", 0.0))
        return sls_score(pr, evc)

    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)

    # elegíveis: lead ok e não publicados
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

    elegiveis.sort(key=_sls_c, reverse=True)
    send_list = elegiveis[:N]

    sent = 0
    for c in send_list:
        try:
            await bot.send_message(GROUP_ID, _fmt_combo_msg(c))
            mark_published_combo(c)
            sent += 1
            await asyncio.sleep(0.7)
        except Exception as e:
            try:
                await m.answer(f"Falhou combo: {e}")
            except Exception:
                pass

    await m.answer(f"✅ Publicadas {sent} múltiplas (pedido: {N}).")




from typing import List, Dict, Any, Optional  # garantir que List/Dict/Any/Optional estejam importados

# --------------------------------------------------------
# SUPER GAMES — TEMPLATE EM VÁRIOS CARDS (UM POR MERCADO)
# --------------------------------------------------------
from typing import List, Dict, Any, Optional

def _format_supercard_cards(game: Dict[str, Any]) -> List[str]:
    """
    Formata UM Super Game em VÁRIOS cards (um por mercado),
    para uso no /supercard_preview (e depois no disparo automático).

    Cada elemento da lista retornada é uma mensagem separada
    pronta para ser enviada via bot.send_message / answer().
    """
    if not game:
        return ["❌ Nenhum Super Game encontrado para hoje."]

    home = game.get("home", "?")
    away = game.get("away", "?")
    league = game.get("league", "?")
    country = game.get("country", "?")
    ko_local = game.get("kickoff_local", "?")

    lam_total   = game.get("lam_total")
    lam_corners = game.get("lam_corners")
    lam_cards   = game.get("lam_cards")

    # Tier: Jogão / Jogaço
    tier_raw = (game.get("tier") or "").lower()
    if "jogaço" in tier_raw or "jogaco" in tier_raw:
        card_name = "JOGAÇO"
        title_icon = "👑"
    else:
        card_name = "JOGÃO"
        title_icon = "🏟️"

    # --- Cabeçalhos ---

    def _header_full() -> List[str]:
        # Cabeçalho COMPLETO (apenas no primeiro card)
        lines = [
            BRAND_LINE,
            "──────────",
            f"{title_icon} <b>{card_name}</b> — {home} vs {away}",
            f"{league} — {country} {flag}",
            f"⏰ Hoje • {ko_local} (Brasília)",
        ]
        if lam_total is not None and lam_corners is not None and lam_cards is not None:
            lines += [
                "",
                "📌 CONTEXTO MODELO (xG / ESCANTEIOS / CARTÕES)",
                f"xG projetado: {lam_total:.2f} • Escanteios: {lam_corners:.1f} • Cartões: {lam_cards:.2f}",
            ]
        lines.append("──────────")
        return lines

    def _header_short() -> List[str]:
        # Cabeçalho dos cards subsequentes (sem bloco de contexto)
        return [
            BRAND_LINE,
            "──────────",
            f"{title_icon} <b>{card_name}</b> — {home} vs {away}",
            f"{league} — {country} {flag}",
            f"⏰ Hoje • {ko_local} (Brasília)",
            "──────────",
        ]

    # --- Separação de mercados ---

    markets   = list(game.get("markets") or [])
    with_odds = [m for m in markets if m.get("odds_market") is not None]
    no_odds   = [m for m in markets if m.get("odds_market") is None]

    # Melhor EV e probabilidade para destaques e aforismo
    best_ev   = None
    best_prob = 0.0
    for m in with_odds:
        ev = m.get("ev_percent")
        pr = m.get("p_model")
        try:
            ev_f = float(ev) if ev is not None else None
        except Exception:
            ev_f = None
        try:
            pr_f = float(pr) if pr is not None else 0.0
        except Exception:
            pr_f = 0.0
        if ev_f is not None:
            if best_ev is None or ev_f > best_ev:
                best_ev   = ev_f
                best_prob = pr_f

    def _ev_badge(ev: Optional[float]) -> str:
        """
        Destaque à direita:
          ⭐️⭐️  para o melhor EV>0 do jogo
          ⭐️    para EV>0
          ""    para EV<=0 ou inválido
        """
        if ev is None:
            return ""
        try:
            ev = float(ev)
        except Exception:
            return ""
        if best_ev is not None and abs(ev - best_ev) < 1e-6 and ev > 0:
            return " ⭐️⭐️"
        if ev > 0:
            return " ⭐️"
        return ""

    # Títulos das seções por "família" de mercado
    family_sections = [
        ("1x2",          "🏆 RESULTADO FINAL"),
        ("Empate Anula", "🛡 EMPATE ANULA APOSTA"),
        ("Dupla Chance", "🎭 DUPLA CHANCE"),
        ("Gols",         "⚽️ GOLS (FT / 1ºT / 2ºT)"),
        ("Escanteio",    "🚩 ESCANTEIOS"),
        ("Escanteios",   "🚩 ESCANTEIOS"),
        ("Cartões",      "🟨 CARTÕES"),
    ]

    used_ids: set[int] = set()
    sections: List[Dict[str, Any]] = []

    # --- Seções principais (com odd de mercado) ---

    for key, title in family_sections:
        fam_markets = [
            m for m in with_odds
            if key.lower() in str(m.get("market_family", "")).lower()
        ]
        if not fam_markets:
            continue

        # CASO ESPECIAL: GOLS → subdividir em FT / 1ºT / 2ºT
        if key.lower() == "gols":
            # separa por família:
            ft_markets  = [m for m in fam_markets if "1º" not in str(m.get("market_family","")) and "2º" not in str(m.get("market_family",""))]
            h1_markets  = [m for m in fam_markets if "1º" in str(m.get("market_family",""))]
            h2_markets  = [m for m in fam_markets if "2º" in str(m.get("market_family",""))]

            # bloco FT
            if ft_markets:
                lines.append("\n⚽️ GOLS FT")
                lines.append("──────────")
                for m in ft_markets:
                    used.add(id(m))
                    ev = m.get("ev_percent")
                    pr = m.get("p_model")
                    odd_mkt = m.get("odds_market")
                    odd_fair = m.get("odd_fair")
                    sel = (m.get("selection_pt") or "").strip()

                    part = sel or "Seleção"
                    detail_parts = []
                    if pr is not None:
                        detail_parts.append(f" | Prob: {float(pr)*100:0.1f}%")
                    if odd_mkt is not None:
                        detail_parts.append(f" | Odd mercado: {float(odd_mkt):0.2f}")
                    if odd_fair is not None:
                        detail_parts.append(f" | Odd justa: {float(odd_fair):0.2f}")
                    if ev is not None:
                        detail_parts.append(f" | EV: {float(ev):+0.1f}%")

                    part += "".join(detail_parts)
                    part = f"{part}{_ev_icon(ev)}"
                    lines.append(part)
                    lines.append("• • •")

            # bloco 1ºT
            if h1_markets:
                lines.append("\n⚽️ GOLS 1º TEMPO")
                lines.append("──────────")
                for m in h1_markets:
                    used.add(id(m))
                    ev = m.get("ev_percent")
                    pr = m.get("p_model")
                    odd_mkt = m.get("odds_market")
                    odd_fair = m.get("odd_fair")
                    sel = (m.get("selection_pt") or "").strip()

                    part = sel or "Seleção"
                    detail_parts = []
                    if pr is not None:
                        detail_parts.append(f" | Prob: {float(pr)*100:0.1f}%")
                    if odd_mkt is not None:
                        detail_parts.append(f" | Odd mercado: {float(odd_mkt):0.2f}")
                    if odd_fair is not None:
                        detail_parts.append(f" | Odd justa: {float(odd_fair):0.2f}")
                    if ev is not None:
                        detail_parts.append(f" | EV: {float(ev):+0.1f}%")

                    part += "".join(detail_parts)
                    part = f"{part}{_ev_icon(ev)}"
                    lines.append(part)
                    lines.append("• • •")

            # bloco 2ºT
            if h2_markets:
                lines.append("\n⚽️ GOLS 2º TEMPO")
                lines.append("──────────")
                for m in h2_markets:
                    used.add(id(m))
                    ev = m.get("ev_percent")
                    pr = m.get("p_model")
                    odd_mkt = m.get("odds_market")
                    odd_fair = m.get("odd_fair")
                    sel = (m.get("selection_pt") or "").strip()

                    part = sel or "Seleção"
                    detail_parts = []
                    if pr is not None:
                        detail_parts.append(f" | Prob: {float(pr)*100:0.1f}%")
                    if odd_mkt is not None:
                        detail_parts.append(f" | Odd mercado: {float(odd_mkt):0.2f}")
                    if odd_fair is not None:
                        detail_parts.append(f" | Odd justa: {float(odd_fair):0.2f}")
                    if ev is not None:
                        detail_parts.append(f" | EV: {float(ev):+0.1f}%")

                    part += "".join(detail_parts)
                    part = f"{part}{_ev_icon(ev)}"
                    lines.append(part)
                    lines.append("• • •")

            # pula o resto do loop, já tratamos GOLS
            continue

        # CASO GENÉRICO (1x2, DNB, DC, ESCANTEIOS, CARTÕES)
        lines.append(f"\n{title}")
        lines.append("──────────")
        for m in fam_markets:
            used.add(id(m))
            ev = m.get("ev_percent")
            pr = m.get("p_model")
            odd_mkt = m.get("odds_market")
            odd_fair = m.get("odd_fair")
            sel = (m.get("selection_pt") or "").strip()

            part = sel or "Seleção"
            detail_parts = []
            if pr is not None:
                detail_parts.append(f" | Prob: {float(pr)*100:0.1f}%")
            if odd_mkt is not None:
                detail_parts.append(f" | Odd mercado: {float(odd_mkt):0.2f}")
            if odd_fair is not None:
                detail_parts.append(f" | Odd justa: {float(odd_fair):0.2f}")
            if ev is not None:
                detail_parts.append(f" | EV: {float(ev):+0.1f}%")

            part += "".join(detail_parts)
            part = f"{part}{_ev_icon(ev)}"
            lines.append(part)
            lines.append("• • •")


    # --- Outros mercados com odd (não encaixados nas famílias acima) ---

    # outros mercados com odd
    others_with_odds = [m for m in with_odds if id(m) not in used]
    if others_with_odds:
        lines.append("\n📊 OUTROS MERCADOS COM ODD")
        for m in others_with_odds:
            ev = m.get("ev_percent")
            pr = m.get("p_model")
            odd_mkt = m.get("odds_market")
            odd_fair = m.get("odd_fair")
            icon = _ev_icon(ev)

            # base do mercado: pega só a parte antes do " — "
            raw_label = m.get("market_label_pt") or ""
            base = raw_label.split("—")[0].strip() if "—" in raw_label else raw_label.strip()

            sel = (m.get("selection_pt") or "").strip()

            if base and sel:
                display = f"{base} — {sel}"
            elif sel:
                display = sel
            else:
                display = base or "Mercado"

            part = f"{display} |"

            detail_parts = []
            if pr is not None:
                detail_parts.append(f" Prob: {float(pr)*100:0.1f}%")
            if odd_mkt is not None:
                detail_parts.append(f" Odd mercado: {float(odd_mkt):0.2f}")
            if odd_fair is not None:
                detail_parts.append(f" Odd justa: {float(odd_fair):0.2f}")
            if ev is not None:
                detail_parts.append(f" EV: {float(ev):+0.1f}%")

            if detail_parts:
                part += "".join(detail_parts)

            # ⭐️/⭐️⭐️ à direita, como combinamos
            part = f"{part}{_ev_icon(ev)}"

            # separador visual
            lines.append(part)
            lines.append("• • •")


    # --- Mercados apenas projetados ---

    # mercados só com odd justa + probabilidade 
    if no_odds:
        lines.append("\n🧮 MERCADOS PROJETADOS")
        for m in no_odds:
            pr = m.get("p_model")
            odd_fair = m.get("odd_fair")

            raw_label = m.get("market_label_pt") or ""
            base = raw_label.split("—")[0].strip() if "—" in raw_label else raw_label.strip()
            sel = (m.get("selection_pt") or "").strip()

            if base and sel:
                display = f"{base} — {sel}"
            elif sel:
                display = sel
            else:
                display = base or "Mercado"

            part = display

            detail_parts = []
            if pr is not None:
                detail_parts.append(f" | Prob: {float(pr)*100:0.1f}%")
            if odd_fair is not None:
                detail_parts.append(f" | Fair: {float(odd_fair):0.2f}")

            if detail_parts:
                part += "".join(detail_parts)

            lines.append(part)
            lines.append("• • •")


    # Nenhuma seção encontrada
    if not sections:
        return ["\n".join(_header_full() + ["Nenhum mercado disponível para este jogo."])]

    # SLS do Super Game para escolher aforismo
    try:
        sls_super = sls_score(float(best_prob), float(best_ev)) if best_ev is not None else 0.0
    except Exception:
        sls_super = 0.0

    af = ""
    if " _pick_aforismo_for_sls" in globals():
        try:
            af = _pick_aforismo_for_sls(sls_super).strip()
        except Exception:
            af = ""

    # --- Monta os cards (um por seção / mercado) ---

    cards: List[str] = []
    total_sections = len(sections)

    for idx_sec, sec in enumerate(sections):
        title         = sec["title"]
        lines_section = sec["lines"]

        # Primeiro card com cabeçalho + contexto; demais com cabeçalho curto
        header_lines = _header_full() if idx_sec == 0 else _header_short()

        body_lines: List[str] = []
        body_lines.append(title)
        body_lines.append("──────────")
        body_lines.append("\n• • •\n".join(lines_section))
        body_lines.append("──────────")

        # Aforismo só no último card
        if idx_sec == total_sections - 1 and af:
            body_lines.append(af)

        card_text = "\n".join(header_lines + body_lines)

        # Segurança extra contra limite do Telegram
        if len(card_text) > TELEGRAM_SAFE_LIMIT:
            card_text = card_text[:TELEGRAM_SAFE_LIMIT - 10] + "\n…"

        cards.append(card_text)

    return cards




@dp.message(Command("supercard_preview"))
async def supercard_preview(m: types.Message):
    if not is_admin(m.from_user.id):
        await m.answer("❌ Este comando é apenas para o administrador.")
        return

    tz_sp = tz.gettz(TZ_NAME)
    today = datetime.now(tz_sp).date()

    data = _load_super_games_for_date(today)
    if not data:
        return await m.answer("❌ Nenhum arquivo de super jogos encontrado para hoje.")

    games = data.get("games") or []
    if not games:
        return await m.answer("❌ O arquivo de super jogos de hoje não tem jogos dentro.")

    # pega o primeiro jogo "válido" (poderia melhorar, mas ok pra preview)
    chosen = sorted(
        games,
        key=lambda g: _super_kickoff_dt_local(g) or datetime.max.replace(tzinfo=tz_sp)
    )[0]

    cards = fmt_super_game_card(chosen, header=data.get("supercard_header") or {})

    if not isinstance(cards, list):
        cards = [cards]

    await m.answer("👁‍🗨 <b>Pré-visualização dos cards que serão enviados:</b>", parse_mode="HTML")
    for txt in cards:
        if not txt or not str(txt).strip():
            continue
        msg = str(txt)
        if "TELEGRAM_SAFE_LIMIT" in globals() and len(msg) > TELEGRAM_SAFE_LIMIT:
            msg = msg[:TELEGRAM_SAFE_LIMIT - 10] + "\n…"
        await m.answer(msg, parse_mode="HTML")
        await asyncio.sleep(0.4)


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
# ---- Convites / Pagamento ----
# ---- Convites / Pagamento ----
@dp.message(Command("join"))
async def join_cmd(m: types.Message):
    uid = m.from_user.id

    # 1) Já tem assinatura ativa?
    if sub_is_active(uid):
        # Já está dentro do canal?
        if await is_in_channel(uid):
            return await m.answer(
                "Você já possui assinatura ativa e válida.\n"
                "E também já está dentro do canal! ✅\n"
                "Se precisar de ajuda, use /help."
            )

        # Não está no canal → gerar (ou reaproveitar) invite exclusivo
        invite = await on_payment_confirmed(uid, send_dm=False)
        if not invite:
            return await m.answer(
                "Sua assinatura está ativa, mas tive um problema ao gerar o link de acesso.\n"
                "Fale comigo aqui no chat que eu resolvo manualmente. ❗"
            )

        return await m.answer(
            "Você já possui assinatura ativa e válida.\n"
            "Aqui está o seu link exclusivo de acesso ao canal (24h, 1 uso):\n"
            f"👉 {invite}\n\n"
            "Ao clicar, o pedido é aprovado automaticamente."
        )

    # 2) Não é assinante → fluxo normal de pagamento
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
        return  # só funciona em DM com o bot

    uid = m.from_user.id

    if not sub_is_active(uid):
        return await m.answer("Sua assinatura não está ativa. Use /join para assinar.")

    if await is_in_channel(uid):
        return await m.answer("Você já está no canal ✅. Se precisar de ajuda, mande /help.")

    # Gera um novo invite exclusivo e devolve no próprio chat
    invite = await on_payment_confirmed(uid, send_dm=False)
    if not invite:
        return await m.answer(
            "Sua assinatura está ativa, mas tive um problema ao gerar o link de acesso.\n"
            "Fale comigo aqui no chat que eu resolvo manualmente. ❗"
        )

    await m.answer(
        "Aqui está o seu link exclusivo de acesso ao canal (24h, 1 uso):\n"
        f"👉 {invite}\n\n"
        "Ao clicar, seu pedido é aprovado automaticamente."
    )


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
    Baixa/atualiza:
      • odds1..odds30.json
      • agenda_editorial.json
      • aforismos.json
      • super_jogos-YYYY-MM-DD.json (dia local em TZ_NAME)
    Usa GITHUB_RAW_BASE para compor URLs.
    """
    global _last_fetch_ts

    now = time.time()
    if (not force) and (now - _last_fetch_ts < _FETCH_MIN_INTERVAL):
        return

    base = (GITHUB_RAW_BASE or "").strip()
    if not base:
        print("[FETCH][SKIP] GITHUB_RAW_BASE não definido")
        return
    base = base.rstrip("/")  # tolera com ou sem barra final

    print(f"[FETCH][CFG] Base={base}")

    # ---------------- odds-YYYY-MM-DD.json (janela de 30 dias) ----------------
    # Janela móvel: alguns dias para trás e para frente, total 30 arquivos possíveis
    tz_sp = tz.gettz(TZ_NAME)
    today_local = datetime.now(tz_sp).date()

    DAYS_BACK = 7    # quantos dias PARA TRÁS do hoje local
    DAYS_FWD  = 22   # quantos dias PARA FRENTE (7 + 22 + 1 = 30 dias na janela)

    for delta in range(-DAYS_BACK, DAYS_FWD + 1):
        d = today_local + timedelta(days=delta)
        fname = f"odds-{d.isoformat()}.json"
        url = f"{base}/{fname}"
        local_path = f"/data/{fname}"
        _download_to(local_path, url)


    # ---------------- agenda_editorial.json ----------------
    agenda_url = f"{base}/agenda_editorial.json"
    if AGENDA_JSON_PATH:
        _download_to(AGENDA_JSON_PATH, agenda_url)

    # ---------------- aforismos.json ----------------
    aforismos_url = f"{base}/aforismos.json"
    if AFORISMOS_FILE:
        _download_to(AFORISMOS_FILE, aforismos_url)
    else:
        _download_to("/data/aforismos.json", aforismos_url)

    # ---------------- super_jogos-YYYY-MM-DD.json ----------------
    try:
        tz_sp = tz.gettz(TZ_NAME)
        today_local = datetime.now(tz_sp).date()
        sj_name = f"super_jogos-{today_local.isoformat()}.json"
        sj_url = f"{base}/{sj_name}"
        sj_local = f"/data/{sj_name}"
        _download_to(sj_local, sj_url)
    except Exception as e:
        print("[FETCH][SUPER][ERR]", repr(e))

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


# ---------- SUPER GAMES: helpers de publicação e carregamento ----------

def _super_today_key() -> str:
    tz_sp = tz.gettz(TZ_NAME)
    return datetime.now(tz_sp).strftime("%Y-%m-%d")

def _super_pub_get() -> Dict[str, Any]:
    base = _published_get() or {}
    if "super_games" not in base:
        base["super_games"] = {}
    return base

def _super_pub_mark(game_id: str):
    base = _super_pub_get()
    today = _super_today_key()
    base["super_games"].setdefault(today, {})
    base["super_games"][today][game_id] = _now()
    _published_set(base)

def _super_pub_is_marked(game_id: str) -> bool:
    base = _super_pub_get()
    today = _super_today_key()
    return bool(base.get("super_games", {}).get(today, {}).get(game_id))

def _load_super_games_for_date(d: date) -> Optional[Dict[str, Any]]:
    """
    Procura em SUPER_GAMES_GLOB o JSON cujo
    supercard_header.target_date_local == d.
    Se houver vários, escolhe o de generated_at mais recente.
    """
    from glob import glob
    pattern = SUPER_GAMES_GLOB or "/data/super_jogos-*.json"
    paths = glob(pattern)
    if not paths:
        return None

    target_str = d.strftime("%Y-%m-%d")
    best_obj = None
    best_ga = ""

    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception as e:
            print("[SUPER_LOAD][ERR]", p, repr(e))
            continue

        hdr = obj.get("supercard_header") or {}
        td = (hdr.get("target_date_local") or "").strip()
        if td != target_str:
            continue

        ga = hdr.get("generated_at") or ""
        if ga > best_ga:
            best_ga = ga
            best_obj = obj

    return best_obj

def _super_game_id(g: Dict[str, Any]) -> str:
    """
    ID estável por jogo: data_local + mandante + visitante + hora local.
    (hash MD5 apenas para compactar.)
    """
    try:
        home = _norm_team(g.get("home", ""))
    except Exception:
        home = str(g.get("home", "")).strip().lower()

    try:
        away = _norm_team(g.get("away", ""))
    except Exception:
        away = str(g.get("away", "")).strip().lower()

    k_local = str(g.get("kickoff_local") or "").strip()
    if not k_local:
        try:
            dt = parser.isoparse(g.get("kickoff_iso"))
            k_local = dt.strftime("%H:%M")
        except Exception:
            k_local = "00:00"

    day = _super_today_key()
    base = f"{day}|{home}|{away}|{k_local}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def _super_pub_is_marked(gid: str) -> bool:
    db = _load_published()
    return db.get("supercards", {}).get(gid) == "done"

def _super_pub_mark(gid: str):
    db = _load_published()
    db.setdefault("supercards", {})[gid] = "done"
    _save_published(db)

def _super_pub_mark_partial(gid: str, idx: int):
    db = _load_published()
    rec = db.setdefault("supercards", {}).setdefault(gid, {"done_idx": -1})
    rec["done_idx"] = idx
    _save_published(db)

def _super_pub_all_sent(gid: str, total_cards: int):
    db = _load_published()
    rec = db.get("supercards", {}).get(gid, {})
    idx = rec.get("done_idx", -1)
    return idx >= (total_cards - 1)


def _super_kickoff_dt_local(g: Dict[str, Any]) -> Optional[datetime]:
    """
    Converte kickoff_iso (que vem com -03:00 no JSON) para datetime no fuso TZ_NAME.
    """
    try:
        dt = parser.isoparse(g.get("kickoff_iso"))
        tz_sp = tz.gettz(TZ_NAME)
        return dt.astimezone(tz_sp)
    except Exception as e:
        print("[SUPER_DT][ERR]", repr(e), g.get("kickoff_iso"))
        return None



#-------------------------------------------------

def _hour_key(dt_utc: datetime) -> str:
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


# ========== PATCH COMPLETO — SELEÇÃO POR REGRA (CORRIGIDO) ==========
async def _select_by_rule(
    data: Dict[str, Any],
    rule: Dict[str, Any],
    now_local: Optional[datetime] = None,
    slot_local: Optional[datetime] = None,
):


    tz_sp = tz.gettz(TZ_NAME)
    now_local = now_local or datetime.now(tz_sp)
    slot_local = slot_local or now_local

    secs = rule.get("sections", [])
    max_cards = int(rule.get("max_cards", 3))

    PICK_PER_MATCH_PER_SLOT  = int(os.environ.get("PICK_PER_MATCH_PER_SLOT", 1))
    INTER_PER_MATCH_PER_SLOT = int(os.environ.get("INTER_PER_MATCH_PER_SLOT", 1))
    INTRA_PER_MATCH_PER_SLOT = int(os.environ.get("INTRA_PER_MATCH_PER_SLOT", 1))

    types_order = [
        t.strip()
        for t in os.environ.get("COMBOS_TYPES_ORDER","duplo,triplo,multi").split(",")
        if t.strip()
    ]

    # ===================== IMPORTANTÍSSIMO =====================
    # Inicializa OUT no topo (antes do primeiro uso)
    # Este era o bug que derrubava todo o scheduler
    # ===========================================================
    out: List[tuple] = []
    pick_by_game  = defaultdict(int)
    inter_by_game = defaultdict(int)
    intra_by_game = defaultdict(int)

    # Coleta de singles (picks) do novo JSON
    all_picks = list(data.get("picks", []) or [])

    raw_singles: List[tuple] = []
    for p in all_picks:

        prob = _f(p.get("p_model", 0.0))         # NOVO CERTO
        ev   = _f(p.get("ev", 0.0))              # EV em %

        # filtro mínimo global (mantém tua filosofia de prob. mínima + EV)
        if prob < MIN_PROB or ev <= MIN_EV:
            continue

        sls = sls_score(prob, ev)
        gid = _game_id_from_pick(p)

        raw_singles.append(("pick", p, sls, gid))

    # ainda ordenamos por SLS para priorizar valor
    raw_singles.sort(key=lambda x: x[2], reverse=True)

    # ---------- SINGLES ----------
    if "singles" in secs:

        # 1) Decora cada pick com o delta (minutos) entre AGORA (slot) e o horário local do jogo
        decorated: List[Tuple[float, str, Dict[str, Any], float, str]] = []
        for kind, payload, sls, gid in raw_singles:
            # horário local do jogo (usa _pick_time_str + _parse_any_dt_local, já existentes)
            dt_loc = _parse_any_dt_local(_pick_time_str(payload))
            if not dt_loc:
                continue

            delta_min = (dt_loc - now_local).total_seconds() / 60.0

            # descartamos jogos já começados ou muito em cima (abaixo do lead mínimo)
            if delta_min < MIN_LEAD_MIN:
                continue

            decorated.append((delta_min, kind, payload, sls, gid))

        # 2) Janela primária = até +60 min | fallback = +60 até +120 min | resto do dia = >120 min
        primary  = [tpl for tpl in decorated if tpl[0] <= 60.0]
        fallback = [tpl for tpl in decorated if 60.0 < tpl[0] <= 120.0]
        others   = [tpl for tpl in decorated if tpl[0] > 120.0]

        # dentro de cada janela, seguimos priorizando SLS
        primary.sort(key=lambda x: x[3], reverse=True)   # x[3] = sls
        fallback.sort(key=lambda x: x[3], reverse=True)
        others.sort(key=lambda x: x[3], reverse=True)

        def _consume(group):
            nonlocal out
            for delta_min, kind, payload, sls, gid in group:

                if len(out) >= max_cards:
                    break

                d_local = _kick_date_local_from_pick(payload)
                already_n = _count_published_picks_for_gid(d_local, gid)

                # máx. 3 picks por jogo no DIA
                if already_n + pick_by_game[gid] >= 3:
                    continue

                # máx. X picks por jogo neste SLOT (PICK_PER_MATCH_PER_SLOT)
                if pick_by_game[gid] >= PICK_PER_MATCH_PER_SLOT:
                    continue

                # evita repetir exatamente o mesmo pick
                if already_published_pick(payload):
                    continue

                out.append(("pick", payload, sls))
                pick_by_game[gid] += 1

        # 3) Consome primeiro até 1h, depois 1–2h, depois o resto do dia (se ainda faltou slot)
        _consume(primary)
        if len(out) < max_cards:
            _consume(fallback)
        if len(out) < max_cards:
            _consume(others)



    # ---------- COMBOS ----------
    raw_intra = []
    raw_inter = []

    def _combo_kind_and_games(c: Dict[str, Any]) -> Tuple[str, Set[str]]:
        games = set()
        for leg in c.get("legs", []) or []:
            mm = leg.get("home") or leg.get("mandante") or ""
            vv = leg.get("away") or leg.get("visitante") or ""
            hh = leg.get("hora_utc") or leg.get("kickoff") or ""

            key = f"{mm}|{vv}|{hh}"
            games.add(hashlib.md5(key.encode("utf-8")).hexdigest()[:10])

        kind = "intra" if len(games) == 1 else "inter"
        return kind, games

    # Coleta combos corretamente do JSON
    for c in (data.get("combos", []) or []):

        pr  = _f(c.get("prob_real_combo", 0.0))
        evc = _f(c.get("ev_combo", 0.0))
        sls = sls_score(pr, evc)

        kind, games = _combo_kind_and_games(c)

        ek = _earliest_leg_kickoff(c)
        if not ek:
            continue
        if (ek - now_local).total_seconds() / 60.0 < MIN_LEAD_MIN:
            continue

        if kind == "intra":
            gid = next(iter(games)) if games else ""
            raw_intra.append(("combo", c, sls, gid))
        else:
            raw_inter.append(("combo", c, sls, games))

    raw_intra.sort(key=lambda x: x[2], reverse=True)
    raw_inter.sort(key=lambda x: x[2], reverse=True)

    # ---------- INTRA ----------
    if len(out) < max_cards and any(s in secs for s in ("intra_game_combos","combos")):
        for kind, payload, sls, gid in raw_intra:

            if len(out) >= max_cards:
                break

            if intra_by_game[gid] >= INTRA_PER_MATCH_PER_SLOT:
                continue

            if already_published_combo(payload):
                continue

            out.append(("combo", payload, sls))
            intra_by_game[gid] += 1

    # ---------- INTER ----------
    if len(out) < max_cards and any(s in secs for s in ("doubles","trebles","multiples","inter_game_multiples","combos")):

        buckets = {"duplo": [], "triplo": [], "multi": []}

        for kind, payload, sls, games in raw_inter:

            legs = payload.get("legs", []) or []
            nlegs = len(legs)
            if nlegs == 2:
                buckets["duplo"].append(("combo", payload, sls, games))
            elif nlegs == 3:
                buckets["triplo"].append(("combo", payload, sls, games))
            else:
                buckets["multi"].append(("combo", payload, sls, games))

        for t in types_order:
            for kind, payload, sls, games in buckets.get(t, []):
                if len(out) >= max_cards:
                    break

                if any(inter_by_game[g] >= INTER_PER_MATCH_PER_SLOT for g in games):
                    continue

                if already_published_combo(payload):
                    continue

                out.append(("combo", payload, sls))

                for g in games:
                    inter_by_game[g] += 1

    return out[:max_cards]
# ===================== END SCHEDULER (CORRIGIDO) =====================


def _combo_core_metrics(c: Dict[str, Any]):
    """
    Extrai métricas do combo a partir dos campos do JSON.
    Retorna: pr, evc, fair, oddc, sls_c, left, right, titulo
    """
    pr   = _f(c.get("prob_real_combo", c.get("prob_ajustada", 0.0)))  # 0..1
    evc  = _f(c.get("ev_combo", c.get("ev", 0.0)))
    # odd justa pode vir pronta (fair_combo/odd_justa) ou calculada
    fair = _to_float_odd(c.get("fair_combo", c.get("odd_justa")))
    if fair <= 0 and pr > 0:
        fair = 1.0 / pr
    oddc = _to_float_odd(c.get("odd_combo"))

    sls_c = sls_score(pr, evc)
    left  = primary_badges(pr, evc)
    right = right_badge_sls(sls_c)
    titulo = c.get("titulo", "Múltipla")
    return pr, evc, fair, oddc, sls_c, left, right, titulo


def _fmt_combo_leg_block(leg: Dict[str, Any]) -> List[str]:
    """
    Renderiza UMA perna no mesmo padrão do card individual.
    """
    # normaliza odd da perna (aceita "@1.90")
    if isinstance(leg.get("odd_mercado"), str):
        leg["odd_mercado"] = _to_float_odd(leg["odd_mercado"])

    liga = leg.get("campeonato", leg.get("league", "—"))
    pais = leg.get("pais", leg.get("country", "—"))

    dd, hh = format_date_hour_from_utc_str(leg.get("hora_utc") or leg.get("hora"))
    when_line = f"🕒 <b>{dd or '—'}</b> | <b>{hh or '—'}</b> (UTC: -3)"

    home = leg.get("mandante") or leg.get("home") or "?"
    away = leg.get("visitante") or leg.get("away") or "?"
    match_line = f"⚽ <b>{home}</b> vs <b>{away}</b>"

    mercado_pt = translate_market(leg.get("mercado") or leg.get("market") or "")
    sel = (leg.get("selecao") or leg.get("selection") or leg.get("pick") or "")
    sel = (sel.replace("1st Half","1º Tempo").replace("2nd Half","2º Tempo")
              .replace("Over","Mais de").replace("Under","Menos de").replace("Goals","gols")
              .replace("BTTS Yes","Ambos Marcam — Sim").replace("BTTS No","Ambos Marcam — Não"))

    pprob = _f(leg.get("prob_real", 0.0))
    pev   = _f(leg.get("ev", 0.0))
    podd  = _to_float_odd(leg.get("odd_mercado"))
    pfair = (1.0/pprob) if pprob > 0 else 0.0

    bloc = [
        f"🏆 {liga} · {pais}",
        when_line,
        match_line,
        "",
        f"Mercado: <b>{mercado_pt}</b>",
        f"Seleção: <b>{sel}</b>",
    ]

    has_any_metric = (pprob > 0) or (podd > 0) or (pev != 0.0)
    if has_any_metric:
        bloc += [
            "",
            f"Prob. real: <b>{pprob*100:.1f}%</b>  |  Odd justa: <b>{('@'+format(pfair,'.2f')) if pfair>0 else '—'}</b>",
            f"Odd mercado: <b>{('@'+format(podd,'.2f')) if podd>0 else '—'}</b>  |  EV: <b>{pev:.1f}%</b>",
        ]

    leg_note = (leg.get("notes_pt") or leg.get("notes_pt") or "").strip()
    if leg_note:
        bloc += ["", f"🎩 <b>BM:</b> {leg_note}"]

    return bloc


def _fmt_combo_msg(c: Dict[str, Any]) -> str:
    """
    Renderiza combos no mesmo padrão do card individual:
    - Cabeçalho com badges e título
    - Linhas: Prob real | Odd Justa   e   Odd Mercado | EV
    - Depois, UM BLOCO por perna (liga/país, data/hora, times, mercado/seleção e métricas)
    - Sem ROI; Odd Justa com 2 casas; times/data/hora em negrito; (UTC: -3)
    """
    pr   = _f(c.get("prob_real_combo", 0.0))
    evc  = _f(c.get("ev_combo", 0.0))
    oddc = _to_float_odd(c.get("odd_combo"))
    fair = (1.0/pr) if pr > 0 else 0.0

    sls_c = sls_score(pr, evc)
    left  = primary_badges(pr, evc)
    right = right_badge_sls(sls_c)
    titulo = c.get("titulo", "Múltipla")

    def _leg_time_str(leg: Dict[str, Any]) -> str:
        for k in ("hora_utc", "hora", "kickoff", "date_GMT", "date_local"):
            v = leg.get(k)
            if isinstance(v, str) and v.strip():
                return v
        return ""

    legs = c.get("pernas") or c.get("legs") or []
    dict_legs, raw_legs = [], []
    for lg in legs:
        if isinstance(lg, dict):
            dict_legs.append(lg)
        else:
            raw_legs.append(str(lg))
    dict_legs = sorted(dict_legs, key=lambda L: _dt_key_or_now(_leg_time_str(L)))

    def _render_leg(leg: Dict[str, Any]) -> List[str]:
        liga = leg.get("campeonato", leg.get("league", "—"))
        pais = leg.get("pais", leg.get("country", "—"))
        dd, hh = format_date_hour_from_utc_str(leg.get("hora_utc") or leg.get("hora"))
        clock_emoji = _clock_emoji_for_hhmm(hh or "")
        when_line = f"{clock_emoji} <b>{dd or '—'}</b> | <b>{hh or '—'}</b> (UTC: -3)"


        home = leg.get("mandante") or leg.get("home") or "?"
        away = leg.get("visitante") or leg.get("away") or "?"
        match_line = f"⚽ <b>{home}</b> vs <b>{away}</b>"

        mercado_pt = translate_market(leg.get("mercado") or leg.get("market") or "")
        sel = (leg.get("selecao") or leg.get("selection") or leg.get("pick") or "")
        sel = (sel
               .replace("1st Half", "1º Tempo")
               .replace("2nd Half", "2º Tempo")
               .replace("Over", "Mais de")
               .replace("Under", "Menos de")
               .replace("Goals", "Gols")
               .replace("BTTS Yes", "Ambos Marcam — Sim")
               .replace("BTTS No", "Ambos Marcam — Não"))

        pprob = _f(leg.get("prob_real") or leg.get("prob") or 0.0)
        pev   = _f(leg.get("ev") or 0.0)
        podd  = _to_float_odd(leg.get("odd_mercado") or leg.get("odd") or 0.0)
        pfair = (1.0/pprob) if pprob > 0 else 0.0

        bloc = [
            "—",
            f"🏆 {liga} · {pais} {flag}",
            when_line,
            match_line,
            "",
            f"{mercado}\nSeleção: {selecao}"
        ]
        if (pprob > 0) or (podd > 0) or (pev != 0.0):
            bloc += [
                "",
                f"Prob. real: <b>{pprob*100:.1f}%</b>  |  Odd Justa: <b>{('@'+format(pfair, '.2f')) if pfair>0 else '—'}</b>",
                f"Odd Mercado: <b>{('@'+format(podd, '.2f')) if podd>0 else '—'}</b>  |  EV: <b>{pev:.1f}%</b>",
            ]

        note = (leg.get("notes_pt") or leg.get("notes_pt") or "").strip()
        if note:
            bloc += ["", f"🎩 <b>BM:</b> {note}"]

        return bloc

    lines = [
        BRAND_LINE,
        "",
        f"{left}<b>{titulo}</b>{right}",
        "",
        f"Prob. real (combo): <b>{pr*100:.1f}%</b>  |  Odd Justa (combo): <b>{('@'+format(fair, '.2f')) if fair>0 else '—'}</b>",
        f"Odd Mercado (combo): <b>{('@'+format(oddc, '.2f')) if oddc>0 else '—'}</b>  |  EV (combo): <b>{evc:.1f}%</b>",
        "",
    ]

    for leg in dict_legs:
        if isinstance(leg.get("odd_mercado"), str):
            leg["odd_mercado"] = _to_float_odd(leg["odd_mercado"])
        lines += _render_leg(leg)
        lines.append("")

    for raw in raw_legs:
        lines.append("—")
        lines.append(f"• {raw}")
        lines.append("")

    lines.append(_pick_aforismo_for_sls(sls_c))
    return "\n".join(str(x) for x in lines)


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




# 🔧 BLOCO NOVO — leitura moderna de odds*.json (com cabeçalho e metadados)
from pathlib import Path

DATA_DIR = Path("/data")  # ou '/opt/render/project/src/data' se for seu caminho real

def _read_json_silent(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


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
    Recebe o dict carregado do odds_*.json e normaliza:
    - picks: data/hora, tradução de mercado/selecao, odd_mercado → float, odd_justa @x.xx, sls
    - combos: aceita 'pernas' (ou 'legs'), normaliza cada perna e adiciona campos de topo
              prob_real_combo, ev_combo, odd_combo, sls
    """
    if not isinstance(data, dict):
        return {}

    picks = data.get("picks", []) or []
    combos = data.get("combos", []) or []

    # ---------- PICKS ----------
    normalized_picks = []
    for p in picks:
        prob = _f(p.get("prob_real") or p.get("prob") or 0.0)   # 0..1
        ev   = _f(p.get("ev") or p.get("EV") or 0.0)

        # data/hora
        data_str, hora_str = format_date_hour_from_utc_str(
            p.get("hora_utc") or p.get("hora") or p.get("kickoff") or p.get("date_GMT") or _pick_time_str(p)
        )

        # mercado/seleção
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

        # odds
        odd_mercado_num = _to_float_odd(p.get("odd_mercado") or p.get("odd_market") or 0.0)
        odd_justa_val = (1.0 / prob) if prob > 0 else 0.0

        newp = dict(p)
        if data_str: newp["data"] = data_str
        if hora_str: newp["hora"] = hora_str
        newp["mercado"] = mercado_pt
        newp["selecao"] = selecao_pt
        newp["odd_mercado"] = odd_mercado_num  # sempre número
        newp["odd_justa"]   = f"@{odd_justa_val:.2f}" if odd_justa_val > 0 else (newp.get("fair_odd") or newp.get("fairOdd") or None)
        newp["ev"]  = ev
        newp["roi"] = float(newp.get("roi") or ev or 0.0)
        newp["sls"] = sls_score(prob, ev)
        normalized_picks.append(newp)

    normalized_picks.sort(key=lambda x: x.get("sls", 0), reverse=True)

    # ---------- COMBOS ----------
    normalized_combos = []
    for c in combos:
        newc = dict(c)

        # Mapear campos de topo
        pr_combo  = _f(c.get("prob_real_combo") or c.get("prob_ajustada") or c.get("prob_combo") or 0.0)
        ev_combo  = _f(c.get("ev_combo") or c.get("ev") or 0.0)
        odd_combo = _to_float_odd(c.get("odd_combo") or c.get("odd") or 0.0)

        # pernas
        legs_in = c.get("pernas") or c.get("legs") or []
        normalized_legs = []
        for leg in legs_in:
            if isinstance(leg, dict):
                dd, hh = format_date_hour_from_utc_str(leg.get("hora_utc") or leg.get("hora") or leg.get("kickoff"))
                leg_norm = dict(leg)
                if dd: leg_norm["data"] = dd
                if hh: leg_norm["hora"] = hh
                leg_norm["mercado"] = translate_market(leg.get("mercado") or leg.get("market") or "")
                leg_norm["selecao"] = (leg.get("selecao") or leg.get("selection") or leg.get("pick") or "")
                leg_norm["odd_mercado"] = _to_float_odd(leg.get("odd_mercado") or leg.get("odd") or 0.0)
                normalized_legs.append(leg_norm)
            else:
                # fallback: manter string crua
                normalized_legs.append(str(leg))

        # SLS do combo (média SLS das pernas com dados)
        perna_sls = []
        for pl in normalized_legs:
            if isinstance(pl, dict):
                pprob = _f(pl.get("prob_real") or pl.get("prob") or 0.0)
                pev   = _f(pl.get("ev") or 0.0)
                if pprob > 0:
                    perna_sls.append(sls_score(pprob, pev))
        sls_c = sum(perna_sls) / len(perna_sls) if perna_sls else sls_score(pr_combo, ev_combo)

        newc["pernas"] = normalized_legs
        newc["prob_real_combo"] = pr_combo
        newc["ev_combo"]        = ev_combo
        newc["odd_combo"]       = odd_combo
        newc["sls"]             = sls_c
        normalized_combos.append(newc)

    normalized_combos.sort(key=lambda x: x.get("sls", 0), reverse=True)

    data["picks"]  = normalized_picks
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
    """
    Badge textual para EV (Corujão) seguindo as mesmas faixas de EV:

         $  5%–24.9%
        💳  25%–44.9%
        💵  45%–74.9%
        💰  >=75%
    """
    if ev >= 75.0:
        return "💰 EV+"
    if ev >= 45.0:
        return "💵 EV+"
    if ev >= 25.0:
        return "💳 EV"
    if ev >= 5.0:
        return "$ EV"
    return "⚪"


def _render_pick_block_for_corujao(p: Dict[str, Any]) -> List[str]:
    # probabilidade e EV já no novo padrão
    prob = _f(p.get("prob_real", 0.0))             # 0..1
    ev   = _f(p.get("ev") or p.get("ev_percent") or p.get("EV_percent") or 0.0)  # em %

    # odds
    odd_num = _to_float_odd(p.get("odd_mercado") or p.get("odd") or 0.0)
    odd_str = f"@{odd_num:.2f}" if odd_num > 0 else "—"
    odd_justa = (1.0 / prob) if prob > 0 else 0.0
    odd_justa_str = f"@{odd_justa:.2f}" if odd_justa > 0 else "—"

    # data/hora + relógio sincronizado com o kickoff
    date_str, hour_str = format_date_hour_from_utc_str(
        p.get("hora_utc") or p.get("hora") or _pick_time_str(p)
    )
    clock_emoji = _clock_emoji_for_hhmm(hour_str or "")
    when_line = f"{clock_emoji} <b>{date_str or '—'}</b> | <b>{hour_str or '—'}</b> {TZ_LABEL}"

    # mercado / seleção no novo padrão (builder já manda limpo)
    raw_market = str(p.get("market") or p.get("mercado") or "").strip()
    raw_sel    = str(p.get("selection") or p.get("selecao") or "").strip()

    mercado_pt = translate_market(raw_market)
    selecao_pt = (raw_sel
        .replace("Over", "Acima de")
        .replace("Under", "Abaixo de")
        .replace("1st Half", "1º Tempo")
        .replace("2nd Half", "2º Tempo")
        .replace("Goals", "gols")
        .replace("BTTS Yes", "Ambas Marcam — Sim")
        .replace("BTTS No",  "Ambas Marcam — Não")
    )

    return [
        f"🏆 {p.get('campeonato','—')} · {p.get('pais','—')}",
        when_line,
        f"⚽ <b>{p.get('mandante','?')}</b> vs <b>{p.get('visitante','?')}</b>",
        "",
        f"Mercado: <b>{mercado_pt}</b>",
        f"Seleção: <b>{selecao_pt}</b>",
        "",
        f"Prob. real: <b>{prob*100:.1f}%</b>  |  Odd justa: <b>{odd_justa_str}</b>",
        f"Odd mercado: <b>{odd_str}</b>  |  EV: <b>{ev:.1f}%</b>",
        "",
        f"🎩<b>BM:</b> {p.get('notes_pt','—')}",
    ]


TELEGRAM_SAFE_LIMIT = 3500  # margem de segurança abaixo dos 4096 chars


async def _send_coruja_card_in_chunks(picks: List[Dict[str, Any]], footer_aphorism: Optional[str]) -> bool:
    """
    Envia o Corujão quebrado em vários cards se passar do limite do Telegram.

    Regras:
      - Cada mensagem começa com BRAND_LINE + separador + título "Na Gaveta da Coruja".
      - Entre cada pick, um separador HR.
      - O aforismo aparece **apenas na última mensagem**, se existir.
    """
    HR = "────────────────────────────────"

    def _s(x) -> str:
        return "" if x is None else (x if isinstance(x, str) else str(x))

    header_title = "🌙🦉 <b>Na Gaveta da Coruja</b>"

    header_block = "\n".join([
        _s(BRAND_LINE),
        HR,
        header_title,
        HR,
    ])

    if not picks:
        return False

    current_text = header_block
    something_sent = False

    for idx, p in enumerate(picks):
        # bloco do pick
        block_lines = _render_pick_block_for_corujao(p)
        block_lines.append(HR)
        block = "\n".join(block_lines)

        # se este bloco não cabe no texto atual, envia o que já temos e começa outro card
        if len(current_text) + 1 + len(block) > TELEGRAM_SAFE_LIMIT:
            await bot.send_message(GROUP_ID, current_text)
            something_sent = True
            current_text = header_block  # novo card, com o mesmo cabeçalho

        # adiciona o bloco ao texto atual
        if current_text:
            current_text += "\n" + block
        else:
            current_text = block

    # agora tratamos o aforismo – **só na última mensagem**
    if footer_aphorism:
        # se não couber junto, mandamos o card atual e abrimos outro só para o aforismo + header
        if len(current_text) + 2 + len(footer_aphorism) > TELEGRAM_SAFE_LIMIT:
            await bot.send_message(GROUP_ID, current_text)
            something_sent = True
            current_text = header_block + "\n" + footer_aphorism
        else:
            current_text += "\n" + footer_aphorism

    # envia o último pedaço restante
    if current_text.strip():
        await bot.send_message(GROUP_ID, current_text)
        something_sent = True

    return something_sent


def render_many_picks_as_one_card(
    picks: List[Dict[str, Any]],
    title: str,
    footer_aphorism: Optional[str],
    *,
    is_corujao: bool = False
) -> str:
    HR = "────────────────────────────────"

    def _s(x) -> str:
        return "" if x is None else (x if isinstance(x, str) else str(x))

    header_title = _s("🌙🦉 <b>Na Gaveta da Coruja</b>") if is_corujao else _s(f"<b>{title}</b>")

    lines: List[str] = [
        _s(BRAND_LINE),
        HR,
        header_title,
        HR,
    ]

    for p in picks:
        prob = float(p.get("prob_real", 0) or 0)
        odd  = _to_float_odd(p.get("odd_mercado", 0))
        fair = _to_float_odd(p.get("fair_odd", 0))  # mantido se quiser usar no futuro

        # Mercado / Seleção no novo padrão OMNIA (já limpos pelo builder)
        mercado = p.get("market") or p.get("mercado") or ""
        selecao = p.get("selection") or p.get("selecao") or ""

        # Data/hora local e relógio sincronizado com o kickoff
        date_str, hour_str = format_date_hour_from_utc_str(
            p.get("hora_utc") or _pick_time_str(p)
        )
        clock_emoji = _clock_emoji_for_hhmm(hour_str or "")

        # EV já em percentual no JSON
        ev_pct = _f(p.get("ev") or p.get("ev_percent") or p.get("EV_percent") or 0.0)

        league  = p.get("campeonato") or p.get("league") or "—"
        country = p.get("pais") or p.get("country") or "—"
        flag    = get_country_flag(country, league)

        bloco = (
            f"⚽ <b>{p.get('mandante','')}</b> vs <b>{p.get('visitante','')}</b>\n"
            f"{clock_emoji} <b>{date_str}</b> | <b>{hour_str}</b> (UTC: -3)\n"
            f"🏆 {league} · {country} {flag}\n\n"
            f"Mercado: <b>{mercado}</b>\n"
            f"Seleção: <b>{selecao}</b>\n\n"
            f"Prob. real: <b>{prob:.1%}</b>  |  "
            f"Odd justa: <b>{('@'+format((1/prob) if prob>0 else 0.0, '.2f')) if prob>0 else '—'}</b>\n"
            f"Odd mercado: <b>{('@'+format(odd,'.2f')) if odd>0 else '—'}</b>  |  "
            f"EV: <b>{ev_pct:.1f}%</b>"
        )

        lines.append(_s(bloco))
        note = (p.get("notes_pt") or "").strip()
        if note:
            lines.append(_s(f"🎩 <b>BM:</b> {note}"))
        lines.append(HR)

    if footer_aphorism:
        lines.append(_s(footer_aphorism))

    return "\n".join(lines)


def _split_card_by_hr(text: str) -> List[str]:
    """
    Divide o card pelo separador HR em blocos (mantendo header/rodapé adequados depois).
    Retorna a lista de blocos (cada um com um ou mais picks).
    """
    HR = "────────────────────────────────"
    parts = text.split(HR)
    # Remove espaços excessivos:
    parts = [p.strip("\n") for p in parts]
    # Colapsa blocos vazios
    parts = [p for p in parts if p.strip()]
    return parts

def _send_long_card_in_chunks(
    bot,
    chat_id: int,
    full_text: str,
    *,
    header_line: Optional[str] = None,
    footer_line: Optional[str] = None,
    safe_budget: int = TELEGRAM_SAFE_BUDGET
):
    """
    Envia o 'full_text' respeitando o limite do Telegram.
    - Tenta quebrar por HR (blocos de pick).
    - Se ainda estourar, faz um split adicional por linha.
    """
    HR = "────────────────────────────────"

    # Parte 1: tentar fatias por bloco (HR)
    blocks = _split_card_by_hr(full_text)

    # Reconstrói com cabeçalho/rodapé por chunk
    chunk_lines: List[str] = []
    chunks: List[str] = []

    def _flush_chunk():
        nonlocal chunk_lines, chunks
        if not chunk_lines:
            return
        body = "\n".join(chunk_lines).strip()
        msg = []
        if header_line:
            msg.append(header_line)
            msg.append(HR)
        if body:
            msg.append(body)
        if footer_line:
            msg.append(HR)
            msg.append(footer_line)
        final = "\n".join(msg).strip()
        if final:
            chunks.append(final)
        chunk_lines = []

    # Header (primeira linha do card geralmente é BRAND_LINE; usamos como header_line)
    # Footer (aforismo) tentaremos preservar ao fim do último chunk)
    # Para extrair header/footer do full_text:
    # - header_line: primeira linha (BRAND_LINE)
    # - footer_line: última linha *se* for um aforismo (heurística simples: começa com <b><i> ou contém "— Bet Masterson")
    all_lines = full_text.splitlines()
    inferred_header = all_lines[0].strip() if all_lines else None
    inferred_footer = None
    for tail in reversed(all_lines[-6:]):  # olha últimas 6 linhas
        if ("— Bet Masterson" in tail) or ("<b><i>" in tail and "</i></b>" in tail):
            inferred_footer = tail
            break
    if header_line is None:
        header_line = inferred_header
    if footer_line is None:
        footer_line = inferred_footer

    for b in blocks:
        candidate = (("\n".join(chunk_lines) + "\n" + HR + "\n" + b) if chunk_lines else b).strip()
        if len(candidate) <= safe_budget:
            # cabe no chunk atual
            if chunk_lines:
                chunk_lines.append(HR)
            chunk_lines.append(b)
        else:
            # fechar chunk atual e iniciar novo com este bloco
            _flush_chunk()
            if len(b) <= safe_budget:
                chunk_lines.append(b)
            else:
                # Parte 2: bloco ainda grande -> quebrar por linhas
                lines = b.splitlines()
                tmp: List[str] = []
                for ln in lines:
                    test = ("\n".join(tmp) + "\n" + ln) if tmp else ln
                    if len(test) <= safe_budget:
                        tmp.append(ln)
                    else:
                        # fecha pedaço
                        if tmp:
                            chunks.append("\n".join(([header_line, HR] if header_line else []) + tmp + ([HR, footer_line] if footer_line else [])))
                            tmp = [ln]
                        else:
                            # linha sozinha já estoura (raro) -> truncar com reticências
                            chunks.append("\n".join(([header_line, HR] if header_line else []) + [ln[:safe_budget-10] + "…"] + ([HR, footer_line] if footer_line else [])))
                            tmp = []
                if tmp:
                    chunks.append("\n".join(([header_line, HR] if header_line else []) + tmp + ([HR, footer_line] if footer_line else [])))

    _flush_chunk()

    # Envio
    async def _send_all():
        for i, c in enumerate(chunks, 1):
            await bot.send_message(chat_id, c)
            await asyncio.sleep(0.5)
    return _send_all()



import random

def _get_night_aphorism() -> Optional[str]:
    """
    Prioriza tags 'corujao' > 'madrugada'/'night'.
    Formatação exigida:
      • EN: negrito + itálico
      • PT: apenas itálico
      • Assinatura com ano quando disponível
    """
    path = AFORISMOS_FILE
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        arr = data["entries"] if isinstance(data, dict) and "entries" in data else (data if isinstance(data, list) else [])
        if not arr:
            return None

        def has_any_tag(a, wanted):
            tags = [str(t).lower() for t in (a.get("tags") or [])]
            return any(t in tags for t in wanted)

        pool = [a for a in arr if has_any_tag(a, {"corujao"})] \
            or [a for a in arr if has_any_tag(a, {"madrugada", "night"})]
        if not pool:
            return None

        a = random.choice(pool)

        # 1) formato bilingual
        en = (a.get("en") or "").strip().rstrip(".")
        pt = (a.get("pt") or "").strip()
        yr = a.get("year")
        if en and pt:
            sig = f"— Bet Masterson, {yr}" if yr else "— Bet Masterson"
            return f"<b><i>{en}.</i></b>\n<i>{pt}</i>\n{sig}"

        # 2) texto único
        txt = (a.get("text") or a.get("quote") or a.get("phrase") or "").strip()
        if txt:
            return f"<b><i>{txt}</i></b>\n— Bet Masterson"

        return None
    except Exception:
        return None

async def _collect_coruja_picks_for_date(d_local):
    tz_sp = tz.gettz(TZ_NAME)
    date_str = d_local.strftime("%Y-%m-%d")
    data = load_odds_for_date(date_str) or {}

    header = data.get("odds_file_header") or {}
    is_ready = bool(header.get("corujao_ready", False))

    coru = data.get("corujao") or {}
    picks = list(coru.get("picks", []) or [])
    if not picks:
        all_p = list((data.get("picks") or []))
        def _is_corujao(p):
            hh = str(p.get("hora","")).strip()
            return _is_corujao_hhmm(hh)
        picks = [p for p in all_p if _is_corujao(p)]
    picks.sort(key=lambda x: _dt_key_or_now(_pick_time_str(x)))
    return picks, is_ready

async def post_coruja_card():
    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    d_local = now_l.date()

    # Coleta picks candidatos ao Corujão
    picks, is_ready = await _collect_coruja_picks_for_date(d_local)
    if not picks:
        return False

    # 🔎 FILTRO NOVO: só entra prob_real ≥ 0.70 e EV > 0
    filtered_picks: List[Dict[str, Any]] = []
    for pick in picks:
        try:
            pr = float(pick.get("prob_real") or 0.0)
            ev = float(pick.get("ev") or 0.0)
        except Exception:
            continue

        if pr >= 0.20 and ev > 0:
            filtered_picks.append(pick)

    if not filtered_picks:
        return False

    # saneia '@'
    for p in filtered_picks:
        if isinstance(p.get("odd_mercado"), str):
            p["odd_mercado"] = _to_float_odd(p["odd_mercado"])
        if isinstance(p.get("fair_odd"), str):
            p["fair_odd"] = _to_float_odd(p["fair_odd"])

    # Agrupa por jogo (gid estável)
    jogos: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for p in filtered_picks:
        gid = _game_id_from_pick(p)
        jogos[gid].append(p)

    # Função para calcular SLS localmente, se não vier pronto
    def _sls_from_pick(px: Dict[str, Any]) -> float:
        try:
            if px.get("sls") is not None:
                return float(px.get("sls"))
        except Exception:
            pass
        pr_loc = float(px.get("prob_real", 0) or 0)
        ev_loc = _to_float_pct(px.get("ev", 0) or 0)
        return sls_score(pr_loc, ev_loc)

    # Aplica regra: no máximo 3 cards por jogo no dia (somando tudo)
    limited_picks: List[Dict[str, Any]] = []
    for gid, arr in jogos.items():
        # quantos desse jogo já saíram hoje
        already_n = _count_published_picks_for_gid(d_local, gid)
        quota = max(0, 3 - already_n)
        if quota <= 0:
            continue

        arr_sorted = sorted(arr, key=_sls_from_pick, reverse=True)
        limited_picks.extend(arr_sorted[:quota])

    if not limited_picks:
        return False

    # Ordena por horário para o card ficar organizado
    limited_picks.sort(key=lambda p: _dt_key_or_now(_pick_time_str(p)))

    aph = _get_night_aphorism()

    # usa o helper de chunks específico do Corujão
    ok = await _send_coruja_card_in_chunks(limited_picks, aph)
    if not ok:
        return False

    # se tudo foi enviado, marca os picks como publicados
    for p in limited_picks:
        mark_published_pick(p)

    return True


async def _send_super_game_card(g: Dict[str, Any], header: Optional[Dict[str, Any]] = None) -> bool:
    """
    Envia um Super Game (Jogão / Jogaço) para o GROUP_ID.

    Usa o template oficial fmt_super_game_card, que retorna
    uma lista de cards (textos). Cada card é enviado como
    uma mensagem separada no grupo.
    """
    if GROUP_ID == 0:
        print("[SUPER][WARN] GROUP_ID=0, não vou enviar mensagem.")
        return False

    try:
        cards = fmt_super_game_card(g, header=header)
        if not isinstance(cards, list):
            cards = [cards]

        sent_any = False
        for txt in cards:
            if not txt or not str(txt).strip():
                continue
            msg = str(txt)
            if "TELEGRAM_SAFE_LIMIT" in globals():
                if len(msg) > TELEGRAM_SAFE_LIMIT:
                    msg = msg[:TELEGRAM_SAFE_LIMIT - 10] + "\n…"
            await bot.send_message(GROUP_ID, msg, parse_mode="HTML")
            sent_any = True
            await asyncio.sleep(0.4)

        return sent_any
    except Exception as e:
        print("[SUPER][SEND_ERR]", repr(e))
        return False



async def scheduler_loop():
    """
    Agenda editorial + corujão + fallback automático.
    Robusto contra:
      - agenda ausente/corrupção (None/shape errado)
      - data None normalizada
      - rede/Telegram intermitente
      - flood/duplicações
    """
    daily_count = 0
    while True:
        try:
            # 0) Mantém /data sincronizado (respeita intervalo interno da função)
            try:
                ensure_data_files(force=False)
            except Exception as e:
                print("[SCHED][FETCH_WARN]", repr(e))
                # 0b) Limpa histórico de JSONs antigos (odds*.json + super_jogos-*.json)
                try:
                    cleanup_history()
                except Exception as e:
                    print("[SCHED][HIST_WARN]", repr(e))

            # 1) Carrega odds do dia e normaliza com fallback para dict vazio
            try:
                data = await load_odds_generic()
                if not isinstance(data, dict):
                    data = {}
                try:
                    data = normalize_odds(data)  # já trata campos dos picks/combos
                except Exception:
                    pass
            except Exception as e:
                print("[SCHED][LOAD_ODDS_ERR]", repr(e))
                data = {}

            # 2) Relógios
            tz_sp    = tz.gettz(TZ_NAME)
            now_utc  = datetime.now(timezone.utc)
            now_local = datetime.now(tz_sp)
            today_sp  = now_local.strftime("%Y-%m-%d")
            today_local = now_local.date()

            # 2-A) DISPARO AUTOMÁTICO DE SUPER GAMES (JOGÃO / JOGAÇO)
            try:
                sg = _load_super_games_for_date(today_local)
                # ------ NOVO SISTEMA DE PUBLICAÇÃO DE SUPER CARDS ------
                if sg:
                    header = sg.get("supercard_header") or {}
                    games  = sg.get("games") or []

                    for g in games:
                        gid = _super_game_id(g)

                        # Se já publicou todos os cards deste jogo → pula
                        if _super_pub_is_marked(gid):
                            continue

                        dt_k = _super_kickoff_dt_local(g)
                        if not dt_k:
                            continue

                        # gera o schedule completo (lista de horários) usando a nova lógica
                        sched = _super_compute_schedule_for_game(g, today_local)
                        if not sched:
                            continue

                        # seleciona qual card deve ser publicado AGORA (janela de ±60s)
                        for idx, tstamp in enumerate(sched):
                            if abs((now_local - tstamp).total_seconds()) <= 60:
                                cards = fmt_super_game_card(g, header=header)
                                if isinstance(cards, list):
                                    if idx < len(cards):
                                        msg = cards[idx]
                                    else:
                                        msg = cards[-1]
                                else:
                                    msg = str(cards)

                                if msg and str(msg).strip():
                                    if "TELEGRAM_SAFE_LIMIT" in globals():
                                        limit = TELEGRAM_SAFE_LIMIT
                                        if len(msg) > limit:
                                            msg = msg[:limit - 10] + "\n…"
                                    await bot.send_message(GROUP_ID, msg, parse_mode="HTML")
                                    _super_pub_mark_partial(gid, idx)
                                break

                        # Se já publicou todos os cards → marcar como concluído
                        if _super_pub_all_sent(gid, len(sched)):
                            _super_pub_mark(gid)

            except Exception as e:
                print("[SUPER][ERR]", repr(e))

            # 3) Corujão (00:00–00:10, 1x por dia)
            try:
                pub = _published_get() or {}
                coruja_key = f"coruja#{today_sp}"
                dt_coruja_utc = datetime.fromisoformat(
                    _to_utc_iso(today_sp, "00:00").replace("Z", "+00:00")
                )
                in_window = timedelta(0) <= (now_utc - dt_coruja_utc) <= timedelta(minutes=10)
                if in_window and not pub.get(coruja_key):
                    ran = await post_coruja_card()
                    if ran:
                        pub[coruja_key] = _now()
                        _published_set(pub)
            except Exception as _e:
                print("[SCHED][CORUJA_ERR]", repr(_e))

            # 4) Agenda editorial (apenas se MODE=editorial)
            agenda = {}
            if MODE == "editorial":
                try:
                    agenda = _safe_load(Path(AGENDA_JSON_PATH), {}) or {}
                    if not isinstance(agenda, dict):
                        agenda = {}
                except Exception as e:
                    print("[SCHED][AGENDA_LOAD_ERR]", repr(e))
                    agenda = {}

            plan = agenda.get("schedule_plan") or []
            if not isinstance(plan, list):
                plan = []

            if not plan:
                await asyncio.sleep(30)
                continue

            for slot in plan:
                try:
                    if not isinstance(slot, dict):
                        continue

                    t_local = slot.get("time_local")
                    if not t_local:
                        continue

                    dt_utc_iso = _to_utc_iso(today_sp, t_local)
                    dt_utc     = datetime.fromisoformat(dt_utc_iso.replace("Z", "+00:00"))

                    slot_local = dt_utc.astimezone(tz_sp)

                    # janela de disparo do slot: até +5 min
                    if not (timedelta(0) <= now_utc - dt_utc <= timedelta(minutes=5)):
                        continue

                    # ----- Seleção por refs especiais -----
                    refs = slot.get("refs") or []
                    if any(r in ("coruja", "coruja_card") for r in refs):
                        try:
                            ran = await post_coruja_card()
                            if ran:
                                key = _key_pub(dt_utc_iso, "coruja_card")
                                pubm = _published_get() or {}
                                pubm[key] = _now()
                                _published_set(pubm)
                        except Exception as e:
                            print("[SCHED][REF_CORUJA_ERR]", repr(e))
                        # refs consomem o slot
                        continue

                    # ----- Seleção automática (fallback) -----
                    items = []
                    if ENABLE_FALLBACK_SELECTION:
                        rule = slot.get("selection_rule") or {}
                        try:
                            items = await _select_by_rule(
                                data,
                                rule,
                                now_local=now_local,
                                slot_local=slot_local,
                            ) or []
                        except Exception as e:
                            print("[SCHED][SELECT_ERR]", repr(e))
                            items = []

                    # Publica cada item (pick/combo) respeitando limites
                    for it in items:
                        if not isinstance(it, (list, tuple)) or len(it) < 3:
                            continue
                        kind, payload, _sls = it

                        if daily_count >= MAX_PUBLICATIONS_PER_DAY:
                            break

                        # freio por hora
                        if _hour_count(dt_utc) >= HOURLY_MAX:
                            continue

                        # hash para anti-repost (payload pode ter tipos não serializáveis)
                        try:
                            ref_hash = hashlib.md5(
                                json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()
                            ).hexdigest()[:10]
                        except Exception:
                            ref_hash = hashlib.md5(str(payload).encode()).hexdigest()[:10]

                        key = _key_pub(dt_utc_iso, ref_hash)
                        last_ts = (_published_get() or {}).get(key)
                        if last_ts and (_now() - int(last_ts)) < MINUTES_BETWEEN_REPOST * 60:
                            continue

                        # publicação (com validações de lead e duplicação)
                        if kind == "pick":
                            kick = _pick_time_str(payload)
                            if not _time_ok_lead(kick, now_local, MIN_LEAD_MIN):
                                continue
                            if already_published_pick(payload):
                                continue
                            # saneia odds em string "@"
                            if isinstance(payload.get("odd_mercado"), str):
                                payload["odd_mercado"] = _to_float_odd(payload["odd_mercado"])
                            if isinstance(payload.get("fair_odd"), str):
                                payload["fair_odd"] = _to_float_odd(payload["fair_odd"])
                            try:
                                await bot.send_message(GROUP_ID, fmt_pick(payload))
                                mark_published_pick(payload)
                                _inc_hour_count(dt_utc)
                            except Exception as e:
                                print("[SCHED][SEND_PICK_ERR]", repr(e))
                                continue

                        elif kind == "combo":
                            ek = _earliest_leg_kickoff(payload)
                            if not ek:
                                continue
                            if (ek - now_local).total_seconds() / 60.0 < MIN_LEAD_MIN:
                                continue
                            if already_published_combo(payload):
                                continue
                            try:
                                await bot.send_message(GROUP_ID, _fmt_combo_msg(payload))
                                mark_published_combo(payload)
                                _inc_hour_count(dt_utc)
                            except Exception as e:
                                print("[SCHED][SEND_COMBO_ERR]", repr(e))
                                continue

                        # marca como publicado (protege contra flood no mesmo minuto)
                        pubm = _published_get() or {}
                        pubm[key] = _now()
                        _published_set(pubm)
                        daily_count += 1
                        await asyncio.sleep(0.7)

                except Exception as slot_err:
                    print("[SCHED][SLOT_ERR]", repr(slot_err))
                    continue

            await asyncio.sleep(30)

        except Exception as e:
            # guarda o loop em pé mesmo com exceções inesperadas
            print("SCHED_LOOP_ERROR:", repr(e))
            await asyncio.sleep(30)


# -------------------- FASTAPI (WEBHOOK CAKTO) --------------------------------------------
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
# ---- Novo: gerar convite ao confirmar pagamento ----
async def on_payment_confirmed(user_id: str | int, send_dm: bool = True) -> Optional[str]:
    """
    Gera link com JOIN REQUEST, registra no invites_map e (opcionalmente) envia DM.
    Retorna a URL do convite ou None em caso de erro.
    """
    try:
        uid = int(user_id)
    except Exception:
        return None

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
        inv[link.invite_link] = {
            "allowed_uid": uid,
            "expire": expire,
            "created_at": _now()
        }
        invites_set(inv)

        if send_dm:
            await bot.send_message(
                uid,
                "✅ Pagamento confirmado!\n"
                "Use este link para solicitar entrada (24h, 1 uso):\n"
                f"{link.invite_link}\n\n"
                "Ao clicar, seu pedido será aprovado automaticamente."
            )

        return link.invite_link

    except Exception as e:
        print("INVITE_LINK_ERROR:", uid, repr(e))
        return None


@app.get("/healthz")
async def healthz():
    return PlainTextResponse("OK")

# ---------------------- DEBUG ENDPOINTS ----------------------

def _debug_check(token: str):
    if not DEBUG_TOKEN:
        raise HTTPException(500, "debug disabled")
    if token != DEBUG_TOKEN:
        raise HTTPException(403, "forbidden")


@app.get("/debug/subs")
async def debug_subs(token: str):
    _debug_check(token)
    return JSONResponse(subs_get())


@app.get("/debug/invites")
async def debug_invites(token: str):
    _debug_check(token)
    return JSONResponse(invites_get())


@app.get("/debug/cakto_events")
async def debug_cakto_events(token: str):
    _debug_check(token)
    data = _safe_load(LOG_PATH, [])
    return JSONResponse(data)

# -------------------------------------------------------------

@app.post("/cakto/webhook")
async def cakto_webhook(request: Request):
    body = await request.body()

    # autenticação via assinatura
    if not _auth_ok(request, body):
        raise HTTPException(401, "unauthorized")

    try:
        payload = json.loads(body.decode("utf-8") or "{}")
    except Exception:
        raise HTTPException(400, "invalid json")

    # ---------- salvar evento bruto ----------
    save_event(payload)

    # ---------- identificar evento ----------
    event = str(payload.get("event", "")).strip().lower()
    raw_status = str(payload.get("status", "")).strip().lower()

    # mapear evento → status interno
    status = ""
    if event in {
        "purchase_approved",
        "subscription_created",
        "subscription_renewed"
    }:
        status = "active"
    elif event in {
        "subscription_canceled",
        "subscription_cancelled",
        "subscription_renewal_refused"
    }:
        status = "cancelled"
    elif event == "subscription_expired":
        status = "expired"
    elif raw_status in {"active","cancelled","expired"}:
        status = raw_status

    # ---------- localizar o telegram_id ----------
    uid = (
        str(payload.get("telegram_id") or "").strip()
        or str(payload.get("ref") or "").strip()
        or str((payload.get("metadata") or {}).get("ref") or "").strip()
        or str((payload.get("custom_fields") or {}).get("telegram_id") or "").strip()
    )

    # ---------- extrair expiração ----------
    expires_at = int(
        payload.get("expires_at")
        or payload.get("subscription_expires_at")
        or 0
    ) or 0

    # ---------- nome do plano ----------
    plan = str(
        payload.get("plan")
        or payload.get("product_name")
        or payload.get("subscription_name")
        or ""
    ).strip() or "Cakto"

    # ---------- atualizar assinatura + link ----------
    if uid and status:
        upsert_sub(uid, status, expires_at, plan)

        if status == "active":
            # envia DM com link para o usuário após pagamento
            await on_payment_confirmed(uid, send_dm=True)
    else:
        # se uid falhou, alerta os admins
        try:
            await notify_admins(
                f"[CAKTO] Evento não mapeado corretamente.\n"
                f"event={event!r} raw_status={raw_status!r} uid={uid!r}\n"
                f"payload={payload}"
            )
        except:
            pass

    return JSONResponse({
        "ok": True,
        "event": event,
        "uid": uid,
        "status": status
    })

# ------------------------- FIM SISTEMA PGTOS - -------------------


@dp.message(Command("games_today"))
async def games_today_cmd(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    tz_sp = tz.gettz(TZ_NAME)
    today_iso = datetime.now(tz_sp).strftime("%Y-%m-%d")

    obj = load_odds_for_date(today_iso)
    if not obj:
        return await m.answer("Não há arquivo odds*.json com target_date_local para hoje.")

    # junta picks normais + corujão e filtra por DATA local == hoje
    all_picks = (obj.get("picks") or []) \
              + (obj.get("singles") or []) \
              + ((obj.get("corujao") or {}).get("picks") or [])
    # evita erro se vier string ou qualquer coisa que não seja dict
    all_picks = [p for p in all_picks if isinstance(p, dict)]

    # mantém a ordenação por horário local inferido
    all_picks.sort(key=lambda p: _dt_key_or_now(_pick_time_str(p)))


    bucket = {}  # gid -> info do jogo
    for p in all_picks:
        tstr = _pick_time_str(p)
        dtl  = _parse_any_dt_local(tstr)
        if not dtl or dtl.strftime("%Y-%m-%d") != today_iso:
            continue
        gid = _game_id_from_pick(p)
        if gid not in bucket:
            bucket[gid] = {
                "hora": tstr,
                "liga": p.get("campeonato",""),
                "pais": p.get("pais",""),
                "home": p.get("mandante",""),
                "away": p.get("visitante",""),
                "count": 0,
            }
        bucket[gid]["count"] += 1

    if not bucket:
        return await m.answer("Nenhum pick encontrado para hoje.")

    lines = [f"🎯 Hoje {today_iso}: {sum(v['count'] for v in bucket.values())} picks (todas janelas)"]
    for gid, info in sorted(bucket.items(), key=lambda kv: _dt_key_or_now(kv[1]["hora"])):
        lines.append(
            f"• <code>{gid}</code> — {info['home']} x {info['away']} — {as_local(info['hora'])}"
        )

    # chunk em mensagens de até ~3800 chars
    chunk, size = [], 0
    for ln in lines:
        if size + len(ln) + 1 > 3800:
            await m.answer("\n".join(chunk))
            chunk, size = [ln], len(ln)+1
        else:
            chunk.append(ln); size += len(ln)+1
    if chunk:
        await m.answer("\n".join(chunk))


@dp.message(Command("games_tomorrow"))
async def games_tomorrow_cmd(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    tomorrow = (now_l.date() + timedelta(days=1))
    tomorrow_iso = tomorrow.strftime("%Y-%m-%d")

    obj = load_odds_for_date(tomorrow_iso)
    if not obj:
        return await m.answer(f"Não há arquivo odds*.json com target_date_local = {tomorrow_iso}.")

    # junta picks normais + picks do bloco corujao (se houver)
    picks = (obj.get("picks") or []) \
          + (obj.get("singles") or []) \
          + ((obj.get("corujao") or {}).get("picks") or [])
    picks = [p for p in picks if isinstance(p, dict)]

    # filtra pelo DATE de amanhã (pelo timestamp do pick)
    bucket = {}
    for p in picks:
        tstr = _pick_time_str(p)        # tua função existente
        dtl  = _parse_any_dt_local(tstr)  # tua função existente
        if not dtl or dtl.date() != tomorrow:
            continue

        gid = _game_id_from_pick(p)  # tua função existente
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

    # chunking seguro
    chunk, s = [], 0
    for ln in lines:
        if s + len(ln) + 1 > 3800:
            await m.answer("\n".join(chunk)); chunk, s = [ln], len(ln)+1
        else:
            chunk.append(ln); s += len(ln)+1
    if chunk:
        await m.answer("\n".join(chunk))

@dp.message(Command("supercard_preview"))
async def cmd_supercard_preview(m: types.Message):
    if not await _require_private(m):
        return

    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    today = now_l.date()

    data = _load_super_games_for_date(today)
    if not data:
        return await m.answer("❌ Nenhum arquivo de super jogos encontrado para hoje.")

    games = data.get("games") or []
    if not games:
        return await m.answer("❌ O arquivo de super jogos de hoje não tem jogos dentro.")

    # ordena por horário
    games_sorted = sorted(
        games,
        key=lambda g: _super_kickoff_dt_local(g) or datetime.max.replace(tzinfo=tz_sp)
    )

    # verifica se o usuário passou um índice: "/supercard_preview 2"
    idx_req = None
    parts = (m.text or "").strip().split()
    if len(parts) > 1:
        try:
            idx_req = int(parts[1])
        except ValueError:
            idx_req = None

    chosen = None

    if idx_req is not None:
        if 1 <= idx_req <= len(games_sorted):
            chosen = games_sorted[idx_req - 1]
        else:
            return await m.answer(f"⚠️ Índice inválido. Use um número entre 1 e {len(games_sorted)}.")
    else:
        # pega o próximo jogo ainda não iniciado, se existir; senão, o primeiro da lista
        upcoming = [g for g in games_sorted if (_super_kickoff_dt_local(g) or now_l) >= now_l]
        chosen = upcoming[0] if upcoming else games_sorted[0]

    # resumo dos super jogos + horário de disparo
    lines = []
    lines.append(f"🧪 <b>Super Games de hoje ({today.strftime('%d/%m/%Y')})</b>\n")
    for i, g in enumerate(games_sorted, start=1):
        dt_k = _super_kickoff_dt_local(g) or now_l
        kickoff_str = dt_k.strftime("%Hh%M")
        lead = SUPER_CARD_LEAD_MIN
        trigger_dt = dt_k - timedelta(minutes=lead)
        trigger_str = trigger_dt.strftime("%Hh%M")

        tier_raw = str(g.get("super_type") or "").lower()
        is_jogaco = bool(g.get("flag_jogaco")) or (tier_raw == "jogaco")
        icon = "👑" if is_jogaco else "🏟"
        title = "Jogaço" if is_jogaco else "Jogão"

        home = g.get("home", "—")
        away = g.get("away", "—")

        lines.append(
            f"{i}) {icon} {title} — {kickoff_str} | {home} vs {away}\n"
            f"   Disparo previsto: {trigger_str} (−{lead} min)"
        )

    await m.answer("\n".join(lines), parse_mode="HTML")

    # prévia do card do jogo escolhido
    card_txt = fmt_super_game_card(chosen, header=data.get("supercard_header"))
    await m.answer("👁‍🗨 <b>Pré-visualização do card a ser enviado:</b>", parse_mode="HTML")
    await m.answer(card_txt, parse_mode="HTML")


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
async def which_source_cmd(m: types.Message):
    tz_sp = tz.gettz(TZ_NAME)
    today_sp = datetime.now(tz_sp).strftime("%Y-%m-%d")
    tomorrow_sp = (datetime.now(tz_sp) + timedelta(days=1)).strftime("%Y-%m-%d")

    def _which(date_str):
        picked = None
        picked_fp = None
        for fp in Path("/data").glob("odds*.json"):
            obj = _read_json_silent(fp)
            if not obj:
                continue
            hdr = (obj.get("odds_file_header") or {})
            meta = (obj.get("meta") or {})
            d1 = (hdr.get("target_date_local") or meta.get("target_date_local") or "").strip()
            if d1 == date_str:
                ga = (meta.get("generated_at") or "")
                if (not picked) or ga > picked[0]:
                    picked = (ga, obj)
                    picked_fp = fp
        return (picked_fp, (picked[1] if picked else None))

    fp_today, obj_today = _which(today_sp)
    fp_tom, obj_tom = _which(tomorrow_sp)

    lines = ["Fontes detectadas (header-based):"]
    if fp_today:
        lines.append(f"Hoje {today_sp} -> {fp_today.name}")
    else:
        lines.append(f"Hoje {today_sp} -> (nenhum odds*.json com esse target_date_local)")

    if fp_tom:
        lines.append(f"Amanhã {tomorrow_sp} -> {fp_tom.name}")
    else:
        lines.append(f"Amanhã {tomorrow_sp} -> (nenhum odds*.json com esse target_date_local)")

    # Também mostra o que existe em /data para auditoria
    avail = ", ".join(sorted([p.name for p in Path('/data').glob('odds*.json')]))
    lines.append(f"Arquivos em /data: {avail or '—'}")

    await m.answer("\n".join(lines))


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
            if delta_min < 0:
                when_txt = "⏳ falta %d min" % abs(delta_min)
            elif delta_min <= 10:
                when_txt = "🟢 janela (%d min atrás)" % delta_min
            else:
                when_txt = "⏱️ passou faz %d min" % delta_min
        except Exception:
            dt_utc_iso = "—"
            when_txt = "⛔ horário inválido"

        # simula seleção
        try:
            picked = await _select_by_rule(data, rule, now_local=now_l)
            pre_count = len(picked)
            err = ""
        except Exception as e:
            picked = []
            pre_count = 0
            err = f"erro seleção: {repr(e)}"

        lines.append(f"#{idx} {t_local} → {dt_utc_iso} ({when_txt})")
        lines.append(
            f"   desc={desc or '—'} "
            f"sections={rule.get('sections', '—')} "
            f"max_cards={rule.get('max_cards', '—')}"
        )

        # linha de resumo
        extra = f"  |  {err}" if err else ""
        lines.append(f"   pré-seleção agora: {pre_count} itens{extra}")

        # listagem detalhada dos picks
        for kind, payload, sls in picked:
            if kind == "pick":
                home = payload.get("mandante") or payload.get("home") or "?"
                away = payload.get("visitante") or payload.get("away") or "?"
                kstr = _pick_time_str(payload) or "?"
                lines.append(f"     • {home} vs {away} @ {kstr} | SLS {sls:.1f}")
            else:
                legs = payload.get("legs", []) or []
                lines.append(f"     • COMBO ({len(legs)} legs) | SLS {sls:.1f}")



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

# -------------- Registro explícito das FUNÇÕES DE COMANDO - MENU - (aiogram v3)--------
dp.message.register(help_cmd, Command("help"))
dp.message.register(post_coruja_cmd, Command("post_coruja"))
dp.message.register(post_combos, Command("post_combos"))   # <<< garante registro
dp.message.register(post_combo, Command("post_combo"))     # já existia
dp.message.register(games_today_cmd, Command("games_today"))
dp.message.register(games_tomorrow_cmd, Command("games_tomorrow"))
dp.message.register(which_source_cmd, Command("which_source"))
dp.message.register(ls_data, Command("ls_data"))
dp.message.register(fetch_update, Command("fetch_update"))
dp.message.register(diag_time, Command("diag_time"))
dp.message.register(diag_odds, Command("diag_odds"))
dp.message.register(pub_show_today, Command("pub_show_today"))
dp.message.register(pub_reset_today, Command("pub_reset_today"))
dp.message.register(status_sub, Command("status_sub"))
dp.message.register(join_cmd, Command("join"))
dp.message.register(refer_cmd, Command("refer"))
dp.message.register(grant_trial_cmd, Command("grant_trial"))
dp.message.register(cmd_enforce_now, Command("enforce_now"))
dp.message.register(cmd_sub_set, Command("sub_set"))
dp.message.register(cmd_entrar, Command("entrar"))


# -------------------- RUN BOTH --------------------
async def run_all():
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)

    # 1) Aplica/atualiza o menu de comandos ANTES do polling começar
    try:
        await _setup_bot_commands()
    except Exception as e:
        print("[SETUP_CMDS][ERR]", repr(e))

    # 2) Cria as tasks principais
    api_task    = asyncio.create_task(server.serve(), name="api_server")
    bot_task    = asyncio.create_task(dp.start_polling(bot), name="bot_polling")
    enf_task    = asyncio.create_task(enforce_loop(), name="enforce_loop")
    notify_task = asyncio.create_task(reminder_loop(), name="reminder_loop")

    START_SCHEDULER = os.getenv("START_SCHEDULER", "true").lower() == "true"

    tasks = [api_task, bot_task, enf_task, notify_task]

    if START_SCHEDULER:
        tasks.append(asyncio.create_task(scheduler_loop(), name="scheduler_loop"))


    # 3) Espera todas as tasks juntas
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        os.environ["TZ"] = TZ_NAME
    except:
        pass
    asyncio.run(run_all())
