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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import ClientSession
from subscriptions import subs_get, subs_set, upsert_sub, sub_is_active
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
import asyncio

# ---------------------- Integração com cakto_payments.py --------------- (mesmo diretório)
from cakto_payments import (
    auth_ok,
    save_cakto_event,
    process_cakto_payload,
    build_checkout_url_core,
    set_trial_active_core,
)
#------------------------------------------------------------------------

# ---------------------- Integração com assinaturas (subscriptions.py) -----------------
from subscriptions import (
    subs_get,
    subs_set,
    upsert_sub,
    sub_is_active,
)
#------------------------------------------------------------------------

# ---------------------- Mapa e-mail ↔ Telegram (email_links.py) -----------------
from email_links import (
    get_link,
    link_email_to_telegram,
    email_links_load, 
)
#------------------------------------------------------------------------
#----------------------Importa TRIAL 7 DIAS (trials.py)
from trials import (
    can_use_trial,
    activate_trial,
)
# ------------------------------------------------------


# --- CONFIG BÁSICA / ENV ----------------------------
BOT_TOKEN  = os.getenv("BOT_TOKEN", "").strip()
GROUP_ID   = int(os.getenv("GROUP_ID", "0"))
AUTHORIZED = {int(x) for x in os.getenv("AUTHORIZED_USERS", "").replace(" ", "").split(",") if x}

# Fuso
TZ_NAME = os.getenv("TZ", "America/Sao_Paulo")
tz_sp = tz.gettz(TZ_NAME) or timezone.utc


# Caminhos locais padrão
ODDS_FILE         = os.getenv("ODDS_FILE", "/data/odds1.json").strip()           # hoje: usamos odds1.json
ODDS_AMANHA_FILE  = os.getenv("ODDS_AMANHA_FILE", "/data/odds_amanha.json").strip()
AFORISMOS_FILE    = os.getenv("AFORISMOS_FILE", "/data/aforismos.json").strip()
PUBLISHED_DB_FILE = os.getenv("PUBLISHED_DB_FILE", "/data/published.json").strip()
PUBLISHED_LOG     = os.getenv("PUBLISHED_LOG", "/data/published_log.json").strip()
FS_PICKS_LOG_FILE = os.getenv("FS_PICKS_LOG_FILE", "/data/fs_picks_log.json").strip()

#-----------------------------SUPER CARDS --------------------------
# Super games / super cards
SUPER_GAMES_GLOB       = os.getenv("SUPER_GAMES_GLOB", "/data/super_jogos-*.json").strip()
ENABLE_SUPER_GAMES_LOOP = os.getenv("ENABLE_SUPER_GAMES_LOOP", "true").lower() == "true"  #false "desliga" o loop
# estado em memória dos Super Games (usado pelo super_games_loop)

# NOVOS PARÂMETROS DE AGENDA DOS SUPERCARDS
SUPER_CARD_FIRST_HOUR       = int(os.getenv("SUPER_CARD_FIRST_HOUR", "7"))   # 07h
SUPER_CARD_FIRST_MINUTE     = int(os.getenv("SUPER_CARD_FIRST_MINUTE", "15"))# 07h15
SUPER_CARD_LAST_MIN_BEFORE  = int(os.getenv("SUPER_CARD_LAST_MIN_BEFORE", "45"))  # último card até 45' antes do KO
SUPER_CARD_LEAD_MIN        = int(os.getenv("SUPER_CARD_LEAD_MIN", "277"))  # 4h37 (277 min) antes do chute inicial




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
FS_WEEKLY_URL   = os.getenv("FS_WEEKLY_URL",   "").strip()  # URL do fs_weekly_picks_*.json no GitHub

# Limiares e outros params
MIN_LEAD_MIN              = int(os.getenv("MIN_LEAD_MIN", "10"))
RESERVE_CUTOFF_HOUR       = int(os.getenv("RESERVE_CUTOFF_HOUR", "15"))
RESERVE_SLS_THRESHOLD     = float(os.getenv("RESERVE_SLS_THRESHOLD", "75"))
RESERVE_EXPIRY_RELEASE_MIN= int(os.getenv("RESERVE_EXPIRY_RELEASE_MIN", "120"))
COMBOS_TYPES_ORDER        = os.getenv("COMBOS_TYPES_ORDER", "duplo,triplo,multi")
MIN_PROB     = float(os.getenv("MIN_PROB", "0.60"))
MIN_EV       = float(os.getenv("MIN_EV", "0.0"))

# ---------------- Pagamentos ------------------------- ( "Config” que o app vai passar para as funções do módulo.)
CAKTO_SECRET = os.getenv("CAKTO_SECRET", "").strip()
CAKTO_SECRET_KEY = os.getenv("CAKTO_SECRET_KEY", "").strip()
CHECKOUT_URL = os.getenv("CHECKOUT_URL", "").strip()
REF_PARAM    = os.getenv("REF_PARAM", "ref").strip() or "ref"
DEBUG_TOKEN = os.getenv("DEBUG_TOKEN", "").strip()
STATIC_INVITE_LINK = os.getenv("STATIC_INVITE_LINK", "").strip()
FORCE_STATIC_INVITE = os.getenv("FORCE_STATIC_INVITE", "false").lower() == "true"
#--------------------------------------------------------------------------------


INVITES_PATH        = os.getenv("INVITES_PATH", "/data/invites_map.json").strip()

PORT                = int(os.getenv("PORT", "8000"))  # Render injeta $PORT
GITHUB_TOKEN        = os.getenv("GITHUB_TOKEN", "").strip()
FETCH_MIN_INTERVAL  = int(os.getenv("FETCH_MIN_INTERVAL", "120"))
SLS_WP              = float(os.getenv("SLS_WP", "0.9"))
SLS_WE              = float(os.getenv("SLS_WE", "0.1"))
DIAMOND_SLS_THRESHOLD = float(os.getenv("DIAMOND_SLS_THRESHOLD", "90.0"))

REMINDER_INTERVAL_SEC = int(os.getenv("REMINDER_INTERVAL_SEC", "1800"))

# Limites de transferência de e-mail (PASSO 6 - anti "Netflix")
EMAIL_TRANSFER_MAX = int(os.getenv("EMAIL_TRANSFER_MAX", "1"))           # Nº máx. de transferências automáticas de e-mail (conta) em id Telegram
EMAIL_TRANSFER_WINDOW_DAYS = int(os.getenv("EMAIL_TRANSFER_WINDOW_DAYS", "30"))  # Janela em dias
EMAIL_TRANSFER_WINDOW_SEC = EMAIL_TRANSFER_WINDOW_DAYS * 86400


# Scheduler/editorial
MODE                     = os.getenv("MODE", "editorial").strip().lower()      # 'editorial' | 'auto'
AGENDA_JSON_PATH         = os.getenv("AGENDA_JSON_PATH", "/data/agenda_editorial.json").strip()
AUTO_SCHEDULING_DEFAULT  = os.getenv("AUTO_SCHEDULING_DEFAULT", "true").lower() == "true"
ENABLE_FALLBACK_SELECTION= os.getenv("ENABLE_FALLBACK_SELECTION", "true").lower() == "true"
MAX_PUBLICATIONS_PER_DAY = int(os.getenv("MAX_PUBLICATIONS_PER_DAY", "100"))
HOURLY_MAX               = int(os.getenv("MAX_PUBLICATIONS_PER_HOUR", "3"))
MINUTES_BETWEEN_REPOST   = int(os.getenv("MINUTES_BETWEEN_REPOST", "240"))


# Quantidade máxima de picks por jogo no Corujão (customizável via Render)
CORUJAO_MAX_PICKS_PER_GAME = int(os.getenv("CORUJAO_MAX_PICKS_PER_GAME", "2"))
# Quantidade máxima TOTAL de picks no Corujão (top N por SLS)
CORUJAO_MAX_PICKS_TOTAL = int(os.getenv("CORUJAO_MAX_PICKS_TOTAL", "5"))

#--------------------------------------------------------------------------
TELEGRAM_HTML_LIMIT = 4096 # -------------------------------------------------------------CORUJÃO e SUPERCARD (TAMANHO CARD)
TELEGRAM_SAFE_BUDGET = 3600  # margem para evitar erro (tags HTML contam no parse) # -----CORUJÃO e SUPERCARD (TAMANHO CARD)
TELEGRAM_SAFE_LIMIT   = TELEGRAM_SAFE_BUDGET  # limite real ~3600 chars por card do Corujão---------------------CORUJÃO e SUPERCARD  (TAMANHO CARD)



if not BOT_TOKEN or not GROUP_ID:
    raise RuntimeError("Defina BOT_TOKEN e GROUP_ID (-100...) no Environment.")
# ---------------------------------------------------

# -------------------- BOT CORE --------------------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher()

# ---- CONTROLE DE ENVIO DO CORUJÃO (para evitar repetição) ----
_CORUJAO_LAST_SENT_DATE = None  # string "YYYY-MM-DD"
#---------------------------------------------------------------

async def _setup_bot_commands():
    """
    Define os comandos que aparecem no menu do Telegram (botão de "/").

    • Assinante (DM): menu ENXUTO com 4 comandos.
    • Admin (DM): usa o MESMO menu enxuto (admin digita os outros comandos na mão).
    • Admin no CANAL/GRUPO: menu completo de administração.
    """

    # --- MENU ENXUTO PARA QUALQUER USUÁRIO NA DM ---
    # Aqui NÃO usamos mais BotCommand diretamente, só dicts.
    public_cmds = [
        {
            "command": "start",
            "description": "Acessar o menu principal",
        },
        {
            "command": "status_sub",
            "description": "Ver status da sua assinatura",
        },
        {
            "command": "help",
            "description": "Ajuda rápida",
        },
        {
            "command": "whoami",
            "description": "Mostrar seu ID (para suporte)",
        },
    ]

    # --- MENU COMPLETO SÓ PARA ADMIN NO CANAL/GRUPO ---
    admin_cmds = [
        {
            "command": "help_admin",
            "description": "Ajuda exclusiva para admins",
        },
        {
            "command": "which_source",
            "description": "Mostrar fontes e paths",
        },
        {
            "command": "ls_data",
            "description": "Listar /data",
        },
        {
            "command": "fetch_update",
            "description": "Forçar atualização de dados",
        },
        {
            "command": "games_today",
            "description": "Listar jogos de hoje",
        },
        {
            "command": "games_tomorrow",
            "description": "Listar jogos de amanhã",
        },
        {
            "command": "post_dica",
            "description": "Publicar dica manual no canal (HTML)",
        },
        {
            "command": "post_pick",
            "description": "Publicar 1 pick",
        },
        {
            "command": "post_combo",
            "description": "Publicar 1 combo",
        },
        {
            "command": "post_combos",
            "description": "Publicar combos",
        },
        {
            "command": "post_coruja",
            "description": "Publicar Corujão",
        },
        {
            "command": "pub_show_today",
            "description": "Ver publicados hoje",
        },
        {
            "command": "pub_stats",
            "description": "Resumo de publicados por dia",
        },
        {
            "command": "pub_reset_today",
            "description": "Zerar publicados de hoje",
        },
        {
            "command": "diag_time",
            "description": "Diagnóstico de horário",
        },
        {
            "command": "diag_odds",
            "description": "Diagnóstico de odds",
        },
        {
            "command": "diag_slots",
            "description": "Diagnóstico da agenda",
        },
        {
            "command": "grant_trial",
            "description": "Conceder trial manual",
        },
        {
            "command": "grant_lifetime",
            "description": "Conceder vitalícia",
        },
        {
            "command": "revoke_sub",
            "description": "Revogar assinatura",
        },
        {
            "command": "sub_set",
            "description": "Ajustar assinatura manual",
        },
        {
            "command": "sub_get",
            "description": "Ver dados crus da assinatura",
        },
        {
            "command": "sub_log",
            "description": "Log administrativo do usuário",
        },
        {
            "command": "enforce_now",
            "description": "Rodar enforcer agora",
        },
        {
            "command": "clean_data",
            "description": "Deletar odds/super_jogos antigos de /data",
        },
    ]

    # 1) Limpa QUALQUER comando antigo em todos os escopos globais
    try:
        await bot.delete_my_commands(scope=BotCommandScopeDefault())
    except Exception:
        pass
    try:
        await bot.delete_my_commands(scope=BotCommandScopeAllPrivateChats())
    except Exception:
        pass
    try:
        await bot.delete_my_commands(scope=BotCommandScopeAllGroupChats())
    except Exception:
        pass
    try:
        await bot.delete_my_commands(scope=BotCommandScopeAllChatAdministrators())
    except Exception:
        pass

    # 2) Define o menu ENXUTO como padrão e para todos os privados (DM)
    await bot.set_my_commands(
        commands=public_cmds,
        scope=BotCommandScopeDefault(),
    )
    await bot.set_my_commands(
        commands=public_cmds,
        scope=BotCommandScopeAllPrivateChats(),
    )

    # 3) Define o menu COMPLETO só para admins do CANAL/GRUPO
    if GROUP_ID:
        try:
            await bot.set_my_commands(
                commands=admin_cmds,
                scope=BotCommandScopeChatAdministrators(chat_id=GROUP_ID),
            )
        except Exception as e:
            print("[SETUP_CMDS][ADMIN_ERR]", repr(e))





# -------------------- STORAGE (/data) --------------------
DATA_DIR  = Path("/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH  = DATA_DIR / "cakto_events.json"
AF_USED   = DATA_DIR / "aforismos_used.json"
PUBLISHED_LOG = DATA_DIR / "published_log.json"
INVITES_MAP = Path(INVITES_PATH)  # { invite_link: {"allowed_uid": int, "expire": ts, "created_at": ts} }


def _now() -> int:
     return int(time.time())

# ---- Wrapper para trial manual (admin) usando core genérico ----
def set_trial_active(user_id: int, days: int = 30, plan_label: str = "Trial (Admin)") -> int:
    """
    Ativa um trial na subs.json por N dias.

    • user_id   : telegram_id do assinante
    • days      : quantidade de dias (default 30)
    • plan_label: texto salvo em 'plan'
    Retorna o timestamp de expiração (UTC).
    """
    return set_trial_active_core(
        user_id=user_id,
        days=days,
        plan_label=plan_label,
        now_ts=_now(),
        upsert_sub=upsert_sub,
    )


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

    REGRAS DE SEGURANÇA:
      1) Nunca deleta arquivos de hoje ou de datas futuras (amanhã inclusive).
      2) Só deleta arquivos cuja data seja estritamente anterior ao cutoff
         (hoje - HIST_KEEP_DAYS). Com HIST_KEEP_DAYS=7, mantém os últimos 7 dias.
      3) Para arquivos sem data no JSON, tenta extrair do nome do arquivo antes
         de usar mtime como último recurso — mas mesmo assim nunca toca em
         arquivos com data >= hoje.
    """
    import re as _re
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
                                    y, mo, d = map(int, ds.split("-"))
                                    fdate = date(y, mo, d)
                                except Exception:
                                    fdate = None

                        # super_jogos-*.json
                        elif pat.startswith("super_jogos"):
                            hdr = obj.get("supercard_header") or {}
                            ds = (hdr.get("target_date_local") or "").strip()
                            if not ds:
                                # extrai data do nome: super_jogos-2026-04-18.json
                                m_name = _re.search(r"(\d{4}-\d{2}-\d{2})", fp.name)
                                if m_name:
                                    ds = m_name.group(1)
                            if ds:
                                try:
                                    y, mo, d = map(int, ds.split("-"))
                                    fdate = date(y, mo, d)
                                except Exception:
                                    fdate = None

                    # fallback para mtime quando não há data no JSON nem no nome
                    if fdate is None:
                        fdate = datetime.fromtimestamp(fp.stat().st_mtime, tz_sp).date()

                    # REGRA DE SEGURANÇA: nunca apaga hoje ou qualquer data futura
                    if fdate >= today:
                        print(f"[HIST][KEEP] {fp} (data={fdate} >= hoje={today}, protegido)")
                        continue

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

    # FS Weekly — baixado separadamente via API do GitHub (veja abaixo)

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


# ===== FS WEEKLY BUILDER — loader alternativo =====
def load_fs_weekly_for_date(date_iso: str) -> Optional[Dict[str, Any]]:
    """
    Procura TODOS os fs_weekly_picks_*.json em /data/ e retorna o que cobre date_iso.
    Se dois arquivos cobrem a mesma data (transição de semana), usa o mais recente.
    """
    try:
        data_dir   = Path("/data")
        candidates = []
        for fp in data_dir.glob("fs_weekly_picks_*.json"):
            try:
                obj = _read_json_silent(fp)
                if not isinstance(obj, dict):
                    continue
                hdr = obj.get("odds_file_header") or {}
                ini = hdr.get("periodo_inicio","")
                fim = hdr.get("periodo_fim","")
                if ini and fim and ini <= date_iso <= fim:
                    candidates.append((fp.stat().st_mtime, fp, obj))
            except Exception:
                continue

        if not candidates:
            return None

        # Mais de um cobrindo a data → usa o mais recente
        candidates.sort(key=lambda x: x[0], reverse=True)
        _, best_file, obj = candidates[0]

        # Filtra picks do dia
        picks_por_data = obj.get("picks_por_data") or {}
        day_picks = picks_por_data.get(date_iso) or []

        # Normaliza campos — preserva o que o parser v2 já gravou
        def _normalize_pick(p: dict) -> dict:
            p = dict(p)
            # prob_real: usa o que o parser gravou; fallback para odd_justa
            if not p.get("prob_real") and not p.get("p_model"):
                oj = p.get("odd_justa") or p.get("odd_fair")
                if oj and float(oj) > 0:
                    p["prob_real"] = round(1.0 / float(oj), 6)
                else:
                    p["prob_real"] = 0.0
            # Garante campos mínimos
            p.setdefault("ev", None)
            p.setdefault("odd_mercado", None)
            p.setdefault("odds_por_casa", {})
            p.setdefault("prob_table", {})
            p.setdefault("ev_picks", [])
            p.setdefault("estadio", None)
            p.setdefault("forma_mandante", None)
            p.setdefault("forma_visitante", None)
            p["_source_fs_weekly"] = True  # flag para fmt_pick_fs
            p["source"] = p.get("source") or "fs_ev_pick"
            return p

        normalized = [_normalize_pick(p) for p in day_picks]

        # Corujão do dia
        corujao_raw = (obj.get("corujao") or {}).get("picks") or []
        corujao_day = [
            _normalize_pick(p) for p in corujao_raw
            if p.get("target_date_local", "") == date_iso
        ]

        return {
            "picks":   [p for p in normalized if not p.get("is_corujao")],
            "combos":  [],
            "corujao": {
                "picks":  corujao_day,
                "combos": [],
            },
            "_source": "fs_weekly",
        }

    except Exception as e:
        print("[FS_WEEKLY][LOAD_ERR]", repr(e))
        return None


def _odds_file_exists_for_date(date_iso: str) -> bool:
    """Verifica se existe odds-YYYY-MM-DD.json válido para a data."""
    try:
        for fp in Path("/data").glob("odds*.json"):
            obj = _read_json_silent(fp)
            if not isinstance(obj, dict):
                continue
            hdr  = obj.get("odds_file_header") or {}
            meta = obj.get("meta") or {}
            d1   = (hdr.get("target_date_local") or meta.get("target_date_local") or "").strip()
            if d1 == date_iso:
                return True
    except Exception:
        pass
    return False
# ===== /FS WEEKLY BUILDER =====


# ─────────────────────────────────────────────────────────────────
# FS WEEKLY — Seleção híbrida por slot (1 filtro + 2 EV)
# ─────────────────────────────────────────────────────────────────
def _select_fs_weekly_slot(
    data: Dict[str, Any],
    slot_local: datetime,
    min_lead_min: float = 60.0,
    max_lead_min: float = 120.0,
) -> List[tuple]:
    """
    Seleciona até 3 picks para um slot a partir do FS Weekly JSON:
      - 1 pick do filtro (maior IHR%) elegível na janela do slot
      - 2 picks de EV+ (maior EV%) dos ev_picks de qualquer jogo elegível

    Retorna lista de tuples ("pick", payload_dict, score) — mesmo formato
    do _select_by_rule para compatibilidade com o publisher.
    """
    all_picks = list(data.get("picks") or [])
    coru_picks = list((data.get("corujao") or {}).get("picks") or [])
    todos = all_picks + coru_picks

    # ── Filtra por janela de kick-off ──
    elegiveis = []
    for p in todos:
        kick_str = p.get("hora_iso") or ""
        try:
            kick_naive = datetime.strptime(kick_str, "%Y-%m-%d %H:%M")
            kick_local = kick_naive.replace(tzinfo=slot_local.tzinfo)
        except Exception:
            continue
        delta = (kick_local - slot_local).total_seconds() / 60.0
        if min_lead_min <= delta <= max_lead_min:
            elegiveis.append(p)

    if not elegiveis:
        return []

    out: List[tuple] = []
    used_game_keys: set = set()

    # ── Pick 1: filtro com maior IHR ──
    filtro_sorted = sorted(elegiveis, key=lambda p: float(p.get("ihr_pct") or 0), reverse=True)
    for p in filtro_sorted:
        gk = p.get("game_key","")
        if already_published_pick(p):
            continue
        out.append(("pick", p, float(p.get("ihr_pct") or 0)))
        used_game_keys.add(gk)
        break

    # ── Picks 2 e 3: EV+ de qualquer jogo elegível ──
    # Coleta todos os ev_picks de todos os jogos elegíveis
    all_ev: List[Dict] = []
    for p in elegiveis:
        ev_picks = p.get("ev_picks") or []
        home = p.get("mandante") or p.get("home") or ""
        away = p.get("visitante") or p.get("away") or ""
        camp = p.get("campeonato") or p.get("league") or ""
        pais = p.get("pais") or p.get("country") or ""
        hora = p.get("hora") or p.get("kickoff_local") or ""
        hora_iso = p.get("hora_iso") or ""
        data_str = p.get("data") or ""
        gk   = p.get("game_key","")
        # Campos do jogo pai que devem descer para cada pick expandido
        forma_mandante  = p.get("forma_mandante")
        forma_visitante = p.get("forma_visitante")
        estadio         = p.get("estadio")
        target_date_local = p.get("target_date_local", "")
        is_corujao      = p.get("is_corujao", False)
        is_world_cup    = p.get("is_world_cup", False)

        for ep in ev_picks:
            ev_pct = float(ep.get("ev_pct") or 0)
            if ev_pct <= 0:
                continue

            # Score de ordenação: usa ev calculado real quando disponível,
            # senão usa ev_pct. Picks com odd real têm prioridade.
            ev_calc = ep.get("ev")
            has_real_odd = bool(ep.get("odd_mercado"))
            sort_score = (
                1 if (has_real_odd and ev_calc and ev_calc > 0) else 0,
                float(ev_calc) if (ev_calc is not None) else 0.0,
                ev_pct,
            )

            pick_ev = {
                "mandante":      home,
                "visitante":     away,
                "campeonato":    camp,
                "pais":          pais,
                "hora":          hora,
                "hora_iso":      hora_iso,
                "data":          data_str,
                "mercado":       ep.get("mercado",""),
                "selecao":       ep.get("selecao",""),
                "market_code":   ep.get("market_code",""),
                "prob_real":     ep.get("prob_real"),
                "p_model":       ep.get("prob_real"),
                "odd_justa":     ep.get("odd_justa"),
                "odd_fair":      ep.get("odd_justa"),
                "odd_mercado":   ep.get("odd_mercado"),   # média das casas
                "odds_por_casa": ep.get("odds_por_casa", {}),
                "n_casas_odds":  ep.get("n_casas_odds", 0),
                "ev":            ev_calc,
                "ev_pct":        ev_pct,
                "EV":            ev_calc if ev_calc is not None else ev_pct,
                "SLS":           ev_calc if ev_calc is not None else ev_pct / 100.0,
                "game_key":      gk,
                "source":        "fs_ev_pick",
                "_source_fs_weekly": True,
                "league_avg_pct":    ep.get("league_avg_pct"),
                "sem_media_liga":    ep.get("sem_media_liga", False),
                "forma_mandante":    forma_mandante,
                "forma_visitante":   forma_visitante,
                "estadio":           estadio,
                "target_date_local": target_date_local,
                "is_corujao":        is_corujao,
                "is_world_cup":      is_world_cup,
                "ihr_pct":       None,
                "ihr_units":     None,
                "ihr_n":         None,
            }
            all_ev.append((sort_score, gk, pick_ev))

    # Ordena: (tem odd real + ev>0, ev calculado, ev_pct) — tudo desc
    all_ev.sort(key=lambda x: x[0], reverse=True)

    ev_added = 0
    ev_game_counts: Dict[str,int] = defaultdict(int)

    for sort_score, gk, pick_ev in all_ev:
        if ev_added >= 2 or len(out) >= 3:
            break
        if gk in used_game_keys:
            continue
        if ev_game_counts[gk] >= 1:
            continue
        if already_published_pick(pick_ev):
            continue
        # Score para o publisher: usa ev calculado se disponível
        pub_score = pick_ev.get("ev") or pick_ev.get("ev_pct") or 0
        out.append(("pick", pick_ev, pub_score))
        used_game_keys.add(gk)
        ev_game_counts[gk] += 1
        ev_added += 1

    return out


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

def _super_compute_schedule_for_game(
    g: Dict[str, Any],
    ref_local: datetime,
    n_cards: int,
) -> List[datetime]:
    """
    Gera uma grade ESTÁVEL de n_cards horários para um super game.

    Regra:
    - Último horário = kickoff_local - SUPER_CARD_LAST_MIN_BEFORE (min).
    - Primeiro horário = max( começo_do_dia + SUPER_CARD_FIRST_HOUR:MINUTE,
                              ref_local + 30s ).
    - Se não há janela (first >= last) → 1 horário em ref_local + 30s.
    - Os n_cards são espaçados uniformemente entre first_dt e last_dt.
    """

    if n_cards <= 0:
        return []

    tz_sp = tz.gettz(TZ_NAME)
    dt_k = _super_kickoff_dt_local(g)
    if not dt_k:
        return []

    # Último horário permitido: KO - X minutos
    last_dt = dt_k - timedelta(minutes=SUPER_CARD_LAST_MIN_BEFORE)

    # Se já passou da janela (ref depois do último possível) → nada
    if ref_local >= last_dt:
        return []

    # Base do dia (meia-noite local do dia do jogo)
    day_floor = datetime(dt_k.year, dt_k.month, dt_k.day, 0, 0, tzinfo=tz_sp)

    # Começo mínimo configurado (ex.: 07:15)
    base_first = day_floor + timedelta(
        hours=SUPER_CARD_FIRST_HOUR,
        minutes=SUPER_CARD_FIRST_MINUTE,
    )

    # Primeiro horário candidato: ou a base do dia, ou agora+30s
    first_dt = max(base_first, ref_local + timedelta(seconds=30))

    # Segurança: se algo der errado e first passar do last, joga tudo para ref+30
    if first_dt >= last_dt:
        return [ref_local + timedelta(seconds=30) for _ in range(n_cards)]

    # Se só tem 1 card: coloca no meio da janela
    if n_cards == 1:
        mid = first_dt + (last_dt - first_dt) / 2
        return [mid]

    # Grade uniforme
    total_seconds = (last_dt - first_dt).total_seconds()
    step = total_seconds / float(n_cards - 1)

    sched: List[datetime] = []
    for i in range(n_cards):
        sched.append(first_dt + timedelta(seconds=step * i))

    return sched





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

def _purge_picks_log(max_days: int = 180) -> None:
    """Remove entradas do fs_picks_log.json com mais de max_days dias."""
    try:
        try:
            with open(FS_PICKS_LOG_FILE, "r", encoding="utf-8") as f:
                log = json.load(f)
        except Exception:
            return
        tz_sp    = tz.gettz(TZ_NAME)
        cutoff   = (datetime.now(tz_sp).date() - timedelta(days=max_days)).isoformat()
        picks    = log.get("picks", [])
        kept     = [p for p in picks if p.get("data_jogo","") >= cutoff]
        removed  = len(picks) - len(kept)
        if removed > 0:
            log["picks"] = kept
            tmp = FS_PICKS_LOG_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(log, f, ensure_ascii=False, indent=2)
            os.replace(tmp, FS_PICKS_LOG_FILE)
            print(f"[FS_PICKS_LOG][PURGE] Removidos {removed} picks anteriores a {cutoff}")
    except Exception as e:
        print("[FS_PICKS_LOG][PURGE_ERR]", repr(e))


def _log_pick_completo(p: Dict[str, Any], tipo: str = "slot") -> None:
    """
    Grava o pick completo no fs_picks_log.json para uso do tracker.
    tipo: "slot" | "corujao"
    Acumula indefinidamente (sem purge) para o tracker avaliar.
    """
    try:
        # Carrega log existente
        try:
            with open(FS_PICKS_LOG_FILE, "r", encoding="utf-8") as f:
                log = json.load(f)
        except Exception:
            log = {"picks": []}

        tz_sp   = tz.gettz(TZ_NAME)
        now_str = datetime.now(tz_sp).isoformat()
        d_kick  = _kick_date_local_from_pick(p)
        date_str = d_kick.isoformat() if d_kick else ""

        # Evita duplicatas pelo mesmo sig
        sig = _pick_signature(p)
        existing_sigs = {e.get("sig") for e in log.get("picks", [])}
        if sig in existing_sigs:
            return

        entry = {
            "sig":           sig,
            "publicado_em":  now_str,
            "tipo":          tipo,          # slot | corujao
            "data_jogo":     date_str,
            # Identificação
            "mandante":      p.get("mandante") or p.get("home",""),
            "visitante":     p.get("visitante") or p.get("away",""),
            "campeonato":    p.get("campeonato") or p.get("league",""),
            "pais":          p.get("pais") or p.get("country",""),
            "hora":          p.get("hora",""),
            "hora_iso":      p.get("hora_iso",""),
            # URLs do jogo (para scraping do tracker)
            "url_jogo":       p.get("url_jogo",""),
            "fs_match_url":   p.get("fs_match_url",""),
            # Mercado
            "mercado":       p.get("mercado") or p.get("market",""),
            "selecao":       p.get("selecao") or p.get("selection",""),
            "market_code":   p.get("market_code",""),
            "source":        p.get("source",""),
            "filter_id":     p.get("filter_id",""),
            "filter_label":  p.get("filter_label",""),
            # Odds e probabilidade
            "prob_real":     p.get("prob_real"),
            "odd_justa":     p.get("odd_justa") or p.get("odd_fair"),
            "odd_mercado":   p.get("odd_mercado"),
            "odds_por_casa": p.get("odds_por_casa", {}),
            "n_casas_odds":  p.get("n_casas_odds", 0),
            "ev":            p.get("ev"),
            "ev_pct":        p.get("ev_pct"),
            # IHR (filtro)
            "ihr_pct":       p.get("ihr_pct"),
            "ihr_units":     p.get("ihr_units"),
            "ihr_n":         p.get("ihr_n"),
            # Resultado (preenchido pelo tracker)
            "resultado":     None,
            "placar_str":    None,
            "lucro":         None,
        }

        log.setdefault("picks", []).append(entry)

        tmp = FS_PICKS_LOG_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False, indent=2)
        os.replace(tmp, FS_PICKS_LOG_FILE)

    except Exception as e:
        print("[FS_PICKS_LOG][ERR]", repr(e))


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

def _combo_hash(c: Dict[str, Any]) -> str:
    legs = c.get("legs", []) or []
    legs_s = "|".join(
        json.dumps(l, ensure_ascii=False, sort_keys=True)
        for l in legs
    )
    base = f"{legs_s}|{c.get('odd_combo','')}|{c.get('fair_combo','')}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()[:12]

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


def _safe_date_str(date_str: str) -> str:
    """
    Evita que o Telegram transforme a data em link automático (telefone/data).

    Estratégia: insere um zero-width space após o primeiro '-' da string.
    Visualmente nada muda, mas quebra o padrão que o Telegram detecta.
    """
    s = str(date_str or "").strip()
    if not s:
        return ""
    # Ex.: "28-11-2025" -> "28-​11-2025" (o caractere invisível está ali)
    return s.replace("-", "-\u200b", 1)

# -------------------------------------

def fmt_pick(p: Dict[str, Any], *, add_debug_line: Optional[str] = None) -> str:
    # Picks do filtro FS Weekly usam card próprio (IHR)
    # Picks de EV do FS (fs_ev_pick) usam card clássico (EV + odds)
    if p.get("source") == "fs_weekly_builder" and p.get("source") != "fs_ev_pick":
        return fmt_pick_fs(p)
    if p.get("_source_fs_weekly") and p.get("source") != "fs_ev_pick":
        return fmt_pick_fs(p)

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

    # -------------- DC (Dupla Chance) --------------
    if "dupla chance" in m_lower or "double chance" in m_lower:
        # 1X → Empate - Mandante
        if s_lower in {"1x", "1x ", "1-x", "1 x", "home or draw", "casa ou empate"}:
            selecao_pt = f"Empate - {home}"
        # X2 → Empate - Visitante
        elif s_lower in {"x2", "x-2", "x 2", "draw or away", "empate ou fora"}:
            selecao_pt = f"Empate - {away}"
        # 12 → Mandante - Visitante
        elif s_lower in {"12", "1-2", "1 2", "home or away", "casa ou fora"}:
            selecao_pt = f"{home} - {away}"

    # -------- Data/hora do jogo em horário LOCAL --------
    kick_iso = _pick_time_str(p)
    dt_local = _parse_any_dt_local(kick_iso) if kick_iso else None

    if dt_local:
        date_str = dt_local.strftime("%d-%m-%Y")
        hour_str = dt_local.strftime("%Hh%M")
    else:
        # fallback se algo vier torto do JSON
        date_str = (p.get("data") or "").strip()
        hour_str = (p.get("hora") or "").strip()

    safe_date = _safe_date_str(date_str)
    clock_emoji = _clock_emoji_for_hhmm(hour_str or "")

    when_line = f"{clock_emoji} <b>{safe_date or '—'}</b> | <b>{hour_str or '—'}</b> {TZ_LABEL}"



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
        _hr(),
        _pick_aforismo_for_sls(sls),
    ]

    if add_debug_line:
        linhas.append(f"\n<code>{add_debug_line}</code>")

    return "\n".join(linhas)


def _fmt_forma_times(p: Dict[str, Any]) -> str:
    """Monta a seção Forma dos Times para cards normais (não corujão)."""
    fm = p.get("forma_mandante")
    fv = p.get("forma_visitante")
    if not fm and not fv:
        return ""

    linhas = [
        "",
        _hr(),
        "📊 <b>Forma dos Times</b>",
        _hr(),
    ]

    def _bloco_time(forma_data: Dict, emoji: str) -> List[str]:
        nome  = forma_data.get("nome","")
        forma = forma_data.get("forma", {})
        stats = forma_data.get("stats", {})
        bl    = []
        bl.append(f"{emoji} <b>{nome}</b>")

        if forma:
            geral = forma.get("Overall", {})
            casa  = forma.get("Home", {})
            fora  = forma.get("Away", {})
            res_g = geral.get("resultados_pt","—")
            res_c = casa.get("resultados_pt","—")
            res_f = fora.get("resultados_pt","—")
            ppg_g = geral.get("ppg","—")
            ppg_c = casa.get("ppg","—")
            ppg_f = fora.get("ppg","—")
            bl.append(f"<b>Forma (Geral/Casa/Fora)</b>")
            bl.append(f"<b>{res_g} / {res_c} / {res_f}</b>")
            bl.append(f"<b>PPJ:</b> <b>{ppg_g} / {ppg_c} / {ppg_f}</b>")

        if stats:
            bl.append("")
            bl.append("<b>Estatística · Geral · Casa · Fora</b>")
            for stat_pt, vals in stats.items():
                g = vals.get("geral","—")
                c = vals.get("casa","—")
                f = vals.get("fora","—")
                bl.append(f"<b>{stat_pt}:</b> <b>{g} · {c} · {f}</b>")
        return bl

    if fm:
        linhas += _bloco_time(fm, "🏠")
    if fv:
        if fm:
            linhas.append("")
        linhas += _bloco_time(fv, "✈️")

    return "\n".join(linhas)


def fmt_pick_fs(p: Dict[str, Any]) -> str:
    """
    Card enriquecido para picks do FS Weekly Builder v2.
    Formato: cabeçalho + mercado + odds + EV + Média da Compet. + Forma dos Times
    Corujão: sem Forma dos Times (detectado pelo campo is_corujao).
    """
    # ── Dados numéricos ──
    prob    = _f(p.get("prob_real") or p.get("p_model") or 0.0)
    ev_raw  = p.get("ev") or p.get("EV")
    ev_val  = float(ev_raw) if ev_raw is not None else None

    # EV na notação da página FS: +0.75 (não percentual)
    if ev_val is not None:
        ev_str = f"{ev_val:+.2f}"
    else:
        ev_str = "—"

    odd_num = _to_float_odd(p.get("odd_mercado"))
    odd_str = f"@{odd_num:.2f}" if odd_num > 0 else "—"

    oj_raw    = p.get("odd_justa") or p.get("odd_fair")
    odd_justa = float(oj_raw) if oj_raw else (1.0/prob if prob > 0 else 0.0)
    oj_str    = f"@{odd_justa:.2f}" if odd_justa > 0 else "—"

    # Média da competição
    league_avg_pct = p.get("league_avg_pct")
    sem_media      = p.get("sem_media_liga", False)
    if sem_media:
        media_str = "Não se aplica"
    elif league_avg_pct is not None:
        media_str = f"{league_avg_pct:.0f}%"
    else:
        media_str = "—"

    # SLS para aforismo
    sls = sls_score(prob, ev_val * 100.0 if ev_val is not None else 0.0) if prob > 0 else 0.5

    # ── Mercado / seleção ──
    mercado_pt, selecao_pt = _fs_market_selecao(p)

    # ── Data/hora ──
    kick_iso = _pick_time_str(p)
    dt_local = _parse_any_dt_local(kick_iso) if kick_iso else None
    if dt_local:
        date_str = dt_local.strftime("%d-%m-%Y")
        hour_str = dt_local.strftime("%Hh%M")
    else:
        date_str = (p.get("data") or "").strip()
        hour_str = (p.get("hora") or "").strip()

    safe_date   = _safe_date_str(date_str)
    clock_emoji = _clock_emoji_for_hhmm(hour_str or "")
    when_line   = f"{clock_emoji} <b>{safe_date or '—'}</b> | <b>{hour_str or '—'}</b> {TZ_LABEL}"

    # ── Liga / país / estádio ──
    league  = p.get("campeonato") or p.get("league") or "—"
    country = p.get("pais") or p.get("country") or "—"
    flag    = get_country_flag(country, league)
    estadio = p.get("estadio") or ""

    # ── Linhas do card ──
    linhas = [
        BRAND_LINE,
        _hr(),
        f"🏆 <b>{league}</b> · {country} {flag}",
        when_line,
        f"⚽ <b>{p.get('mandante','?')}</b> vs <b>{p.get('visitante','?')}</b>",
    ]
    if estadio:
        linhas.append(f"🏟 <b>{estadio}</b>")
    linhas += [
        "",
        f"🎯 Mercado: <b>{mercado_pt}</b>",
        f"Seleção: <b>{selecao_pt}</b>",
        "",
        f"Prob. real: <b>{prob*100:.0f}%</b>  |  Odd justa: <b>{oj_str}</b>",
        f"Odd mercado: <b>{odd_str}</b>  |  EV: <b>{ev_str}</b>",
        f"Média da Compet.: <b>{media_str}</b>",
    ]

    # Forma dos times — só para cards normais (não corujão)
    is_coru = p.get("is_corujao", False)
    if not is_coru:
        forma_section = _fmt_forma_times(p)
        if forma_section:
            linhas.append(forma_section)

    linhas += [_hr(), _pick_aforismo_for_sls(sls)]
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
            f"{title_icon} <b>{card_name}</b> — <b>{home}</b> vs <b>{away}</b>",
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
            f"{title_icon} <b>{card_name}</b> — <b>{home}</b> vs <b>{away}</b>",
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
        return ""

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

                    # seleção em negrito
                    sel_disp = f"<b>{sel}</b>"

                    pr   = _fmt_prob(mm.get("p_model"))
                    omkt = _fmt_odd_mkt(mm.get("odds_market"))
                    ofair= _fmt_fair(mm.get("odd_fair"))
                    ev_t = _fmt_ev(mm.get("ev_percent"))
                    badge= _ev_badge(mm.get("ev_percent"))

                    parts = [sel_disp]
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

            # Ajuste específico para EMPATE ANULA APOSTA → nomes dos times
            if key.lower() == "empate anula":
                sel_lower = sel.lower()
                if sel_lower in ("casa", "home", "1"):
                    sel = home
                elif sel_lower in ("fora", "away", "2"):
                    sel = away

            # Ajuste específico para DUPLA CHANCE → combinações de times
            if key.lower() == "dupla chance":
                sel_upper = sel.upper().replace(" ", "")
                if sel_upper in ("1X", "1-X"):
                    sel = f"{home} - Empate"
                elif sel_upper in ("12", "1-2"):
                    sel = f"{home} - {away}"
                elif sel_upper in ("X2", "X-2"):
                    sel = f"Empate - {away}"

            # seleção em negrito
            sel_disp = f"<b>{sel}</b>"

            pr   = _fmt_prob(m.get("p_model"))
            omkt = _fmt_odd_mkt(m.get("odds_market"))
            ofair= _fmt_fair(m.get("odd_fair"))
            ev_t = _fmt_ev(m.get("ev_percent"))
            badge = _ev_badge(m.get("ev_percent"))

            parts = [sel_disp]
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
            display_disp = f"<b>{display}</b>"

            pr    = _fmt_prob(m.get("p_model"))
            omkt  = _fmt_odd_mkt(m.get("odds_market"))
            ofair = _fmt_fair(m.get("odd_fair"))
            ev_t  = _fmt_ev(m.get("ev_percent"))
            badge = _ev_badge(m.get("ev_percent"))

            parts = [display_disp]
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
                display_disp = f"<b>{display}</b>"

                pr    = _fmt_prob(m.get("p_model"))
                ofair = _fmt_fair_short(m.get("odd_fair"))
                parts = [display_disp]
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

            display_disp = f"<b>{display}</b>"

            pr    = _fmt_prob(m.get("p_model"))
            ofair = _fmt_fair_short(m.get("odd_fair"))

            parts = [display_disp]
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

    # ----------------- AFORISMO (DESATIVADO PARA SUPERCARDS) -----------------
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

        txt = "\n".join(header_lines + body)

        if "TELEGRAM_SAFE_LIMIT" in globals():
            limit = TELEGRAM_SAFE_LIMIT
            if len(txt) > limit:
                txt = txt[:limit - 10] + "\n…"

        cards.append(txt)

    return cards



# -------------------- CHECKOUT helpers --------------------
def build_checkout_url(ref: int | None = None) -> str:
    return build_checkout_url_core(
        checkout_url=CHECKOUT_URL,
        ref_param=REF_PARAM,
        ref=ref,
    )

def set_trial_active(user_id: int, days: int = 30, plan_label: str = "trial"):
    return set_trial_active_core(
        now_ts=_now(),
        upsert_sub=upsert_sub,
        user_id=user_id,
        days=days,
        plan_label=plan_label,
    )



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
    """
    Mensagem de boas-vindas do Bet Masterson + menu principal de ações.
    Se vier com parâmetro 'trial' (deep link de indicação), mostra
    direto as instruções do trial.
    """

    # Se o comando vier de grupo/canal, só orienta a chamar no privado
    if m.chat.type != "private":
        return await m.answer(
            "👋 Para ver o menu de assinatura e falar comigo, me chame no privado.\n"
            "Abra o meu perfil e toque em <b>Iniciar</b>.",
            parse_mode="HTML",
        )

    # Detecta deep link com parâmetro trial: /start trial
    args = (m.text or "").strip().split()
    if len(args) > 1 and args[1].lower() == "trial":
        texto = (
            "🎁 <b>Você foi convidado — 7 dias grátis no Canal Bet Masterson!</b>\n\n"
            "Um amigo te presenteou com acesso gratuito por 7 dias, com todos os cards "
            "e picks de valor publicados diariamente.\n\n"
            "<b>Para ativar seu acesso gratuito:</b>\n"
            "Envie aqui no chat o comando abaixo com o seu e-mail:\n\n"
            "<code>/trial seuemail@exemplo.com</code>\n\n"
            "• Substitua pelo seu e-mail pessoal.\n"
            "• Cada e-mail só pode usar o trial <b>uma vez</b>.\n"
            "• Após ativar, use /entrar para receber o link de acesso ao canal."
        )
        return await m.answer(texto, parse_mode="HTML")

    # Link de checkout com parâmetro de referência (se configurado)
    checkout_link = build_checkout_url(m.from_user.id)

    # Teclado inline principal
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💳 Quero assinar agora",
                    url=checkout_link or CHECKOUT_URL or "https://example.com",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔁 Renovar minha assinatura",
                    url=checkout_link or CHECKOUT_URL or "https://example.com",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔐 Já paguei, quero ativar com meu e-mail",
                    callback_data="start_activate",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🎁 7 dias grátis (promo de lançamento)",
                    callback_data="start_trial_info",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🤝 Presentear um amigo com 7 dias grátis no Canal Bet Masterson",
                    callback_data="start_refer_info",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🛠️ Suporte Bet Masterson",
                    callback_data="start_support",
                ),
            ],
        ]
    )

    texto = (
        "🎩 <b>Bem-vindo ao círculo de Bet Masterson.</b>\n\n"
        "Esta não é uma casa de falsos milagres. Esta é uma oficina de leitura de jogos.\n"
        "O canal existe para te mostrar onde as probabilidades estão a teu favor, "
        "sem prometer dinheiro fácil, nem bilhete mágico. Bet Masterson está do teu lado e não do lado das Bets!\n\n"
        "Aqui você encontra:\n"
        "• Cards diários com <b>odds do mercado</b>, <b>probabilidades reais</b>, "
        "<b>odds justas</b> calculadas e somente <b>picks de valor</b>.\n"
        "• Utilizamos os dados mais confiáveis e completos do mercado mundial de estatísticas esportivas.\n"
        "• Análises didáticas, com as melhores informações que existem, nas suas mãos diariamente.\n\n"
        "Escolha abaixo o que deseja:"
    )

    await m.answer(texto, reply_markup=keyboard, parse_mode="HTML")


@dp.callback_query(lambda c: c.data == "start_support")
async def cb_start_support(c: types.CallbackQuery):
    # Garante que a conversa é privada
    if c.message.chat.type != "private":
        await c.answer()
        return

    await c.message.answer(
        "🛠️ <b>Suporte Bet Masterson</b>\n\n"
        "Se você precisa de ajuda com pagamento, ativação de e-mail ou acesso ao canal, "
        "envie sua mensagem assim:\n\n"
        "<code>/suporte descreva aqui a sua dúvida</code>\n\n"
        "Eu encaminho sua mensagem diretamente para a equipe.",
        parse_mode="HTML",
    )
    await c.answer()

@dp.callback_query(lambda c: c.data == "start_activate")
async def cb_start_activate(c: types.CallbackQuery):
    """
    Fluxo guiado para quem já pagou e só precisa ativar a assinatura
    usando o e-mail da compra.
    """
    # Garante que é conversa privada
    if c.message.chat.type != "private":
        await c.answer()
        return

    texto = (
        "🔐 <b>Ativar assinatura com seu e-mail</b>\n\n"
        "Se você <b>já fez o pagamento</b> pela plataforma, o próximo passo é "
        "vincular o e-mail da compra ao seu Telegram.\n\n"
        "Basta enviar aqui no chat, exatamente neste formato:\n\n"
        "<code>/ativar seu-email@exemplo.com</code>\n\n"
        "Substitua <code>seu-email@exemplo.com</code> pelo e-mail que você usou no pagamento.\n\n"
        "Se tiver alguma dúvida, tire um print e o envie anexado à sua mensagem via:\n"
        "<code>/suporte descreva aqui o que aconteceu</code>\n"
    )

    await c.message.answer(texto, parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data == "start_trial_info")
async def cb_start_trial_info(c: types.CallbackQuery):
    """
    Explica ao usuário como funciona o período grátis de 30 dias # Altera dias no trials.py
    (promoção de lançamento) e solicita o e-mail.
    """
    if c.message.chat.type != "private":
        await c.answer()
        return

    texto = (
        "🎁 <b>Promoção de Lançamento — 7 dias grátis</b>\n\n"
        "Você pode experimentar o Canal Bet Masterson gratuitamente por 7 dias,"
        " com acesso integral às dezenas de cards publicados com as informações completas dos melhores picks do dia de jogos do mundo todo.\n\n"
        "<b>Como funciona:</b>\n"
        "• O acesso é liberado por 7 dias corridos.\n"
        "• No último dia do período, você receberá uma mensagem com o link para se tornar assinante mensal do Canal.\n"
        "• Você pode utilizar o trial <b>apenas uma vez</b>.\n"
        "<b>Para começar:</b>\n"
        "Envie aqui no chat o comando /trial seguido do <b>seu e-mail pessoal</b>:\n\n"
        "<code>/trial seuemail@exemplo.com</code>\n\n"
    )

    await c.message.answer(texto, parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data == "start_refer_info")
async def cb_start_refer_info(c: types.CallbackQuery):
    """
    Explica como presentear um amigo com 30 dias grátis:
    gera um deep-link para o menu do bot, já marcado com 'start=trial',
    que o assinante pode copiar e enviar para quem quiser.
    """

    # Garante que a conversa é privada
    if c.message.chat.type != "private":
        await c.answer()
        return

    # Descobre o @username do bot em tempo de execução
    me = await bot.get_me()
    username = (me.username or "").strip()

    if not username:
        # fallback de segurança: só mensagem explicativa
        texto = (
            "🤝 <b>Presentear um amigo com 30 dias grátis</b>\n\n"
            "Você pode convidar um amigo para experimentar o Canal Bet Masterson "
            "por 30 dias gratuitamente.\n\n"
            "No entanto, não consegui identificar o nome de usuário do bot "
            "para montar o link automático.\n\n"
            "Peça suporte em:\n"
            "<code>/suporte não consegui gerar o link de presente</code>"
        )
        await c.message.answer(texto, parse_mode="HTML")
        await c.answer()
        return

    # Deep link para o /start com um parâmetro 'trial'
    deep_link = f"https://t.me/{username}?start=trial"

    texto = (
    "🤝 <b>Assinante pode presentear um(a) amigo(a) com 7 dias grátis</b>\n\n"
    "Você pode convidar um(a) amigo(a) para experimentar o Canal Bet Masterson "
    "por <b>7 dias gratuitamente</b>, com acesso aos mesmos cards e informações que você vê.\n\n"
    "<b>Como usar:</b>\n"
    "1. Copie o link abaixo:\n\n"
    f"<code>{deep_link}</code>\n\n"
    "2. Envie esse link para o seu amigo (WhatsApp, Telegram, onde quiser).\n"
    "3. Quando ele(a) abrir o link no Telegram, o bot já vai pedir o e-mail.\n"
    "4. Ele(a) só precisa enviar:\n"
    "<code>/trial emaildoamigo@exemplo.com</code>\n\n"
    "Depois é só usar <b>/entrar</b> para receber o link de acesso ao canal."
)

    await c.message.answer(texto, parse_mode="HTML")
    await c.answer()





from aiogram.filters import Command

@dp.message(Command("help"))
async def help_cmd(m: types.Message):
    # DEBUG simples
    print(f"[HELP] hit: chat={m.chat.id} type={m.chat.type} user={m.from_user.id}")

    public_help = "\n".join([
        "🎩 <b>Como usar o Bet Masterson Bot</b>",
        "",
        "• <b>/start</b> — Acessar o bot. Abre o <b>menu principal com botões</b>,",
        "  onde você consegue ver opções de assinatura, renovar, ativar com e-mail,",
        "  e até presentear um amigo com dias de acesso.",
        "",
        "• <b>/status_sub</b> — Ver se sua assinatura está ativa e <b>até quando</b> ela vale.",
        "  Útil quando você quer saber se ainda tem acesso ao canal ou se está perto de vencer.",
        "",
        "• <b>/help</b> — Tirar dúvidas rápidas sobre como o bot funciona.",
        "  Se estiver perdido, use /help e /start: os dois te colocam de volta nos trilhos.",
        "",
        "• <b>/whoami</b> — Mostrar o seu <b>ID do Telegram</b>.",
        "  Normalmente o suporte pode te pedir esse número para localizar sua conta",
        "  em casos específicos (pagamento antigo, troca de celular, etc.).",
        "",
        "🔄 <b>Se você sair do canal ou trocar de celular</b>",
        "• Enquanto sua assinatura estiver ativa, basta enviar:",
        "  <code>/entrar</code>",
        "  que eu reenvio o convite para o Canal Bet Masterson.",
    ])

    # Bloco avançado só para admin
    admin_help = (
        "\n🛠️ <b>Comandos avançados (admin)</b>\n"
        "Use /help_admin para documentação completa de cada comando.\n\n"
        "📦 FS Weekly: /fs_status · /fs_jogos_hoje · /fs_jogos_amanha\n"
        "🗓 Agenda: /agenda_hoje · /elegiveis · /pub_manual N · /preview N\n"
        "📋 Publicações: /pub_hoje · /pub_pendentes · /pub_reset confirmar\n"
        "🏥 Diagnóstico: /saude · /which_source · /ls_data · /fetch_update · /diag_time · /clean_data\n"
        "🌙 Corujão: /post_coruja\n"
        "👤 Assinaturas: /status_user · /grant_trial · /grant_lifetime · /revoke_sub\n"
        "           /sub_set · /sub_get · /sub_log · /enforce_now\n"
        "📨 Suporte: /resp <id> <texto>\n"
        "🔗 Outros: /test_invite · /supercard_preview · /ping · /whoami · /gid"
    )
    texto = public_help
    if is_admin(m.from_user.id):
        texto = texto + "\n\n" + admin_help

    await m.answer(texto, parse_mode="HTML")


@dp.message(Command("help_admin"))
async def help_admin_cmd(m: Message):
    """Painel completo de ajuda para administradores."""
    if m.from_user.id not in AUTHORIZED:
        return await m.answer("❌ Comando exclusivo da administração.")

    txt = (
        "<b>PAINEL DO ADMIN — Bet Masterson</b>\n"
        "Todos os comandos são exclusivos para administradores.\n\n"

        "📦 <b>FS Weekly — Arquivo e jogos</b>\n"
        "• /fs_status — Saúde do arquivo FS Weekly em disco: nome, período coberto, "
        "total de picks, picks de hoje/amanhã, tamanho e horário de download.\n"
        "• /fs_jogos_hoje — Lista todos os jogos de hoje no FS Weekly: horário, liga, "
        "país, EV picks disponíveis, forma dos times e estádio.\n"
        "• /fs_jogos_amanha — Mesma listagem para o dia seguinte.\n\n"

        "🗓 <b>Agenda e picks</b>\n"
        "• /agenda_hoje — Simula slots futuros da agenda: janela de kickoff e picks "
        "selecionados agora (1 IHR + 2 EV). Ignora publicações já realizadas.\n"
        "• /elegiveis — Picks de hoje que passam nos critérios (prob, EV, lead). "
        "Dividido em: entram no scheduler, fora de slot e já publicados. "
        "Use índices #N para /preview e /pub_manual.\n"
        "• /pub_manual N — Publica imediatamente o pick #N no canal e marca como publicado.\n"
        "• /preview N — Envia o card do pick #N só para você, sem publicar no canal.\n\n"

        "📋 <b>Publicações</b>\n"
        "• /pub_hoje — Tudo publicado hoje em ordem cronológica (jogo, mercado, prob, EV).\n"
        "• /pub_pendentes — Picks elegíveis ainda não publicados com kickoff futuro "
        "e em qual slot entrarão (ou 'fora de slot').\n"
        "• /pub_reset confirmar — Zera o registro de publicações de hoje. Use com cautela.\n\n"

        "🏥 <b>Saúde e diagnóstico</b>\n"
        "• /saude — Painel unificado: arquivo ativo, hora, picks hoje/amanhã, "
        "publicados, próximo slot e parâmetros.\n"
        "• /which_source — Arquivos de odds/super_jogos/FS Weekly ativos em /data.\n"
        "• /ls_data — Lista todos os arquivos em /data com tamanho e mtime.\n"
        "• /fetch_update — Força download de todos os arquivos de dados do GitHub.\n"
        "• /diag_time — Hora local e UTC, timezone e parâmetros de operação.\n"
        "• /clean_data — Remove arquivos antigos de /data fora do período de retenção.\n\n"

        "🌙 <b>Corujão</b>\n"
        "• /post_coruja — Força a publicação do Corujão agora, sem aguardar 00:00.\n\n"

        "👤 <b>Assinaturas / Usuários</b>\n"
        "• /status_user — Status detalhado: assinatura, plano, expiração e canal.\n"
        "• /grant_trial — Conceder trial de 7 dias com convite ao canal.\n"
        "• /grant_lifetime — Conceder assinatura vitalícia.\n"
        "• /revoke_sub — Revogar assinatura e remover do canal.\n"
        "• /sub_set — Ajustar manualmente status/expiração/plano.\n"
        "• /sub_get — Consultar assinatura pelo Telegram ID.\n"
        "• /sub_log — Histórico administrativo das alterações.\n"
        "• /enforce_now — Rodar o enforcer imediatamente.\n\n"

        "📨 <b>Suporte</b>\n"
        "• Assinantes enviam /suporte <mensagem> em DM.\n"
        "• /resp <telegram_id> <texto> — Responde em DM para o assinante.\n"
        "• Ou reply na mensagem encaminhada + /resp <texto>.\n\n"

        "🔗 <b>Outros</b>\n"
        "• /test_invite — Gera link de convite de teste.\n"
        "• /supercard_preview — Prévia dos Super Cards do dia.\n"
        "• /ping · /whoami · /gid — Debug rápido.\n\n"

        "<i>Apenas admins enxergam este comando.</i>"
    )
    await m.answer(txt, parse_mode="HTML")


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
        Destaque à direita — desativado: retorna sempre vazio.
        """
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


# ---- Helper de STATUS (mensagem única para admin e público) ----
async def _build_status_message(uid: int) -> str:
    subs = subs_get().get(str(uid)) or {}

    # --- STATUS (tradução + ícone) ---
    raw_status = (subs.get("status") or "—").lower()

    if raw_status == "active":
        status_emoji = "🟢"
        status_label = "Ativa"
    elif raw_status == "expired":
        status_emoji = "🔴"
        status_label = "Expirada"
    else:
        status_emoji = "⚪"
        status_label = raw_status.capitalize()

    # --- DATA DE EXPIRAÇÃO ---
    exp = int(subs.get("expires_at") or 0)
    if exp > 0:
        try:
            dt_local = datetime.utcfromtimestamp(exp).astimezone(tz.gettz(TZ_NAME))
            exp_txt = dt_local.strftime("%d/%m/%Y %H:%M") + " UTC:-3"
        except Exception:
            exp_txt = "—"
    else:
        exp_txt = "—"

    # --- STATUS NO CANAL ---
    channel_status = await _get_member_status(uid)

    # --- MENSAGEM FINAL ---
    msg = (
        f"👤 <b>ID do usuário</b>: <code>{uid}</code>\n"
        f"{status_emoji} <b>Assinatura</b>: <b>{status_label}</b>\n"
        f"🗓️ <b>Expira em</b>: <b>{exp_txt}</b>\n"
        f"📡 <b>Status no canal</b>: <b>{channel_status}</b>"
    )

    return msg


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
            return await m.answer("Uso: /status_user &lt;telegram_id&gt;")
    else:
        uid = m.from_user.id

    msg = await _build_status_message(uid)
    await m.answer(msg)


# ---- Status de assinatura (público) ----
@dp.message(Command("status_sub"))
async def status_sub(m: types.Message):
    h = subs_get().get(str(m.from_user.id))
    if not h:
        return await m.answer("❌ Nenhuma assinatura encontrada.")
    msg = await _build_status_message(m.from_user.id)
    await m.answer(msg)

# -------- Suporte -------------------------
@dp.message(Command("suporte"))
async def suporte_cmd(m: types.Message):
    # Garante que o suporte é pedido no privado
    if m.chat.type != "private":
        return await m.answer(
            "Envie sua mensagem de suporte em conversa privada comigo.\n"
            "Abra o meu perfil e toque em <b>Iniciar</b>.",
            parse_mode="HTML",
        )

    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await m.answer(
            "Para falar com o suporte, use assim:\n\n"
            "<code>/suporte descreva aqui a sua dúvida</code>",
            parse_mode="HTML",
        )

    body = parts[1].strip()
    user = m.from_user
    username = f"@{user.username}" if user.username else "—"

    # obter e-mail associado (via email_links.json)
    from email_links import get_email_by_telegram

    email_associado = get_email_by_telegram(str(user.id)) or "— (nenhum registrado)"

    msg_admin = (
        "🛠️ <b>Nova mensagem de suporte</b>\n\n"
        f"ID: <code>{user.id}</code>\n"
        f"Nome: {user.full_name}\n"
        f"Username: {username}\n"
        f"E-mail associado: <code>{email_associado}</code>\n\n"
        f"Texto:\n{body}"
    )


    try:
        await notify_admins(msg_admin)
    except Exception:
        # Não derruba o fluxo se der erro ao notificar
        pass

    return await m.answer(
        "✅ Sua mensagem foi enviada ao suporte.\n"
        "Assim que possível, alguém da equipe responde por aqui mesmo."
    )

@dp.message(Command("responder", "resp"))
async def responder_cmd(m: types.Message):
    """
    Comando de ADMIN para responder mensagens de suporte.

    Modos de uso:
      1) /resp 220361810 Sua resposta aqui
      2) (respondendo à mensagem de suporte do bot)
         /resp Sua resposta aqui
    """
    # Só admins podem usar
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Este comando é restrito à equipe do Bet Masterson.")

    text = (m.text or "").strip()
    parts = text.split(maxsplit=2)  # /resp [talvez_id] mensagem...

    target_id: Optional[int] = None
    body: Optional[str] = None

    # Caso 1: /resp 220361810 Mensagem...
    if len(parts) >= 3 and parts[1].isdigit():
        try:
            target_id = int(parts[1])
        except Exception:
            target_id = None
        body = parts[2].strip() if len(parts) >= 3 else ""

    # Caso 2: /resp Mensagem... (em reply à mensagem de suporte)
    else:
        # tenta pegar o ID da mensagem respondida
        if m.reply_to_message and m.reply_to_message.text:
            import re
            match = re.search(r"ID:\s*(\d+)", m.reply_to_message.text)
            if match:
                try:
                    target_id = int(match.group(1))
                except Exception:
                    target_id = None

        # corpo da resposta: TUDO após "/resp "
        body = text[len("/resp"):].strip()


    # Validações
    if not target_id or not body:
        return await m.answer(
            "Uso:\n"
            "• <code>/resp 220361810 sua resposta aqui</code>\n"
            "ou, respondendo à mensagem de suporte do bot:\n"
            "• <code>/resp sua resposta aqui</code>",
            parse_mode="HTML",
        )

    # Envia a resposta para o assinante, em nome do Bet Masterson
    resposta = (
        "🎩 <b>Bet Masterson</b>\n\n"
        f"{body}"
    )

    try:
        await bot.send_message(target_id, resposta, parse_mode="HTML")
    except Exception as e:
        print("[SUPORTE][RESP_ERR]", {"to": target_id, "err": repr(e)})
        return await m.answer(
            "❌ Não consegui enviar a mensagem para o usuário.\n"
            "Verifique o ID ou tente novamente."
        )

    await m.answer(
        f"✅ Resposta enviada para o usuário <code>{target_id}</code>.",
        parse_mode="HTML",
    )



# ---- Convites / Pagamento ----
@dp.message(Command("join"))
async def join_cmd(m: types.Message):
    """
    Comando oficial de entrada no canal (alias: /entrar).
    Verifica assinatura ativa ou trial, gera convite e envia.
    """
    uid = str(m.from_user.id)

    # Recupera registro completo da assinatura
    subs = subs_get()
    rec = subs.get(uid)
    if not isinstance(rec, dict):
        return await m.answer(
            "❌ Você ainda não possui assinatura ativa ou período de teste.\n"
            "Para começar, use:\n\n"
            "<b>/trial seu-email@exemplo.com</b>",
            parse_mode="HTML",
        )

    status = (rec.get("status") or "").lower()
    plan = rec.get("plan") or ""
    exp_ts = int(rec.get("expires_at") or 0)
    now = int(time.time())

    # Verificação de validade
    if status not in ("active", "trial"):
        return await m.answer(
            "❌ Sua assinatura não está ativa no momento.\n"
            "Se você acredita que isto seja um erro, fale com o suporte.",
            parse_mode="HTML",
        )

    if exp_ts and now >= exp_ts:
        return await m.answer(
            "⚠️ Seu período de acesso chegou ao fim.\n\n"
            "Para continuar utilizando o canal, finalize sua assinatura.\n"
            "Se você acredita que isto seja um erro, fale com o suporte.",
            parse_mode="HTML",
        )

    # Geração do convite
    try:
        invite = await on_payment_confirmed(uid, send_dm=False)
    except Exception as e:
        print("[JOIN_CMD][INVITE_ERR]", repr(e))
        return await m.answer(
            "❌ Ocorreu um erro ao gerar o link de acesso.\n"
            "Por favor, tente novamente em instantes.",
            parse_mode="HTML",
        )

    # Mensagens personalizadas
    if status == "trial":
        # Texto exclusivo para quem está no período grátis
        if exp_ts:
            exp_dt = datetime.fromtimestamp(exp_ts).strftime("%d/%m %H:%M")
            validade_txt = f"Seu período de teste está ativo até <b>{exp_dt}</b>."
        else:
            validade_txt = "Seu período de teste está ativo."

        return await m.answer(
            "🎉 <b>Você está no seu período de teste de 30 dias.</b>\n"
            f"{validade_txt}\n\n"
            "Aqui está o seu link exclusivo de acesso ao canal (24h, 1 uso):\n"
            f"👉 {invite}\n\n"
            "Aproveite seu acesso completo ao Canal Bet Masterson.",
            parse_mode="HTML",
        )

    else:
        # Texto para assinaturas pagas normais
        return await m.answer(
            "🔐 <b>Assinatura ativa e válida!</b>\n\n"
            "Aqui está o seu link exclusivo de acesso ao canal (24h, 1 uso):\n"
            f"👉 {invite}\n\n"
            "Ao clicar, o pedido é aprovado automaticamente.",
            parse_mode="HTML",
        )


@dp.message(Command("renovar"))
async def renovar_cmd(m: types.Message):
    """
    Atalho explícito para renovar a assinatura.
    Reusa o mesmo fluxo de /join.
    """
    return await join_cmd(m)


@dp.message(Command("ativar"))
async def ativar_cmd(m: types.Message):
    """
    Ativa a assinatura via e-mail, seguindo a regra:
        1 e-mail ativo = 1 Telegram ativo por vez
    """
    # 1) Garantir que é DM com o bot
    if m.chat.type != "private":
        return await m.answer(
            "Este comando só pode ser usado em conversa privada com o bot.\n"
            "Abra o chat com o bot e envie:\n"
            "<code>/ativar seu-email@exemplo.com</code>"
        )

    parts = (m.text or "").strip().split()
    if len(parts) < 2:
        return await m.answer(
            "Use assim:\n\n"
            "<code>/ativar seu-email@exemplo.com</code>"
        )

    email = parts[1].strip().lower()

    # validação mínima de e-mail
    if "@" not in email or "." not in email:
        return await m.answer(
            "Isso não parece um e-mail válido.\n\n"
            "Exemplo de uso:\n"
            "<code>/ativar seu-email@exemplo.com</code>"
        )

    # 2) Consultar Cakto → cakto_events.json (via find_email_subscription)
    from cakto_payments import find_email_subscription

    info = find_email_subscription(email)
    if not info:
        return await m.answer(
            f"❌ Não encontrei nenhuma assinatura associada ao e-mail "
            f"<b>{email}</b>.\n\n"
            "Se você acabou de concluir o pagamento, aguarde alguns instantes "
            "e tente novamente."
        )

    status = (info.get("status") or "").lower()
    plan = info.get("plan") or "Cakto"
    expires_at = int(info.get("expires_at") or 0)
    now = int(time.time())

    # alinha com sub_is_active
    if status not in ("active", "trial") or (expires_at and expires_at <= now):

        return await m.answer(
            "Encontrei uma assinatura para este e-mail, mas ela não está ativa no momento.\n"
            f"Status atual: <b>{status}</b>.\n\n"
            "Se você acredita que isso é um erro, fale com o suporte informando "
            "o e-mail usado na compra."
        )

    # 3) E-mail tem assinatura ACTIVE → aplicar regra 1 e-mail = 1 Telegram
    telegram_id = str(m.from_user.id)

    link_info = get_link(email)  # do email_links.py

    # --------- Caso A: e-mail nunca vinculado ---------
    if not link_info:
        print("[ATIVAR][CASE_A] email sem vínculo prévio ->", email, "uid=", telegram_id)

        # cria vínculo
        link_email_to_telegram(email, telegram_id)

        # grava/atualiza assinatura no subs.json
        upsert_sub(telegram_id, "active", expires_at, plan, email=email)

        # gera convite e envia DM
        try:
            print("[ATIVAR][CASE_A] chamando on_payment_confirmed uid=", telegram_id)
            invite = await on_payment_confirmed(telegram_id, send_dm=True)
            print("[ATIVAR][CASE_A] retorno on_payment_confirmed:", repr(invite))
        except Exception as e:
            print("[ATIVAR][CASE_A][INVITE_ERR]", repr(e))

        return await m.answer(
            "✅ Assinatura ativada com sucesso!\n\n"
            f"E-mail: <b>{email}</b>\n"
            f"Plano: <b>{plan}</b>"
        )

    # --------- Caso B: e-mail já vinculado a ESTE mesmo Telegram ---------
    previous_owner = str(link_info.get("telegram_id") or "")

    if previous_owner == telegram_id:
        print("[ATIVAR][CASE_B] email já vinculado ao mesmo uid ->", email, "uid=", telegram_id)

        # apenas renova/atualiza validade
        upsert_sub(telegram_id, "active", expires_at, plan, email=email)

        try:
            print("[ATIVAR][CASE_B] chamando on_payment_confirmed uid=", telegram_id)
            invite = await on_payment_confirmed(telegram_id, send_dm=True)
            print("[ATIVAR][CASE_B] retorno on_payment_confirmed:", repr(invite))
        except Exception as e:
            print("[ATIVAR][CASE_B][INVITE_ERR]", repr(e))

        return await m.answer(
            "🔄 Assinatura renovada/atualizada!\n\n"
            f"E-mail: <b>{email}</b>\n"
            f"Plano: <b>{plan}</b>"
        )

    # --------- Caso C: e-mail vinculado a OUTRO Telegram (transferência) ---------
    # Regra “anti-Netflix”: move o acesso para o novo dono,
    # marca o antigo como transferred e tenta removê-lo do canal.

    old_tg = previous_owner
    print("[ATIVAR][CASE_C] transferência de email ->", email, "old_tg=", old_tg, "new_tg=", telegram_id)

    # PASSO 6 — Limite de transferências (anti "Netflix")
    # Usa transfer_count e last_transfer gravados em email_links.json
    transfer_count = int(link_info.get("transfer_count") or 0)
    last_transfer_ts = int(link_info.get("last_transfer") or 0)
    now_ts = int(time.time())

    # Regra: se já atingiu o limite e a última transferência foi dentro da janela, bloqueia
    if transfer_count >= EMAIL_TRANSFER_MAX and last_transfer_ts and (now_ts - last_transfer_ts) < EMAIL_TRANSFER_WINDOW_SEC:
        # Log específico de transferência suspeita
        print("[ATIVAR][CASE_C][LIMIT] Transferência bloqueada por excesso.", {
            "email": email,
            "old_tg": old_tg,
            "new_tg": telegram_id,
            "transfer_count": transfer_count,
            "last_transfer": last_transfer_ts,
            "window_sec": EMAIL_TRANSFER_WINDOW_SEC,
        })
        try:
            # Notifica administradores (se possível)
            await notify_admins(
                "⚠️ Transferência suspeita bloqueada.\n"
                f"E-mail: {email}\n"
                f"Old TG: <code>{old_tg}</code>\n"
                f"New TG: <code>{telegram_id}</code>\n"
                f"Transferências: {transfer_count}\n"
            )
        except Exception:
            pass

        return await m.answer(
            "⚠️ Detectei muitas transferências recentes para este e-mail.\n\n"
            "Por segurança, o sistema bloqueou novas mudanças automáticas de dispositivo.\n"
            "Fale comigo aqui no chat (ou com o suporte), informando este e-mail, "
            "para que possamos revisar e liberar o acesso manualmente."
        )

    # marca o antigo como transferred em subs.json
    subs = subs_get()
    old_rec = subs.get(old_tg)
    if isinstance(old_rec, dict):
        old_rec["status"] = "transferred"
        old_rec["updated_at"] = int(time.time())
        subs[old_tg] = old_rec
        subs_set(subs)

        # tenta expulsar do canal (ban/unban para invalidar convite)
        try:
            await bot.ban_chat_member(GROUP_ID, int(old_tg))
            await bot.unban_chat_member(GROUP_ID, int(old_tg))
        except Exception:
            # não derruba o fluxo se der erro aqui
            pass

    # move o vínculo de e-mail para o novo Telegram
    link_email_to_telegram(email, telegram_id)

    # ativa o novo dono
    upsert_sub(telegram_id, "active", expires_at, plan, email=email)

    # gera novo convite e envia DM
    try:
        print("[ATIVAR][CASE_C] chamando on_payment_confirmed uid=", telegram_id)
        invite = await on_payment_confirmed(telegram_id, send_dm=True)
        print("[ATIVAR][CASE_C] retorno on_payment_confirmed:", repr(invite))
    except Exception as e:
        print("[ATIVAR][CASE_C][INVITE_ERR]", repr(e))

    return await m.answer(
        "🔁 <b>Transferência realizada.</b>\n\n"
        f"O e-mail <b>{email}</b> agora está vinculado a este Telegram.\n"
        f"Plano: <b>{plan}</b>\n\n"
        "Se o e-mail estava em outro dispositivo/conta, o acesso foi movido para cá."
    )

@dp.message(Command("trial"))
async def cmd_trial(m: Message):
    """
    Ativa o trial de 7 dias para o PRÓPRIO usuário.

    Uso:
        /trial seu-email@exemplo.com
    """
    parts = (m.text or "").strip().split()
    if len(parts) != 2:
        await m.answer(
            "Envie assim:\n\n<b>/trial seu-email@exemplo.com</b>",
            parse_mode="HTML",
        )
        return

    email = parts[1].strip().lower()
    tg_id = m.from_user.id

    # 1) Verifica se pode usar trial (e-mail e telegram_id)
    ok, reason = can_use_trial(tg_id, email)
    if not ok:
        # reason deve ser uma mensagem amigável (já tratada em trials.py)
        await m.answer(f"❌ {reason}", parse_mode="HTML")
        return

    # 2) Ativa o trial de fato
    rec = activate_trial(
        telegram_id=tg_id,
        email=email,
        source="self",
    )

    exp_ts = int(rec.get("expires_at", 0) or 0)
    if exp_ts > 0:
        exp_dt = datetime.fromtimestamp(exp_ts).strftime("%d/%m %H:%M")
        exp_txt = f"Válido até: <b>{exp_dt}</b>"
    else:
        exp_txt = "Válido por tempo limitado."

    await m.answer(
        "🎉 <b>Trial ativado!</b>\n"
        f"E-mail: <b>{rec.get('email')}</b>\n"
        f"{exp_txt}\n\n"
        "Use /entrar para receber o convite do canal.",
        parse_mode="HTML",
    )


@dp.message(Command("entrar"))
async def entrar_cmd(m: types.Message):
    # Alias direto para o /join, sem duplicar lógica
    return await join_cmd(m)


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
        "Envie ao seu amigo. Ao concluir o pagamento, ele receberá o acesso ao Canal."
    )
# ------------------------- TRIAL + VITALICIA + REVOGAÇÃO DE AMBOS--------------------
@dp.message(Command("grant_trial"))
async def grant_trial_cmd(m: types.Message):
    """
    Concede trial manual para um usuário por N dias.

    Uso: /grant_trial <telegram_id> [dias=30]
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    parts = (m.text or "").strip().split()
    if len(parts) < 2:
        return await m.answer("Uso: /grant_trial &lt;telegram_id&gt; [dias=30]")

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

    # Ativa trial na base de assinaturas
    exp = set_trial_active(target, days=days, plan_label="trial")

    # Loga ação administrativa
    subs = subs_get()
    uid = str(target)
    rec = subs.get(uid) or {}
    log = list(rec.get("admin_actions") or [])
    log.append({
        "ts": _now(),
        "action": "grant_trial",
        "days": int(days),
        "admin_id": int(m.from_user.id),
    })
    rec["admin_actions"] = log
    subs[uid] = rec
    subs_set(subs)

    # Cria convite temporário
    try:
        expire_inv = _now() + 2 * 60 * 60  # 2h
        invite = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            name=f"Trial {target}",
            expire_date=expire_inv,
            member_limit=1,
            creates_join_request=True,
        )

        inv = invites_get()
        inv[invite.invite_link] = {
            "allowed_uid": int(target),
            "expire": int(expire_inv),
            "created_at": _now(),
        }
        invites_set(inv)

        # Texto de expiração local
        exp_txt = datetime.utcfromtimestamp(exp).astimezone(tz.gettz(TZ_NAME)).strftime("%d/%m/%Y %H:%M") + f" {TZ_NAME}"

        await m.answer(
            f"✅ Trial concedido a <code>{target}</code> por {days} dias.\n"
            f"Expira em: <b>{exp_txt}</b>\n"
            f"Convite (2h, 1 uso):\n{invite.invite_link}"
        )

        # Tenta avisar o usuário
        try:
            await bot.send_message(
                target,
                "🎟️ Você recebeu um TRIAL para o grupo Bet Masterson.\n"
                f"Use este link nas próximas 2 horas (1 uso):\n{invite.invite_link}",
            )
        except Exception:
            pass

    except Exception as e:
        await m.answer(f"❌ Erro ao criar convite trial.\n<code>{e}</code>")


@dp.message(Command("grant_lifetime"))
async def grant_lifetime_cmd(m: types.Message):
    """
    Concede assinatura vitalícia (sem expiração) para um usuário.

    Uso: /grant_lifetime <telegram_id>
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    parts = (m.text or "").strip().split()
    if len(parts) < 2:
        return await m.answer("Uso: /grant_lifetime &lt;telegram_id&gt;")


    try:
        target = int(parts[1])
    except Exception:
        return await m.answer("ID inválido.")

    uid = str(target)

    # 1) Cria/atualiza assinatura vitalícia
    upsert_sub(uid, status="active", expires_at=0, plan="lifetime")

    # 2) Loga ação administrativa
    subs = subs_get()
    rec = subs.get(uid) or {}
    log = list(rec.get("admin_actions") or [])
    log.append({
        "ts": _now(),
        "action": "grant_lifetime",
        "admin_id": int(m.from_user.id),
    })
    rec["admin_actions"] = log
    subs[uid] = rec
    subs_set(subs)

    # 3) Gera convite como em pagamento aprovado
    try:
        invite = await on_payment_confirmed(target, send_dm=True)
    except Exception:
        invite = None

    if invite:
        await m.answer(
            f"✅ Assinatura vitalícia concedida a <code>{target}</code>.\n"
            "Convite enviado ao usuário (ou já ativo)."
        )
    else:
        await m.answer(
            f"✅ Assinatura vitalícia concedida a <code>{target}</code>, "
            "mas houve falha ao gerar/enviar o convite. Verifique manualmente."
        )


@dp.message(Command("revoke_sub"))
async def revoke_sub_cmd(m: types.Message):
    """
    Cancela assinatura manual (trial admin ou vitalícia) e registra quem revogou.

    Uso: /revoke_sub <telegram_id>
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    parts = (m.text or "").strip().split()
    if len(parts) < 2:
        return await m.answer("Uso: /revoke_sub <telegram_id>")

    try:
        target = int(parts[1])
    except Exception:
        return await m.answer("ID inválido.")

    uid = str(target)

    subs = subs_get()
    rec = subs.get(uid) or {}
    prev_status = rec.get("status", "unknown")
    prev_plan = rec.get("plan", "unknown")

    # 1) Marca como cancelada e expirada agora
    upsert_sub(uid, status="cancelled", expires_at=_now(), plan=prev_plan or "manual")

    # 2) Loga ação de revogação
    subs = subs_get()
    rec = subs.get(uid) or {}
    log = list(rec.get("admin_actions") or [])
    log.append({
        "ts": _now(),
        "action": "revoke_sub",
        "admin_id": int(m.from_user.id),
        "prev_status": prev_status,
        "prev_plan": prev_plan,
    })
    rec["admin_actions"] = log
    subs[uid] = rec
    subs_set(subs)

    # 3) Enforce imediato para tirar do canal, se estiver dentro
    try:
        await enforce_once()
    except Exception:
        pass

    await m.answer(
        "🧹 Assinatura cancelada.\n"
        f"id={uid} | status_anterior={prev_status} | plano_anterior={prev_plan}"
    )

#-------------------------------------------------

@dp.message(Command("test_invite"))
async def test_invite_cmd(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    invite = await on_payment_confirmed(m.from_user.id, send_dm=False)
    if invite:
        await m.answer(f"✅ Invite gerado com sucesso:\n{invite}")
    else:
        await m.answer("❌ Falha ao gerar invite. Veja logs INVITE_LINK_ERROR no servidor.")

        
# ---- Novo: reemitir convite (assinante ativo) ----
@dp.message(Command("enforce_now"))
async def cmd_enforce_now(m: types.Message):
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    await m.answer("⏳ Executando enforcer... aguarde.")
    await enforce_once()
    # Gera relatório rápido do estado atual
    subs = subs_get()
    now  = _now()
    lines = ["✅ <b>Enforcer executado.</b>\n\nSituação atual:\n"]
    for uid_str, h in sorted(subs.items()):
        uid_safe = html.escape(str(uid_str))  # escapa < > & para HTML seguro
        try:
            int(uid_str)  # ignora chaves inválidas
        except Exception:
            lines.append(f"⚠️ <code>{uid_safe}</code> — chave inválida (ignorada)")
            continue
        status = (h.get("status") or "").lower()
        exp    = int(h.get("expires_at") or 0)
        is_active = status in ("active", "trial") and (exp == 0 or exp > now)
        emoji = "🟢" if is_active else "🔴"
        exp_txt = _as_dt_local(exp) if exp else "sem expiração"
        lines.append(f"{emoji} <code>{uid_safe}</code> | {status} | {exp_txt}")
    await m.answer("\n".join(lines), parse_mode="HTML")

@dp.message(Command("sub_set"))
async def cmd_sub_set(m: types.Message):
    """
    Uso: /sub_set <telegram_id> <status> [expires_ts|+dias] [plan]
      - status: active | expired | cancelled | trial | transferred | remove
      - expires_ts: timestamp (UTC) OU "+<dias>" (ex.: +30)
      - plan: rótulo livre ("Mensal", "Bet Masterson", "trial_launch", "lifetime"...)
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    parts = (m.text or "").split()
    if len(parts) < 3:
        # precisa escapar "<" e ">" por causa do parse_mode=HTML
        return await m.answer(
            "Uso: /sub_set &lt;telegram_id&gt; &lt;status&gt; [expires_ts|+dias] [plan]"
        )

    # --- TELEGRAM ID ---
    try:
        uid = int(parts[1])
    except Exception:
        return await m.answer("telegram_id inválido.")

    status = parts[2].lower().strip()

    # -----------------------------
    # MODO DELETE: /sub_set 220361810 remove
    # -----------------------------
    if status in {"remove", "del", "delete"}:
        subs = subs_get()
        rec = subs.pop(str(uid), None)
        subs_set(subs)

        if rec:
            return await m.answer(
                f"🧹 Registro REMOVIDO de subs.json para id={uid}."
            )
        else:
            return await m.answer(
                f"⚠️ Não havia registro de assinatura para id={uid}."
            )

    # -----------------------------
    # MODO NORMAL: criar/atualizar assinatura
    # -----------------------------
    exp = 0
    plan = parts[4] if len(parts) >= 5 else "manual"

    if len(parts) >= 4:
        arg = parts[3].strip()
        if arg.startswith("+"):
            # "+30" = 30 dias a partir de agora
            try:
                days = max(1, int(arg[1:]))
                exp = _now() + days * 86400
            except Exception:
                exp = 0
        else:
            # timestamp bruto
            try:
                exp = int(arg)
            except Exception:
                exp = 0

    upsert_sub(str(uid), status, exp, plan)

    await m.answer(
        f"✅ Assinatura ajustada manualmente:\n"
        f"id={uid} | status={status} | expires_at={exp} | plan={plan}"
    )

    # se não estiver active, já roda o enforcer para tirar do canal se necessário
    if status != "active":
        await enforce_once()

@dp.message(Command("sub_get"))
async def cmd_sub_get(m: types.Message):
    """
    Mostra o registro cru de assinatura em subs.json (debug rápido).

    Uso: /sub_get <telegram_id>
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    parts = (m.text or "").strip().split()
    if len(parts) < 2:
        return await m.answer("Uso: /sub_get &lt;telegram_id&gt;")

    try:
        target = int(parts[1])
    except Exception:
        return await m.answer("ID inválido.")

    uid = str(target)
    subs = subs_get()
    rec = subs.get(uid)

    if not rec:
        return await m.answer("❌ Nenhuma assinatura encontrada para esse ID.")

    # formata os dados crus de forma legível
    status = rec.get("status", "—")
    plan = rec.get("plan", "—")
    exp = int(rec.get("expires_at") or 0)
    email = rec.get("email") or "—"

    if exp:
        try:
            dt_local = datetime.utcfromtimestamp(exp).astimezone(tz.gettz(TZ_NAME))
            exp_txt = dt_local.strftime("%d/%m/%Y %H:%M") + f" {TZ_NAME}"
        except Exception:
            exp_txt = str(exp)
    else:
        exp_txt = "0 (sem expiração)"

    lines: list[str] = []
    lines.append("📄 <b>Registro de assinatura (subs.json)</b>")
    lines.append(f"id = <code>{uid}</code>")
    lines.append(f"status: <b>{status}</b>")
    lines.append(f"plano: <b>{plan}</b>")
    lines.append(f"expira em: <b>{exp_txt}</b>")
    lines.append(f"e-mail: <code>{email}</code>")
    lines.append("")
    lines.append("<i>Para detalhes de histórico admin, use /sub_log &lt;telegram_id&gt;.</i>")

    return await m.answer("\n".join(lines))


@dp.message(Command("sub_log"))
async def sub_log_cmd(m: types.Message):
    """
    Mostra o histórico administrativo de uma assinatura (admin_actions).

    Uso: /sub_log <telegram_id>
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    parts = (m.text or "").strip().split()
    if len(parts) < 2:
        # importa escapar o "<" e ">" porque usamos parse_mode=HTML
        return await m.answer("Uso: /sub_log &lt;telegram_id&gt;")

    try:
        target = int(parts[1])
    except Exception:
        return await m.answer("ID inválido.")

    uid = str(target)
    subs = subs_get()
    rec = subs.get(uid)

    if not rec:
        return await m.answer("❌ Nenhuma assinatura encontrada para esse ID.")

    status = rec.get("status", "—")
    plan = rec.get("plan", "—")
    exp = int(rec.get("expires_at") or 0)

    if exp:
        exp_dt = datetime.utcfromtimestamp(exp).astimezone(tz.gettz(TZ_NAME))
        exp_txt = exp_dt.strftime("%d/%m/%Y %H:%M") + f" {TZ_NAME}"
    else:
        exp_txt = "sem expiração (0)"

    lines: list[str] = []
    lines.append("📜 <b>Histórico de assinatura</b>")
    lines.append(f"id = <code>{uid}</code>")
    lines.append(f"status atual: <b>{status}</b>")
    lines.append(f"plano atual: <b>{plan}</b>")
    lines.append(f"expira em: <b>{exp_txt}</b>")

    actions = rec.get("admin_actions") or []
    if not actions:
        lines.append("")
        lines.append("🛠️ Nenhuma ação administrativa registrada.")
        return await m.answer("\n".join(lines))

    lines.append("")
    lines.append("🛠️ <b>Ações administrativas</b>")

    for i, a in enumerate(actions, start=1):
        ts = int(a.get("ts") or 0)
        if ts:
            dt = datetime.utcfromtimestamp(ts).astimezone(tz.gettz(TZ_NAME))
            ts_txt = dt.strftime("%d/%m/%Y %H:%M") + f" {TZ_NAME}"
        else:
            ts_txt = "—"

        action = a.get("action", "?")
        admin_id = a.get("admin_id", "—")
        extra_parts: list[str] = []

        if "days" in a:
            extra_parts.append(f"dias={a['days']}")
        if "prev_status" in a:
            extra_parts.append(f"prev_status={a['prev_status']}")
        if "prev_plan" in a:
            extra_parts.append(f"prev_plan={a['prev_plan']}")

        extras_txt = (" | " + ", ".join(extra_parts)) if extra_parts else ""

        lines.append(
            f"{i}) [{ts_txt}] ação={action} | admin_id={admin_id}{extras_txt}"
        )

    return await m.answer("\n".join(lines))



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
    """
    Percorre subs.json e expulsa do canal qualquer usuário cuja
    assinatura não esteja ativa.

    Regra de atividade (idêntica a sub_is_active):
      - status IN ("active", "trial")  E
      - expires_at == 0  OU  expires_at > now
    Qualquer chave com ID inválido (ex.: "<220361810>") é ignorada
    com segurança — int() vai lançar exceção e o bloco pula.
    """
    try:
        subs = subs_get()
        now  = _now()
        kicked = []
        skipped_admin = []
        skipped_notmember = []
        skipped_active = []

        for uid_str, h in list(subs.items()):
            # --- validar chave: deve ser inteiro puro ---
            try:
                uid_int = int(uid_str)
            except (ValueError, TypeError):
                print(f"[ENFORCE][SKIP] chave inválida ignorada: {uid_str!r}")
                continue

            status = (h.get("status") or "").lower().strip()
            exp    = int(h.get("expires_at") or 0)

            # --- ignorar admins/owner do canal ---
            try:
                if await is_channel_admin(uid_int):
                    skipped_admin.append(uid_str)
                    continue
            except Exception:
                pass

            # --- decidir se assinatura está ativa (mesma lógica de sub_is_active) ---
            is_active = (
                status in ("active", "trial")
                and (exp == 0 or exp > now)
            )

            if is_active:
                skipped_active.append(uid_str)
                continue

            # --- se expirou mas status ainda é "trial"/"active", atualiza para "expired" ---
            if status in ("active", "trial") and exp and exp <= now:
                try:
                    upsert_sub(uid_str, "expired", exp,
                               h.get("plan", ""), email=h.get("email"))
                    print(f"[ENFORCE][EXPIRED] uid={uid_str} status atualizado para expired")
                except Exception as e:
                    print(f"[ENFORCE][EXPIRED_ERR] uid={uid_str}", repr(e))

            # --- banir diretamente sem checar status antes ---
            # Em canais privados a API do Telegram não permite consultar
            # o status de membros que nunca interagiram com o bot via DM.
            # Banimos direto — se não estiver no canal, o Telegram ignora.
            try:
                await bot.ban_chat_member(GROUP_ID, uid_int)
                await bot.unban_chat_member(GROUP_ID, uid_int)
                kicked.append(uid_str)
                print(f"[ENFORCE][KICKED] uid={uid_str} status={status} exp={exp}")
            except Exception as e:
                skipped_notmember.append(uid_str)
                print(f"[ENFORCE][KICK_ERR] uid={uid_str}", repr(e))

        # --- resumo do ciclo ---
        print(
            f"[ENFORCE][DONE] kicked={kicked} | "
            f"skipped_active={len(skipped_active)} | "
            f"skipped_notmember={skipped_notmember} | "
            f"skipped_admin={skipped_admin}"
        )

    except Exception as e:
        print("[ENFORCE_ONCE_ERROR]", repr(e))

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

            # URL genérica de checkout/assinatura (configurada no Render)
            checkout_url = os.getenv("CHECKOUT_URL", "").strip()

            for uid, h in list(subs.items()):
                try:
                    uid_int = int(uid)
                except Exception:
                    continue

                status = (h.get("status", "").lower())
                exp    = int(h.get("expires_at") or 0)
                if not exp:
                    continue

                # ---------------- STATUS / FLUXO TRIAL → PAGO ----------------
                prev_status = str(h.get("last_notified_status", "")).lower()

                # Guarda que este usuário esteve em trial (sem DM)
                if status == "trial" and prev_status != "trial":
                    h["last_notified_status"] = "trial"
                    changed = True

                # Mudou de trial para active → bem-vindo ao plano pago
                if status == "active" and prev_status == "trial":
                    msg = (
                        "🎉 <b>Bem-vindo ao plano pago do Bet Masterson</b>\n\n"
                        "Seu período de teste foi concluído e agora você está no plano completo.\n"
                        f"Sua assinatura está válida até <b>{_as_dt_local(exp)} {TZ_NAME}</b>.\n\n"
                        "Sempre que quiser, use /status_sub aqui na DM para conferir a situação "
                        "da sua assinatura."
                    )
                    await _dm(uid_int, msg)
                    h["last_notified_status"] = "active"
                    # já marca o novo vencimento como notificado, para não duplicar mensagem de renovação
                    h["last_notified_exp"] = exp
                    changed = True

                # Atualiza status inicial para quem já estava ativo antes dessa lógica existir
                if status == "active" and prev_status not in ("trial", "active"):
                    h["last_notified_status"] = "active"
                    changed = True

                # ---------------- RENOVAÇÃO / PRORROGAÇÃO ----------------
                last_exp = int(h.get("last_notified_exp") or 0)

                # Se a assinatura está ativa e o expires_at aumentou em relação ao que já foi notificado,
                # interpretamos como renovação/prorrogação e mandamos a mensagem adequada.
                if status == "active" and exp > 0 and last_exp > 0 and exp > last_exp:
                    msg = (
                        "✅ <b>Renovação confirmada</b>\n\n"
                        f"Sua assinatura foi prorrogada até <b>{_as_dt_local(exp)} {TZ_NAME}</b>.\n\n"
                        "Você não precisa fazer mais nada; seu acesso ao canal segue garantido "
                        "enquanto a assinatura estiver ativa."
                    )
                    await _dm(uid_int, msg)
                    h["last_notified_exp"] = exp
                    changed = True
                elif status == "active" and exp > 0 and last_exp == 0:
                    # Primeira vez que registramos esse vencimento. Não enviamos mensagem
                    # de renovação aqui, apenas marcamos como baseline.
                    h["last_notified_exp"] = exp
                    changed = True

                # ---------------- LEMBRETES DE VENCIMENTO ----------------
                if status not in ("active", "trial"):
                    subs[uid] = h
                    continue

                dleft = _days_left(exp)

                if dleft == 3 and not h.get("notified_3d"):
                    msg = (
                        "⏰ <b>Lembrete de renovação</b>\n"
                        f"Sua assinatura vence em 3 dias (até <b>{_as_dt_local(exp)} {TZ_NAME}</b>)."
                    )
                    if checkout_url:
                        msg += (
                            "\n\n👉 Para renovar e manter o acesso ao canal, use o link:\n"
                            f"{checkout_url}"
                        )
                    await _dm(uid_int, msg)
                    h["notified_3d"] = True
                    changed = True

                if dleft == 0 and not h.get("notified_0d"):
                    msg = (
                        "⏰ <b>Último dia de assinatura</b>\n"
                        f"Sua assinatura expira hoje (<b>{_as_dt_local(exp)} {TZ_NAME}</b>)."
                    )
                    if checkout_url:
                        msg += (
                            "\n\n👉 Se quiser continuar no canal, renove sua assinatura pelo link:\n"
                            f"{checkout_url}"
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

    # ---------------- odds-YYYY-MM-DD.json (HOJE + AMANHÃ) ----------------
    # Execução limpa: buscamos apenas os arquivos de odds do dia local de hoje
    # e de amanhã. Serve para Corujão / próximos jogos sem ficar varrendo o mês.
    tz_sp = tz.gettz(TZ_NAME)
    today_local = datetime.now(tz_sp).date()

    DAYS_FWD = 1  # 0 = hoje, 1 = amanhã

    for delta in range(0, DAYS_FWD + 1):
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

    # ---------------- super_jogos-YYYY-MM-DD.json (hoje + amanhã) ----------------
    try:
        tz_sp = tz.gettz(TZ_NAME)
        today_local = datetime.now(tz_sp).date()
        tomorrow_local = today_local + timedelta(days=1)
        for sj_date in (today_local, tomorrow_local):
            sj_name = f"super_jogos-{sj_date.isoformat()}.json"
            sj_url = f"{base}/{sj_name}"
            sj_local = f"/data/{sj_name}"
            _download_to(sj_local, sj_url)
    except Exception as e:
        print("[FETCH][SUPER][ERR]", repr(e))

    # ---------------- fs_weekly_picks_*.json ----------------
    # Tenta candidatos de nome: inicio pode ser qualquer dia dos últimos 7.
    # Para cada dia D em [hoje-6 .. hoje], testa fs_weekly_picks_D-(D+7).json
    # Baixa o primeiro que existir no GitHub (raw direto, sem API).
    # FS Weekly — lista o repo GitHub e baixa todos fs_weekly_picks_*.json
    # load_fs_weekly_for_date() escolhe o correto pela data (periodo_inicio/fim)
    try:
        import urllib.request as _ur
        api_url = f"https://api.github.com/repos/zuk4-hub/bm_data/contents/"
        gh_headers = {"User-Agent": "bm-bot"}
        if GITHUB_TOKEN:
            gh_headers["Authorization"] = f"token {GITHUB_TOKEN}"
        req = _ur.Request(api_url, headers=gh_headers)
        with _ur.urlopen(req, timeout=10) as resp:
            listing = json.loads(resp.read())
        for item in listing:
            if not isinstance(item, dict): continue
            name = item.get("name", "")
            if not name.startswith("fs_weekly_picks_") or not name.endswith(".json"): continue
            fs_url   = f"{base}/{name}"
            fs_local = f"/data/{name}"
            _download_to(fs_local, fs_url)
            if Path(fs_local).exists() and Path(fs_local).stat().st_size > 100:
                print(f"[FETCH][FS_WEEKLY] {name}")
    except Exception as e:
        print(f"[FETCH][FS_WEEKLY][ERR] {repr(e)}")

    _last_fetch_ts = now

    # Purge automático do log de picks (uma vez por dia) — mantém 180 dias
    try:
        _purge_picks_log(max_days=180)
    except Exception:
        pass

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

def _super_pub_last_idx(gid: str) -> int:
    db = _load_published()
    rec = db.get("supercards", {}).get(gid, {})
    if isinstance(rec, dict):
        try:
            return int(rec.get("done_idx", -1))
        except Exception:
            return -1
    return -1



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


# ========== PATCH COMPLETO — SELEÇÃO POR REGRA (CORRIGIDO, SLOTS 0–2h) ==========
async def _select_by_rule(
    data: Dict[str, Any],
    rule: Dict[str, Any],
    now_local: Optional[datetime] = None,
    slot_local: Optional[datetime] = None,
):

    tz_sp = tz.gettz(TZ_NAME)
    now_local = now_local or datetime.now(tz_sp)
    # AGORA É O MAIS IMPORTANTE: todas as janelas são calculadas em relação ao SLOT
    slot_local = slot_local or now_local

    secs = rule.get("sections", [])
    # DEFINIÇÃO DURA: no máximo 3 cards por slot, independente da agenda
    max_cards = 3

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
    # ===========================================================
    out: List[tuple] = []
    pick_by_game  = defaultdict(int)
    inter_by_game = defaultdict(int)
    intra_by_game = defaultdict(int)

    # Coleta de singles (picks)
    all_picks = list(data.get("picks", []) or [])

    raw_singles: List[tuple] = []
    for p in all_picks:

        prob = _f(p.get("p_model", 0.0))   # prob do modelo
        ev   = _f(p.get("ev", 0.0))        # EV em %

        # filtro mínimo global
        if prob < MIN_PROB or ev <= MIN_EV:
            continue

        sls = sls_score(prob, ev)
        gid = _game_id_from_pick(p)

        raw_singles.append(("pick", p, sls, gid))

    # ordena por SLS desc
    raw_singles.sort(key=lambda x: x[2], reverse=True)

    # ---------- SINGLES ----------
    if "singles" in secs:

        # 1) Decora cada pick com delta em minutos ENTRE SLOT e horário local do jogo
        decorated: List[Tuple[float, str, Dict[str, Any], float, str]] = []
        for kind, payload, sls, gid in raw_singles:
            dt_loc = _parse_any_dt_local(_pick_time_str(payload))
            if not dt_loc:
                continue

            # *** AQUI TROCAMOS now_local POR slot_local ***
            delta_min = (dt_loc - slot_local).total_seconds() / 60.0

            # descarta jogos já começados / muito em cima
            if delta_min < MIN_LEAD_MIN:
                continue

            # *** REGRA DURA: só olha até 2h depois do SLOT ***
            if delta_min > 120.0:
                continue

            decorated.append((delta_min, kind, payload, sls, gid))

        # 2) Janela primária = até +60 min | fallback = +60 até +120 min
        primary  = [tpl for tpl in decorated if tpl[0] <= 60.0]
        fallback = [tpl for tpl in decorated if 60.0 < tpl[0] <= 120.0]

        # prioriza SLS dentro de cada janela
        primary.sort(key=lambda x: x[3], reverse=True)
        fallback.sort(key=lambda x: x[3], reverse=True)

        def _consume(group):
            nonlocal out
            for delta_min, kind, payload, sls, gid in group:

                if len(out) >= max_cards:
                    break

                d_local = _kick_date_local_from_pick(payload)
                already_n = _count_published_picks_for_gid(d_local, gid)

                # máx. 3 picks por jogo no dia
                if already_n + pick_by_game[gid] >= 3:
                    continue

                # máx. X picks por jogo neste slot
                if pick_by_game[gid] >= PICK_PER_MATCH_PER_SLOT:
                    continue

                # evita repetir exatamente o mesmo pick
                if already_published_pick(payload):
                    continue

                out.append(("pick", payload, sls))
                pick_by_game[gid] += 1

        # 3) Consome 1ª hora, depois 2ª hora. NUNCA além de 2h.
        _consume(primary)
        if len(out) < max_cards:
            _consume(fallback)

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

    # Coleta combos
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

                # protege para não encher um jogo só de inters
                # (usa um id sintético com frozenset dos jogos envolvidos)
                gid_synth = "|".join(sorted(list(games)))
                if inter_by_game[gid_synth] >= INTER_PER_MATCH_PER_SLOT:
                    continue

                if already_published_combo(payload):
                    continue

                out.append(("combo", payload, sls))
                inter_by_game[gid_synth] += 1

    return out

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

        # -------- Data/hora da perna em horário LOCAL --------
        # Usa a mesma lógica dos picks: _pick_time_str + _parse_any_dt_local
        kick_iso = _pick_time_str(leg)
        dt_local = _parse_any_dt_local(kick_iso) if kick_iso else None

        if dt_local:
            dd = dt_local.strftime("%d-%m-%Y")
            hh = dt_local.strftime("%Hh%M")
        else:
            # fallback bruto se algo vier muito torto
            dd = (leg.get("data") or "").strip()
            hh = (leg.get("hora") or "").strip()

        safe_dd = _safe_date_str(dd)
        clock_emoji = _clock_emoji_for_hhmm(hh or "")
        when_line = f"{clock_emoji} <b>{safe_dd or '—'}</b> | <b>{hh or '—'}</b> {TZ_LABEL}"


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
            f"🏆 {liga} · {pais}",
            when_line,
            match_line,
            "",
            f"💳 Mercado: <b>{mercado_pt}</b>",
            f"Seleção: <b>{sel}</b>",
        ]

        has_any_metric = (pprob > 0) or (podd > 0) or (pev != 0.0)
        if has_any_metric:
            bloc += [
                "",
                f"Prob. real: <b>{pprob*100:.1f}%</b>  |  Odd justa: <b>{('@'+format(pfair,'.2f')) if pfair>0 else '—'}</b>",
                f"Odd mercado: <b>{('@'+format(podd,'.2f')) if podd>0 else '—'}</b>  |  EV: <b>{pev:.1f}%</b>",
            ]

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
    # padrões genéricos (usamos quando não há mapeamento literal)
}

# Mapeamento market_code FS Weekly → (mercado_display, selecao_display)
# Formato: Mercado = categoria; Seleção = o que se aposta
FS_MARKET_CODE_MAP = {
    "OU_UNDER05":    ("Gols",              "Menos de 0.5"),
    "OU_OVER45":     ("Gols",              "Mais de 4.5"),
    "OU_1H_OVER35":  ("Gols — 1º Tempo",   "Mais de 3.5"),
    "OU_2H_UNDER05": ("Gols — 2º Tempo",   "Menos de 0.5"),
    "RESULT_HOME":   ("Resultado",         "Vitória Mandante"),
    "RESULT_2H_X":   ("Resultado 2º Tempo","Empate"),
}

def _fs_market_selecao(p: dict):
    """Retorna (mercado_pt, selecao_pt) para picks do FS Weekly."""
    code = (p.get("market_code") or "").upper()
    if code in FS_MARKET_CODE_MAP:
        return FS_MARKET_CODE_MAP[code]
    # Fallback: usa os campos já gravados no JSON
    mercado = translate_market(str(p.get("mercado") or p.get("market") or "").strip())
    selecao = str(p.get("selecao") or p.get("selection") or "").strip()
    return mercado, selecao

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

def _badge_ev(ev: Any) -> str:
    """
    Devolve um 'selo' visual para o EV:
      • 💎 EV ≥ 20%
      • ⭐️ EV ≥ 10%
      • ✨ EV >  0
      • "" caso contrário
    """
    try:
        v = float(ev)
    except Exception:
        return ""
    if v >= 20:
        return "💎"
    if v >= 10:
        return "⭐️"
    if v > 0:
        return "✨"
    return ""


def _render_pick_block_for_corujao(p: Dict[str, Any]) -> str:
    """
    Bloco textual de UMA aposta dentro do Corujão, sem cabeçalho de liga/jogo.
    Usa o MESMO padrão de badges da rotina de cards (primary_badges + right_badge_sls),
    e a mesma lógica de mercado/seleção do fmt_pick.
    Para picks do FS Weekly Builder, usa bloco de métricas próprio (IHR).
    """
    # ── Picks do FS Weekly (EV) — formato corujão (sem Forma dos Times) ──
    if p.get("_source_fs_weekly") or p.get("source") in ("fs_weekly_builder","fs_ev_pick"):
        mercado_pt, selecao_pt = _fs_market_selecao(p)
        prob    = _f(p.get("prob_real") or p.get("p_model") or 0.0)
        ev_raw  = p.get("ev") or p.get("EV")
        ev_val  = float(ev_raw) if ev_raw is not None else None
        ev_str  = f"{ev_val:+.2f}" if ev_val is not None else "—"
        odd_num = _to_float_odd(p.get("odd_mercado"))
        odd_str = f"@{odd_num:.2f}" if odd_num > 0 else "—"
        oj_raw  = p.get("odd_justa") or p.get("odd_fair")
        odd_justa = float(oj_raw) if oj_raw else (1.0/prob if prob > 0 else 0.0)
        oj_str  = f"@{odd_justa:.2f}" if odd_justa > 0 else "—"
        # Média da competição
        league_avg_pct = p.get("league_avg_pct")
        sem_media      = p.get("sem_media_liga", False)
        if sem_media:
            media_str = "Não se aplica"
        elif league_avg_pct is not None:
            media_str = f"{league_avg_pct:.0f}%"
        else:
            media_str = "—"
        lines_fs = [
            f"🎯 Mercado: <b>{mercado_pt}</b>",
            f"Seleção: <b>{selecao_pt}</b>",
            "",
            f"Prob. real: <b>{prob*100:.0f}%</b>  |  Odd justa: <b>{oj_str}</b>",
            f"Odd mercado: <b>{odd_str}</b>  |  EV: <b>{ev_str}</b>",
            f"Média da Compet.: <b>{media_str}</b>",
        ]
        return "\n".join(lines_fs)

    lines: List[str] = []

    # ---------------- NÚMEROS BÁSICOS ----------------
    pr     = float(p.get("prob_real") or p.get("p_model") or 0.0)   # 0..1
    ev_val = float(p.get("ev") or p.get("EV") or 0.0)               # já em %

    prob   = pr
    ev_pct = ev_val

    left = primary_badges(prob, ev_pct)

    try:
        sls_val = float(p.get("sls") or p.get("SLS") or 0.0)
    except Exception:
        sls_val = 0.0
    right = right_badge_sls(sls_val)

    # ---------------- MERCADO / SELEÇÃO ----------------
    raw_market = str(p.get("market") or p.get("mercado") or "").strip()
    raw_sel    = str(p.get("selection") or p.get("selecao") or p.get("pick") or "").strip()

    home = p.get("mandante") or p.get("home") or "Casa"
    away = p.get("visitante") or p.get("away") or "Fora"

    # Mercado em PT (vindo do builder + translate_market)
    mercado_pt = translate_market(raw_market) or raw_market or "—"

    # Seleção: tradução padrão para O/U, BTTS etc.
    selecao_pt = (
        raw_sel
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

    # -------------- DNB (Empate Anula) --------------
    if "empate anula aposta" in m_lower or "dnb" in m_lower:
        if s_lower in {"home", "1", "casa", "mandante"}:
            selecao_pt = f"{home}"
        elif s_lower in {"away", "2", "fora", "visitante"}:
            selecao_pt = f"{away}"

    # -------------- DC (Dupla Chance) --------------
    if "dupla chance" in m_lower or "double chance" in m_lower:
        # 1X → Empate - Mandante
        if s_lower in {"1x", "1x ", "1-x", "1 x", "home or draw", "casa ou empate"}:
            selecao_pt = f"Empate - {home}"
        # X2 → Empate - Visitante
        elif s_lower in {"x2", "x-2", "x 2", "draw or away", "empate ou fora"}:
            selecao_pt = f"Empate - {away}"
        # 12 → Mandante - Visitante
        elif s_lower in {"12", "1-2", "1 2", "home or away", "casa ou fora"}:
            selecao_pt = f"{home} - {away}"

    # ---------------- CABEÇALHO E BADGES ----------------
    odd_mercado = _to_float_odd(p.get("odd_mercado") or p.get("odds_market"))
    pr_clamped  = max(0.0, min(1.0, pr))

    title_line = f"{left}Mercado: <b>{mercado_pt}</b>"
    if right:
        title_line += right
    lines.append(title_line)
    lines.append(f"Seleção: <b>{selecao_pt}</b>")

    # ---------------- PROBABILIDADE E ODDS ----------------
    if pr_clamped > 0:
        odd_fair_calc = 1.0 / pr_clamped
        odd_fair_txt  = f"@{odd_fair_calc:.2f}"
    else:
        odd_fair_txt = "—"

    odd_merc_txt = f"@{odd_mercado:.2f}" if odd_mercado and odd_mercado > 0 else "—"

    lines.append(
        f"Prob. real: <b>{pr_clamped:.1%}</b>  |  Odd justa: <b>{odd_fair_txt}</b>"
    )
    lines.append(
        f"Odd mercado: <b>{odd_merc_txt}</b>  |  EV: <b>{ev_val:.1f}%</b>"
    )

    return "\n".join(lines)






async def _send_coruja_card_in_chunks(
    picks: List[Dict[str, Any]],
    footer_aphorism: Optional[str]
) -> bool:
    """
    Envia o bloco do Corujão em UMA sequência de mensagens, respeitando TELEGRAM_SAFE_LIMIT
    e evitando flood de 'Too Many Requests'.

    A PARTIR DE AGORA:

      • NÃO há mais limite por jogo aqui dentro.
      • Recebe diretamente os TOP N GLOBAL por SLS (já filtrados em post_coruja_card).
      • PARA CADA PICK repete o cabeçalho completo do jogo (🏆 / 🕠 / ⚽️),
        mesmo que vários picks sejam do mesmo jogo.
    """
    if not picks:
        return False

    if GROUP_ID == 0:
        print("[CORUJAO][WARN] GROUP_ID = 0, não há para onde enviar.")
        return False

    # Delay pequeno entre mensagens para evitar flood
    try:
        SEND_DELAY = float(os.getenv("CORUJAO_SEND_DELAY", "0.8"))
    except Exception:
        SEND_DELAY = 0.8

    HR = "──────────"
    messages: List[str] = []

    # Cabeçalho global do Corujão (sempre no topo de cada mensagem)
    header_global = [
        BRAND_LINE,
        HR,
        "🌙🦉 Corujão — Na Gaveta da Coruja",
        HR,
    ]

    # Começa com o cabeçalho global
    current_lines: List[str] = list(header_global)

    for p in picks:
        # Dados básicos do jogo
        pais = (p.get("pais") or p.get("country") or "—").strip()
        liga = (p.get("campeonato") or p.get("league") or "—").strip()
        home = (p.get("mandante") or p.get("home") or "—").strip()
        away = (p.get("visitante") or p.get("away") or "—").strip()
        hora = (p.get("hora") or p.get("kickoff_local") or "").strip()

        flag = get_country_flag(pais, liga)

        # Cabeçalho COMPLETO do jogo PARA ESTE PICK
        jogo_header = [
            HR,
            f"🏆 {liga} · {pais} {flag}".rstrip(),
        ]
        if hora:
            clock_emoji = _clock_emoji_for_hhmm(hora)
            jogo_header.append(f"{clock_emoji} Hoje | <b>{hora}</b> (UTC: -3)")

        jogo_header.append(f"⚽️ <b>{home}</b> vs <b>{away}</b>")

        # Bloco do pick (Mercado / Seleção / Prob / Odds / EV / Nota BM)
        pick_block = _render_pick_block_for_corujao(p).strip()
        if not pick_block:
            continue

        bloco_pick = [
            HR,
            pick_block,
        ]

        bloco_completo = jogo_header + bloco_pick

        candidate = current_lines + bloco_completo
        joined = "\n".join(candidate)

        if len(joined) > TELEGRAM_SAFE_LIMIT:
            # Fecha o card atual e começa outro com header_global + bloco deste pick
            messages.append("\n".join(current_lines))
            current_lines = list(header_global) + bloco_completo
        else:
            current_lines.extend(bloco_completo)

    # Fecha o último card lógico
    if current_lines:
        messages.append("\n".join(current_lines))

    if not messages:
        return False

    # --------- AFORISMO NO ÚLTIMO CARD (SEM CRIAR CARD EXTRA) ---------
    if footer_aphorism:
        footer_clean = footer_aphorism.strip()
        if footer_clean and messages:
            last = messages[-1]
            extra = f"\n{HR}\n{footer_clean}"

            # Só adiciona o aforismo se ELE INTEIRO couber no último card.
            # Assim evitamos cortar no meio de tags HTML (<i>, <b>, etc.)
            try:
                limit = TELEGRAM_SAFE_LIMIT
            except NameError:
                limit = None

            if limit is None or (len(last) + len(extra) <= limit):
                messages[-1] = last + extra
            # Se não couber, simplesmente NÃO coloca aforismo,
            # para não gerar erro de parse no Telegram.
    # ------------------------------------------------------------------

    
    # Envia os chunks na ordem, com delay
    sent_any = False
    for msg in messages:
        if not msg or not msg.strip():
            continue
        await bot.send_message(GROUP_ID, msg, parse_mode="HTML")
        sent_any = True
        await asyncio.sleep(SEND_DELAY)

    return sent_any






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

    # Mesma lógica de primazia do scheduler: FS Weekly > Omnia
    _fs = load_fs_weekly_for_date(date_str)
    if _fs and isinstance(_fs, dict):
        data = _fs
        is_ready = True

        # ── FS Weekly: mesma lógica dos slots (1 filtro + 2 EV) ──
        # Janela corujão: kick-off entre 00h00 e 06h59 BRT do dia
        tz_sp2 = tz.gettz(TZ_NAME)
        now_l2 = datetime.now(tz_sp2)

        # Slot virtual = meia-noite do dia
        slot_coru = datetime(d_local.year, d_local.month, d_local.day,
                             0, 0, tzinfo=tz_sp2)

        # Janela: kick entre 00h00 e 06h59 (420 min após meia-noite)
        coru_items = _select_fs_weekly_slot(
            data,
            slot_local=slot_coru,
            min_lead_min=0.0,
            max_lead_min=420.0,
        )

        # Converte tuples → lista de picks
        picks = [payload for kind, payload, _score in coru_items]
        picks.sort(key=lambda x: _dt_key_or_now(_pick_time_str(x)))
        return picks, is_ready

    else:
        data = load_odds_for_date(date_str) or {}
        header = data.get("odds_file_header") or {}
        is_ready = bool(header.get("corujao_ready", True))

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

async def post_coruja_card() -> bool:
    """
    Publica o Corujão:

      • Usa o bloco 'corujao.picks' do odds-AAAA-MM-DD.json (se existir),
        senão filtra os picks normais por horário de madrugada.
      • Mantém APENAS picks com EV > 0.
      • Respeita limite TOTAL de picks e limite por jogo.
      • Usa _send_coruja_card_in_chunks para montar o card único.
      • Marca os picks como publicados (para agenda não reutilizar).
    """
    tz_sp = tz.gettz(TZ_NAME)
    now_l = datetime.now(tz_sp)
    d_local = now_l.date()

    # --- Guarda-corpo: só permite 1 Corujão por dia (memória em RAM) ---
    global _CORUJAO_LAST_SENT_DATE
    date_str = d_local.strftime("%Y-%m-%d")

    if _CORUJAO_LAST_SENT_DATE == date_str:
        # Já mandamos Corujão hoje, não repete
        print(f"[CORUJAO][SKIP_ALREADY_SENT] já enviado em {date_str}")
        return False
    # -------------------------------------------------------------------

    # Coleta picks candidatos ao Corujão
    try:
        picks, is_ready = await _collect_coruja_picks_for_date(d_local)
    except Exception as e:
        print("[CORUJAO][ERR_COLLECT]", repr(e))
        return False

    if not picks:
        print("[CORUJAO][NO_PICKS] Nenhum jogo válido no bloco Corujão do arquivo do dia.")
        return False

    # FILTRO:
    # - Pick do filtro FS (source=fs_weekly_builder): aceita se tiver ihr_pct
    # - Pick de EV FS (source=fs_ev_pick): aceita se tiver ev_pct > 0
    # - Pick Omnia: aceita se EV > 0
    filtered_picks: List[Dict[str, Any]] = []
    for pick in picks:
        src = pick.get("source","")
        if src == "fs_weekly_builder":
            if pick.get("ihr_pct") is not None:
                filtered_picks.append(pick)
        elif src == "fs_ev_pick":
            if float(pick.get("ev_pct") or 0) > 0:
                filtered_picks.append(pick)
        else:
            try:
                ev = float(pick.get("ev") or 0.0)
            except Exception:
                continue
            if ev > 0:
                filtered_picks.append(pick)

    if not filtered_picks:
        print("[CORUJAO][NO_PICKS_VALID] Nenhum pick válido para o Corujão.")
        return False

    # Saneia odds em string "@"
    for p in filtered_picks:
        if isinstance(p.get("odd_mercado"), str):
            p["odd_mercado"] = _to_float_odd(p["odd_mercado"])
        if isinstance(p.get("fair_odd"), str):
            p["fair_odd"] = _to_float_odd(p["fair_odd"])

    # Ordena os picks EXCLUSIVAMENTE por SLS (melhores primeiro),
    # ignorando ordem de horário aqui. O horário só entra na renderização.
    def _sls_from_pick(px: Dict[str, Any]) -> float:
        # Sempre recalcula o SLS global a partir de prob_real e ev
        try:
            pr_loc = float(px.get("prob_real") or 0.0)
        except Exception:
            pr_loc = 0.0
        try:
            ev_loc = float(px.get("ev") or 0.0)
        except Exception:
            ev_loc = 0.0
        # ev_loc já está em %, sls_score assume EV em %
        return sls_score(pr_loc, ev_loc)



    # Ordena por SLS global (melhores primeiro)
    sorted_picks = sorted(
        filtered_picks,
        key=lambda p: _sls_from_pick(p),
        reverse=True,
    )

    # Limites: TOTAL e POR JOGO
    max_total = CORUJAO_MAX_PICKS_TOTAL or 5
    max_per_game = CORUJAO_MAX_PICKS_PER_GAME or max_total

    selected: List[Dict[str, Any]] = []
    per_game_counts: Dict[str, int] = {}

    for p in sorted_picks:
        if len(selected) >= max_total:
            break
        try:
            gid = _game_id_from_pick(p)
        except Exception:
            gid = None

        if gid:
            cnt = per_game_counts.get(gid, 0)
            if cnt >= max_per_game:
                # já atingiu o limite para este jogo, pula
                continue
            per_game_counts[gid] = cnt + 1

        selected.append(p)

    if not selected:
        print("[CORUJAO][NO_SELECTED] Nenhum pick passou no filtro de limites.")
        return False

    sorted_picks = selected

    # ----------------------------------------------------------------------
    # Aforismo da madrugada
    aph = _get_night_aphorism()

    ok = await _send_coruja_card_in_chunks(sorted_picks, aph)
    if not ok:
        print("[CORUJAO][SEND_FAIL] Falha ao enviar Corujão.")
        return False

    # Marca todos como publicados e loga para o tracker
    for p in sorted_picks:
        try:
            mark_published_pick(p)
            _log_pick_completo(p, tipo="corujao")
        except Exception as e:
            print("[CORUJAO][MARK_ERR]", repr(e))

    # Atualiza controle de data para não repetir o Corujão neste dia
    _CORUJAO_LAST_SENT_DATE = date_str
    print(f"[CORUJAO][SEND_OK] registrado envio de {date_str}")

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

def _super_get_state(gid: str) -> Dict[str, Any]:
    return GLOBAL_SUPER_GAMES.get(gid) or {}

def _super_set_state(gid: str, **kwargs) -> None:
    st = GLOBAL_SUPER_GAMES.get(gid) or {}
    st.update(kwargs)
    GLOBAL_SUPER_GAMES[gid] = st


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
            # IMPORTANTE: roda SEMPRE (não apenas quando ensure_data_files falha)
            try:
                cleanup_history()
            except Exception as e:
                print("[SCHED][HIST_WARN]", repr(e))

            # 1) Carrega odds do dia e normaliza com fallback para dict vazio
            try:
                # LÓGICA DE PRIMAZIA: FS Weekly tem prioridade quando disponível.
                # Fallback: Omnia (odds-YYYY-MM-DD.json).
                tz_sp_chk  = tz.gettz(TZ_NAME)
                _today_chk = datetime.now(tz_sp_chk).strftime("%Y-%m-%d")
                _fs = load_fs_weekly_for_date(_today_chk)
                if _fs and isinstance(_fs, dict) and _fs.get("picks") is not None:
                    data = _fs
                    _data_source = "fs_weekly"
                    print(f"[SCHED] Fonte: FS Weekly ({_today_chk}) — {len(_fs.get('picks') or [])} picks")
                    # NAO passa por normalize_odds — preserva _source_fs_weekly nos picks
                else:
                    data = await load_odds_generic()
                    _data_source = "omnia"
                    print(f"[SCHED] Fonte: Omnia ({_today_chk})")
                    if not isinstance(data, dict):
                        data = {}
                    try:
                        data = normalize_odds(data)
                    except Exception:
                        pass
            except Exception as e:
                print("[SCHED][LOAD_ODDS_ERR]", repr(e))
                data = {}
                _data_source = "error"

            # 2) Relógios
            tz_sp    = tz.gettz(TZ_NAME)
            now_utc  = datetime.now(timezone.utc)
            now_local = datetime.now(tz_sp)
            today_sp  = now_local.strftime("%Y-%m-%d")
            today_local = now_local.date()

            # 2-A) DISPARO AUTOMÁTICO DE SUPER GAMES (JOGÃO / JOGAÇO)
            try:
                # usa a data local já calculada acima (today_local)
                sg = _load_super_games_for_date(today_local)
                if sg:
                    header = sg.get("supercard_header") or {}
                    games  = sg.get("games") or []

                    for g in games:
                        gid = _super_game_id(g)

                        # Se já publicou TODOS os cards deste jogo → pula
                        if _super_pub_is_marked(gid):
                            continue

                        # horário local do chute inicial
                        dt_k = _super_kickoff_dt_local(g)
                        if not dt_k:
                            continue
                        dt_k = dt_k.astimezone(tz_sp)

                        # monta TODOS os cards desse jogo (na ordem correta)
                        cards = fmt_super_game_card(g, header=header)
                        if not isinstance(cards, list):
                            cards = [cards]
                        if not cards:
                            _super_pub_mark(gid)
                            continue

                        total = len(cards)

                        # ----------------- AGENDA CONGELADA POR JOGO -----------------
                        state = GLOBAL_SUPER_GAMES.get(gid) or {}
                        sched = state.get("sched")

                        if not sched:
                            # calcula agenda fixa entre o início do dia e KO-45
                            d = dt_k.date()
                            base_first = datetime(
                                d.year,
                                d.month,
                                d.day,
                                SUPER_CARD_FIRST_HOUR,
                                SUPER_CARD_FIRST_MINUTE,
                                0,
                                tzinfo=tz_sp,
                            )

                            last_dt = dt_k - timedelta(minutes=SUPER_CARD_LAST_MIN_BEFORE)

                            # se já passou da janela, encerra jogo
                            if now_local >= last_dt or now_local >= dt_k:
                                _super_pub_mark(gid)
                                continue

                            # primeiro horário real: ou base do dia ou agora+30s
                            if now_local <= base_first:
                                first_dt = base_first
                            else:
                                first_dt = now_local + timedelta(seconds=30)

                            # segurança: janela degenerada
                            if first_dt >= last_dt:
                                sched = [first_dt for _ in range(total)]
                            elif total == 1:
                                sched = [first_dt]
                            else:
                                total_seconds = (last_dt - first_dt).total_seconds()
                                step = total_seconds / float(total - 1)
                                sched = [
                                    first_dt + timedelta(seconds=step * i)
                                    for i in range(total)
                                ]

                            state["sched"] = sched
                            GLOBAL_SUPER_GAMES[gid] = state
                        else:
                            sched = state.get("sched") or []

                        # se por algum motivo não há grade, encerra
                        if not sched:
                            _super_pub_mark(gid)
                            continue

                        # garante comprimentos coerentes
                        if len(sched) < total:
                            total = len(sched)
                            cards = cards[:total]
                        elif len(sched) > total:
                            sched = sched[:total]
                            state["sched"] = sched
                            GLOBAL_SUPER_GAMES[gid] = state

                        # índice do último card enviado (persistido em published.json)
                        last_idx = _super_pub_last_idx(gid)

                        # se já mandou todos, encerra jogo
                        if last_idx >= total - 1:
                            _super_pub_mark(gid)
                            continue

                        # próximo card candidato (compensa atrasados AOS POUCOS)
                        next_idx = last_idx + 1
                        t_target = sched[next_idx]

                        delta = (now_local - t_target).total_seconds()

                        # ainda não chegou a janela desse card (faltam >60s)
                        if delta < -60:
                            continue

                        # chegou a hora ou estamos atrasados: manda SÓ ESTE card
                        msg = cards[next_idx]
                        if not msg or not str(msg).strip():
                            _super_pub_mark_partial(gid, next_idx)
                            if next_idx >= total - 1:
                                _super_pub_mark(gid)
                            continue

                        if "TELEGRAM_SAFE_LIMIT" in globals():
                            limit = TELEGRAM_SAFE_LIMIT
                            if len(msg) > limit:
                                msg = msg[:limit - 10] + "\n…"

                        await bot.send_message(GROUP_ID, msg, parse_mode="HTML")
                        _super_pub_mark_partial(gid, next_idx)

                        # se este foi o último card desse jogo, marca como concluído
                        if next_idx >= total - 1:
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

                    # NOVA REGRA: slot ativo do horário dele até 2h depois
                    delta = now_utc - dt_utc
                    if delta < timedelta(0) or delta > timedelta(hours=2):
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
                            # FS Weekly → seleção híbrida (1 filtro + 2 EV)
                            if data.get("_source") == "fs_weekly":
                                items = _select_fs_weekly_slot(
                                    data,
                                    slot_local=slot_local,
                                    min_lead_min=MIN_LEAD_MIN,
                                    max_lead_min=120.0,
                                ) or []
                            else:
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
                                _log_pick_completo(payload, tipo="slot")
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
        

        tier_raw = str(g.get("super_type") or "").lower()
        is_jogaco = bool(g.get("flag_jogaco")) or (tier_raw == "jogaco")
        icon = "👑" if is_jogaco else "🏟"
        title = "Jogaço" if is_jogaco else "Jogão"

        home = g.get("home", "—")
        away = g.get("away", "—")

        lines.append(
                f"{i}) {icon} {title} — {kickoff_str} | {home} vs {away}"
            )

           
    await m.answer("\n".join(lines), parse_mode="HTML")

    # prévia do card do jogo escolhido
    card_txt = fmt_super_game_card(chosen, header=data.get("supercard_header"))
    await m.answer("👁‍🗨 <b>Pré-visualização do card a ser enviado:</b>", parse_mode="HTML")
    await m.answer(card_txt, parse_mode="HTML")


@dp.message(Command("pub_reset_today"))  # alias legado — use /pub_reset
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
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    tz_sp = tz.gettz(TZ_NAME)
    today_sp    = datetime.now(tz_sp).strftime("%Y-%m-%d")
    tomorrow_sp = (datetime.now(tz_sp) + timedelta(days=1)).strftime("%Y-%m-%d")

    def _which_odds(date_str):
        picked, picked_fp = None, None
        for fp in Path("/data").glob("odds*.json"):
            obj = _read_json_silent(fp)
            if not obj:
                continue
            hdr  = obj.get("odds_file_header") or {}
            meta = obj.get("meta") or {}
            d1   = (hdr.get("target_date_local") or meta.get("target_date_local") or "").strip()
            if d1 == date_str:
                ga = meta.get("generated_at") or ""
                if (not picked) or ga > picked[0]:
                    picked, picked_fp = (ga, obj), fp
        return picked_fp

    fp_today = _which_odds(today_sp)
    fp_tom   = _which_odds(tomorrow_sp)

    lines = ["<b>📂 Fontes detectadas (header-based):</b>"]

    # --- ODDS ---
    if fp_today:
        lines.append(f"✅ Hoje {today_sp} → <code>{fp_today.name}</code>")
    else:
        lines.append(f"❌ Hoje {today_sp} → nenhum odds*.json com esse target_date_local")

    if fp_tom:
        lines.append(f"✅ Amanhã {tomorrow_sp} → <code>{fp_tom.name}</code>")
    else:
        lines.append(f"❌ Amanhã {tomorrow_sp} → nenhum odds*.json com esse target_date_local")

    # --- SUPER JOGOS ---
    lines.append("")
    lines.append("<b>🃏 Super Jogos encontrados em /data:</b>")
    super_files = sorted(Path("/data").glob("super_jogos-*.json"))
    if super_files:
        for fp in super_files:
            obj = _read_json_silent(fp)
            hdr = (obj.get("supercard_header") or {}) if isinstance(obj, dict) else {}
            ds  = hdr.get("target_date_local", "?")
            n_games = len(obj.get("games") or []) if isinstance(obj, dict) else "?"
            lines.append(f"  📋 <code>{fp.name}</code> → data={ds}, jogos={n_games}")
    else:
        lines.append("  (nenhum super_jogos-*.json encontrado)")

    # --- FS WEEKLY em /data ---
    lines.append("")
    lines.append("<b>📦 FS Weekly em /data:</b>")
    fs_files = sorted(Path("/data").glob("fs_weekly_picks_*.json"))
    if fs_files:
        for fp in fs_files:
            obj = _read_json_silent(fp)
            hdr = (obj.get("odds_file_header") or {}) if isinstance(obj, dict) else {}
            ini = hdr.get("periodo_inicio", "?")
            fim = hdr.get("periodo_fim", "?")
            tam = _human(fp.stat().st_size)
            mtime = datetime.fromtimestamp(fp.stat().st_mtime, tz.gettz(TZ_NAME)).strftime("%d/%m %H:%M")
            lines.append(f"  📋 <code>{fp.name}</code>")
            lines.append(f"      Período: {ini} → {fim} | {tam} | {mtime}")
    else:
        lines.append("  ❌ Nenhum fs_weekly_picks_*.json encontrado")

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
    info = _fs_weekly_file_info()
    fs_line = (
        f"📦 FS Weekly: <code>{info['arquivo']}</code> — {info['tamanho']} — {info['mtime']}"
        if info.get("arquivo") else "📦 FS Weekly: não encontrado"
    )
    lines = [
        "<b>✅ Fetch concluído</b>",
        fs_line,
        _stat_file(str(AGENDA_JSON_PATH)),
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

def save_event(event_dict: Dict[str, Any]) -> None:
    save_cakto_event(
        log_path=LOG_PATH,
        event_dict=event_dict,
        safe_load=_safe_load,
        safe_save=_safe_save,
    )


# ---- Novo: gerar convite ao confirmar pagamento ----
async def on_payment_confirmed(user_id: str | int, send_dm: bool = True) -> Optional[str]:
    """
    Gera (ou reutiliza) um link de convite para o canal e,
    opcionalmente, envia por DM para o usuário.

    Regras:
      - Se FORCE_STATIC_INVITE e STATIC_INVITE_LINK estiverem setados,
        usa SEMPRE o link estático.
      - Caso contrário, usa invites_map para criar 1 link por usuário,
        com expiração (ex.: 24h) e member_limit=1.
    """
    try:
        uid = int(user_id)
    except Exception:
        print("[INVITE][ERROR] user_id inválido em on_payment_confirmed:", repr(user_id))
        return None

    # Log de entrada SEMPRE
    print("[INVITE] on_payment_confirmed ENTRY:", {
        "uid": uid,
        "group_id": GROUP_ID,
        "send_dm": send_dm,
    })

    # Sanidade de GROUP_ID
    if not GROUP_ID or int(GROUP_ID) == 0:
        print("[INVITE][SKIP] GROUP_ID não configurado ou inválido:", GROUP_ID)
        return None

    # Conferir se o usuário está ativo em subs.json
    try:
        active = sub_is_active(uid)
    except Exception as e:
        print("[INVITE][ERROR] sub_is_active falhou:", {"uid": uid, "err": repr(e)})
        active = False

    if not active:
        print("[INVITE][SKIP] Usuário não está 'active' em subs.json:", uid)
        # ainda assim retornamos None, mas LOGAMOS o motivo
        return None

    # 1) Se houver link estático forçado, usa ele
    if FORCE_STATIC_INVITE and STATIC_INVITE_LINK:
        link_url = STATIC_INVITE_LINK.strip()
        print("[INVITE] Usando STATIC_INVITE_LINK para uid", uid, "->", link_url)

        if send_dm:
            try:
                await bot.send_message(
                    uid,
                    "✅ Pagamento confirmado! Aqui está o seu convite para o canal:\n"
                    f"{link_url}"
                )
                await bot.send_message(
                    uid,
                    "🎩 Bem-vindo ao Bet Masterson.\n\n"
                    "Clique no link acima para entrar no canal.\n\n"
                    "Assim que entrar, leia com atenção a mensagem fixada no topo do canal (📌). "
                    "Ali eu explico como o canal funciona, o significado de Prob. Real, Odd Justa, EV "
                    "e as orientações de uso.\n\n"                    
                )
            except Exception as e:
                print("[INVITE][DM_ERROR][STATIC]", {"uid": uid, "err": repr(e)})

        return link_url

    # 2) Convite dinâmico (1 por usuário) usando invites_map.json
    try:
        invites_map = invites_get()
    except Exception as e:
        print("[INVITE][ERROR] Falha ao ler invites_map:", repr(e))
        invites_map = {}

    if not isinstance(invites_map, dict):
        invites_map = {}

    now_ts = _now()
    reuse_link: Optional[str] = None

    # Procura se já existe um convite para este uid
    for link_url, meta in list(invites_map.items()):
        if not isinstance(meta, dict):
            continue
        if int(meta.get("allowed_uid") or 0) != uid:
            continue

        exp = int(meta.get("expire") or 0)
        # Se não tiver expiração ou ainda estiver válido (> agora + 60s)
        if exp == 0 or exp > now_ts + 60:
            reuse_link = link_url
            print("[INVITE] Reutilizando invite existente para uid", uid, "->", link_url)
            break

    # Se já temos um link válido, só enviar DM (se pedido) e retornar
    if reuse_link:
        if send_dm:
            try:
                await bot.send_message(
                    uid,
                    "✅ Seu acesso ao canal está ativo.\n"
                    "Aqui está o seu convite atual:\n"
                    f"{reuse_link}"
                )
                await bot.send_message(
                    uid,
                    "🎩 Bem-vindo ao Bet Masterson.\n\n"
                    "Clique no link acima para entrar no canal.\n\n"
                    "Assim que entrar, leia com atenção a mensagem fixada no topo do canal (📌). "
                    "Ali eu explico como o canal funciona, o significado de Prob. Real, Odd Justa, EV "
                    "e as orientações de uso.\n\n"                    
                )
            except Exception as e:
                print("[INVITE][DM_ERROR][REUSE]", {"uid": uid, "err": repr(e)})
        return reuse_link

    # 3) Criar um novo invite link
    expire_ts = now_ts + 24 * 3600  # 24h de validade

    try:
        print("[INVITE] Criando novo invite para uid", uid, "no grupo", GROUP_ID)
        link_obj = await bot.create_chat_invite_link(
            chat_id=GROUP_ID,
            expire_date=expire_ts,
            member_limit=1,
            creates_join_request=False,
        )
    except Exception as e:
        # AQUI é o log que /test_invite manda você olhar
        print("INVITE_LINK_ERROR:", {"uid": uid, "err": repr(e)})
        return None

    invite_url = link_obj.invite_link
    print("[INVITE] link criado com sucesso:", invite_url)

    # Atualiza mapa em disco
    invites_map[invite_url] = {
        "allowed_uid": uid,
        "expire": expire_ts,
        "created_at": now_ts,
    }
    try:
        invites_set(invites_map)
    except Exception as e:
        print("[INVITE][ERROR] Falha ao salvar invites_map:", repr(e))

    # 4) Enviar DM com o link, se solicitado
    if send_dm:
        try:
            await bot.send_message(
                uid,
                "✅ Pagamento confirmado! Aqui está o seu convite para o canal:\n"
                f"{invite_url}"
            )
            await bot.send_message(
                uid,
                "🎩 Bem-vindo ao Bet Masterson.\n\n"
                "Clique no link acima para entrar no canal.\n\n"
                "Assim que entrar, leia com atenção a mensagem fixada no topo do canal (📌). "
                "Ali eu explico como o canal funciona, o significado de Prob. Real, Odd Justa, EV "
                "e as orientações de uso.\n\n"                    
            )
        except Exception as e:
            print("[INVITE][DM_ERROR][NEW]", {"uid": uid, "err": repr(e)})

    return invite_url


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

@app.get("/debug/email_links")
async def debug_email_links(token: str):
    _debug_check(token)
    return JSONResponse(email_links_load())


@app.get("/export/picks_log")
async def export_picks_log(token: str, date: str = ""):
    """
    Exporta o log completo de picks publicados para o tracker.
    Uso: GET /export/picks_log?token=DEBUG_TOKEN&date=2026-05-23
    Se date vazio, retorna todos.
    """
    _debug_check(token)
    try:
        with open(FS_PICKS_LOG_FILE, "r", encoding="utf-8") as f:
            log = json.load(f)
    except Exception:
        log = {"picks": []}

    picks = log.get("picks", [])
    if date:
        picks = [p for p in picks if p.get("data_jogo","").startswith(date)]

    return JSONResponse({
        "total": len(picks),
        "date_filter": date or "all",
        "picks": picks,
    })


@app.delete("/admin/picks_log/purge")
async def purge_picks_log(token: str, before_date: str = ""):
    """
    Remove entradas do log anteriores a before_date (YYYY-MM-DD).
    Uso: DELETE /admin/picks_log/purge?token=DEBUG_TOKEN&before_date=2026-05-01
    """
    _debug_check(token)
    try:
        with open(FS_PICKS_LOG_FILE, "r", encoding="utf-8") as f:
            log = json.load(f)
    except Exception:
        return JSONResponse({"removed": 0, "remaining": 0})

    picks = log.get("picks", [])
    if before_date:
        kept    = [p for p in picks if p.get("data_jogo","") >= before_date]
        removed = len(picks) - len(kept)
        log["picks"] = kept
        with open(FS_PICKS_LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False, indent=2)
    else:
        removed = 0
        kept = picks

    return JSONResponse({"removed": removed, "remaining": len(kept)})

@app.post("/debug/rebuild_subs")
async def debug_rebuild_subs(token: str):
    _debug_check(token)
    result = await rebuild_subs_from_events(
        log_path=LOG_PATH,
        safe_load=_safe_load,
        upsert_sub=upsert_sub,
    )
    return JSONResponse(result)


@app.post("/debug/set_email")
async def debug_set_email(token: str, telegram_id: str, email: str):
    """
    Atualiza apenas o campo 'email' de um registro em subs.json.
    Não altera status, expires_at, plan ou qualquer outro campo.
    """
    _debug_check(token)
    subs = subs_get()
    if telegram_id not in subs:
        return JSONResponse({"ok": False, "error": "telegram_id não encontrado"}, status_code=404)
    subs[telegram_id]["email"] = str(email).strip().lower()
    subs_set(subs)
    return JSONResponse({"ok": True, "telegram_id": telegram_id, "email": subs[telegram_id]["email"]})


@app.post("/debug/clean_subs")
async def debug_clean_subs(token: str):
    """Remove chaves inválidas (não-inteiras) do subs.json, ex.: '<220361810>'."""
    _debug_check(token)
    subs = subs_get()
    removed = []
    kept = {}
    for k, v in subs.items():
        try:
            int(k)
            kept[k] = v
        except (ValueError, TypeError):
            removed.append(k)
    subs_set(kept)
    return JSONResponse({"ok": True, "removed": removed, "remaining": len(kept)})


# -------------------------------------------------------------

@app.post("/cakto/webhook")
async def cakto_webhook(request: Request):
    """
    Endpoint fino: delega autenticação + parsing + processamento
    para o módulo cakto_payments.
    """
    body = await request.body()

    # autenticação via assinatura (módulo)
    if not auth_ok(
        request,
        body,
        secret=CAKTO_SECRET,
        secret_key=CAKTO_SECRET_KEY or None,
    ):
        raise HTTPException(401, "unauthorized")

    # JSON do payload
    try:
        payload = json.loads(body.decode("utf-8") or "{}")
    except Exception:
        raise HTTPException(400, "invalid json")

    # salvar evento bruto no LOG_PATH
    save_cakto_event(
        log_path=LOG_PATH,
        event_dict=payload,
        safe_load=_safe_load,
        safe_save=_safe_save,
    )

    # processar lógica de assinatura / plano / status via módulo
    result = await process_cakto_payload(
        payload,
        upsert_sub=upsert_sub,
        on_payment_confirmed=on_payment_confirmed,
        notify_admins=notify_admins,
    )

    return JSONResponse(result)



# ------------------------- FIM SISTEMA PGTOS - -------------------


@dp.message(Command("clean_data"))
async def clean_data_cmd(m: types.Message):
    """
    Limpeza manual de /data: deleta odds*.json e super_jogos-*.json
    cuja data-alvo seja anterior a hoje - HIST_KEEP_DAYS.
    Uso: /clean_data
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    tz_sp  = tz.gettz(TZ_NAME)
    today  = datetime.now(tz_sp).date()
    cutoff = today - timedelta(days=max(HIST_KEEP_DAYS, 0))

    deleted: List[str] = []
    kept:    List[str] = []
    errors:  List[str] = []

    patterns = ["odds*.json", "super_jogos-*.json"]
    for pat in patterns:
        for fp in sorted(DATA_DIR.glob(pat)):
            try:
                obj   = _read_json_silent(fp)
                fdate = None

                if isinstance(obj, dict):
                    if pat.startswith("odds"):
                        hdr  = obj.get("odds_file_header") or {}
                        meta = obj.get("meta") or {}
                        ds   = (hdr.get("target_date_local") or meta.get("target_date_local") or "").strip()
                    else:  # super_jogos
                        hdr = obj.get("supercard_header") or {}
                        ds  = (hdr.get("target_date_local") or "").strip()

                    if ds:
                        try:
                            yy, mo, dd = map(int, ds.split("-"))
                            fdate = date(yy, mo, dd)
                        except Exception:
                            fdate = None

                # fallback: mtime do arquivo
                if fdate is None:
                    fdate = datetime.fromtimestamp(fp.stat().st_mtime, tz_sp).date()

                if fdate < cutoff:
                    fp.unlink(missing_ok=True)
                    deleted.append(f"{fp.name} (data={fdate})")
                else:
                    kept.append(f"{fp.name} (data={fdate})")

            except Exception as e:
                errors.append(f"{fp.name}: {repr(e)}")

    lines = [f"<b>🧹 /clean_data — cutoff={cutoff} (HIST_KEEP_DAYS={HIST_KEEP_DAYS})</b>", ""]

    if deleted:
        lines.append(f"<b>🗑 Deletados ({len(deleted)}):</b>")
        for x in deleted:
            lines.append(f"  • {x}")
    else:
        lines.append("✅ Nenhum arquivo para deletar.")

    if kept:
        lines.append(f"\n<b>✅ Mantidos ({len(kept)}):</b>")
        for x in kept:
            lines.append(f"  • {x}")

    if errors:
        lines.append(f"\n<b>⚠️ Erros ({len(errors)}):</b>")
        for x in errors:
            lines.append(f"  • {x}")

    msg = "\n".join(lines)
    if len(msg) > 3800:
        msg = msg[:3800] + "\n…(truncado)"

    await m.answer(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════════════
# /post_dica  — Publica uma "Dica do BM" manual no canal
#
# USO (no DM do bot, apenas admin):
#   /post_dica <texto em HTML do Telegram>
#
# O texto pode conter tags <b>, <i>, <code> e \n para quebras de linha.
#
# FLUXO ESPERADO:
#   1. Claude analisa os HTMLs do dia e gera o texto formatado em HTML
#   2. Admin copia o texto e manda: /post_dica [texto]
#   3. Bot publica no GROUP_ID com parse_mode HTML
#   4. Bot confirma no DM do admin com preview do que foi publicado
# ═══════════════════════════════════════════════════════════════════════

@dp.message(Command("post_dica"))
async def post_dica_cmd(m: types.Message):
    """Publica uma dica manual (texto HTML) no canal."""

    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    # Extrai tudo após "/post_dica " como o corpo da mensagem
    full_text = m.text or ""
    parts = full_text.split(maxsplit=1)

    if len(parts) < 2 or not parts[1].strip():
        return await m.answer(
            "⚠️ <b>Uso:</b> <code>/post_dica [texto em HTML]</code>\n\n"
            "Cole o texto da dica logo após o comando, com tags "
            "<code>&lt;b&gt;</code>, <code>&lt;i&gt;</code> e <code>&lt;code&gt;</code>.\n\n"
            "<b>Exemplo:</b>\n"
            "<code>/post_dica 🎯 &lt;b&gt;DICA DO BM&lt;/b&gt;</code>"
        )

    corpo = parts[1].strip()

    # Substitui \n literal por quebra de linha real
    # (facilita copiar/colar em linha única quando necessário)
    corpo = corpo.replace("\\n", "\n")

    try:
        await bot.send_message(
            GROUP_ID,
            corpo,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        # Confirma no DM com preview truncado
        preview = corpo[:300] + ("…" if len(corpo) > 300 else "")
        await m.answer(
            f"✅ <b>Dica publicada no canal.</b>\n\n"
            f"<b>Preview:</b>\n<code>{preview}</code>"
        )
    except Exception as e:
        await m.answer(
            f"❌ <b>Falha ao publicar.</b>\n"
            f"Verifique se o HTML está correto.\n\n"
            f"<code>{e}</code>"
        )


# -------------- Registro explícito das FUNÇÕES DE COMANDO - MENU - (aiogram v3)--------
dp.message.register(help_cmd, Command("help"))
dp.message.register(post_dica_cmd, Command("post_dica"))
dp.message.register(post_coruja_cmd, Command("post_coruja"))
dp.message.register(post_combos, Command("post_combos"))   # <<< garante registro
dp.message.register(post_combo, Command("post_combo"))     # já existia
dp.message.register(games_today_cmd, Command("games_today"))
dp.message.register(games_tomorrow_cmd, Command("games_tomorrow"))
dp.message.register(which_source_cmd, Command("which_source"))
dp.message.register(ls_data, Command("ls_data"))
dp.message.register(fetch_update, Command("fetch_update"))
dp.message.register(diag_time, Command("diag_time"))
dp.message.register(pub_reset_today, Command("pub_reset_today"))
dp.message.register(status_sub, Command("status_sub"))
dp.message.register(join_cmd, Command("join"))
dp.message.register(renovar_cmd, Command("renovar"))
dp.message.register(refer_cmd, Command("refer"))
dp.message.register(grant_trial_cmd, Command("grant_trial"))
dp.message.register(grant_lifetime_cmd, Command("grant_lifetime"))
dp.message.register(revoke_sub_cmd, Command("revoke_sub"))
dp.message.register(sub_log_cmd, Command("sub_log"))
dp.message.register(cmd_enforce_now, Command("enforce_now"))
dp.message.register(cmd_sub_set, Command("sub_set"))
dp.message.register(cmd_sub_get, Command("sub_get"))
dp.message.register(clean_data_cmd, Command("clean_data"))




# ═══════════════════════════════════════════════════════════════════
# PAINEL ADMIN v2 — Novos comandos
# ═══════════════════════════════════════════════════════════════════

def _fs_weekly_file_info() -> dict:
    """Retorna metadados do fs_weekly_picks_*.json mais recente em /data/."""
    best: dict = {"arquivo": None}
    try:
        for fp in sorted(Path("/data").glob("fs_weekly_picks_*.json")):
            obj = _read_json_silent(fp)
            if not isinstance(obj, dict):
                continue
            hdr = obj.get("odds_file_header") or {}
            ini = hdr.get("periodo_inicio", "")
            fim = hdr.get("periodo_fim", "")
            if not ini or not fim:
                continue
            total = sum(len(v) for v in (obj.get("picks_por_data") or {}).values())
            st = fp.stat()
            mtime_raw = st.st_mtime
            mtime = datetime.fromtimestamp(mtime_raw, tz.gettz(TZ_NAME)).strftime("%d/%m %H:%M")
            if best["arquivo"] is None or mtime_raw > best.get("_mtime_raw", 0):
                best = {
                    "arquivo": fp.name,
                    "periodo_inicio": ini,
                    "periodo_fim": fim,
                    "total_picks": total,
                    "mtime": mtime,
                    "tamanho": _human(st.st_size),
                    "_mtime_raw": mtime_raw,
                }
    except Exception as e:
        best["erro"] = str(e)
    return best


def _fs_picks_for_date(date_iso: str) -> list:
    """Picks normais + corujão do FS Weekly para date_iso."""
    try:
        for fp in sorted(Path("/data").glob("fs_weekly_picks_*.json"),
                         key=lambda p: p.stat().st_mtime, reverse=True):
            obj = _read_json_silent(fp)
            if not isinstance(obj, dict):
                continue
            hdr = obj.get("odds_file_header") or {}
            ini = hdr.get("periodo_inicio", "")
            fim = hdr.get("periodo_fim", "")
            if ini and fim and ini <= date_iso <= fim:
                ppd   = obj.get("picks_por_data") or {}
                picks = list(ppd.get(date_iso) or [])
                coru  = [p for p in ((obj.get("corujao") or {}).get("picks") or [])
                         if p.get("target_date_local", "") == date_iso]
                return picks + coru
    except Exception:
        pass
    return []


def _agenda_slots_today() -> list:
    """Slots da agenda editorial para hoje."""
    try:
        if MODE == "editorial":
            agenda = _safe_load(Path(AGENDA_JSON_PATH), {}) or {}
            plan = agenda.get("schedule_plan") or []
            if isinstance(plan, list) and plan:
                return plan
    except Exception:
        pass
    return []


def _pick_display_line(idx: int, p: dict, published: bool) -> str:
    home  = p.get("mandante") or p.get("home") or "?"
    away  = p.get("visitante") or p.get("away") or "?"
    hora  = p.get("hora") or "?"
    camp  = p.get("campeonato") or "?"
    merc  = p.get("mercado") or "?"
    sel   = p.get("selecao") or "?"
    prob  = float(p.get("prob_real") or p.get("p_model") or 0)
    ev    = float(p.get("ev") or 0)
    ihr   = p.get("ihr_pct")
    prob_s = f"{prob*100:.0f}%" if prob else "—"
    ev_s   = f"{ev*100:.1f}%" if ev and abs(ev) < 10 else (f"{ev:.1f}%" if ev else "—")
    ihr_s  = f" IHR {float(ihr):.0f}%" if ihr else ""
    pub_s  = " ✅pub" if published else ""
    gk = p.get("game_key") or ""
    mc = p.get("market_code") or ""
    ref = f" [{gk}·{mc}]" if gk and mc else ""
    return (f"#{idx} {hora} | {home} × {away} | {camp}\n"
            f"   {merc} — {sel} | prob {prob_s} EV {ev_s}{ihr_s}{pub_s}{ref}")


async def _send_chunks(m, lines: list, parse_mode: str = "HTML") -> None:
    chunk, size = [], 0
    for ln in lines:
        if size + len(ln) + 1 > 3800:
            await m.answer("\n".join(chunk), parse_mode=parse_mode)
            chunk, size = [ln], len(ln) + 1
        else:
            chunk.append(ln)
            size += len(ln) + 1
    if chunk:
        await m.answer("\n".join(chunk), parse_mode=parse_mode)


# Cache para /pub_manual e /preview
_ELEGIVEIS_CACHE: list = []
_ELEGIVEIS_CACHE_TS: float = 0.0


@dp.message(Command("fs_status"))
async def fs_status_cmd(m: types.Message):
    """
    /fs_status — Saúde do arquivo FS Weekly em disco.
    Mostra nome, período coberto, total de picks, picks de hoje/amanhã,
    tamanho e quando foi baixado. Use para confirmar que o arquivo certo está ativo.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    info = _fs_weekly_file_info()
    tz_sp = tz.gettz(TZ_NAME)
    today_iso    = datetime.now(tz_sp).strftime("%Y-%m-%d")
    tomorrow_iso = (datetime.now(tz_sp).date() + timedelta(days=1)).strftime("%Y-%m-%d")
    if not info.get("arquivo"):
        return await m.answer("❌ Nenhum fs_weekly_picks_*.json encontrado em /data/.")
    ph = len(_fs_picks_for_date(today_iso))
    pa = len(_fs_picks_for_date(tomorrow_iso))
    lines = [
        "📦 <b>FS Weekly — Status</b>",
        f"📄 <b>Arquivo:</b> <code>{info['arquivo']}</code>",
        f"📅 <b>Período:</b> {info['periodo_inicio']} → {info['periodo_fim']}",
        f"🗃 <b>Total picks:</b> {info['total_picks']} | 📦 {info['tamanho']} | 🕒 {info['mtime']}",
        f"📆 Hoje ({today_iso}): {ph} picks | Amanhã ({tomorrow_iso}): {pa} picks",
    ]
    if info.get("erro"):
        lines.append(f"⚠️ Erro: {info['erro']}")
    await m.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("fs_jogos_hoje"))
async def fs_jogos_hoje_cmd(m: types.Message):
    """
    /fs_jogos_hoje — Todos os jogos de hoje no FS Weekly.
    Horário, liga, país, mandante × visitante, EV picks, forma e estádio.
    Mostra o universo completo sem aplicar filtros de qualidade.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    tz_sp = tz.gettz(TZ_NAME)
    today_iso = datetime.now(tz_sp).strftime("%Y-%m-%d")
    picks = _fs_picks_for_date(today_iso)
    if not picks:
        return await m.answer(f"❌ Nenhum pick para hoje ({today_iso}) no FS Weekly.")
    jogos: dict = {}
    for p in picks:
        gk = p.get("game_key") or _game_id_from_pick(p)
        if gk not in jogos:
            jogos[gk] = {"hora": p.get("hora","?"), "camp": p.get("campeonato","?"),
                         "pais": p.get("pais","?"), "home": p.get("mandante") or p.get("home","?"),
                         "away": p.get("visitante") or p.get("away","?"),
                         "ev_picks": 0, "tem_forma": False, "tem_estadio": False,
                         "is_coru": bool(p.get("is_corujao"))}
        jogos[gk]["ev_picks"] += len(p.get("ev_picks") or [])
        if p.get("forma_mandante"): jogos[gk]["tem_forma"] = True
        if p.get("estadio"):        jogos[gk]["tem_estadio"] = True
    lines = [f"⚽ <b>Jogos hoje — {today_iso}</b> ({len(jogos)} jogos)\n"]
    for j in sorted(jogos.values(), key=lambda x: x["hora"]):
        tags = (" 🌙" if j["is_coru"] else "") + (" 📊" if j["tem_forma"] else "") + (" 🏟" if j["tem_estadio"] else "")
        lines.append(f"🕒 {j['hora']} | <b>{j['home']}</b> × <b>{j['away']}</b>{tags}\n"
                     f"   {j['camp']} · {j['pais']} | EV picks: {j['ev_picks']}")
    await _send_chunks(m, lines)


@dp.message(Command("fs_jogos_amanha"))
async def fs_jogos_amanha_cmd(m: types.Message):
    """
    /fs_jogos_amanha — Todos os jogos de amanhã no FS Weekly.
    Mesma estrutura do /fs_jogos_hoje para o dia seguinte.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    tz_sp = tz.gettz(TZ_NAME)
    tomorrow_iso = (datetime.now(tz_sp).date() + timedelta(days=1)).strftime("%Y-%m-%d")
    picks = _fs_picks_for_date(tomorrow_iso)
    if not picks:
        return await m.answer(f"❌ Nenhum pick para amanhã ({tomorrow_iso}) no FS Weekly.")
    jogos: dict = {}
    for p in picks:
        gk = p.get("game_key") or _game_id_from_pick(p)
        if gk not in jogos:
            jogos[gk] = {"hora": p.get("hora","?"), "camp": p.get("campeonato","?"),
                         "pais": p.get("pais","?"), "home": p.get("mandante") or p.get("home","?"),
                         "away": p.get("visitante") or p.get("away","?"),
                         "ev_picks": 0, "tem_forma": False, "tem_estadio": False,
                         "is_coru": bool(p.get("is_corujao"))}
        jogos[gk]["ev_picks"] += len(p.get("ev_picks") or [])
        if p.get("forma_mandante"): jogos[gk]["tem_forma"] = True
        if p.get("estadio"):        jogos[gk]["tem_estadio"] = True
    lines = [f"📅 <b>Jogos amanhã — {tomorrow_iso}</b> ({len(jogos)} jogos)\n"]
    for j in sorted(jogos.values(), key=lambda x: x["hora"]):
        tags = (" 🌙" if j["is_coru"] else "") + (" 📊" if j["tem_forma"] else "") + (" 🏟" if j["tem_estadio"] else "")
        lines.append(f"🕒 {j['hora']} | <b>{j['home']}</b> × <b>{j['away']}</b>{tags}\n"
                     f"   {j['camp']} · {j['pais']} | EV picks: {j['ev_picks']}")
    await _send_chunks(m, lines)


@dp.message(Command("agenda_hoje"))
async def agenda_hoje_cmd(m: types.Message):
    """
    /agenda_hoje — Simulação dos slots futuros da agenda editorial para hoje.
    Para cada slot mostra a janela de kickoff e quais picks seriam selecionados
    agora (1 IHR + 2 EV). Não reflete picks já publicados.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    plan = _agenda_slots_today()
    if not plan:
        return await m.answer("❌ Sem agenda editorial (agenda vazia ou MODE≠editorial).")
    tz_sp     = tz.gettz(TZ_NAME)
    now_local = datetime.now(tz_sp)
    today_iso = now_local.strftime("%Y-%m-%d")
    fs = load_fs_weekly_for_date(today_iso)
    if not fs or not fs.get("picks"):
        return await m.answer("❌ Sem dados FS Weekly para hoje.")
    lines = [f"🗓 <b>Agenda hoje — {today_iso}</b>",
             f"⏰ Agora: {now_local.strftime('%H:%M')} | ⚠️ Simulação ignora publicações realizadas\n"]
    for slot in plan:
        t_local = slot.get("time_local", "")
        if not t_local:
            continue
        try:
            dt_utc    = datetime.fromisoformat(_to_utc_iso(today_iso, t_local).replace("Z", "+00:00"))
            slot_local = dt_utc.astimezone(tz_sp)
        except Exception:
            continue
        delta_min = (slot_local - now_local).total_seconds() / 60.0
        if delta_min < -120:
            status = "⏱ encerrado"
        elif delta_min < 0:
            status = f"🟡 em janela (há {abs(int(delta_min))} min)"
        else:
            status = f"⏳ em {int(delta_min)} min"
        kick_ini = slot_local + timedelta(minutes=MIN_LEAD_MIN)
        kick_fim = slot_local + timedelta(minutes=120)
        lines.append(f"━━ Slot <b>{t_local}</b> — {status}")
        lines.append(f"   Janela kick: {kick_ini.strftime('%H:%M')}–{kick_fim.strftime('%H:%M')}")
        items = _select_fs_weekly_slot(fs, slot_local=slot_local,
                                       min_lead_min=MIN_LEAD_MIN, max_lead_min=120.0)
        if not items:
            lines.append("   (sem picks elegíveis nesta janela)")
        else:
            for rank, (kind, p, score) in enumerate(items, 1):
                home = p.get("mandante") or p.get("home") or "?"
                away = p.get("visitante") or p.get("away") or "?"
                hora = p.get("hora") or "?"
                merc = p.get("mercado") or "?"
                sel  = p.get("selecao") or "?"
                tipo = "IHR" if rank == 1 else "EV"
                sc_s = f"{score*100:.1f}%" if score < 10 else f"{score:.1f}%"
                lines.append(f"   #{rank} {tipo} {sc_s} | {hora} {home} × {away}")
                lines.append(f"      {merc} — {sel}")
        lines.append("")
    await _send_chunks(m, lines)


@dp.message(Command("elegiveis"))
async def elegiveis_cmd(m: types.Message):
    """
    /elegiveis — Picks de hoje que passam nos critérios do scheduler.
    Mostra prob, EV, IHR e kickoff. Dividido em: entram no scheduler,
    fora de slot (candidatos ao /pub_manual) e já publicados.
    Use os índices #N com /preview N e /pub_manual N.
    O cache dura 10 minutos.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")

    global _ELEGIVEIS_CACHE, _ELEGIVEIS_CACHE_TS
    import time as _time

    tz_sp     = tz.gettz(TZ_NAME)
    now_local = datetime.now(tz_sp)
    today_iso = now_local.strftime("%Y-%m-%d")

    fs = load_fs_weekly_for_date(today_iso)
    if not fs:
        return await m.answer("❌ Sem dados FS Weekly para hoje.")

    all_picks = list(fs.get("picks") or [])
    coru      = list((fs.get("corujao") or {}).get("picks") or [])
    todos     = all_picks + coru

    plan = _agenda_slots_today()
    slots_futuros = []
    for slot in plan:
        t = slot.get("time_local", "")
        if not t:
            continue
        try:
            dt_utc   = datetime.fromisoformat(_to_utc_iso(today_iso, t).replace("Z", "+00:00"))
            sl_local = dt_utc.astimezone(tz_sp)
            if sl_local > now_local - timedelta(hours=2):
                slots_futuros.append(sl_local)
        except Exception:
            continue

    candidatos = []
    for p in todos:
        hora_iso = p.get("hora_iso") or ""
        try:
            kick = datetime.strptime(hora_iso, "%Y-%m-%d %H:%M").replace(tzinfo=tz_sp)
        except Exception:
            continue
        if (kick - now_local).total_seconds() / 60.0 < MIN_LEAD_MIN:
            continue
        prob = float(p.get("prob_real") or p.get("p_model") or 0)
        ihr  = p.get("ihr_pct")
        # Pick principal IHR
        if ihr and prob >= MIN_PROB:
            pub   = already_published_pick(p)
            cobre = any(MIN_LEAD_MIN <= (kick - sl).total_seconds()/60.0 <= 120.0 for sl in slots_futuros)
            candidatos.append({"pick": p, "tipo": "IHR", "score": float(ihr), "pub": pub, "cobre_slot": cobre})
        # EV picks
        for ep in (p.get("ev_picks") or []):
            ev_ep   = float(ep.get("ev") or 0)
            prob_ep = float(ep.get("prob_real") or 0)
            if prob_ep < MIN_PROB or ev_ep <= 0:
                continue
            pick_exp = dict(ep)
            pick_exp.update({
                "mandante": p.get("mandante") or p.get("home",""),
                "visitante": p.get("visitante") or p.get("away",""),
                "campeonato": p.get("campeonato",""), "pais": p.get("pais",""),
                "hora": p.get("hora",""), "hora_iso": hora_iso, "data": p.get("data",""),
                "game_key": p.get("game_key",""),
                "forma_mandante": p.get("forma_mandante"), "forma_visitante": p.get("forma_visitante"),
                "estadio": p.get("estadio"), "target_date_local": p.get("target_date_local",""),
                "is_corujao": p.get("is_corujao", False), "is_world_cup": p.get("is_world_cup", False),
                "source": "fs_ev_pick", "_source_fs_weekly": True,
            })
            pub   = already_published_pick(pick_exp)
            cobre = any(MIN_LEAD_MIN <= (kick - sl).total_seconds()/60.0 <= 120.0 for sl in slots_futuros)
            candidatos.append({"pick": pick_exp, "tipo": "EV", "score": ev_ep, "pub": pub, "cobre_slot": cobre})

    if not candidatos:
        return await m.answer("❌ Nenhum pick elegível para hoje.")

    candidatos.sort(key=lambda c: (0 if c["tipo"]=="IHR" else 1, -c["score"]))
    _ELEGIVEIS_CACHE    = [c["pick"] for c in candidatos]
    _ELEGIVEIS_CACHE_TS = _time.time()

    em_slot    = [c for c in candidatos if c["cobre_slot"] and not c["pub"]]
    fora_slot  = [c for c in candidatos if not c["cobre_slot"] and not c["pub"]]
    publicados = [c for c in candidatos if c["pub"]]

    lines = [f"🎯 <b>Elegíveis hoje — {today_iso}</b>",
             f"Total: {len(candidatos)} | Scheduler: {len(em_slot)} | Fora de slot: {len(fora_slot)} | Publicados: {len(publicados)}\n"]
    idx = 1
    if em_slot:
        lines.append("✅ <b>Entram no scheduler</b>")
        for c in em_slot:
            lines.append(_pick_display_line(idx, c["pick"], False)); idx += 1
        lines.append("")
    if fora_slot:
        lines.append("⚠️ <b>Fora de slot — use /pub_manual N</b>")
        for c in fora_slot:
            lines.append(_pick_display_line(idx, c["pick"], False)); idx += 1
        lines.append("")
    if publicados:
        lines.append("☑️ <b>Já publicados</b>")
        for c in publicados:
            lines.append(_pick_display_line(idx, c["pick"], True)); idx += 1
    lines.append("\n💡 /preview N para ver o card · /pub_manual N para publicar")
    await _send_chunks(m, lines)


@dp.message(Command("preview"))
async def preview_cmd(m: types.Message):
    """
    /preview N — Envia o card do pick #N (do /elegiveis) só para você, sem publicar.
    Ex: /preview 3. O cache de elegíveis dura 10 minutos.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    import time as _time
    args = (m.text or "").split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer("Uso: /preview N  (N = índice do /elegiveis)")
    idx = int(args[1]) - 1
    if not _ELEGIVEIS_CACHE or idx < 0 or idx >= len(_ELEGIVEIS_CACHE):
        return await m.answer("❌ Índice inválido ou cache vazio. Rode /elegiveis primeiro.")
    if _time.time() - _ELEGIVEIS_CACHE_TS > 600:
        return await m.answer("⚠️ Cache expirado (>10 min). Rode /elegiveis novamente.")
    pick = _ELEGIVEIS_CACHE[idx]
    try:
        card = fmt_pick(pick)
    except Exception:
        try:
            card = fmt_pick_fs(pick)
        except Exception as e:
            return await m.answer(f"❌ Erro ao formatar: {e}")
    await m.answer(f"👁 <b>Preview #{idx+1}</b> (não publicado)\n\n{card}", parse_mode="HTML")


@dp.message(Command("pub_manual"))
async def pub_manual_cmd(m: types.Message):
    """
    /pub_manual N — Publica imediatamente o pick #N (do /elegiveis) no canal.
    Marca como publicado para o scheduler não repetir.
    Só funciona com kickoff futuro e lead mínimo respeitado.
    Ex: /pub_manual 5. O cache dura 10 minutos.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    import time as _time
    args = (m.text or "").split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.answer("Uso: /pub_manual N  (N = índice do /elegiveis)")
    idx = int(args[1]) - 1
    if not _ELEGIVEIS_CACHE or idx < 0 or idx >= len(_ELEGIVEIS_CACHE):
        return await m.answer("❌ Índice inválido ou cache vazio. Rode /elegiveis primeiro.")
    if _time.time() - _ELEGIVEIS_CACHE_TS > 600:
        return await m.answer("⚠️ Cache expirado (>10 min). Rode /elegiveis novamente.")
    pick = _ELEGIVEIS_CACHE[idx]
    tz_sp     = tz.gettz(TZ_NAME)
    now_local = datetime.now(tz_sp)
    if not _time_ok_lead(_pick_time_str(pick), now_local, MIN_LEAD_MIN):
        return await m.answer(f"❌ Kickoff muito próximo (mínimo {MIN_LEAD_MIN} min).")
    if already_published_pick(pick):
        return await m.answer("❌ Pick já publicado. Rode /elegiveis para atualizar.")
    try:
        card = fmt_pick(pick)
    except Exception:
        try:
            card = fmt_pick_fs(pick)
        except Exception as e:
            return await m.answer(f"❌ Erro ao formatar: {e}")
    try:
        await bot.send_message(GROUP_ID, card, parse_mode="HTML")
        mark_published_pick(pick)
        _log_pick_completo(pick, tipo="manual")
        home = pick.get("mandante") or pick.get("home","?")
        away = pick.get("visitante") or pick.get("away","?")
        await m.answer(f"✅ Publicado!\n<b>{home} × {away}</b>\n{pick.get('mercado','')} — {pick.get('selecao','')}", parse_mode="HTML")
    except Exception as e:
        await m.answer(f"❌ Falha: <code>{e}</code>", parse_mode="HTML")


@dp.message(Command("pub_hoje"))
async def pub_hoje_cmd(m: types.Message):
    """
    /pub_hoje — Histórico de publicações de hoje em ordem cronológica.
    Mostra hora de publicação, jogo, mercado, seleção, prob e EV.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    tz_sp     = tz.gettz(TZ_NAME)
    today_iso = datetime.now(tz_sp).strftime("%Y-%m-%d")
    db        = _load_published()
    day_picks = (db.get("picks") or {}).get(today_iso) or {}
    if not day_picks:
        return await m.answer(f"📭 Nenhum pick publicado hoje ({today_iso}).")

    fs    = load_fs_weekly_for_date(today_iso)
    todos = list((fs or {}).get("picks") or []) + list(((fs or {}).get("corujao") or {}).get("picks") or [])
    idx_by_sig: dict = {}
    for p in todos:
        sig = _pick_signature(p)
        if sig: idx_by_sig[sig] = p
        for ep in (p.get("ev_picks") or []):
            ep_exp = dict(ep)
            ep_exp.update({"mandante": p.get("mandante",""), "visitante": p.get("visitante",""),
                           "campeonato": p.get("campeonato",""), "pais": p.get("pais",""),
                           "hora": p.get("hora",""), "hora_iso": p.get("hora_iso",""),
                           "data": p.get("data",""), "game_key": p.get("game_key","")})
            sig_ep = _pick_signature(ep_exp)
            if sig_ep: idx_by_sig[sig_ep] = ep_exp

    def _fmt_ts(ts):
        try:
            return datetime.fromisoformat(ts.replace("Z","")).replace(tzinfo=timezone.utc).astimezone(tz_sp).strftime("%H:%M")
        except Exception: return "??"

    items = sorted(day_picks.items(), key=lambda kv: (kv[1] or {}).get("ts",""))
    lines = [f"📋 <b>Publicados hoje — {today_iso}</b> ({len(items)} picks)\n"]
    for sig, meta in items:
        meta = meta or {}
        hhmm = _fmt_ts(meta.get("ts") or "")
        p = idx_by_sig.get(sig)
        if p:
            home = p.get("mandante") or p.get("home","?")
            away = p.get("visitante") or p.get("away","?")
            prob = float(p.get("prob_real") or p.get("p_model") or 0)
            ev   = float(p.get("ev") or 0)
            prob_s = f"{prob*100:.0f}%" if prob else "—"
            ev_s   = f"{ev*100:.1f}%" if abs(ev) < 10 else f"{ev:.1f}%"
            lines.append(f"🕐 {hhmm} | <b>{home} × {away}</b>\n"
                         f"   {p.get('mercado','')} — {p.get('selecao','')} | prob {prob_s} EV {ev_s}")
        else:
            lines.append(f"🕐 {hhmm} | sig={sig[:12]}… (pick não localizado no FS Weekly)")
    await _send_chunks(m, lines)


@dp.message(Command("pub_pendentes"))
async def pub_pendentes_cmd(m: types.Message):
    """
    /pub_pendentes — Picks elegíveis ainda não publicados com kickoff futuro.
    Mostra em qual slot entrarão ou 'fora de slot' (candidatos ao /pub_manual).
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    tz_sp     = tz.gettz(TZ_NAME)
    now_local = datetime.now(tz_sp)
    today_iso = now_local.strftime("%Y-%m-%d")
    fs = load_fs_weekly_for_date(today_iso)
    if not fs:
        return await m.answer("❌ Sem dados FS Weekly para hoje.")

    plan = _agenda_slots_today()
    slots_futuros = []
    for slot in plan:
        t = slot.get("time_local","")
        if not t: continue
        try:
            dt_utc   = datetime.fromisoformat(_to_utc_iso(today_iso, t).replace("Z","+00:00"))
            sl_local = dt_utc.astimezone(tz_sp)
            if sl_local > now_local - timedelta(hours=2):
                slots_futuros.append((t, sl_local))
        except Exception: continue

    todos = list(fs.get("picks") or []) + list((fs.get("corujao") or {}).get("picks") or [])
    pendentes = []
    for p in todos:
        hora_iso = p.get("hora_iso") or ""
        try:
            kick = datetime.strptime(hora_iso, "%Y-%m-%d %H:%M").replace(tzinfo=tz_sp)
        except Exception: continue
        if (kick - now_local).total_seconds()/60.0 < MIN_LEAD_MIN: continue
        for ep in (p.get("ev_picks") or []):
            ev_ep   = float(ep.get("ev") or 0)
            prob_ep = float(ep.get("prob_real") or 0)
            if prob_ep < MIN_PROB or ev_ep <= 0: continue
            pick_exp = dict(ep)
            pick_exp.update({"mandante": p.get("mandante",""), "visitante": p.get("visitante",""),
                             "campeonato": p.get("campeonato",""), "pais": p.get("pais",""),
                             "hora": p.get("hora",""), "hora_iso": hora_iso,
                             "data": p.get("data",""), "game_key": p.get("game_key",""),
                             "forma_mandante": p.get("forma_mandante"), "forma_visitante": p.get("forma_visitante"),
                             "estadio": p.get("estadio"), "target_date_local": p.get("target_date_local",""),
                             "is_corujao": p.get("is_corujao",False), "is_world_cup": p.get("is_world_cup",False),
                             "source": "fs_ev_pick", "_source_fs_weekly": True})
            if already_published_pick(pick_exp): continue
            slot_label = "fora de slot"
            for t_str, sl_local in slots_futuros:
                if MIN_LEAD_MIN <= (kick - sl_local).total_seconds()/60.0 <= 120.0:
                    slot_label = f"slot {t_str}"; break
            pendentes.append({"pick": pick_exp, "slot": slot_label, "ev": ev_ep})

    if not pendentes:
        return await m.answer("✅ Nenhum pick pendente.")
    pendentes.sort(key=lambda x: x["slot"])
    lines = [f"⏳ <b>Pendentes hoje — {today_iso}</b> ({len(pendentes)} picks)\n"]
    for i, item in enumerate(pendentes, 1):
        p = item["pick"]
        ev_s = f"{item['ev']*100:.1f}%" if abs(item['ev']) < 10 else f"{item['ev']:.1f}%"
        lines.append(f"#{i} 📌 <b>{item['slot']}</b> | {p.get('hora','?')} "
                     f"{p.get('mandante','?')} × {p.get('visitante','?')}\n"
                     f"   {p.get('mercado','')} — {p.get('selecao','')} | EV {ev_s}")
    lines.append("\n💡 Use /pub_manual N para picks fora de slot")
    await _send_chunks(m, lines)


@dp.message(Command("pub_reset"))
async def pub_reset_cmd(m: types.Message):
    """
    /pub_reset confirmar — Zera o registro de publicações de hoje.
    Permite repostar picks já publicados. Use com cautela.
    Envie sem argumento para ver a confirmação necessária.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    args = (m.text or "").split()
    if len(args) < 2 or args[1].lower() != "confirmar":
        return await m.answer("⚠️ Apagará o registro de picks publicados hoje.\nPara confirmar: /pub_reset confirmar")
    tz_sp     = tz.gettz(TZ_NAME)
    today_iso = datetime.now(tz_sp).strftime("%Y-%m-%d")
    db = _load_published()
    n_antes = len((db.get("picks") or {}).get(today_iso) or {})
    (db.setdefault("picks", {}))[today_iso] = {}
    _save_published(db)
    await m.answer(f"✅ Registro de hoje resetado.\n{n_antes} pick(s) apagados do log de {today_iso}.")


@dp.message(Command("saude"))
async def saude_cmd(m: types.Message):
    """
    /saude — Painel unificado de diagnóstico do bot.
    Arquivo FS Weekly ativo, hora local/UTC, picks hoje/amanhã,
    publicados hoje, próximo slot e parâmetros ativos.
    """
    if not is_admin(m.from_user.id):
        return await m.answer("🚫 Acesso restrito.")
    tz_sp     = tz.gettz(TZ_NAME)
    now_local = datetime.now(tz_sp)
    now_utc   = datetime.now(timezone.utc)
    today_iso    = now_local.strftime("%Y-%m-%d")
    tomorrow_iso = (now_local.date() + timedelta(days=1)).strftime("%Y-%m-%d")

    info = _fs_weekly_file_info()
    ph   = len(_fs_picks_for_date(today_iso))
    pa   = len(_fs_picks_for_date(tomorrow_iso))
    db   = _load_published()
    pub_hoje = len((db.get("picks") or {}).get(today_iso) or {})

    plan = _agenda_slots_today()
    proximo_slot  = "—"
    proximo_items = []
    fs = load_fs_weekly_for_date(today_iso)
    for slot in plan:
        t = slot.get("time_local","")
        if not t: continue
        try:
            dt_utc   = datetime.fromisoformat(_to_utc_iso(today_iso, t).replace("Z","+00:00"))
            sl_local = dt_utc.astimezone(tz_sp)
            delta_min = (sl_local - now_local).total_seconds()/60.0
            if delta_min > 0:
                proximo_slot = f"{t} (em {int(delta_min)} min)"
                if fs:
                    proximo_items = _select_fs_weekly_slot(fs, slot_local=sl_local,
                                                           min_lead_min=MIN_LEAD_MIN, max_lead_min=120.0)
                break
        except Exception: continue

    lines = [
        "🏥 <b>Saúde do Bot — Bet Masterson</b>",
        f"\n🕒 Local: {now_local.strftime('%d/%m %H:%M:%S')} | UTC: {now_utc.strftime('%d/%m %H:%M:%S')}",
        "\n📦 <b>FS Weekly</b>",
        f"   {info.get('arquivo','❌ não encontrado')}",
    ]
    if info.get("periodo_inicio"):
        lines.append(f"   {info['periodo_inicio']} → {info['periodo_fim']} | {info['tamanho']} | {info['mtime']}")
    lines += [
        f"\n📆 Hoje: {ph} picks | Amanhã: {pa} picks | Publicados hoje: {pub_hoje}",
        f"\n🗓 Próximo slot: {proximo_slot}",
    ]
    for rank, (kind, p, score) in enumerate(proximo_items, 1):
        tipo = "IHR" if rank == 1 else "EV"
        lines.append(f"   #{rank} {tipo} | {p.get('hora','?')} {p.get('mandante','?')} × {p.get('visitante','?')} | {p.get('mercado','')}")
    lines += [
        "\n⚙️ <b>Parâmetros</b>",
        f"   MIN_PROB={MIN_PROB:.0%} MIN_EV={MIN_EV:.1f} SLS_WP={SLS_WP} SLS_WE={SLS_WE}",
        f"   MIN_LEAD={MIN_LEAD_MIN}min HOURLY_MAX={HOURLY_MAX} MAX/DIA={MAX_PUBLICATIONS_PER_DAY} MODE={MODE}",
    ]
    await m.answer("\n".join(lines), parse_mode="HTML")



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
