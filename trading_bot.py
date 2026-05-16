#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import logging
from flask import Flask, request, jsonify
import os
import threading
import redis

# ============================================================================ #
# CONFIGURATION
# ============================================================================ #

CONFIG = {
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID': os.environ.get('TELEGRAM_CHAT_ID', ''),
    
    'SYMBOLS': {
        'AAVE/USDT':        {'exchange': 'okx', 'scalp': True},
        'APT/USDT':         {'exchange': 'okx', 'scalp': True},
        'ARB/USDT':         {'exchange': 'okx', 'scalp': True},
        'AVAX/USDT':        {'exchange': 'okx', 'scalp': True},
        'BTC/USDT':         {'exchange': 'okx', 'scalp': True},
        'ETH/USDT':         {'exchange': 'okx', 'scalp': True},
        'INJ/USDT':         {'exchange': 'okx', 'scalp': True},
        'LINK/USDT':        {'exchange': 'okx', 'scalp': True},
        'LTC/USDT':         {'exchange': 'okx', 'scalp': True},
        'NEAR/USDT':        {'exchange': 'okx', 'scalp': True},
        'SOL/USDT':         {'exchange': 'okx', 'scalp': True},
        'SUI/USDT':         {'exchange': 'okx', 'scalp': True},
        'UNI/USDT':         {'exchange': 'okx', 'scalp': True},
        'XRP/USDT':         {'exchange': 'okx', 'scalp': True},
        'BCH/USDT':         {'exchange': 'okx', 'scalp': False},
        'BNB/USDT':         {'exchange': 'okx', 'scalp': False},
        'BONK/USDT':        {'exchange': 'okx', 'scalp': False},
        'COMP/USDT':        {'exchange': 'okx', 'scalp': False},
        'CRV/USDT':         {'exchange': 'okx', 'scalp': False},
        'CVX/USDT':         {'exchange': 'okx', 'scalp': False},
        'DOGE/USDT':        {'exchange': 'okx', 'scalp': False},
        'DYDX/USDT':        {'exchange': 'okx', 'scalp': False},
        'ENA/USDT':         {'exchange': 'okx', 'scalp': False},
        'ETC/USDT':         {'exchange': 'okx', 'scalp': False},
        'FET/USDT':         {'exchange': 'okx', 'scalp': False},
        'FIL/USDT':         {'exchange': 'okx', 'scalp': False},
        'HBAR/USDT':        {'exchange': 'okx', 'scalp': False},
        'LDO/USDT':         {'exchange': 'okx', 'scalp': False},
        'ONDO/USDT':        {'exchange': 'okx', 'scalp': False},
        'ONT/USDT':         {'exchange': 'okx', 'scalp': False},
        'PENGU/USDT':       {'exchange': 'okx', 'scalp': False},
        'PEPE/USDT':        {'exchange': 'okx', 'scalp': False},
        'RENDER/USDT':      {'exchange': 'okx', 'scalp': False},
        'SAND/USDT':        {'exchange': 'okx', 'scalp': False},
        'SKY/USDT':         {'exchange': 'okx', 'scalp': False},
        'STX/USDT':         {'exchange': 'okx', 'scalp': False},
        'TIA/USDT':         {'exchange': 'okx', 'scalp': False},
        'USELESS/USDT':     {'exchange': 'okx', 'scalp': False},
        'ZEC/USDT':         {'exchange': 'okx', 'scalp': False},
    },
    
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,
    'HEARTBEAT_INTERVAL_SECONDS': int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", 21600)),
    'BARK_TOKEN': os.environ.get('BARK_TOKEN', ''),  # legacy
    'SCALP_BOT_TOKEN': os.environ.get('SCALP_BOT_TOKEN', ''),
    'NTFY_TOPIC': os.environ.get('NTFY_TOPIC', ''),
    'TAPBIT_BOT_URL': os.environ.get('TAPBIT_BOT_URL', ''),  # ex: https://tapbit-bot.up.railway.app
    'JOURNAL_BOT_URL': os.environ.get('JOURNAL_BOT_URL', ''),  # ex: https://journal-bot.up.railway.app
    'WEBHOOK_PORT': int(os.environ.get("PORT", 5000)),
    'WEBHOOK_HOST': '0.0.0.0',
}

# ============================================================================ #
# ETAT GLOBAL
# ============================================================================ #

LAST_SIGNALS = {}
LAST_SIGNAL_EVENTS = {}
MOMENTUM_STATE = {}

# ============================================================================ #
# STATISTIQUES HEBDOMADAIRES
# ============================================================================ #

WEEKLY_STATS = {}
WEEKLY_START = datetime.now(timezone.utc)
PREP_BUFFER = []  # Buffer des alertes de preparation
STATE_LOCK = threading.RLock()  # RLock réentrant — évite deadlock should_send dans SCALP

def track_alert(symbol, strategy):
    if symbol not in WEEKLY_STATS:
        WEEKLY_STATS[symbol] = {
            'SAFE': 0, 'CONFLUENCE': 0, 'TREND': 0, 'CONTEXT4H': 0, 'SWING': 0, 'PULSE': 0, 'MOMENTUM': 0,
        }
    if strategy not in WEEKLY_STATS[symbol]:
        WEEKLY_STATS[symbol][strategy] = 0
    WEEKLY_STATS[symbol][strategy] += 1

exchanges = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/')
def home():
    total_symbols = len(CONFIG['SYMBOLS'])
    okx_count = sum(1 for ex in CONFIG['SYMBOLS'].values() if ex.get('exchange') == 'okx')
    return f"""
    <h1>Trading Bot Multi-Strategy</h1>
    <p>Status: Running</p>
    <p>Total assets: {total_symbols} | OKX: {okx_count}</p>
    <p>Strategies: SAFE + MOMENTUM + CONTEXT</p>
    """

# ============================================================================ #
# REDIS
# ============================================================================ #

REDIS_CLIENT = None

def init_redis():
    global REDIS_CLIENT
    redis_url = os.environ.get('REDIS_URL')
    if not redis_url:
        logger.warning("⚠️ REDIS_URL non défini — état en mémoire uniquement")
        return
    try:
        REDIS_CLIENT = redis.from_url(redis_url, decode_responses=True)
        REDIS_CLIENT.ping()
        logger.info("✅ Redis connecté")
    except Exception as e:
        logger.error(f"❌ Redis erreur: {e}")
        REDIS_CLIENT = None


    # ========================================================================
    # LOGIQUE SCALP : ADX 4H DI aligné + ST AI 1H dans le sens → flip ST AI 15m
    # Pyramiding : flip ST AI 15m + guard — cooldown 1H
    # ========================================================================
def persist_runtime_state():
    if not REDIS_CLIENT:
        return
    with STATE_LOCK:
        payload = {
            'momentum_state':     MOMENTUM_STATE,
            'weekly_stats':       WEEKLY_STATS,
            'weekly_start':       WEEKLY_START.isoformat(),
            'last_signals':       LAST_SIGNALS,
            'last_signal_events': LAST_SIGNAL_EVENTS,
            'st_ai_15m':          dict(ST_AI_15M),
            'st_context_15m':     dict(ST_CONTEXT_15M),
            'adx_state':          dict(ADX_STATE),
            'scalp_positions':    dict(SCALP_POSITIONS),
            'st_context_1d':      dict(ST_CONTEXT_1D),
            'st_context_3d':      dict(ST_CONTEXT_3D),
            'st_context_lt_1h':   dict(ST_CONTEXT_LT_1H),
            'st_context_lt_4h':   dict(ST_CONTEXT_LT_4H),
            'st_context_lt_15m':  dict(ST_CONTEXT_LT_15M),
            'pyra_enabled':       dict(PYRA_ENABLED),
        }
        try:
            REDIS_CLIENT.set('bot_state', json.dumps(payload))
        except Exception as e:
            logger.error(f"❌ Redis save error: {e}")


def audit_log(data, status="reçu"):
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": data.get("symbol"),
        "tf": data.get("tf"),
        "type": data.get("type"),
        "status": status
    }
    if not REDIS_CLIENT:
        try:
            import os as _os
            _os.makedirs("logs", exist_ok=True)
            with open("logs/alerts.jsonl", "a", encoding="utf-8") as f:
                import json as _json
                f.write(_json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass
        return
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "sym": data.get('symbol'),
        "type": data.get('type'),
        "strategy": data.get('strategy'),
        "tf": data.get('tf'),
        "val": data.get('value'),
        "price": data.get('price'),
        "status": status
    }
    try:
        REDIS_CLIENT.lpush('audit_trail', json.dumps(entry))
        REDIS_CLIENT.ltrim('audit_trail', 0, 999)
    except Exception as e:
        logger.error(f"❌ Erreur audit Redis: {e}")


def load_runtime_state():
    global MOMENTUM_STATE, WEEKLY_STATS, WEEKLY_START, LAST_SIGNALS, LAST_SIGNAL_EVENTS
    if not REDIS_CLIENT:
        logger.info("ℹ️ Redis non disponible — démarrage à froid")
        return
    try:
        raw = REDIS_CLIENT.get('bot_state')
        if not raw:
            logger.info("ℹ️ Aucun état persistant trouvé dans Redis — démarrage à froid")
            return

        payload = json.loads(raw)
        MOMENTUM_STATE      = payload.get('momentum_state', {})
        WEEKLY_STATS        = payload.get('weekly_stats', {})
        LAST_SIGNALS        = payload.get('last_signals', {})
        LAST_SIGNAL_EVENTS  = payload.get('last_signal_events', {})
        ST_AI_15M.update(payload.get('st_ai_15m', {}))
        ST_CONTEXT_15M.update(payload.get('st_context_15m', {}))
        ADX_STATE.update(payload.get('adx_state', {}))
        SCALP_POSITIONS.update(payload.get('scalp_positions', {}))
        ST_CONTEXT_1D.update(payload.get('st_context_1d', {}))
        ST_CONTEXT_3D.update(payload.get('st_context_3d', {}))
        ST_CONTEXT_LT_1H.update(payload.get('st_context_lt_1h', {}))
        ST_CONTEXT_LT_4H.update(payload.get('st_context_lt_4h', {}))
        ST_CONTEXT_LT_15M.update(payload.get('st_context_lt_15m', {}))
        PYRA_ENABLED.update(payload.get('pyra_enabled', {}))
        # Nettoyer les assets hors watchlist chargés depuis Redis
        stale = [s for s in list(MOMENTUM_STATE.keys()) if s not in CONFIG['SYMBOLS']]
        for s in stale:
            del MOMENTUM_STATE[s]
        if stale:
            logger.info(f'[REDIS] Supprimé {len(stale)} assets obsolètes: {stale}')

        weekly_start_raw = payload.get('weekly_start')
        if weekly_start_raw:
            WEEKLY_START = datetime.fromisoformat(weekly_start_raw)

        logger.info(
            f"✅ État restauré depuis Redis | "
            f"momentum={len(MOMENTUM_STATE)}"
        )
    except Exception as e:
        logger.error(f"❌ Redis load error: {e}")

# ============================================================================ #
# INITIALISATION EXCHANGES
# ============================================================================ #

def init_exchanges():
    global exchanges
    exchanges['okx'] = 'okx'
    logger.info("✅ Exchange OKX configuré (webhook mode — pas d'API)")

# ============================================================================ #
# FONCTIONS TELEGRAM
# ============================================================================ #

def escape_html(text):
    """Échappe les caractères HTML dans le texte (hors balises intentionnelles)."""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def format_price(price: float) -> str:
    """Formate un prix en évitant les zéros pour les très petits assets."""
    if price == 0:
        return "N/A"
    if price < 0.0001:
        return f"{price:.8f}"
    if price < 0.01:
        return f"{price:.6f}"
    if price < 1:
        return f"{price:.4f}"
    return f"{price:.2f}"

def get_market_context_info() -> str:
    """Retourne la dernière zone ST Context connue pour BTC et ETH."""
    def ctx_str(symbol):
        ctx_1h = MOMENTUM_STATE.get(symbol, {}).get('st_context_1h')
        ctx_4h = MOMENTUM_STATE.get(symbol, {}).get('st_context_4h')
        parts = []
        if ctx_1h: parts.append(f"1H:{ctx_1h.upper()}")
        if ctx_4h: parts.append(f"4H:{ctx_4h.upper()}")
        return ', '.join(parts) if parts else 'N/A'
    btc = ctx_str('BTC/USDT')
    eth = ctx_str('ETH/USDT')
    return f"\n📊 BTC: {btc} | ETH: {eth}"

def send_bark(title: str, body: str, group: str = "TradingBot"):
    """Legacy — remplacé par send_ntfy."""
    send_ntfy(title, body)

def send_telegram_scalp(msg):
    """Envoie une alerte sur le bot Telegram dédié SCALP."""
    token = CONFIG.get('SCALP_BOT_TOKEN', '')
    if not token:
        send_telegram(msg)  # fallback sur le bot principal
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'parse_mode': 'HTML'
        }, timeout=10)
        if resp.status_code == 200:
            logger.info("✅ Message Scalp Bot envoyé")
        else:
            logger.warning(f"⚠️ Scalp Bot erreur: {resp.status_code}")
            send_telegram(msg)  # fallback
    except Exception as e:
        logger.error(f"Scalp Bot error: {e}")
        send_telegram(msg)  # fallback

def send_telegram_with_buttons(msg, callback_key, token=None, chat_id=None,
                               journal_symbol=None, journal_strategy=None,
                               journal_direction=None, journal_price=None):
    """Envoie un message Telegram avec boutons Pyramiding / Ignorer / Journal."""
    tok  = token   or CONFIG.get('TELEGRAM_BOT_TOKEN', '')
    chat = chat_id or CONFIG.get('TELEGRAM_CHAT_ID', '')
    if not tok or not chat:
        return
    try:
        # Ligne 1 : Pyramiding + Ignorer
        row1 = [
            {"text": "📈 Activer pyramiding", "callback_data": f"pyra_on:{callback_key}"},
            {"text": "❌ Ignorer",             "callback_data": f"pyra_off:{callback_key}"},
        ]
        # Ligne 2 : bouton Journal (si les infos sont disponibles)
        rows = [row1]
        if journal_symbol and journal_strategy and journal_direction and journal_price is not None:
            # Encode les données du trade dans le callback_data
            # Format : "journal_log:{symbol}|{strategy}|{direction}|{price}"
            sym_safe = str(journal_symbol).replace('|', '')
            jdata = f"journal_log:{sym_safe}|{journal_strategy}|{journal_direction}|{journal_price}"
            # Telegram limite callback_data à 64 octets — on tronque si nécessaire
            if len(jdata.encode()) <= 64:
                rows.append([{"text": "📓 Logger ce trade", "callback_data": jdata}])
            else:
                logger.warning(f"[JOURNAL] callback_data trop long ({len(jdata.encode())} octets), bouton ignoré")

        keyboard = {"inline_keyboard": rows}
        requests.post(
            f"https://api.telegram.org/bot{tok}/sendMessage",
            json={"chat_id": chat, "text": msg, "parse_mode": "HTML", "reply_markup": keyboard},
            timeout=10
        )
        logger.info("✅ Message Telegram avec boutons envoyé")
    except Exception as e:
        logger.error(f"Telegram buttons error: {e}")
        send_telegram(msg)  # fallback sans boutons


def send_telegram_ttmtf(msg):
    """Envoie une alerte sur le bot @TTMTF_bot (PULSE + CONFLUENCE + TREND)."""
    token = CONFIG.get('TELEGRAM_BOT_TOKEN', '')
    if not token:
        send_telegram(msg)  # fallback
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'parse_mode': 'HTML'
        }, timeout=10)
        if resp.status_code != 200:
            logger.warning(f'⚠️ TTMTF Bot erreur: {resp.status_code}')
            send_telegram(msg)  # fallback
        else:
            logger.info('✅ Message TTMTF Bot envoyé')
    except Exception as e:
        logger.error(f'TTMTF Bot error: {e}')
        send_telegram(msg)  # fallback

def send_ntfy(title: str, body: str):
    """Envoie une notification via ntfy.sh (fonctionne sans VPN en Chine)."""
    topic = CONFIG.get('NTFY_TOPIC', '')
    if not topic:
        return
    import re as _re
    clean_title = _re.sub(r'<[^>]+>', '', title).strip()
    clean_body  = _re.sub(r'<[^>]+>', '', body).strip()
    try:
        r = requests.post(
            f"https://ntfy.sh/{topic}",
            data=clean_body.encode('utf-8'),
            headers={'Title': clean_title.encode('utf-8'), 'Priority': 'high', 'Tags': 'chart_increasing'},
            timeout=10
        )
        if r.status_code == 200:
            logger.info("✅ ntfy envoyé")
        else:
            logger.warning(f"ntfy erreur: {r.status_code} {r.text}")
    except Exception as e:
        logger.error(f"ntfy error: {e}")

def send_telegram(msg):
    if not CONFIG['TELEGRAM_BOT_TOKEN'] or not CONFIG['TELEGRAM_CHAT_ID']:
        logger.warning("⚠️ Telegram non configuré (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID manquants)")
        return
    url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
    payload = {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'parse_mode': 'HTML'}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("✅ Message Telegram envoyé")
        elif resp.status_code == 429:
            retry_after = resp.json().get('parameters', {}).get('retry_after', 30)
            logger.warning(f"⚠️ Telegram rate limit — retry after {retry_after}s")
            time.sleep(retry_after)
            resp2 = requests.post(url, json=payload, timeout=10)
            if resp2.status_code == 200:
                logger.info("✅ Message Telegram envoyé (après retry)")
            else:
                logger.error(f"❌ Telegram retry échoué: {resp2.status_code} {resp2.text}")
        else:
            logger.error(f"❌ Telegram erreur HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"❌ Erreur Telegram: {e}")
    # Envoi parallèle via ntfy
    if CONFIG.get('NTFY_TOPIC'):
        try:
            import re as _re
            lines = [l.strip() for l in msg.split('\n') if l.strip()]
            title = _re.sub(r'<[^>]+>', '', lines[0]).strip() if lines else "TradingBot"
            threading.Thread(target=send_ntfy, args=(title, msg), daemon=True).start()
        except Exception as e:
            logger.error(f"ntfy dispatch: {e}")


def send_start_notification():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    redis_status = "✅ Redis connecté" if REDIS_CLIENT else "⚠️ Redis non disponible"
    msg = (
        "🤖 <b>[BOT STARTED]</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Total Assets: {len(CONFIG['SYMBOLS'])}\n"
        f"💾 {redis_status}\n\n"
        "📋 <b>STRATEGIES:</b>\n\n"
        "1️⃣ <b>CONFLUENCE</b>\n"
        "   • ST Context 3D + ST Context 4H aligné\n"
        "   • Anti-chop: ST Context 3D opposé + ADX 1D DI opposé\n"
        "   • Signal: Flip ST AI 4H / Pyramiding: flip ST AI 4H (guard)\n"
        "   • Clôture: ST Context 3D inversé\n\n"
        "2️⃣ <b>TREND</b>\n"
        "   • ST Context 1D + Bias 1D (EMA21/SMA55) + ST Context 4H aligné\n"
        "   • Anti-chop: ST Context 1H opposé\n"
        "   • Signal: Flip ST AI 4H / Pyramiding: flip ST AI 4H (guard)\n"
        "   • Clôture: Bias 1D inversé\n\n"
        "3️⃣ <b>CONTEXT 4H</b>\n"
        "   • Principale: ST Context 4H CT + 1H alignés + LT neutre\n"
        "   • Secondaire: ST Context 4H LT + 1H alignés + CT neutre\n"
        "   • Anti-chop DMI ADX 1H / Signal: Flip ST AI 1H / Cooldown 4H\n\n"
        "4️⃣ <b>PULSE</b>\n"
        "   • Bias 4H + Bias 15m + ADX 1H DMI + ST Context 15m (anti-chop)\n"
        "   • Signal: Flip ST AI 15m / Pyramiding: guard (30min) — 38 assets\n\n"
        "5️⃣ <b>SWING</b>\n"
        "   • ADX 4H DI aligné + flip ST AI 1H\n"
        "   • Pyramiding: guard (1H) — 38 assets\n\n"


        "━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {now}"
    )
    send_telegram(msg)


def send_weekly_report():
    global WEEKLY_STATS, WEEKLY_START

    now = datetime.now(timezone(timedelta(hours=8)))
    week_start = WEEKLY_START.astimezone(timezone(timedelta(hours=8)))
    total_alerts = sum(sum(strats.values()) for strats in WEEKLY_STATS.values())

    msg = (
        "📊 <b>[RAPPORT HEBDOMADAIRE]</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 Semaine du {week_start.strftime('%d/%m')} au {now.strftime('%d/%m/%Y')}\n"
        f"🔔 Total alertes: <b>{total_alerts}</b>\n\n"
    )
    total_confluence = sum(s.get('CONFLUENCE', 0)  for s in WEEKLY_STATS.values())
    total_context   = sum(s.get('CONTEXT', 0)     for s in WEEKLY_STATS.values())
    total_trend     = sum(s.get('TREND', 0)       for s in WEEKLY_STATS.values())
    total_momentum  = sum(s.get('MOMENTUM', 0)    for s in WEEKLY_STATS.values())
    total_swing     = sum(s.get('SWING', 0)       for s in WEEKLY_STATS.values())
    total_pulse     = sum(s.get('PULSE', 0)       for s in WEEKLY_STATS.values())
    total_scalp     = sum(s.get('SCALP', 0)       for s in WEEKLY_STATS.values())

    msg += (
        "📋 <b>Par stratégie:</b>\n"
        f"  • CONFLUENCE: {total_confluence}\n"
        f"  • CONTEXT: {total_context}\n"
        f"  • TREND: {total_trend}\n"
        f"  • SWING: {total_swing}\n"
        f"  • PULSE: {total_pulse}\n"
        f"  • SCALP: {total_scalp}\n"
        f"  • MOMENTUM: {total_momentum}\n\n"
    )

    assets_with_alerts = {
        symbol: stats for symbol, stats in WEEKLY_STATS.items()
        if sum(stats.values()) > 0
    }

    if assets_with_alerts:
        msg += "📈 <b>Par asset:</b>\n"
        for symbol, stats in sorted(assets_with_alerts.items(), key=lambda x: sum(x[1].values()), reverse=True):
            base = symbol.replace('/USDT', '')
            details = []
            if stats.get('SAFE', 0):       details.append(f"S:{stats['SAFE']}")
            if stats.get('MOMENTUM', 0):   details.append(f"M:{stats['MOMENTUM']}")
            if stats.get('CONTEXT', 0):    details.append(f"C:{stats['CONTEXT']}")
            if stats.get('CONTEXT_A', 0):  details.append(f"CA:{stats['CONTEXT_A']}")
            if stats.get('CONTEXT_B', 0):  details.append(f"CB:{stats['CONTEXT_B']}")
            if stats.get('CONTEXT_B+', 0): details.append(f"CB+:{stats['CONTEXT_B+']}")
            msg += f"  • {base}: {sum(stats.values())} ({', '.join(details)})\n"
    else:
        msg += "📈 <b>Par asset:</b> Aucune alerte cette semaine\n"

    msg += f"\n⏰ {now.strftime('%d/%m/%Y %H:%M')} (Taiwan)"
    send_telegram(msg)
    logger.info("📊 Rapport hebdomadaire envoyé")

    WEEKLY_STATS.clear()
    WEEKLY_START = datetime.now(timezone.utc)
    persist_runtime_state()



def send_prep_report():
    """Envoie un rapport groupé des assets en préparation — appelé toutes les heures."""
    global PREP_BUFFER
    with STATE_LOCK:
        entries = list(PREP_BUFFER)
        PREP_BUFFER.clear()
    if not entries:
        return
    now = datetime.now(timezone.utc).strftime('%H:%M UTC')
    msg = '⏳ <b>Assets en préparation</b> — ' + now + '\n' + '━' * 20

    # Group by strategy and direction
    groups = {}
    for e in entries:
        key = e['strat'] + '_' + e['dir']
        if key not in groups:
            groups[key] = []
        groups[key].append(e['sym'].replace('/USDT', '') + ' $' + str(round(e['price'], 4)))

    for key in sorted(groups.keys()):
        strat, direction = key.split('_', 1)
        emoji = "🟢" if direction == "LONG" else "🔴"
        msg += '\n\n<b>' + strat + ' ' + direction + '</b>\n'
        msg += '\n'.join([emoji + ' ' + x for x in groups[key]]) + '\n'
    send_telegram(msg)
    logger.info(f"[PREP REPORT] {len(entries)} assets envoyés")


def prep_report_scheduler():
    """Envoie le rapport de préparation à HH:05 chaque heure."""
    logger.info("⏰ Scheduler rapport préparation démarré (HH:05 UTC)")
    while True:
        now = datetime.now(timezone.utc)
        next_run = now.replace(minute=5, second=0, microsecond=0)
        if now.minute >= 5:
            next_run = (now + timedelta(hours=1)).replace(minute=5, second=0, microsecond=0)
        wait = (next_run - now).total_seconds()
        time.sleep(wait)
        send_prep_report()

def weekly_report_scheduler():
    logger.info("⏰ Scheduler rapport hebdomadaire démarré (dimanche minuit Taiwan)")
    while True:
        now = datetime.now(timezone(timedelta(hours=8)))
        if now.weekday() == 6 and now.hour == 0 and now.minute == 0:
            send_weekly_report()
            time.sleep(61)
        else:
            time.sleep(30)


def tv_alert_watchdog():
    """Vérifie toutes les heures que les webhooks TradingView arrivent bien."""
    bot_start_time = time.time()
    time.sleep(6 * 3600)
    logger.info("🔍 TV Alert Watchdog démarré")
    MAX_AGE = {'15m': 3600, '1h': 3*3600, '4h': 6*3600, '1d': 30*3600}
    while True:
        time.sleep(3600)
        now = time.time()
        uptime = now - bot_start_time
        missing = []
        for tf, max_age in MAX_AGE.items():
            # Ne pas alerter si le bot n'a pas encore tourné assez longtemps
            # pour avoir eu une chance de recevoir ce TF
            if uptime < max_age + 3600:
                continue
            last_ts = LAST_WEBHOOK_TS.get(tf)
            if last_ts is None:
                missing.append(f"  • TF {tf.upper()}: jamais reçu")
            elif (now - last_ts) > max_age:
                age_h = (now - last_ts) / 3600
                missing.append(f"  • TF {tf.upper()}: dernier reçu il y a {age_h:.1f}H")
        if missing:
            details = "\n".join(missing)
            send_telegram(
                "🚨 <b>[ALERTE] Webhooks TradingView manquants</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"{details}\n\n"
                "➡️ Vérifier et redémarrer les alertes sur TradingView"
            )
            logger.warning(f"[TV WATCHDOG] Alertes manquantes: {missing}")

def heartbeat_scheduler():
    interval = max(300, int(CONFIG['HEARTBEAT_INTERVAL_SECONDS']))
    logger.info(f"💓 Heartbeat scheduler démarré (interval={interval}s)")
    while True:
        time.sleep(interval)
        redis_status = "✅" if REDIS_CLIENT else "⚠️ non dispo"
        msg = (
            "💓 <b>[BOT HEARTBEAT]</b>\n"
            f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n"
            f"📊 Assets: {len(CONFIG['SYMBOLS'])}\n"
            f"🧠 State momentum: {len(MOMENTUM_STATE)}\n"
            f"💾 Redis: {redis_status}"
        )
        send_telegram(msg)

# ============================================================================ #
# UTILITAIRES
# ============================================================================ #

def format_tv_symbol(s):
    if ':' in s:
        s = s.split(':')[-1]
    if s.endswith('.P'):
        s = s[:-2]
    for q in ['USDT', 'USDC', 'BUSD']:
        if s.endswith(q) and '/' not in s:
            return f"{s.replace(q, '')}/{q}"
    return s

def get_exchange_for_symbol(symbol):
    sym_cfg = CONFIG['SYMBOLS'].get(symbol)
    if not sym_cfg:
        return None
    return exchanges.get(sym_cfg.get('exchange'))

def parse_st_context_value(val, trend_level=1.96):
    """
    Convertit la valeur brute du ST Context (plot_1 = Short time context) en 'buy', 'sell' ou None.
    Accepte les strings 'buy'/'sell' (rétrocompatibilité) et les valeurs numériques
    envoyées par TradingView via {{plot_1}}.
      plot_1 > +trend_level  → zone baissière → 'sell'
      plot_1 < -trend_level  → zone haussière → 'buy'
      entre les deux         → neutre         → None
    """
    if str(val).lower() in ['buy', 'sell', 'neutral']:
        return None if str(val).lower() == 'neutral' else str(val).lower()
    try:
        ct_value = float(val)
        if ct_value > trend_level:    return 'sell'
        elif ct_value < -trend_level: return 'buy'
        else:                         return None
    except (ValueError, TypeError):
        logger.warning(f"[WARN] ST Context valeur invalide: '{val}'")
        return None

def is_signal_fresh(last_ts, max_age_seconds):
    """Retourne True si un signal horodaté est encore frais."""
    try:
        if last_ts is None:
            return False
        return (datetime.now(timezone.utc).timestamp() - float(last_ts)) <= max_age_seconds
    except (TypeError, ValueError):
        return False

def parse_supertrend_value(val):
    """Convertit la valeur brute du SuperTrend AI en 'buy' ou 'sell'.
    Accepte 'buy'/'sell' (ancien format) et '1'/'0' (nouveau format via {{plot_2}}).
    """
    s = str(val).strip().lower()
    if s == 'buy'  or s == '1': return 'buy'
    if s == 'sell' or s == '0': return 'sell'
    try:
        return 'buy' if float(s) >= 0.5 else 'sell'
    except (ValueError, TypeError):
        logger.warning(f"[WARN] SuperTrend valeur invalide: '{val}'")
        return None

def parse_ema200_value(val):
    normalized = str(val).strip().lower()
    if normalized in {'', 'none', 'null', 'na', 'n/a', 'nan'}:
        return None
    try:
        return float(normalized)
    except (ValueError, TypeError):
        return None

def normalize_tf(tf_raw):
    tf = str(tf_raw or '').strip().lower()
    tf_aliases = {
        '15': '15m', '60': '1h', '1hr': '1h', '1hour': '1h',
        '240': '4h', '4hr': '4h', '4hour': '4h',
        'd': '1d', '1day': '1d',
        '2day': '2d', '3day': '3d',
    }
    return tf_aliases.get(tf, tf)

def normalize_alert_type(alert_type_raw):
    normalized = str(alert_type_raw or '').strip().lower().replace(' ', '').replace('-', '_')
    type_aliases = {
        'ema_200': 'ema200', 'ema': 'ema200',
        'super_trend': 'supertrend', 'st': 'supertrend',
        'stcontext': 'st_context',
    }
    return type_aliases.get(normalized, normalized)

def get_ema200_raw_value(data, val_raw):
    if val_raw not in (None, ''):
        return val_raw
    for key in ('ema200', 'ema_200', 'ema'):
        if key in data and data.get(key) not in (None, ''):
            return data.get(key)
    return val_raw

def parse_bias_value(val, val2=None):
    normalized = str(val).strip().lower()
    if normalized in {'bull', 'bear'}:
        return normalized
    if val2 is None:
        return None
    try:
        ema_value = float(val)
        sma_value = float(val2)
    except (ValueError, TypeError):
        return None
    return 'bull' if ema_value > sma_value else 'bear'

def build_event_id(data, symbol, strat, tf, alert_type, val):
    candle_ts = data.get('candle_ts') or data.get('bar_time') or data.get('time') or data.get('timestamp')
    if candle_ts is None:
        return None
    return f"{symbol}|{strat}|{tf}|{alert_type}|{val}|{candle_ts}"

def should_send(symbol, key, event_id=None, cooldown=None):
    now = time.time()
    k = f"{symbol}:{key}"
    effective_cooldown = cooldown if cooldown is not None else CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']
    with STATE_LOCK:
        if event_id:
            previous_event = LAST_SIGNAL_EVENTS.get(k)
            if previous_event == event_id:
                return False
            LAST_SIGNAL_EVENTS[k] = event_id
        if k not in LAST_SIGNALS or (now - LAST_SIGNALS[k] > effective_cooldown):
            LAST_SIGNALS[k] = now
            return True
    return False

# États SCALP — ST AI 15min + contexte 15min
ST_AI_15M: dict = {}       # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_15M: dict = {}  # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_1D:  dict = {}  # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_3D:  dict = {}  # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_LT_1H:  dict = {}  # Long term context 1H
ST_CONTEXT_LT_4H:  dict = {}  # Long term context 4H (plot_2)
ADX_STATE: dict = {}  # symbol -> {adx, di_plus, di_minus, adx_rising}
PREP_STATE: dict = {}
PYRA_ENABLED: dict = {}  # f'{symbol}_{strat}' -> True si pyramiding activé  # strategy -> {'LONG': set(), 'SHORT': set()} — assets en préparation
ST_CONTEXT_LT_15M: dict = {}  # Long term context 15m

# Timestamps derniers webhooks TradingView par tf (pour heartbeat)
LAST_WEBHOOK_TS: dict = {}  # tf -> timestamp

# Positions SCALP
SCALP_POSITIONS: dict = {}      # pos_key -> position dict

def init_symbol_states(symbol):
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'bias_1d': None, 'bias_2d': None, 'bias_3d': None,
            'st_context_1h': None, 'st_context_4h': None,
            'st_context_1h_ts': None, 'st_context_4h_ts': None, 'st_context_15m_ts': None, 'st_context_1d_ts': None, 'st_context_3d_ts': None, 'st_context_lt_1h_ts': None, 'st_context_lt_15m_ts': None, 'st_context_lt_4h_ts': None, 'st_context_5m_ts': None,
            'st_ai_5m': None, 'last_st_5m': None, 'st_context_5m': None, 'bias_5m': None,
            'st_1h': None, 'st_4h': None,
            'last_st_4h': None,   # dernier flip 4H (guard pyramiding)
            'last_st_15m': None,  # dernier flip 15min (guard pyramiding)
            # Nouveaux états pour CONTEXT v2 et SCALP
            'bias_1h': None, 'bias_4h': None, 'bias_15m': None, 'st_ai_15m': None,
        }


# ============================================================================ #
# WEBHOOK HANDLER
# ============================================================================ #

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        logger.warning("⚠️ Webhook sans données")
        return jsonify({'status': 'no_data'}), 400

    symbol      = format_tv_symbol(data.get('symbol', ''))
    strat       = data.get('strategy', '').lower()
    tf          = normalize_tf(data.get('tf', ''))
    alert_type  = normalize_alert_type(data.get('type', ''))
    val_raw     = data.get('value', '')
    val2_raw    = data.get('value2')
    val         = str(val_raw).strip().lower()
    try:
        price = float(data.get('price', 0) or 0)
    except (TypeError, ValueError):
        price = 0.0

    if alert_type in {'bias', 'bias_9_26'}:
        bias_value = parse_bias_value(val_raw, val2_raw)
        if bias_value is not None:
            val = bias_value
        else:
            logger.warning(f"[WARN] BIAS valeur invalide pour {symbol}: value='{val_raw}' value2='{val2_raw}'")

    logger.info(f"📥 Webhook: {symbol} | strat={strat} | tf={tf} | type={alert_type} | val={val} | price={price}")
    # Tracker le dernier webhook reçu par tf
    LAST_WEBHOOK_TS[tf] = time.time()
    audit_log(data, status="reçu")
    event_id = build_event_id(data, symbol, strat, tf, alert_type, val)

    if symbol not in CONFIG['SYMBOLS']:
        logger.info(f"⏭️ {symbol} non dans la watchlist")
        audit_log(data, status="ignoré_watchlist")
        return jsonify({'status': 'ignored', 'reason': 'not_in_watchlist'}), 200

    exchange_name = CONFIG['SYMBOLS'][symbol].get('exchange', 'okx')
    init_symbol_states(symbol)

    # Mise à jour globale des contextes (indépendante de la stratégie du webhook)
    m = MOMENTUM_STATE[symbol]
    now_ts = datetime.now(timezone.utc).timestamp()
    if alert_type == 'bias':
        bias_val = val.lower() if isinstance(val, str) else None
        if bias_val in ('bull', 'bear', 'neutral'):
            if tf == '4h':
                m['bias_4h'] = bias_val if bias_val != 'neutral' else None
                logger.info(f"[BIAS TV] {symbol} bias_4h = {bias_val}")
            elif tf == '1d':
                m['bias_1d'] = bias_val if bias_val != 'neutral' else None
                logger.info(f"[BIAS TV] {symbol} bias_1d = {bias_val}")
            elif tf == '1h':
                m['bias_1h'] = bias_val if bias_val != 'neutral' else None
                logger.info(f"[BIAS TV] {symbol} bias_1h = {bias_val}")
            elif tf == '15m':
                m['bias_15m'] = bias_val if bias_val != 'neutral' else None
                logger.info(f"[BIAS TV] {symbol} bias_15m = {bias_val}")

    if alert_type == 'st_context':
        parsed_ctx = parse_st_context_value(val)
        if tf == '1h':
            m['st_context_1h'] = parsed_ctx
            m['st_context_1h_ts'] = now_ts
        elif tf == '4h':
            m['st_context_4h'] = parsed_ctx
            m['st_context_4h_ts'] = now_ts
        elif tf == '15m':
            ST_CONTEXT_15M[symbol] = parsed_ctx
            m['st_context_15m_ts'] = now_ts
        elif tf == '1d':
            ST_CONTEXT_1D[symbol] = parsed_ctx
            m['st_context_1d_ts'] = now_ts
        elif tf == '3d':
            ST_CONTEXT_3D[symbol] = parsed_ctx
            m['st_context_3d_ts'] = now_ts

    if alert_type == 'st_context_lt':
        parsed_ctx_lt = parse_st_context_value(val)
        if tf == '1h':
            ST_CONTEXT_LT_1H[symbol] = parsed_ctx_lt
            m['st_context_lt_1h_ts'] = now_ts
        elif tf == '15m':
            ST_CONTEXT_LT_15M[symbol] = parsed_ctx_lt
            m['st_context_lt_15m_ts'] = now_ts
        elif tf in ('4h', 'lt_4h'):
            ST_CONTEXT_LT_4H[symbol] = parsed_ctx_lt
            m['st_context_lt_4h_ts'] = now_ts

    ema200_value = None
    if alert_type == 'ema200' and tf == '1h':
        ema200_raw = get_ema200_raw_value(data, val_raw)
        ema200_value = parse_ema200_value(ema200_raw)
        if ema200_value is None:
            normalized_ema_raw = str(ema200_raw).strip().lower()
            if normalized_ema_raw in {'', 'none', 'null', 'na', 'n/a', 'nan'}:
                if should_send(symbol, "ema200_missing"):
                    logger.info(f"[INFO] EMA200 absente pour {symbol}: '{normalized_ema_raw}'")
            else:
                logger.warning(f"[WARN] EMA200 valeur invalide pour {symbol}: '{ema200_raw}'")

    # ========================================================================
    # ========================================================================
    # MISE À JOUR DES ÉTATS (ST AI, relai Tapbit, guards)
    # ========================================================================
    if strat in ['momentum', 'context', 'trend', 'scalp', 'swing', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'supertrend' and tf == '1h':
            prev_1h = m.get('st_1h')
            m['st_1h'] = parse_supertrend_value(val)
            m['st_1h_flipped'] = bool(prev_1h is not None and m['st_1h'] is not None and m['st_1h'] != prev_1h)
        if alert_type == 'supertrend' and tf == '4h':
            prev_4h = m.get('st_4h')
            m['st_4h'] = parse_supertrend_value(val)
            m['st_4h_flipped'] = bool(prev_4h is not None and m['st_4h'] is not None and m['st_4h'] != prev_4h)
            if m['st_4h_flipped']:
                m['last_st_4h'] = prev_4h
            # Relai vers bot Tapbit
            tapbit_url = CONFIG.get('TAPBIT_BOT_URL', '')
            if tapbit_url and symbol in CONFIG['SYMBOLS']:
                def _relay_4h(sym=symbol, v=val_raw, p=price):
                    try:
                        requests.post(f"{tapbit_url}/webhook", json={
                            'symbol': sym, 'type': 'supertrend', 'tf': '4h',
                            'value': v, 'price': p, 'strategy': 'trend'
                        }, timeout=5)
                    except Exception as e:
                        logger.debug(f"[TAPBIT] Relai 4H échoué {sym}: {e}")
                threading.Thread(target=_relay_4h, daemon=True).start()
        if alert_type == 'supertrend' and tf == '15m':
            prev_15m = m.get('st_ai_15m')
            st_15m_val = parse_supertrend_value(val)
            m['st_ai_15m'] = st_15m_val
            if prev_15m and st_15m_val and st_15m_val != prev_15m:
                m['last_st_15m'] = prev_15m  # garde la valeur précédente pour le guard
            ST_AI_15M[symbol] = st_15m_val

    # ========================================================================
    # ========================================================================
    # LOGIQUE CONFLUENCE : ST Context 3D + ST Context 4H aligné → flip ST AI 4H
    # Anti-chop : ST Context 3D opposé OU ADX 1D DI opposé dominant → annulé
    # ========================================================================
    if strat in ['confluence', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'supertrend' and tf == '4h':
            st_4h_val  = parse_supertrend_value(val)
            prev_4h    = m.get('st_4h')
            flipped_4h = (st_4h_val is not None and prev_4h is not None and st_4h_val != prev_4h)
            m['st_4h'] = st_4h_val
            if flipped_4h and prev_4h:
                m['last_st_4h'] = prev_4h

            if flipped_4h:
                ctx_3d     = ST_CONTEXT_3D.get(symbol)
                ctx_4h     = m.get('st_context_4h')
                adx_1d     = ADX_STATE.get(f'{symbol}_1d', {})
                di_plus_1d = adx_1d.get('di_plus', 0)
                di_minus_1d= adx_1d.get('di_minus', 0)

                direction_c = "LONG" if st_4h_val == 'buy' else "SHORT"
                opp_ctx     = 'sell' if direction_c == 'LONG' else 'buy'

                ctx_3d_ok   = ctx_3d == st_4h_val
                ctx_4h_ok   = ctx_4h == st_4h_val
                # Anti-chop ADX 1D : ok si neutre ou aligné, annulé si DI opposé dominant
                adx_1d_ok   = not ((di_minus_1d > di_plus_1d and direction_c == 'LONG') or
                                   (di_plus_1d > di_minus_1d and direction_c == 'SHORT'))

                close_msg_c = (
                    "\n\n📋 <b>Clôture :</b> ST Context 3D inversé"
                )

                pos_key_c = f"{symbol}_CONFLUENCE"
                with STATE_LOCK:
                    pos_c = SCALP_POSITIONS.get(pos_key_c)
                    if pos_c and pos_c['direction'] != direction_c:
                        pos_c = None; is_entry_c = False; is_pyra_c = False
                    else:
                        opp_4h_c = 'sell' if st_4h_val == 'buy' else 'buy'
                        guard_ok  = m.get('last_st_4h') == opp_4h_c
                        is_entry_c = (ctx_3d_ok and ctx_4h_ok and adx_1d_ok and pos_c is None)
                        is_pyra_c  = bool(pos_c and pos_c['direction'] == direction_c
                                          and ctx_3d_ok and ctx_4h_ok and adx_1d_ok and guard_ok)
                    if is_entry_c and should_send(symbol, f"conf_entry_{st_4h_val}", event_id=event_id, cooldown=14400):
                        SCALP_POSITIONS[pos_key_c] = {'direction': direction_c, 'entry_count': 1}
                        pos_c = SCALP_POSITIONS[pos_key_c]
                    else:
                        is_entry_c = False

                if is_entry_c and pos_c:
                    emoji = "🟢" if direction_c == "LONG" else "🔴"
                    ctx_3d_txt = ctx_3d.upper() if ctx_3d else "NEUTRE"
                    ctx_4h_txt = ctx_4h.upper() if ctx_4h else "NEUTRE"
                    send_telegram_with_buttons(
                        f"{emoji} <b>[CONFLUENCE - ENTREE 4H]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_c}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"✅ ST Context 3D: {ctx_3d_txt} (filtre directionnel)\n"
                        f"✅ ST Context 4H: {ctx_4h_txt} (contexte)\n"
                        f"✅ ADX 1D: +DI={di_plus_1d:.1f} | -DI={di_minus_1d:.1f} (anti-chop)\n"
                        f"✅ SuperTrend AI 4H: {st_4h_val.upper()} (SIGNAL)"
                        f"{close_msg_c}"
                        f"{get_market_context_info()}",
                        f"{symbol}_CONFLUENCE",
                        journal_symbol=symbol, journal_strategy='CONFLUENCE',
                        journal_direction=direction_c, journal_price=price
                    )
                    track_alert(symbol, 'CONFLUENCE')
                    logger.info(f"[CONFLUENCE] Entrée: {symbol} {direction_c}")

                elif is_pyra_c and PYRA_ENABLED.get(f"{symbol}_CONFLUENCE", False) and should_send(symbol, f"conf_pyra_{st_4h_val}", event_id=event_id, cooldown=14400):
                    with STATE_LOCK:
                        pos_c['entry_count'] += 1
                        entry_count_c = pos_c['entry_count']
                    emoji = "🟢" if direction_c == "LONG" else "🔴"
                    ctx_3d_txt = ctx_3d.upper() if ctx_3d else "NEUTRE"
                    ctx_4h_txt = ctx_4h.upper() if ctx_4h else "NEUTRE"
                    send_telegram_ttmtf(
                        f"{emoji} <b>[CONFLUENCE - PYRAMIDING #{entry_count_c}]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_c}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"✅ ST Context 3D: {ctx_3d_txt}\n"
                        f"✅ ST Context 4H: {ctx_4h_txt}\n"
                        f"✅ SuperTrend AI 4H: {st_4h_val.upper()} (PYRAMIDING)\n"
                        f"🛡️ Guard: flip opposé validé"
                        f"{close_msg_c}"
                        f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'CONFLUENCE')
                    logger.info(f"[CONFLUENCE] Pyramiding #{entry_count_c}: {symbol} {direction_c}")

    # ========================================================================
    # LOGIQUE TREND : ST Context 1D + Bias 1D → ST Context 4H aligné + flip ST AI 4H
    # Anti-chop : ST Context 1H opposé → annulé
    # ========================================================================
    if strat in ['trend', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'supertrend' and tf == '4h':
            st_4h_val  = parse_supertrend_value(val)
            prev_4h    = m.get('st_4h')
            flipped_4h = (st_4h_val is not None and prev_4h is not None and st_4h_val != prev_4h)
            m['st_4h'] = st_4h_val
            if flipped_4h and prev_4h:
                m['last_st_4h'] = prev_4h

            if flipped_4h:
                # Recalculer Bias 1D en temps réel
                try:
                    df_1d_rt  = fetch_ohlcv_okx(symbol, '1d', limit=100)
                    bias_1d_v = calc_bias_okx(df_1d_rt, ema_len=21, sma_len=55) if df_1d_rt is not None else m.get('bias_1d')
                    if df_1d_rt is not None: m['bias_1d'] = bias_1d_v  # ne pas écraser un bias TV si OKX indisponible
                except Exception:
                    bias_1d_v = m.get('bias_1d')

                ctx_1d_t   = ST_CONTEXT_1D.get(symbol)
                ctx_4h_t   = m.get('st_context_4h')
                ctx_1h_t   = m.get('st_context_1h')

                direction_t = "LONG" if st_4h_val == 'buy' else "SHORT"
                exp_bias    = 'bull' if direction_t == 'LONG' else 'bear'
                opp_ctx     = 'sell' if direction_t == 'LONG' else 'buy'

                ctx_1d_ok   = ctx_1d_t == st_4h_val
                bias_1d_ok  = bias_1d_v == exp_bias
                ctx_4h_ok   = ctx_4h_t == st_4h_val
                no_chop_1h  = ctx_1h_t != opp_ctx

                close_msg_t = "\n\n📋 <b>Clôture :</b> Bias 1D inversé"

                pos_key_t = f"{symbol}_TREND"
                with STATE_LOCK:
                    pos_t = SCALP_POSITIONS.get(pos_key_t)
                    if pos_t and pos_t['direction'] != direction_t:
                        pos_t = None; is_entry_t = False; is_pyra_t = False
                    else:
                        opp_4h_t = 'sell' if st_4h_val == 'buy' else 'buy'
                        guard_ok  = m.get('last_st_4h') == opp_4h_t
                        is_entry_t = (ctx_1d_ok and bias_1d_ok and ctx_4h_ok and no_chop_1h and pos_t is None)
                        is_pyra_t  = bool(pos_t and pos_t['direction'] == direction_t
                                          and ctx_1d_ok and bias_1d_ok and ctx_4h_ok and no_chop_1h and guard_ok)
                    if is_entry_t and should_send(symbol, f"trend_entry_{st_4h_val}", event_id=event_id, cooldown=14400):
                        SCALP_POSITIONS[pos_key_t] = {'direction': direction_t, 'entry_count': 1}
                        pos_t = SCALP_POSITIONS[pos_key_t]
                    else:
                        is_entry_t = False

                if is_entry_t and pos_t:
                    emoji = "🟢" if direction_t == "LONG" else "🔴"
                    ctx_1d_txt = ctx_1d_t.upper() if ctx_1d_t else "NEUTRE"
                    ctx_4h_txt = ctx_4h_t.upper() if ctx_4h_t else "NEUTRE"
                    ctx_1h_txt = ctx_1h_t.upper() if ctx_1h_t else "NEUTRE"
                    send_telegram_with_buttons(
                        f"{emoji} <b>[TREND - ENTREE 4H]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_t}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"✅ ST Context 1D: {ctx_1d_txt} (filtre)\n"
                        f"✅ Bias 1D: {(bias_1d_v or '?').upper()} (EMA21/SMA55)\n"
                        f"✅ ST Context 4H: {ctx_4h_txt} (signal)\n"
                        f"✅ ST Context 1H: {ctx_1h_txt} (anti-chop)\n"
                        f"✅ SuperTrend AI 4H: {st_4h_val.upper()} (SIGNAL)"
                        f"{close_msg_t}"
                        f"{get_market_context_info()}",
                        f"{symbol}_TREND",
                        journal_symbol=symbol, journal_strategy='TREND',
                        journal_direction=direction_t, journal_price=price
                    )
                    track_alert(symbol, 'TREND')
                    logger.info(f"[TREND] Entrée: {symbol} {direction_t}")

                elif is_pyra_t and PYRA_ENABLED.get(f"{symbol}_TREND", False) and should_send(symbol, f"trend_pyra_{st_4h_val}", event_id=event_id, cooldown=14400):
                    with STATE_LOCK:
                        pos_t['entry_count'] += 1
                        entry_count_t = pos_t['entry_count']
                    emoji = "🟢" if direction_t == "LONG" else "🔴"
                    ctx_4h_txt = ctx_4h_t.upper() if ctx_4h_t else "NEUTRE"
                    ctx_1h_txt = ctx_1h_t.upper() if ctx_1h_t else "NEUTRE"
                    send_telegram_ttmtf(
                        f"{emoji} <b>[TREND - PYRAMIDING #{entry_count_t}]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_t}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"✅ Bias 1D: {(bias_1d_v or '?').upper()} (EMA21/SMA55)\n"
                        f"✅ ST Context 4H: {ctx_4h_txt}\n"
                        f"✅ ST Context 1H: {ctx_1h_txt} (anti-chop)\n"
                        f"✅ SuperTrend AI 4H: {st_4h_val.upper()} (PYRAMIDING)\n"
                        f"🛡️ Guard: flip opposé validé"
                        f"{close_msg_t}"
                        f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'TREND')
                    logger.info(f"[TREND] Pyramiding #{entry_count_t}: {symbol} {direction_t}")

    # ========================================================================
    # ========================================================================
    # ========================================================================
    # ========================================================================
    # ========================================================================
    # LOGIQUE PULSE v4 : Bias 4H + Bias 15m + ADX 1H DMI + ST Context 15m → flip ST AI 15m
    # Anti-chop : ST Context 15m opposé OU DMI écart > 8 → bloqué
    # Pyramiding : flip ST AI 15m + guard — cooldown 30min
    # ========================================================================
    if strat in ['pulse', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'supertrend' and tf == '15m':
            st_15m_val  = m.get('st_ai_15m')
            prev_15m    = m.get('last_st_15m')
            flipped_15m = (st_15m_val is not None and prev_15m is not None and st_15m_val != prev_15m)

            if flipped_15m:
                # Recalculer Bias 4H et 15m en temps réel
                try:
                    df_4h_rt  = fetch_ohlcv_okx(symbol, '4h',  limit=100)
                    df_15m_rt = fetch_ohlcv_okx(symbol, '15m', limit=50)
                    bias_4h_v  = calc_bias_okx(df_4h_rt,  ema_len=21, sma_len=55) if df_4h_rt  is not None else m.get('bias_4h')
                    bias_15m_v = calc_bias_okx(df_15m_rt, ema_len=8,  sma_len=20) if df_15m_rt is not None else m.get('bias_15m')
                    if df_4h_rt  is not None: m['bias_4h']  = bias_4h_v
                    if df_15m_rt is not None: m['bias_15m'] = bias_15m_v
                except Exception:
                    bias_4h_v  = m.get('bias_4h')
                    bias_15m_v = m.get('bias_15m')

                ctx_15m_p   = ST_CONTEXT_15M.get(symbol)
                adx_1h_p    = ADX_STATE.get(f'{symbol}_1h', {})
                di_plus_1h  = adx_1h_p.get('di_plus', 0)
                di_minus_1h = adx_1h_p.get('di_minus', 0)
                adx_val_1h  = adx_1h_p.get('adx', 0)
                adx_rising  = adx_1h_p.get('adx_rising', False)

                direction_p = "LONG" if st_15m_val == 'buy' else "SHORT"
                exp_bias    = 'bull' if direction_p == 'LONG' else 'bear'
                opp_ctx     = 'sell' if direction_p == 'LONG' else 'buy'

                bias_4h_ok  = bias_4h_v == exp_bias
                bias_15m_ok = bias_15m_v == exp_bias
                ctx_15m_ok  = ctx_15m_p == st_15m_val  # aligné obligatoire

                if direction_p == 'LONG':
                    dmi_gap = di_minus_1h - di_plus_1h
                else:
                    dmi_gap = di_plus_1h - di_minus_1h

                dmi_blocked = dmi_gap > 8
                dmi_weak    = 4 < dmi_gap <= 8

                if dmi_blocked:
                    adx_status = f"\U0001f6ab ADX 1H opposé fort (écart={dmi_gap:.1f}) → bloqué"
                elif dmi_weak:
                    adx_status = f"\u26a0\ufe0f Opposition faible DI (écart \u2248 {dmi_gap:.1f}) → possible retournement"
                elif adx_val_1h >= 20 and adx_rising and (
                    (direction_p == 'LONG' and di_plus_1h >= di_minus_1h) or
                    (direction_p == 'SHORT' and di_minus_1h >= di_plus_1h)):
                    adx_status = f"\u2705 ADX 1H même sens ({adx_val_1h:.1f} \u2191 | +DI={di_plus_1h:.1f} | -DI={di_minus_1h:.1f})"
                else:
                    adx_status = f"\u27a1\ufe0f ADX 1H neutre ({adx_val_1h:.1f} | +DI={di_plus_1h:.1f} | -DI={di_minus_1h:.1f})"

                all_ok = bias_4h_ok and bias_15m_ok and ctx_15m_ok and not dmi_blocked

                pos_key_p = f"{symbol}_PULSE"
                with STATE_LOCK:
                    pos_p = SCALP_POSITIONS.get(pos_key_p)
                    if pos_p and pos_p['direction'] != direction_p:
                        pos_p = None; is_entry_p = False; is_pyra_p = False
                    else:
                        is_entry_p = (all_ok and pos_p is None)
                        opp_15m_p  = 'sell' if st_15m_val == 'buy' else 'buy'
                        guard_ok_p = m.get('last_st_15m') == opp_15m_p
                        is_pyra_p  = bool(pos_p and pos_p['direction'] == direction_p
                                          and bias_4h_ok and bias_15m_ok and ctx_15m_ok
                                          and not dmi_blocked and guard_ok_p)
                    if is_entry_p and should_send(symbol, f"pulse_entry_{st_15m_val}", event_id=event_id, cooldown=3600):
                        SCALP_POSITIONS[pos_key_p] = {'direction': direction_p, 'entry_count': 1}
                        pos_p = SCALP_POSITIONS[pos_key_p]
                    else:
                        is_entry_p = False

                if is_entry_p and pos_p:
                    emoji = "\U0001f7e2" if direction_p == "LONG" else "\U0001f534"
                    ctx_txt = ctx_15m_p.upper() if ctx_15m_p else "NEUTRE"
                    send_telegram_with_buttons(
                        f"{emoji} <b>[PULSE - ENTREE]</b> {symbol}\n"
                        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                        f"\U0001f4c8 Direction: {direction_p}\n"
                        f"\U0001f4b0 Price: ${format_price(price)}\n"
                        f"\U0001f3e6 Exchange: {exchange_name.upper()}\n"
                        f"\u23f0 {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"\u2705 Bias 4H: {(bias_4h_v or '?').upper()} (EMA21/SMA55)\n"
                        f"\u2705 Bias 15m: {(bias_15m_v or '?').upper()} (EMA8/SMA20)\n"
                        f"\u2705 ST Context 15m: {ctx_txt} (anti-chop)\n"
                        f"{adx_status}\n"
                        f"\u2705 SuperTrend AI 15m: {st_15m_val.upper()} (SIGNAL)"
                        f"{get_market_context_info()}",
                        f"{symbol}_PULSE",
                        journal_symbol=symbol, journal_strategy='PULSE',
                        journal_direction=direction_p, journal_price=price
                    )
                    track_alert(symbol, 'PULSE')
                    logger.info(f"[PULSE] Entrée: {symbol} {direction_p}")

                elif is_pyra_p and PYRA_ENABLED.get(f"{symbol}_PULSE", False) and should_send(symbol, f"pulse_pyra_{st_15m_val}", event_id=event_id, cooldown=1800):
                    with STATE_LOCK:
                        pos_p['entry_count'] += 1
                        m['last_st_15m'] = None
                        entry_count_p = pos_p['entry_count']
                    emoji = "\U0001f7e2" if direction_p == "LONG" else "\U0001f534"
                    ctx_txt = ctx_15m_p.upper() if ctx_15m_p else "NEUTRE"
                    send_telegram_ttmtf(
                        f"{emoji} <b>[PULSE - PYRAMIDING #{entry_count_p}]</b> {symbol}\n"
                        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                        f"\U0001f4c8 Direction: {direction_p}\n"
                        f"\U0001f4b0 Price: ${format_price(price)}\n"
                        f"\U0001f3e6 Exchange: {exchange_name.upper()}\n"
                        f"\u23f0 {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"\u2705 Bias 4H: {(bias_4h_v or '?').upper()} (EMA21/SMA55)\n"
                        f"\u2705 Bias 15m: {(bias_15m_v or '?').upper()} (EMA8/SMA20)\n"
                        f"\u2705 ST Context 15m: {ctx_txt}\n"
                        f"{adx_status}\n"
                        f"\u2705 SuperTrend AI 15m: {st_15m_val.upper()} (PYRAMIDING)\n"
                        f"\U0001f6e1\ufe0f Guard: flip opposé validé"
                        f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'PULSE')
                    logger.info(f"[PULSE] Pyramiding #{entry_count_p}: {symbol} {direction_p}")

    # ========================================================================
    

    # ========================================================================
    # LOGIQUE CONTEXT 4H :
    # Principale : ST Context 4H CT aligné + 1H aligné + 4H LT neutre + DMI
    # Secondaire : ST Context 4H LT aligné + 1H aligné + 4H CT neutre + DMI
    # Triple     : ST Context 4H CT + 1H + 15m alignés (pas de DMI)
    # Signal : flip ST AI 1H / Pyramiding : flip ST AI 1H + guard / Cooldown 4H
    # ========================================================================
    if strat in ['context4h', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'supertrend' and tf == '1h':
            st_1h_val  = parse_supertrend_value(val)
            prev_1h    = m.get('st_1h')
            flipped_1h = (st_1h_val is not None and prev_1h is not None and st_1h_val != prev_1h)
            m['st_1h'] = st_1h_val
            if flipped_1h and prev_1h:
                m['last_st_1h'] = prev_1h

            if flipped_1h:
                ctx_4h_ct  = m.get('st_context_4h')
                ctx_4h_lt  = ST_CONTEXT_LT_4H.get(symbol)
                ctx_1h     = m.get('st_context_1h')
                ctx_15m_z  = ST_CONTEXT_15M.get(symbol)

                adx_1h_c   = ADX_STATE.get(f'{symbol}_1h', {})
                di_plus    = adx_1h_c.get('di_plus', 0)
                di_minus   = adx_1h_c.get('di_minus', 0)
                adx_val    = adx_1h_c.get('adx', 0)
                adx_rising = adx_1h_c.get('adx_rising', False)

                direction_c4 = "LONG" if st_1h_val == 'buy' else "SHORT"
                opp_ctx      = 'sell' if direction_c4 == 'LONG' else 'buy'

                if direction_c4 == 'LONG':
                    dmi_gap = di_minus - di_plus
                else:
                    dmi_gap = di_plus - di_minus

                dmi_blocked = dmi_gap > 8
                dmi_weak    = 4 < dmi_gap <= 8

                if dmi_blocked:
                    adx_status_c4 = f"\u26d4 ADX 1H opposé fort (écart={dmi_gap:.1f}) \u2192 bloqué"
                elif dmi_weak:
                    adx_status_c4 = f"\u26a0\ufe0f Opposition faible DI (écart \u2248 {dmi_gap:.1f}) \u2192 possible retournement"
                elif adx_val >= 20 and adx_rising and (
                    (direction_c4 == 'LONG' and di_plus > di_minus) or
                    (direction_c4 == 'SHORT' and di_minus > di_plus)):
                    adx_status_c4 = f"\u2705 ADX 1H même sens ({adx_val:.1f} \u2191 | +DI={di_plus:.1f} | -DI={di_minus:.1f})"
                else:
                    adx_status_c4 = f"\u27a1\ufe0f ADX 1H neutre ({adx_val:.1f} | +DI={di_plus:.1f} | -DI={di_minus:.1f})"

                ctx_4h_ct_aligned = ctx_4h_ct == st_1h_val
                ctx_1h_aligned    = ctx_1h    == st_1h_val
                ctx_4h_lt_neutral = ctx_4h_lt != opp_ctx
                ctx_4h_lt_aligned = ctx_4h_lt == st_1h_val
                ctx_4h_ct_neutral = (ctx_4h_ct != opp_ctx and ctx_4h_ct != st_1h_val)
                ctx_15m_aligned   = ctx_15m_z == st_1h_val

                is_main      = ctx_4h_ct_aligned and ctx_1h_aligned and ctx_4h_lt_neutral and not dmi_blocked
                is_secondary = ctx_4h_lt_aligned and ctx_1h_aligned and ctx_4h_ct_neutral and not dmi_blocked
                is_triple    = ctx_4h_ct_aligned and ctx_1h_aligned and ctx_15m_aligned

                pos_key_c4 = f"{symbol}_CONTEXT4H"
                with STATE_LOCK:
                    pos_c4 = SCALP_POSITIONS.get(pos_key_c4)
                    if pos_c4 and pos_c4['direction'] != direction_c4:
                        pos_c4 = None; is_entry_c4 = False; is_pyra_c4 = False
                    else:
                        is_entry_c4 = ((is_main or is_secondary or is_triple) and pos_c4 is None)
                        opp_1h_c4   = 'sell' if st_1h_val == 'buy' else 'buy'
                        guard_ok_c4 = m.get('last_st_1h') == opp_1h_c4
                        is_pyra_c4  = bool(pos_c4 and pos_c4['direction'] == direction_c4
                                           and (is_main or is_secondary or is_triple) and guard_ok_c4)
                    if is_entry_c4 and should_send(symbol, f"c4h_entry_{st_1h_val}", event_id=event_id, cooldown=14400):
                        SCALP_POSITIONS[pos_key_c4] = {'direction': direction_c4, 'entry_count': 1}
                        pos_c4 = SCALP_POSITIONS[pos_key_c4]
                    else:
                        is_entry_c4 = False

                close_msg_c4 = "\n\n\U0001f4cb <b>Clôture :</b> ST Context 4H opposé ou perte de force"

                if is_entry_c4 and pos_c4:
                    emoji      = "\U0001f7e2" if direction_c4 == "LONG" else "\U0001f534"
                    tag        = "PRINCIPALE" if is_main else "TRIPLE 4H+1H+15m" if is_triple else "SECONDAIRE"
                    ctx_ct_txt = ctx_4h_ct.upper() if ctx_4h_ct else "NEUTRE"
                    ctx_lt_txt = ctx_4h_lt.upper() if ctx_4h_lt else "NEUTRE"
                    ctx_1h_txt = ctx_1h.upper()    if ctx_1h    else "NEUTRE"
                    ctx_15_txt = ctx_15m_z.upper() if ctx_15m_z else "NEUTRE"
                    msg = (
                        f"{emoji} <b>[CONTEXT 4H - {tag}]</b> {symbol}\n"
                        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                        f"\U0001f4c8 Direction: {direction_c4}\n"
                        f"\U0001f4b0 Price: ${format_price(price)}\n"
                        f"\U0001f3e6 Exchange: {exchange_name.upper()}\n"
                        f"\u23f0 {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"\u2705 ST Context 4H CT: {ctx_ct_txt}\n"
                        f"\u2705 ST Context 4H LT: {ctx_lt_txt}\n"
                        f"\u2705 ST Context 1H: {ctx_1h_txt}\n"
                        f"\u2705 ST Context 15m: {ctx_15_txt}{'  ✅' if is_triple else ''}\n"
                        f"{adx_status_c4}\n"
                        f"\u2705 SuperTrend AI 1H: {st_1h_val.upper()} (SIGNAL)"
                        f"{close_msg_c4}"
                        f"{get_market_context_info()}"
                    )
                    send_telegram_with_buttons(
                        msg, f"{symbol}_CONTEXT4H",
                        journal_symbol=symbol, journal_strategy='CONTEXT4H',
                        journal_direction=direction_c4, journal_price=price
                    )
                    track_alert(symbol, 'CONTEXT4H')
                    logger.info(f"[CONTEXT4H] Entrée {tag}: {symbol} {direction_c4}")

                elif is_pyra_c4 and PYRA_ENABLED.get(f"{symbol}_CONTEXT4H", False) and should_send(symbol, f"c4h_pyra_{st_1h_val}", event_id=event_id, cooldown=14400):
                    with STATE_LOCK:
                        pos_c4['entry_count'] += 1
                        entry_count_c4 = pos_c4['entry_count']
                    emoji      = "\U0001f7e2" if direction_c4 == "LONG" else "\U0001f534"
                    ctx_ct_txt = ctx_4h_ct.upper() if ctx_4h_ct else "NEUTRE"
                    ctx_lt_txt = ctx_4h_lt.upper() if ctx_4h_lt else "NEUTRE"
                    ctx_1h_txt = ctx_1h.upper()    if ctx_1h    else "NEUTRE"
                    send_telegram(
                        f"{emoji} <b>[CONTEXT 4H - PYRAMIDING #{entry_count_c4}]</b> {symbol}\n"
                        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                        f"\U0001f4c8 Direction: {direction_c4}\n"
                        f"\U0001f4b0 Price: ${format_price(price)}\n"
                        f"\U0001f3e6 Exchange: {exchange_name.upper()}\n"
                        f"\u23f0 {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"\u2705 ST Context 4H CT: {ctx_ct_txt}\n"
                        f"\u2705 ST Context 4H LT: {ctx_lt_txt}\n"
                        f"\u2705 ST Context 1H: {ctx_1h_txt}\n"
                        f"{adx_status_c4}\n"
                        f"\u2705 SuperTrend AI 1H: {st_1h_val.upper()} (PYRAMIDING)\n"
                        f"\U0001f6e1\ufe0f Guard: flip opposé validé"
                        f"{close_msg_c4}"
                        f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'CONTEXT4H')
                    logger.info(f"[CONTEXT4H] Pyramiding #{entry_count_c4}: {symbol} {direction_c4}")

    # LOGIQUE CONTEXT 1H : Bias 4H + Bias 1H → flip ST AI 15m
    # Anti-chop : ST Context 15m opposé → annulé
    # Pyramiding : flip ST AI 15m + guard — cooldown 1H
    # ========================================================================
    # ========================================================================
    # LOGIQUE SWING : ADX 4H DI aligné + ST AI 1H dans le sens → flip ST AI 1H
    # Pas d'anti-chop — filtres suffisamment forts
    # Pyramiding : flip ST AI 1H + guard — cooldown 1H
    # ========================================================================
        # ── Entrée secondaire : Bias 4H + flip ST AI 1H ────────────────
        if alert_type == 'supertrend' and tf == '1h':
            st_1h_val_p2  = parse_supertrend_value(val)
            prev_1h_p2    = m.get('st_1h_pulse2')
            flipped_1h_p2 = (st_1h_val_p2 is not None and prev_1h_p2 is not None and st_1h_val_p2 != prev_1h_p2)
            m['st_1h_pulse2'] = st_1h_val_p2
            if flipped_1h_p2 and prev_1h_p2:
                m['last_st_1h_pulse2'] = prev_1h_p2

            if flipped_1h_p2:
                # Recalculer Bias 4H en temps réel
                try:
                    df_4h_p2  = fetch_ohlcv_okx(symbol, '4h', limit=100)
                    bias_4h_p2 = calc_bias_okx(df_4h_p2, ema_len=21, sma_len=55) if df_4h_p2 is not None else m.get('bias_4h')
                    if df_4h_p2 is not None: m['bias_4h'] = bias_4h_p2
                except Exception:
                    bias_4h_p2 = m.get('bias_4h')

                ctx_15m_p2  = ST_CONTEXT_15M.get(symbol)
                direction_p2 = "LONG" if st_1h_val_p2 == 'buy' else "SHORT"
                exp_bias_p2  = 'bull' if direction_p2 == 'LONG' else 'bear'
                opp_ctx_p2   = 'sell' if direction_p2 == 'LONG' else 'buy'

                bias_4h_ok_p2 = bias_4h_p2 == exp_bias_p2
                # Anti-chop : ST Context 15m opposé → bloqué (neutre = OK)
                no_chop_p2    = ctx_15m_p2 != opp_ctx_p2

                pos_key_p2 = f"{symbol}_PULSE"
                with STATE_LOCK:
                    pos_p2 = SCALP_POSITIONS.get(pos_key_p2)
                    if pos_p2 and pos_p2['direction'] != direction_p2:
                        pos_p2 = None; is_entry_p2 = False; is_pyra_p2 = False
                    else:
                        is_entry_p2 = (bias_4h_ok_p2 and no_chop_p2 and pos_p2 is None)
                        opp_1h_p2   = 'sell' if st_1h_val_p2 == 'buy' else 'buy'
                        guard_ok_p2 = m.get('last_st_1h_pulse2') == opp_1h_p2
                        is_pyra_p2  = bool(pos_p2 and pos_p2['direction'] == direction_p2
                                           and bias_4h_ok_p2 and no_chop_p2 and guard_ok_p2)
                    if is_entry_p2 and should_send(symbol, f"pulse2_entry_{st_1h_val_p2}", event_id=event_id, cooldown=3600):
                        SCALP_POSITIONS[pos_key_p2] = {'direction': direction_p2, 'entry_count': 1}
                        pos_p2 = SCALP_POSITIONS[pos_key_p2]
                    else:
                        is_entry_p2 = False

                if is_entry_p2 and pos_p2:
                    emoji = "\U0001f7e2" if direction_p2 == "LONG" else "\U0001f534"
                    ctx_txt_p2 = ctx_15m_p2.upper() if ctx_15m_p2 else "NEUTRE"
                    send_telegram_with_buttons(
                        f"{emoji} <b>[PULSE - ENTREE 1H]</b> {symbol}\n"
                        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                        f"\U0001f4c8 Direction: {direction_p2}\n"
                        f"\U0001f4b0 Price: ${format_price(price)}\n"
                        f"\U0001f3e6 Exchange: {exchange_name.upper()}\n"
                        f"\u23f0 {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"\u2705 Bias 4H: {(bias_4h_p2 or '?').upper()} (EMA21/SMA55)\n"
                        f"\u2705 ST Context 15m: {ctx_txt_p2} (anti-chop)\n"
                        f"\u2705 SuperTrend AI 1H: {st_1h_val_p2.upper()} (SIGNAL)"
                        f"{get_market_context_info()}",
                        f"{symbol}_PULSE",
                        journal_symbol=symbol, journal_strategy='PULSE',
                        journal_direction=direction_p2, journal_price=price
                    )
                    track_alert(symbol, 'PULSE')
                    logger.info(f"[PULSE] Entrée 1H: {symbol} {direction_p2}")

                elif is_pyra_p2 and PYRA_ENABLED.get(f"{symbol}_PULSE", False) and should_send(symbol, f"pulse2_pyra_{st_1h_val_p2}", event_id=event_id, cooldown=3600):
                    with STATE_LOCK:
                        pos_p2['entry_count'] += 1
                        entry_count_p2 = pos_p2['entry_count']
                    emoji = "\U0001f7e2" if direction_p2 == "LONG" else "\U0001f534"
                    send_telegram_ttmtf(
                        f"{emoji} <b>[PULSE - PYRAMIDING 1H #{entry_count_p2}]</b> {symbol}\n"
                        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                        f"\U0001f4c8 Direction: {direction_p2}\n"
                        f"\U0001f4b0 Price: ${format_price(price)}\n"
                        f"\U0001f3e6 Exchange: {exchange_name.upper()}\n"
                        f"\u23f0 {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"\u2705 Bias 4H: {(bias_4h_p2 or '?').upper()} (EMA21/SMA55)\n"
                        f"\u2705 SuperTrend AI 1H: {st_1h_val_p2.upper()} (PYRAMIDING)\n"
                        f"\U0001f6e1\ufe0f Guard: flip opposé validé"
                        f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'PULSE')
                    logger.info(f"[PULSE] Pyramiding 1H #{entry_count_p2}: {symbol} {direction_p2}")

    if strat in ['swing', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'supertrend' and tf == '1h':
            st_1h_val_sw  = parse_supertrend_value(val)
            prev_1h_sw    = m.get('st_1h_swing')
            flipped_1h_sw = (st_1h_val_sw is not None and prev_1h_sw is not None and st_1h_val_sw != prev_1h_sw)
            m['st_1h_swing'] = st_1h_val_sw
            if flipped_1h_sw and prev_1h_sw:
                m['last_st_1h_swing'] = prev_1h_sw

            if flipped_1h_sw:
                adx_4h_sw   = ADX_STATE.get(f'{symbol}_4h', {})
                di_plus_4h  = adx_4h_sw.get('di_plus', 0)
                di_minus_4h = adx_4h_sw.get('di_minus', 0)
                st_1h_cur   = m.get('st_1h')

                direction_sw = "LONG" if st_1h_val_sw == 'buy' else "SHORT"

                # ADX 4H DI dans le bon sens
                adx_4h_ok_sw = (di_plus_4h >= di_minus_4h and direction_sw == 'LONG') or \
                               (di_minus_4h >= di_plus_4h and direction_sw == 'SHORT')
                # ST AI 1H dans le bon sens (direction courante confirmée par le flip)
                st_1h_ok_sw  = (st_1h_val_sw == 'buy'  and direction_sw == 'LONG') or \
                               (st_1h_val_sw == 'sell' and direction_sw == 'SHORT')

                pos_key_sw = f"{symbol}_SWING"
                with STATE_LOCK:
                    pos_sw = SCALP_POSITIONS.get(pos_key_sw)
                    if pos_sw and pos_sw['direction'] != direction_sw:
                        pos_sw = None; is_entry_sw = False; is_pyra_sw = False
                    else:
                        is_entry_sw = (adx_4h_ok_sw and st_1h_ok_sw and pos_sw is None)
                        opp_1h_sw   = 'sell' if st_1h_val_sw == 'buy' else 'buy'
                        guard_ok_sw = m.get('last_st_1h_swing') == opp_1h_sw
                        is_pyra_sw  = bool(pos_sw and pos_sw['direction'] == direction_sw
                                           and adx_4h_ok_sw and st_1h_ok_sw and guard_ok_sw)
                    if is_entry_sw and should_send(symbol, f"swing_entry_{st_1h_val_sw}", event_id=event_id, cooldown=3600):
                        SCALP_POSITIONS[pos_key_sw] = {'direction': direction_sw, 'entry_count': 1}
                        pos_sw = SCALP_POSITIONS[pos_key_sw]
                    else:
                        is_entry_sw = False

                if is_entry_sw and pos_sw:
                    emoji = "\U0001f7e2" if direction_sw == "LONG" else "\U0001f534"
                    send_telegram_with_buttons(
                        f"{emoji} <b>[SWING - ENTREE]</b> {symbol}\n"
                        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                        f"\U0001f4c8 Direction: {direction_sw}\n"
                        f"\U0001f4b0 Price: ${format_price(price)}\n"
                        f"\U0001f3e6 Exchange: {exchange_name.upper()}\n"
                        f"\u23f0 {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"\u2705 ADX 4H: +DI={di_plus_4h:.1f} | -DI={di_minus_4h:.1f} (DI aligné)\n"
                        f"\u2705 SuperTrend AI 1H: {st_1h_val_sw.upper()} (SIGNAL)"
                        f"{get_market_context_info()}",
                        f"{symbol}_SWING",
                        journal_symbol=symbol, journal_strategy='SWING',
                        journal_direction=direction_sw, journal_price=price
                    )
                    track_alert(symbol, 'SWING')
                    logger.info(f"[SWING] Entrée: {symbol} {direction_sw}")

                elif is_pyra_sw and PYRA_ENABLED.get(f"{symbol}_SWING", False) and should_send(symbol, f"swing_pyra_{st_1h_val_sw}", event_id=event_id, cooldown=3600):
                    with STATE_LOCK:
                        pos_sw['entry_count'] += 1
                        entry_count_sw = pos_sw['entry_count']
                    emoji = "\U0001f7e2" if direction_sw == "LONG" else "\U0001f534"
                    send_telegram_ttmtf(
                        f"{emoji} <b>[SWING - PYRAMIDING #{entry_count_sw}]</b> {symbol}\n"
                        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                        f"\U0001f4c8 Direction: {direction_sw}\n"
                        f"\U0001f4b0 Price: ${format_price(price)}\n"
                        f"\U0001f3e6 Exchange: {exchange_name.upper()}\n"
                        f"\u23f0 {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                        f"\u2705 ADX 4H: +DI={di_plus_4h:.1f} | -DI={di_minus_4h:.1f}\n"
                        f"\u2705 SuperTrend AI 1H: {st_1h_val_sw.upper()} (PYRAMIDING)\n"
                        f"\U0001f6e1\ufe0f Guard: flip opposé validé"
                        f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'SWING')
                    logger.info(f"[SWING] Pyramiding #{entry_count_sw}: {symbol} {direction_sw}")

    persist_runtime_state()
    return jsonify({'status': 'ok'}), 200


@app.route('/telegram_callback', methods=['POST'])
def telegram_callback():
    secret_path = os.environ.get('TELEGRAM_WEBHOOK_SECRET', '')
    if secret_path and request.args.get('secret') != secret_path:
        return jsonify({'ok': False}), 403
    """Reçoit les callbacks des boutons inline Telegram."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'ok': True}), 200
    try:
        callback      = data.get('callback_query', {})
        callback_id   = callback.get('id')
        callback_data = callback.get('data', '')
        chat_id       = callback.get('message', {}).get('chat', {}).get('id')
        msg_id        = callback.get('message', {}).get('message_id')
        user          = callback.get('from', {}).get('first_name', 'User')

        tok = CONFIG.get('TELEGRAM_BOT_TOKEN', '')
        if tok and callback_id:
            requests.post(f"https://api.telegram.org/bot{tok}/answerCallbackQuery",
                         json={"callback_query_id": callback_id}, timeout=5)

        if callback_data.startswith('pyra_on:'):
            key = callback_data[len('pyra_on:'):]
            with STATE_LOCK:
                PYRA_ENABLED[key] = True
            logger.info(f"[PYRA] Activé par {user}: {key}")
            if tok and chat_id and msg_id:
                requests.post(f"https://api.telegram.org/bot{tok}/editMessageReplyMarkup",
                             json={"chat_id": chat_id, "message_id": msg_id,
                                   "reply_markup": {"inline_keyboard": [[
                                       {"text": "✅ Pyramiding activé", "callback_data": "noop"}
                                   ]]}}, timeout=5)

        elif callback_data.startswith('pyra_off:'):
            key = callback_data[len('pyra_off:'):]
            with STATE_LOCK:
                PYRA_ENABLED.pop(key, None)
            logger.info(f"[PYRA] Désactivé par {user}: {key}")
            if tok and chat_id and msg_id:
                requests.post(f"https://api.telegram.org/bot{tok}/editMessageReplyMarkup",
                             json={"chat_id": chat_id, "message_id": msg_id,
                                   "reply_markup": {"inline_keyboard": [[
                                       {"text": "❌ Pyramiding ignoré", "callback_data": "noop"}
                                   ]]}}, timeout=5)

        elif callback_data.startswith('journal_log:'):
            # Relai vers le Journal Bot
            payload_str = callback_data[len('journal_log:'):]
            parts = payload_str.split('|')
            if len(parts) == 4:
                j_symbol, j_strategy, j_direction, j_price_str = parts
                journal_url = CONFIG.get('JOURNAL_BOT_URL', '').rstrip('/')
                user_id_str = str(callback.get('from', {}).get('id', ''))
                if journal_url:
                    def _relay_journal(url, sym, strat, direc, price_s, uid, cid):
                        try:
                            resp = requests.post(
                                f"{url}/log_entry",
                                json={
                                    'symbol':    sym,
                                    'strategy':  strat,
                                    'direction': direc,
                                    'price':     price_s,
                                    'user_id':   uid,
                                    'chat_id':   cid,
                                },
                                timeout=8
                            )
                            logger.info(f"[JOURNAL] Relai log_entry → {resp.status_code}")
                        except Exception as e:
                            logger.error(f"[JOURNAL] Relai erreur: {e}")
                    threading.Thread(
                        target=_relay_journal,
                        args=(journal_url, j_symbol, j_strategy, j_direction,
                              j_price_str, user_id_str, str(chat_id)),
                        daemon=True
                    ).start()
                    # Mettre à jour le bouton pour confirmer le clic
                    if tok and chat_id and msg_id:
                        try:
                            # Reconstruire le keyboard sans le bouton Journal (remplacé)
                            requests.post(
                                f"https://api.telegram.org/bot{tok}/editMessageReplyMarkup",
                                json={"chat_id": chat_id, "message_id": msg_id,
                                      "reply_markup": {"inline_keyboard": [[
                                          {"text": "📓 ✅ Envoyé au journal", "callback_data": "noop"}
                                      ]]}},
                                timeout=5
                            )
                        except Exception:
                            pass
                else:
                    logger.warning("[JOURNAL] JOURNAL_BOT_URL non configuré — callback ignoré")
            else:
                logger.warning(f"[JOURNAL] callback_data mal formé: {payload_str}")

    except Exception as e:
        logger.error(f"[CALLBACK] Erreur: {e}")
    return jsonify({'ok': True}), 200


@app.route('/prep_report', methods=['GET', 'POST'])
def force_prep_report():
    """Force l'envoi immédiat des listes PREP pour toutes les stratégies."""
    global PREP_STATE
    PREP_STATE = {}  # Reset pour forcer le renvoi
    check_prep_alerts()
    return jsonify({'status': 'ok', 'message': 'Rapport PREP envoyé'}), 200


@app.route('/refresh', methods=['POST'])
def refresh_indicators():
    if not require_admin_secret():
        return jsonify({'error': 'unauthorized'}), 401
    """Relance immédiatement le calcul des indicateurs OKX (Bias, ADX).
    Body optionnel: {"symbol": "BTC/USDT"} pour un seul asset.
    Sans body: relance pour tous les assets.
    """
    data = request.get_json(silent=True) or {}
    symbol_filter = data.get('symbol')

    if symbol_filter:
        symbol = format_tv_symbol(symbol_filter)
        if symbol not in CONFIG['SYMBOLS']:
            return jsonify({'error': f'{symbol} non dans la watchlist'}), 404
        symbols = [symbol]
    else:
        symbols = list(CONFIG['SYMBOLS'].keys())

    def _run():
        logger.info(f"[REFRESH] Calcul forcé pour {len(symbols)} assets...")
        for sym in symbols:
            try:
                update_indicators_for_symbol(sym)
            except Exception as e:
                logger.error(f"[REFRESH] {sym}: {e}")
        logger.info("[REFRESH] Terminé")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'status': 'ok', 'message': f'Refresh lancé pour {len(symbols)} assets'}), 200


@app.route('/reset_state', methods=['POST'])
def reset_state_all():
    if not require_admin_secret():
        return jsonify({'error': 'unauthorized'}), 401
    """Remet tout le state à zéro."""
    MOMENTUM_STATE.clear()
    LAST_SIGNALS.clear()
    LAST_SIGNAL_EVENTS.clear()
    ST_AI_15M.clear()
    ST_CONTEXT_15M.clear()
    SCALP_POSITIONS.clear()
    ST_CONTEXT_1D.clear()
    ST_CONTEXT_LT_1H.clear()
    ST_CONTEXT_LT_4H.clear()
    ST_CONTEXT_3D.clear()
    ST_CONTEXT_LT_15M.clear()
    ADX_STATE.clear()
    PREP_STATE.clear()
    PYRA_ENABLED.clear()
    persist_runtime_state()
    logger.info("🔄 State complet remis à zéro")
    return jsonify({'status': 'reset', 'message': 'État complet remis à zéro'}), 200

@app.route('/reset_state/<path:symbol>', methods=['POST'])
def reset_state_symbol(symbol):
    if not require_admin_secret():
        return jsonify({'error': 'unauthorized'}), 401
    """Remet à zéro l'état d'un seul asset. Ex: /reset_state/CVX/USDT"""
    symbol = symbol.upper().replace('-', '/')
    if symbol not in CONFIG['SYMBOLS']:
        return jsonify({'status': 'error', 'message': f'{symbol} non trouvé dans la watchlist'}), 404
    MOMENTUM_STATE.pop(symbol, None)
    ST_AI_15M.pop(symbol, None)
    ST_CONTEXT_15M.pop(symbol, None)
    ST_CONTEXT_1D.pop(symbol, None)
    ST_CONTEXT_3D.pop(symbol, None)
    ST_CONTEXT_LT_1H.pop(symbol, None)
    ST_CONTEXT_LT_4H.pop(symbol, None)
    ST_CONTEXT_LT_15M.pop(symbol, None)
    for k in ['', '_1h', '_4h', '_1d']:
        ADX_STATE.pop(f'{symbol}{k}', None)
    for strat in ['CONFLUENCE', 'TREND', 'PULSE', 'SCALP', 'CONTEXT4H']:
        PYRA_ENABLED.pop(f'{symbol}_{strat}', None)
        SCALP_POSITIONS.pop(f'{symbol}_{strat}', None)
   
    keys_to_remove = [k for k in LAST_SIGNALS if k.startswith(f"{symbol}:")]
    for k in keys_to_remove:
        LAST_SIGNALS.pop(k, None)
        LAST_SIGNAL_EVENTS.pop(k, None)
    persist_runtime_state()
    logger.info(f"🔄 State remis à zéro pour {symbol}")
    return jsonify({'status': 'reset', 'symbol': symbol, 'message': f'État de {symbol} remis à zéro'}), 200





def fetch_ohlcv_okx(symbol, timeframe, limit=250):
    """Fetch OHLCV depuis l API publique OKX (sans cle API)."""
    try:
        inst_id = symbol.replace('/', '-')
        tf_map = {'1h': '1H', '4h': '4H', '1d': '1D', '2h': '2H', '3h': '3H', '15m': '15m'}
        bar = tf_map.get(timeframe, timeframe.upper())
        url = f'https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={min(limit, 300)}'
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get('code') != '0' or not data.get('data'):
            logger.error(f"[OKX] API error {symbol} {timeframe}: {data.get('msg', 'no data')}")
            return None
        rows = [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])]
                for r in reversed(data['data'])]
        df = pd.DataFrame(rows, columns=['ts','open','high','low','close','volume'])
        return df
    except Exception as e:
        logger.error(f"[OKX] fetch_ohlcv {symbol} {timeframe}: {e}")
        return None


def calc_adx_okx(df, length=11, threshold=20):
    """Calcule ADX + DI sur les données OHLCV."""
    try:
        high  = df['high']
        low   = df['low']
        close = df['close']
        # True Range
        tr = (high - low).combine((high - close.shift(1)).abs(), max).combine((low - close.shift(1)).abs(), max)
        # Directional Movement
        dm_plus  = (high - high.shift(1)).clip(lower=0)
        dm_minus = (low.shift(1) - low).clip(lower=0)
        dm_plus  = dm_plus.where(dm_plus >= dm_minus, 0)
        dm_minus = dm_minus.where(dm_minus >= dm_plus, 0)
        # Smooth with Wilder EMA
        atr     = tr.ewm(alpha=1/length, adjust=False).mean()
        di_plus  = 100 * dm_plus.ewm(alpha=1/length, adjust=False).mean() / atr
        di_minus = 100 * dm_minus.ewm(alpha=1/length, adjust=False).mean() / atr
        dx      = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus)
        adx     = dx.ewm(alpha=1/length, adjust=False).mean()
        return {
            'adx':       round(float(adx.iloc[-1]), 2),
            'di_plus':   round(float(di_plus.iloc[-1]), 2),
            'di_minus':  round(float(di_minus.iloc[-1]), 2),
            'adx_rising': float(adx.iloc[-1]) > float(adx.iloc[-2]),
        }
    except Exception:
        return None

def calc_bias_okx(df, ema_len=13, sma_len=30):
    """EMA13 vs SMA30 — CarréBias."""
    close   = df['close']
    ema_val = close.ewm(span=ema_len, adjust=False).mean().iloc[-1]
    sma_val = close.rolling(window=sma_len).mean().iloc[-1]
    return 'bull' if ema_val > sma_val else 'bear'

def calc_ema200_okx(df):
    """EMA200 sur le close."""
    return float(df['close'].ewm(span=200, adjust=False).mean().iloc[-2])


def calc_bias_2d(symbol):
    """Calcule le Bias 2D en agrégeant les bougies 1D par paires."""
    try:
        df_1d = fetch_ohlcv_okx(symbol, '1d', limit=100)
        if df_1d is None or len(df_1d) < 40:
            return None
        df_2d = df_1d.groupby(df_1d.index // 2).agg({
            'open': 'first', 'high': 'max', 'low': 'min',
            'close': 'last', 'volume': 'sum'
        }).reset_index(drop=True)
        return calc_bias_okx(df_2d)
    except Exception as e:
        logger.error(f'[OKX] calc_bias_2d {symbol}: {e}')
        return None

def update_indicators_for_symbol(symbol):
    """Met a jour tous les indicateurs calculables pour un asset."""
    try:
        # Fetch bougies
        df_1h  = fetch_ohlcv_okx(symbol, '1h',  limit=250)
        df_4h  = fetch_ohlcv_okx(symbol, '4h',  limit=200)
        df_1d  = fetch_ohlcv_okx(symbol, '1d',  limit=100)
        df_3d  = fetch_ohlcv_okx(symbol, '1d',  limit=200)  # aggregate pour 3D

        if df_1h is None or df_4h is None or df_1d is None:
            return

        # Calculs
        bias_1h  = calc_bias_okx(df_1h, ema_len=8, sma_len=20)
        bias_4h  = calc_bias_okx(df_4h, ema_len=21, sma_len=55)
        bias_1d  = calc_bias_okx(df_1d, ema_len=21, sma_len=55)
        bias_2d  = calc_bias_2d(symbol)
        ema200_1h = calc_ema200_okx(df_1h)

        # Bias 3D — agreger bougies 1D par triplets
        try:
            df_3d_agg = df_3d.groupby(df_3d.index // 3).agg({
                'open': 'first', 'high': 'max', 'low': 'min',
                'close': 'last', 'volume': 'sum'
            }).reset_index(drop=True)
            bias_3d = calc_bias_okx(df_3d_agg, ema_len=21, sma_len=55)
        except Exception:
            bias_3d = None

        # Bias 15m pour pyramiding SCALP
        try:
            df_15m_bias = fetch_ohlcv_okx(symbol, '15m', limit=50)
            if df_15m_bias is not None and len(df_15m_bias) >= 30:
                bias_15m = calc_bias_okx(df_15m_bias, ema_len=8, sma_len=20)
                adx_data = calc_adx_okx(df_15m_bias)
                if adx_data:
                    ADX_STATE[symbol] = adx_data
                    if symbol in MOMENTUM_STATE:
                        MOMENTUM_STATE[symbol]['bias_15m'] = bias_15m
        except Exception as e:
            logger.error(f'[OKX] bias_15m {symbol}: {e}')
        # ADX 1H (Len=12, Threshold=22)
        try:
            adx_1h_data = calc_adx_okx(df_1h, length=10, threshold=20)
            if adx_1h_data:
                ADX_STATE[f'{symbol}_1h'] = adx_1h_data
        except Exception as e:
            logger.debug(f'[OKX] ADX 1H {symbol}: {e}')
        # ADX 4H (Len=14, Threshold=23)
        try:
            adx_4h_data = calc_adx_okx(df_4h, length=14, threshold=23)
            if adx_4h_data:
                ADX_STATE[f'{symbol}_4h'] = adx_4h_data
        except Exception as e:
            logger.debug(f'[OKX] ADX 4H {symbol}: {e}')
        # ADX 1D (Len=14)
        try:
            adx_1d_data = calc_adx_okx(df_1d, length=14)
            if adx_1d_data:
                ADX_STATE[f'{symbol}_1d'] = adx_1d_data
        except Exception as e:
            logger.debug(f'[OKX] ADX 1D {symbol}: {e}')


        price = float(df_1h['close'].iloc[-1])

        with STATE_LOCK:
            if symbol in MOMENTUM_STATE:
                if bias_2d: MOMENTUM_STATE[symbol]['bias_2d'] = bias_2d
                if bias_3d: MOMENTUM_STATE[symbol]['bias_3d'] = bias_3d
                MOMENTUM_STATE[symbol]['bias_1d']  = bias_1d
                MOMENTUM_STATE[symbol]['bias_1h']  = bias_1h
                MOMENTUM_STATE[symbol]['bias_4h']  = bias_4h



        logger.info(f"[OKX] {symbol} mis a jour — B1H={bias_1h} B4H={bias_4h} B1D={bias_1d} B3D={bias_3d} EMA200={ema200_1h:.4f}")

    except Exception as e:
        logger.error(f"[OKX] update_indicators {symbol}: {e}")



def check_prep_alerts():
    """Vérifie les assets en préparation et envoie une alerte groupée si la liste change."""
    global PREP_STATE

    new_prep = {
        'CONFLUENCE': {'LONG': set(), 'SHORT': set()},
        'TREND':      {'LONG': set(), 'SHORT': set()},
        'PULSE':      {'LONG': set(), 'SHORT': set()},
        'SWING':      {'LONG': set(), 'SHORT': set()},
    }

    with STATE_LOCK:
        state_copy    = dict(MOMENTUM_STATE)
        adx_copy      = dict(ADX_STATE)
        symbols_conf  = CONFIG['SYMBOLS']

    for symbol, m in state_copy.items():
        if symbol not in symbols_conf: continue  # ignorer les assets hors watchlist
        is_scalp = symbols_conf.get(symbol, {}).get('scalp', False)

        # ── CONFLUENCE : ST Context 3D + ST Context 4H aligné ───────
        ctx_3d_c   = ST_CONTEXT_3D.get(symbol)
        ctx_4h_c   = m.get('st_context_4h')
        adx_1d_c   = adx_copy.get(f'{symbol}_1d', {})
        di_plus_1d_c  = adx_1d_c.get('di_plus', 0)
        di_minus_1d_c = adx_1d_c.get('di_minus', 0)

        for direction in ('LONG', 'SHORT'):
            exp_ctx = 'buy' if direction == 'LONG' else 'sell'
            opp_ctx = 'sell' if direction == 'LONG' else 'buy'
            ctx_3d_ok = ctx_3d_c == exp_ctx
            ctx_4h_ok = ctx_4h_c == exp_ctx
            # Anti-chop ADX 1D : pas DI opposé dominant
            adx_1d_ok = not ((di_minus_1d_c > di_plus_1d_c and direction == 'LONG') or
                             (di_plus_1d_c > di_minus_1d_c and direction == 'SHORT'))
            if ctx_3d_ok and ctx_4h_ok and adx_1d_ok:
                new_prep['CONFLUENCE'][direction].add(symbol)

        # ── TREND : ST Context 1D + Bias 1D + ST Context 4H ─────────
        ctx_1d_t  = ST_CONTEXT_1D.get(symbol)
        bias_1d_v = m.get('bias_1d')
        ctx_4h_t  = m.get('st_context_4h')
        ctx_1h_t  = m.get('st_context_1h')

        for direction in ('LONG', 'SHORT'):
            exp_ctx = 'buy' if direction == 'LONG' else 'sell'
            exp_bias = 'bull' if direction == 'LONG' else 'bear'
            opp_ctx  = 'sell' if direction == 'LONG' else 'buy'
            ctx_1d_ok  = ctx_1d_t == exp_ctx
            bias_1d_ok = bias_1d_v == exp_bias
            ctx_4h_ok  = ctx_4h_t == exp_ctx
            no_chop_1h = ctx_1h_t != opp_ctx if ctx_1h_t is not None else True
            if ctx_1d_ok and bias_1d_ok and ctx_4h_ok and no_chop_1h:
                new_prep['TREND'][direction].add(symbol)

        # ── PULSE : ST Context 4H + Bias 4H + ST Context 15m ────────
        ctx_4h_p  = m.get('st_context_4h')
        bias_4h_v = m.get('bias_4h')
        ctx_15m_p = ST_CONTEXT_15M.get(symbol)
        ctx_1h_p  = m.get('st_context_1h')

        for direction in ('LONG', 'SHORT'):
            exp_ctx  = 'buy' if direction == 'LONG' else 'sell'
            exp_bias = 'bull' if direction == 'LONG' else 'bear'
            opp_ctx  = 'sell' if direction == 'LONG' else 'buy'
            ctx_4h_ok  = ctx_4h_p == exp_ctx
            bias_4h_ok = bias_4h_v == exp_bias
            ctx_15m_ok = ctx_15m_p == exp_ctx
            # None = neutre = ne bloque pas, mais opp_ctx = bloque
            no_chop_1h = ctx_1h_p != opp_ctx if ctx_1h_p is not None else True
            # PULSE v3 : Bias 4H + ST Context 15m (anti-chop DMI calculé au signal)
            if bias_4h_ok and ctx_15m_ok:
                new_prep['PULSE'][direction].add(symbol)

        # ── SWING : ADX 4H DI aligné ─────────────────────────────
        adx_4h_sw   = adx_copy.get(f'{symbol}_4h', {})
        di_plus_4h  = adx_4h_sw.get('di_plus', 0)
        di_minus_4h = adx_4h_sw.get('di_minus', 0)

        for direction in ('LONG', 'SHORT'):
            di_aligned = (di_plus_4h >= di_minus_4h and direction == 'LONG') or \
                         (di_minus_4h >= di_plus_4h and direction == 'SHORT')
            if di_aligned:
                new_prep['SWING'][direction].add(symbol)

    # ── Comparer avec l'état précédent et envoyer si changement ──────
    changed_msgs = []

    for strat in ('CONFLUENCE', 'TREND', 'PULSE', 'SWING'):
        old_state = PREP_STATE.get(strat, {'LONG': set(), 'SHORT': set()})
        new_long  = new_prep[strat]['LONG']
        new_short = new_prep[strat]['SHORT']
        old_long  = old_state.get('LONG', set())
        old_short = old_state.get('SHORT', set())

        if new_long != old_long or new_short != old_short:
            lines = [f"⏳ <b>[PREP {strat}]</b>"]
            if new_long:
                symbols_str = "  ".join(sorted(s.replace('/USDT', '') for s in new_long))
                lines.append(f"🟢 LONG  : {symbols_str}")
            if new_short:
                symbols_str = "  ".join(sorted(s.replace('/USDT', '') for s in new_short))
                lines.append(f"🔴 SHORT : {symbols_str}")
            if not new_long and not new_short:
                lines.append("— Aucun asset en préparation")
            lines.append(f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%H:%M (Shanghai)')}")
            changed_msgs.append("\n".join(lines))

    PREP_STATE = {
        'CONFLUENCE': {'LONG': new_prep['CONFLUENCE']['LONG'], 'SHORT': new_prep['CONFLUENCE']['SHORT']},
        'TREND':      {'LONG': new_prep['TREND']['LONG'],      'SHORT': new_prep['TREND']['SHORT']},
        'PULSE':      {'LONG': new_prep['PULSE']['LONG'],       'SHORT': new_prep['PULSE']['SHORT']},
        'SWING':      {'LONG': new_prep['SWING']['LONG'],       'SHORT': new_prep['SWING']['SHORT']},
    }

    if changed_msgs:
        full_msg = "\n\n".join(changed_msgs)
        send_telegram(full_msg)
        logger.info(f"[PREP] Alerte envoyée: {len(changed_msgs)} stratégie(s) modifiée(s)")



def bias4h_report_scheduler():
    """Envoie toutes les 4H un rapport des Bias 4H de tous les assets."""
    logger.info("📊 Scheduler rapport Bias 4H démarré (toutes les 4H)")
    # Attendre 10 minutes après démarrage pour que les données soient chargées
    time.sleep(600)
    while True:
        try:
            with STATE_LOCK:
                bull_assets = sorted([
                    s.replace('/USDT', '') for s, m in MOMENTUM_STATE.items()
                    if m.get('bias_4h') == 'bull'
                ])
                bear_assets = sorted([
                    s.replace('/USDT', '') for s, m in MOMENTUM_STATE.items()
                    if m.get('bias_4h') == 'bear'
                ])
                none_assets = sorted([
                    s.replace('/USDT', '') for s, m in MOMENTUM_STATE.items()
                    if m.get('bias_4h') is None
                ])

            bull_str = "  ".join(bull_assets) if bull_assets else "—"
            bear_str = "  ".join(bear_assets) if bear_assets else "—"
            none_str = "  ".join(none_assets) if none_assets else "—"

            msg = (
                f"📊 <b>[BIAS 4H — {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%H:%M (Shanghai)')}]</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🟢 <b>BULL ({len(bull_assets)})</b> : {bull_str}\n\n"
                f"🔴 <b>BEAR ({len(bear_assets)})</b> : {bear_str}\n\n"
                f"⬜ <b>N/A ({len(none_assets)})</b> : {none_str}"
            )
            send_telegram(msg)
            logger.info(f"[BIAS4H] Rapport envoyé — {len(bull_assets)} bull, {len(bear_assets)} bear")
        except Exception as e:
            logger.error(f"[BIAS4H] Erreur rapport: {e}")

        # Attendre la prochaine heure multiple de 4
        now = datetime.now(timezone.utc)
        hours_to_next = 4 - (now.hour % 4)
        next_4h = now.replace(minute=5, second=0, microsecond=0) + timedelta(hours=hours_to_next)
        wait = (next_4h - now).total_seconds()
        time.sleep(max(300, wait))

def indicators_scheduler():
    """Recalcule tous les indicateurs depuis OKX toutes les heures."""
    logger.info("[OKX] Scheduler indicateurs démarré (toutes les 15 minutes)")
    # Premier calcul au démarrage après 30s
    time.sleep(30)
    while True:
        logger.info(f"[OKX] Calcul indicateurs pour {len(CONFIG['SYMBOLS'])} assets...")
        for symbol in CONFIG['SYMBOLS']:
            update_indicators_for_symbol(symbol)
            time.sleep(0.5)  # rate limit OKX
        persist_runtime_state()
        check_prep_alerts()
        logger.info("[OKX] Mise a jour indicateurs terminée")
        # Attendre la prochaine bougie 15m
        now = datetime.now(timezone.utc)
        minutes_to_next = 15 - (now.minute % 15)
        next_15m = now + timedelta(minutes=minutes_to_next)
        next_15m = next_15m.replace(second=10, microsecond=0)
        wait = (next_15m - now).total_seconds()
        logger.info(f"[OKX] Prochain calcul dans {int(wait)}s")
        time.sleep(max(60, wait))


def send_market_sentiment():
    """Calcule et envoie le sentiment de marché basé sur les biais 2D et 4H."""
    try:
        with STATE_LOCK:
            state_copy = dict(MOMENTUM_STATE)

        total = len(state_copy)
        if total == 0:
            return

        bulls_2d = sum(1 for m in state_copy.values() if m.get('bias_2d') == 'bull')
        bears_2d = total - bulls_2d
        pct_2d   = round(bulls_2d / total * 100)

        bulls_4h = sum(1 for m in state_copy.values() if m.get('bias_4h') == 'bull')
        bears_4h = total - bulls_4h
        pct_4h   = round(bulls_4h / total * 100)

        def sentiment_label(pct):
            if pct >= 60:   return "🟢 BULLISH"
            elif pct <= 40: return "🔴 BEARISH"
            else:           return "🟡 NEUTRE"

        label_2d = sentiment_label(pct_2d)
        label_4h = sentiment_label(pct_4h)

        msg = (
            f"📊 <b>Sentiment de marché</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🕯 <b>Long terme (2D)</b> : {label_2d}\n"
            f"   {bulls_2d} bulls / {bears_2d} bears — {pct_2d}%\n\n"
            f"⚡ <b>Court terme (4H)</b> : {label_4h}\n"
            f"   {bulls_4h} bulls / {bears_4h} bears — {pct_4h}%\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}"
        )
        send_telegram(msg)
        logger.info(f"[SENTIMENT] 2D: {pct_2d}% bull | 4H: {pct_4h}% bull")
    except Exception as e:
        logger.error(f"[SENTIMENT] Erreur: {e}")


def sentiment_scheduler():
    """Envoie le sentiment de marché toutes les 4H (à 00:02, 04:02, 08:02, 12:02, 16:02, 20:02 UTC)."""
    logger.info("[SENTIMENT] Scheduler démarré (toutes les 4H)")
    while True:
        now  = datetime.now(timezone.utc)
        # Prochaine bougie 4H fermée : 00, 04, 08, 12, 16, 20 + 2min
        next_4h = now.replace(minute=2, second=0, microsecond=0)
        if next_4h.hour % 4 != 0:
            hours_ahead = 4 - (next_4h.hour % 4)
            next_4h = next_4h + timedelta(hours=hours_ahead)
        if next_4h <= now:
            next_4h += timedelta(hours=4)
        wait = (next_4h - now).total_seconds()
        logger.info(f"[SENTIMENT] Prochain envoi dans {int(wait)}s")
        time.sleep(wait)
        send_market_sentiment()

# ============================================================================ #
# INITIALISATION AU DEMARRAGE (compatible gunicorn)
# ============================================================================ #

def startup():
    try:
        logger.info("🚀 Démarrage du bot...")
        init_redis()
        load_runtime_state()
        init_exchanges()
        send_start_notification()

        scheduler_thread = threading.Thread(target=weekly_report_scheduler, daemon=True)
        scheduler_thread.start()

        heartbeat_thread = threading.Thread(target=heartbeat_scheduler, daemon=True)
        heartbeat_thread.start()
        # Configurer le webhook Telegram pour les boutons inline
        try:
            tok = CONFIG.get('TELEGRAM_BOT_TOKEN', '')
            if tok:
                wh_url = 'https://trading-bot-multi-strategy-production.up.railway.app/telegram_callback'
                requests.post(f'https://api.telegram.org/bot{tok}/setWebhook', json={'url': wh_url}, timeout=10)
                logger.info(f'✅ Telegram webhook configuré: {wh_url}')
        except Exception as e:
            logger.warning(f'⚠️ Telegram webhook setup: {e}')
        bias4h_thread = threading.Thread(target=bias4h_report_scheduler, daemon=True)
        bias4h_thread.start()

        prep_thread = threading.Thread(target=prep_report_scheduler, daemon=True)
        prep_thread.start()

        indicators_thread = threading.Thread(target=indicators_scheduler, daemon=True)
        indicators_thread.start()

        sentiment_thread = threading.Thread(target=sentiment_scheduler, daemon=True)
        sentiment_thread.start()

        watchdog_thread = threading.Thread(target=tv_alert_watchdog, daemon=True)
        watchdog_thread.start()

        logger.info("⏰ Schedulers démarrés (rapport hebdo + heartbeat + prep report + indicateurs OKX + sentiment 4H + TV watchdog)")
    except Exception as e:
        logger.error(f"❌ Erreur au démarrage: {e}")

# Démarrer les schedulers seulement dans le worker principal
if os.environ.get('ENABLE_SCHEDULERS', '1') == '1':
    startup_thread = threading.Thread(target=startup, daemon=True)
    startup_thread.start()

if __name__ == '__main__':
    logger.info(f"✅ Bot démarré sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)
