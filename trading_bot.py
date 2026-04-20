#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import json
import time
import requests
from datetime import datetime, timezone, timedelta
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
        'AAVE/USDT': {'exchange': 'okx', 'scalp': True},
        'APT/USDT': {'exchange': 'okx', 'scalp': True},
        'ARB/USDT': {'exchange': 'okx', 'scalp': True},
        'ATOM/USDT': {'exchange': 'okx', 'scalp': False},
        'AVAX/USDT': {'exchange': 'okx', 'scalp': True},
        'AXS/USDT': {'exchange': 'okx', 'scalp': False},
        'BNB/USDT': {'exchange': 'okx', 'scalp': False},
        'BONK/USDT': {'exchange': 'okx', 'scalp': False},
        'BTC/USDT': {'exchange': 'okx', 'scalp': True},
        'CRV/USDT': {'exchange': 'okx', 'scalp': False},
        'CVX/USDT': {'exchange': 'okx', 'scalp': False},
        'DOGE/USDT': {'exchange': 'okx', 'scalp': True},
        'ADA/USDT': {'exchange': 'okx', 'scalp': True},
        'DYDX/USDT': {'exchange': 'okx', 'scalp': False},
        'LDO/USDT': {'exchange': 'okx', 'scalp': False},
        'DOT/USDT': {'exchange': 'okx', 'scalp': False},
        'ENA/USDT': {'exchange': 'okx', 'scalp': False},
        'ETH/USDT': {'exchange': 'okx', 'scalp': True},
        'FET/USDT': {'exchange': 'okx', 'scalp': False},
        'FIL/USDT': {'exchange': 'okx', 'scalp': False},
        'FLOKI/USDT': {'exchange': 'okx', 'scalp': False},
        'GALA/USDT': {'exchange': 'okx', 'scalp': False},
        'HBAR/USDT': {'exchange': 'okx', 'scalp': False},
        'HYPE/USDT': {'exchange': 'okx', 'scalp': False},
        'IMX/USDT': {'exchange': 'okx', 'scalp': False},
        'INJ/USDT': {'exchange': 'okx', 'scalp': True},
        'JTO/USDT': {'exchange': 'okx', 'scalp': False},
        'JUP/USDT': {'exchange': 'okx', 'scalp': False},
        'LINK/USDT': {'exchange': 'okx', 'scalp': True},
        'LTC/USDT': {'exchange': 'okx', 'scalp': True},
        'MANA/USDT': {'exchange': 'okx', 'scalp': False},
        'MOVE/USDT': {'exchange': 'okx', 'scalp': False},
        'NEAR/USDT': {'exchange': 'okx', 'scalp': True},
        'ONDO/USDT': {'exchange': 'okx', 'scalp': False},
        'OP/USDT': {'exchange': 'okx', 'scalp': True},
        'PENDLE/USDT': {'exchange': 'okx', 'scalp': False},
        'PENGU/USDT': {'exchange': 'okx', 'scalp': False},
        'PEPE/USDT': {'exchange': 'okx', 'scalp': False},
        'PYTH/USDT': {'exchange': 'okx', 'scalp': False},
        'RAY/USDT': {'exchange': 'okx', 'scalp': False},
        'RENDER/USDT': {'exchange': 'okx', 'scalp': False},
        'SAND/USDT': {'exchange': 'okx', 'scalp': False},
        'SEI/USDT': {'exchange': 'okx', 'scalp': False},
        'SOL/USDT': {'exchange': 'okx', 'scalp': True},
        'STX/USDT': {'exchange': 'okx', 'scalp': False},
        'SUI/USDT': {'exchange': 'okx', 'scalp': True},
        'TIA/USDT': {'exchange': 'okx', 'scalp': False},
        'TON/USDT': {'exchange': 'okx', 'scalp': False},
        'VIRTUAL/USDT': {'exchange': 'okx', 'scalp': False},
        'WIF/USDT': {'exchange': 'okx', 'scalp': False},
        'WLD/USDT': {'exchange': 'okx', 'scalp': False},
        'XRP/USDT': {'exchange': 'okx', 'scalp': True},
        'ZK/USDT': {'exchange': 'okx', 'scalp': False},
        'ZRO/USDT': {'exchange': 'okx', 'scalp': False},
        'UNI/USDT': {'exchange': 'okx', 'scalp': True},
        'SHIB/USDT': {'exchange': 'okx', 'scalp': False},
        'ENJ/USDT': {'exchange': 'okx', 'scalp': False},
        'APE/USDT': {'exchange': 'okx', 'scalp': False},
        'CORE/USDT': {'exchange': 'okx', 'scalp': False},
        'TURBO/USDT': {'exchange': 'okx', 'scalp': False},
        'MEW/USDT': {'exchange': 'okx', 'scalp': False},
        'NEIRO/USDT': {'exchange': 'okx', 'scalp': False},
        'STRK/USDT': {'exchange': 'okx', 'scalp': False},
        'BERA/USDT': {'exchange': 'okx', 'scalp': False},
        'SONIC/USDT':   {'exchange': 'okx', 'scalp': False},
        'USELESS/USDT': {'exchange': 'okx', 'scalp': False},
        'BCH/USDT':   {'exchange': 'okx', 'scalp': True},
        'ETC/USDT':   {'exchange': 'okx', 'scalp': True},
        'NEO/USDT':   {'exchange': 'okx', 'scalp': True},
        'POL/USDT':   {'exchange': 'okx', 'scalp': True},
        'S/USDT':     {'exchange': 'okx', 'scalp': False},
        'THETA/USDT': {'exchange': 'okx', 'scalp': True},
        'XLM/USDT':   {'exchange': 'okx', 'scalp': True},
        'ZEC/USDT':   {'exchange': 'okx', 'scalp': True},
    },
    
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,
    'HEARTBEAT_INTERVAL_SECONDS': int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", 21600)),
    'BARK_TOKEN': os.environ.get('BARK_TOKEN', ''),  # legacy
    'NTFY_TOPIC': os.environ.get('NTFY_TOPIC', ''),
    'TAPBIT_BOT_URL': os.environ.get('TAPBIT_BOT_URL', ''),  # ex: https://tapbit-bot.up.railway.app
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
            'SAFE': 0, 'CONFLUENCE': 0, 'CONTEXT': 0, 'TREND': 0,
            'SWING': 0, 'SCALP': 0, 'MOMENTUM': 0,
        }
    if strategy in WEEKLY_STATS[symbol]:
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
            'scalp_positions':    dict(SCALP_POSITIONS),
        }
        try:
            REDIS_CLIENT.set('bot_state', json.dumps(payload))
        except Exception as e:
            logger.error(f"❌ Redis save error: {e}")


def audit_log(data, status="reçu"):
    if not REDIS_CLIENT:
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
        SCALP_POSITIONS.update(payload.get('scalp_positions', {}))

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
        "   • MACD 3D + Bias 1D + Bias 4H\n"
        "   • Zone: ST Context 15m / Signal: Flip ST AI 15m\n"
        "   • Pyramiding: ST Context 15m + flip ST AI 15m (guard)\n\n"
        "2️⃣ <b>CONTEXT</b>\n"
        "   • ST Context 4H + MACD 4H\n"
        "   • Zone: ST Context 15m / Signal: Flip ST AI 15m\n"
        "   • Pyramiding: Bias 4H + ST Context 15m (guard)\n\n"
        "3️⃣ <b>TREND</b>\n"
        "   • Bias 3D + MACD 1H + ST Context 1H + anti-chop ST Context 1D\n"
        "   • Entrée: flip ST AI 15m / Pyramiding: flip ST AI 1H (guard)\n\n"
        "4️⃣ <b>SWING</b>\n"
        "   • Bias 1D + anti-chop ST Context 1D\n"
        "   • 1ère entrée: ST Context 1H + ST Context 15m + flip ST AI 15m\n"
        "   • Pyramiding: ST Context 15m + flip ST AI 15m (guard)\n"
        "   • 73 assets\n\n"
        "5️⃣ <b>SCALP</b>\n"
        "   • ST Context LT 1H + MACD 1H (filtres)\n"
        "   • Signal: ST Context ST 15m\n"
        "   • Anti-chop: annulé si ST Context LT 15m en zone\n"
        "   • 24 assets\n\n"
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

    total_safe      = sum(s.get('SAFE', 0)        for s in WEEKLY_STATS.values())
    total_confluence = sum(s.get('CONFLUENCE', 0)  for s in WEEKLY_STATS.values())
    total_context   = sum(s.get('CONTEXT', 0)     for s in WEEKLY_STATS.values())
    total_trend     = sum(s.get('TREND', 0)       for s in WEEKLY_STATS.values())
    total_momentum  = sum(s.get('MOMENTUM', 0)    for s in WEEKLY_STATS.values())
    total_swing     = sum(s.get('SWING', 0)       for s in WEEKLY_STATS.values())
    total_scalp     = sum(s.get('SCALP', 0)       for s in WEEKLY_STATS.values())

    msg += (
        "📋 <b>Par stratégie:</b>\n"
        f"  • CONFLUENCE: {total_confluence}\n"
        f"  • CONTEXT: {total_context}\n"
        f"  • TREND: {total_trend}\n"
        f"  • SWING: {total_swing}\n"
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
            f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
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

def send_to_tapbit_bot(symbol: str, bias_3d: str, macd_1d: str):
    """Envoie Bias 3D et MACD 1D au bot Tapbit après chaque calcul OKX."""
    url = CONFIG.get('TAPBIT_BOT_URL', '')
    if not url:
        return
    try:
        # Bias 3D
        requests.post(f"{url}/webhook", json={
            'symbol': symbol, 'type': 'bias_3d', 'value': bias_3d,
            'tf': '3d', 'strategy': 'trend', 'price': 0
        }, timeout=5)
        # MACD 1D
        requests.post(f"{url}/webhook", json={
            'symbol': symbol, 'type': 'macd_1d', 'value': macd_1d,
            'tf': '1d', 'strategy': 'trend', 'price': 0
        }, timeout=5)
    except Exception as e:
        logger.debug(f"[TAPBIT] Envoi échoué {symbol}: {e}")

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
        '60': '1h', '1hr': '1h', '1hour': '1h',
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
ST_CONTEXT_LT_1H:  dict = {}  # Long term context 1H
ST_CONTEXT_LT_15M: dict = {}  # Long term context 15m

# Timestamps derniers webhooks TradingView par tf (pour heartbeat)
LAST_WEBHOOK_TS: dict = {}  # tf -> timestamp

# Positions SCALP
SCALP_POSITIONS: dict = {}      # pos_key -> position dict

def init_symbol_states(symbol):
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'bias_1d': None, 'bias_2d': None, 'bias_3d': None, 'macd_2d': None,
            'st_context_1h': None, 'st_context_4h': None,
            'st_context_1h_ts': None, 'st_context_4h_ts': None, 'st_context_15m_ts': None, 'st_context_1d_ts': None, 'st_context_lt_1h_ts': None, 'st_context_lt_15m_ts': None,
            'st_1h': None, 'st_4h': None, 'st_1d': None, 'macd_1d': None, 'macd_1h': None, 'macd_4h': None, 'macd_3d': None,
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
        if alert_type == 'macd_1d':
            v = str(val_raw).strip().lower()
            if v in ('bull', 'bear'): m['macd_1d'] = v
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
        if alert_type == 'supertrend' and tf == '1d':  m['st_1d'] = parse_supertrend_value(val)
        if alert_type == 'supertrend' and tf == '15m':
            prev_15m = m.get('st_ai_15m')
            st_15m_val = parse_supertrend_value(val)
            m['st_ai_15m'] = st_15m_val
            if prev_15m and st_15m_val and st_15m_val != prev_15m:
                m['last_st_15m'] = prev_15m  # garde la valeur précédente pour le guard
            ST_AI_15M[symbol] = st_15m_val

    # LOGIQUE CONTEXT : ST Context 4H → ST AI 1H
    # ========================================================================
    if strat in ['context', 'momentum_context', 'all']:
        m = MOMENTUM_STATE[symbol]
        if alert_type == 'st_context' and tf == '4h':
            old_val = m.get('st_context_4h')
            logger.info(f"[CONTEXT] {symbol} ST Context 4H: {old_val} → {m['st_context_4h']}")

            # PREP CONFLUENCE : ST Context 4H + ST Context 1H + Bias 3D tous alignés
            if m['st_context_4h'] is not None:
                _dir_prep    = "LONG" if m['st_context_4h'] == 'buy' else "SHORT"
                _expected    = m['st_context_4h']  # 'buy' ou 'sell'
                _bias_3d     = m.get('bias_3d')
                _ctx_1h      = m.get('st_context_1h')
                _bias_3d_ok  = (_bias_3d == 'bull' and _dir_prep == 'LONG') or (_bias_3d == 'bear' and _dir_prep == 'SHORT')
                _ctx_1h_ok   = _ctx_1h == _expected
                if _bias_3d_ok and _ctx_1h_ok and should_send(symbol, f"context_prep_{_expected}", event_id=event_id):
                    with STATE_LOCK:
                        PREP_BUFFER.append({'strat': 'CONFLUENCE', 'dir': _dir_prep, 'sym': symbol, 'price': price})
                    logger.info(f"[PREP] CONFLUENCE {_dir_prep} {symbol} — Bias 3D + ST Context 4H + 1H alignés")

        # SIGNAL : ST AI 1H flip dans le sens du context 4H
    # ========================================================================
    # ========================================================================
    # LOGIQUE CONFLUENCE : MACD 3D + Bias 1D + Bias 4H → ST Context 15m + flip ST AI 15m
    # ========================================================================
    if strat in ['confluence', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'st_context' and tf == '15m':
            parsed_ctx_15m = parse_st_context_value(val)
            ST_CONTEXT_15M[symbol] = parsed_ctx_15m
            m['st_context_15m_ts'] = now_ts

        if alert_type == 'supertrend' and tf == '15m':
            st_15m_val  = m.get('st_ai_15m')  # déjà mis à jour par le bloc global
            prev_15m    = m.get('last_st_15m')  # valeur précédente (guard)
            flipped_15m = (st_15m_val is not None and prev_15m is not None and st_15m_val != prev_15m)

            if flipped_15m:
                macd_3d_v   = m.get('macd_3d')
                bias_1d_v   = m.get('bias_1d')
                bias_4h_v   = m.get('bias_4h')
                ctx_15m     = ST_CONTEXT_15M.get(symbol)
                direction_c = "LONG" if st_15m_val == 'buy' else "SHORT"
                expected    = st_15m_val
                exp_bias    = 'bull' if direction_c == 'LONG' else 'bear'
                macd_3d_ok  = (macd_3d_v == 'bull' and direction_c == 'LONG') or (macd_3d_v == 'bear' and direction_c == 'SHORT')
                bias_1d_ok  = bias_1d_v == exp_bias
                bias_4h_ok  = bias_4h_v == exp_bias
                ctx_15m_ok  = ctx_15m == expected

                pos_key = f"{symbol}_CONFLUENCE"
                with STATE_LOCK:
                    pos = SCALP_POSITIONS.get(pos_key)
                    if pos and pos['direction'] != direction_c:
                        pos['opposite_seen'] = True
                        pos = None
                        is_entry = False
                        is_pyra  = False
                    else:
                        is_entry = (macd_3d_ok and bias_1d_ok and bias_4h_ok and ctx_15m_ok and pos is None)
                        last_15m = m.get('last_st_15m')
                        opp_15m  = 'sell' if st_15m_val == 'buy' else 'buy'
                        guard_ok = last_15m == opp_15m
                        is_pyra  = bool(pos and pos['direction'] == direction_c and macd_3d_ok and bias_1d_ok and bias_4h_ok and ctx_15m_ok and guard_ok)

                    if is_entry and should_send(symbol, f"conf_entry_{st_15m_val}", event_id=event_id, cooldown=3600):
                        SCALP_POSITIONS[pos_key] = {'direction': direction_c, 'entry_count': 1, 'opposite_seen': False}
                        pos = SCALP_POSITIONS[pos_key]

                if is_entry and pos:
                    emoji = "🟢" if direction_c == "LONG" else "🔴"
                    send_telegram(
                        f"{emoji} <b>[CONFLUENCE - ENTREE 15M]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_c}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ MACD 3D: {macd_3d_v.upper()}\n"
                        f"✅ Bias 1D: {bias_1d_v.upper()}\n"
                        f"✅ Bias 4H: {bias_4h_v.upper()}\n"
                        f"✅ ST Context 15m: {ctx_15m.upper()}\n"
                        f"✅ SuperTrend AI 15m: {st_15m_val.upper()} (SIGNAL)"
                    f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'CONFLUENCE')
                    logger.info(f"[CONFLUENCE] Entrée: {symbol} {direction_c}")
                elif is_pyra and should_send(symbol, f"conf_pyra_{st_15m_val}", event_id=event_id, cooldown=3600):
                    with STATE_LOCK:
                        pos['entry_count'] += 1
                        m['last_st_15m'] = None
                        entry_count = pos['entry_count']
                    emoji = "🟢" if direction_c == "LONG" else "🔴"
                    send_telegram(
                        f"{emoji} <b>[CONFLUENCE - PYRAMIDING #{entry_count}]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_c}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ MACD 3D: {macd_3d_v.upper()}\n"
                        f"✅ Bias 1D: {bias_1d_v.upper()}\n"
                        f"✅ Bias 4H: {bias_4h_v.upper()}\n"
                        f"✅ ST Context 15m: {ctx_15m.upper()}\n"
                        f"✅ SuperTrend AI 15m: {st_15m_val.upper()} (PYRAMIDING)\n"
                        f"🛡️ Guard: flip opposé validé"
                    f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'CONFLUENCE')
                    logger.info(f"[CONFLUENCE] Pyramiding #{entry_count}: {symbol} {direction_c}")

    # ========================================================================
    # LOGIQUE CONTEXT : ST Context 4H + MACD 4H → ST Context 15m + flip ST AI 15m
    # Pyramiding : Bias 4H + ST Context 15m + flip ST AI 15m + guard
    # ========================================================================
    if strat in ['context', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'supertrend' and tf == '15m':
            st_15m_val  = m.get('st_ai_15m')  # déjà mis à jour par le bloc global
            prev_15m    = m.get('last_st_15m')  # valeur précédente (guard)
            flipped_15m = (st_15m_val is not None and prev_15m is not None and st_15m_val != prev_15m)

            if flipped_15m:
                ctx_4h      = m.get('st_context_4h')
                macd_4h_v   = m.get('macd_4h')
                bias_4h_v   = m.get('bias_4h')
                ctx_15m     = ST_CONTEXT_15M.get(symbol)
                direction_c = "LONG" if st_15m_val == 'buy' else "SHORT"
                expected    = st_15m_val
                exp_bias    = 'bull' if direction_c == 'LONG' else 'bear'
                ctx_4h_ok   = ctx_4h == expected
                macd_4h_ok  = (macd_4h_v == 'bull' and direction_c == 'LONG') or (macd_4h_v == 'bear' and direction_c == 'SHORT')
                bias_4h_ok  = bias_4h_v == exp_bias
                ctx_15m_ok  = ctx_15m == expected

                pos_key = f"{symbol}_CONTEXT"
                with STATE_LOCK:
                    pos = SCALP_POSITIONS.get(pos_key)
                    if pos and pos['direction'] != direction_c:
                        pos = None
                        is_entry = False
                        is_pyra  = False
                    else:
                        is_entry = (ctx_4h_ok and macd_4h_ok and ctx_15m_ok and pos is None)
                        last_15m = m.get('last_st_15m')
                        opp_15m  = 'sell' if st_15m_val == 'buy' else 'buy'
                        guard_ok = last_15m == opp_15m
                        is_pyra  = bool(pos and pos['direction'] == direction_c and ctx_4h_ok and macd_4h_ok and bias_4h_ok and ctx_15m_ok and guard_ok)

                    if is_entry and should_send(symbol, f"ctx_entry_{st_15m_val}", event_id=event_id, cooldown=3600):
                        SCALP_POSITIONS[pos_key] = {'direction': direction_c, 'entry_count': 1}
                        pos = SCALP_POSITIONS[pos_key]

                if is_entry and pos:
                    emoji = "🟢" if direction_c == "LONG" else "🔴"
                    send_telegram(
                        f"{emoji} <b>[CONTEXT - ENTREE 15M]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_c}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ ST Context 4H: {ctx_4h.upper()}\n"
                        f"✅ MACD 4H: {macd_4h_v.upper()} (8/17/9)\n"
                        f"✅ ST Context 15m: {ctx_15m.upper()}\n"
                        f"✅ SuperTrend AI 15m: {st_15m_val.upper()} (SIGNAL)"
                    f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'CONTEXT')
                    logger.info(f"[CONTEXT] Entrée: {symbol} {direction_c}")
                elif is_pyra and should_send(symbol, f"ctx_pyra_{st_15m_val}", event_id=event_id, cooldown=3600):
                    with STATE_LOCK:
                        pos['entry_count'] += 1
                        m['last_st_15m'] = None
                        entry_count = pos['entry_count']
                    emoji = "🟢" if direction_c == "LONG" else "🔴"
                    send_telegram(
                        f"{emoji} <b>[CONTEXT - PYRAMIDING #{entry_count}]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_c}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ ST Context 4H: {ctx_4h.upper()}\n"
                        f"✅ Bias 4H: {bias_4h_v.upper()}\n"
                        f"✅ ST Context 15m: {ctx_15m.upper()}\n"
                        f"✅ SuperTrend AI 15m: {st_15m_val.upper()} (PYRAMIDING)\n"
                        f"🛡️ Guard: flip opposé validé"
                    f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'CONTEXT')
                    logger.info(f"[CONTEXT] Pyramiding #{entry_count}: {symbol} {direction_c}")

    # ========================================================================
    # LOGIQUE TREND : Bias 3D + MACD 1H + ST Context 1H + anti-chop ST Context 1D
    # Entrée : flip ST AI 15m / Pyramiding : flip ST AI 1H + guard
    # ========================================================================
    if strat in ['trend', 'all']:
        m = MOMENTUM_STATE[symbol]

        # ── Entrée : flip ST AI 15m ──────────────────────────────────────────
        if alert_type == 'supertrend' and tf == '15m':
            st_15m_val  = m.get('st_ai_15m')
            prev_15m    = m.get('last_st_15m')
            flipped_15m = (st_15m_val is not None and prev_15m is not None and st_15m_val != prev_15m)

            if flipped_15m:
                bias_3d_v   = m.get('bias_3d')
                macd_1h_v   = m.get('macd_1h')
                ctx_1h      = m.get('st_context_1h')
                ctx_1d      = ST_CONTEXT_1D.get(symbol)
                direction_t = "LONG" if st_15m_val == 'buy' else "SHORT"
                exp_bias    = 'bull' if direction_t == 'LONG' else 'bear'
                opp_ctx     = 'sell' if direction_t == 'LONG' else 'buy'
                bias_3d_ok  = bias_3d_v == exp_bias
                macd_1h_ok  = (macd_1h_v == 'bull' and direction_t == 'LONG') or (macd_1h_v == 'bear' and direction_t == 'SHORT')
                ctx_1h_ok   = ctx_1h == st_15m_val
                no_chop_1d  = ctx_1d != opp_ctx  # anti-chop

                pos_key = f"{symbol}_TREND"
                with STATE_LOCK:
                    pos = SCALP_POSITIONS.get(pos_key)
                    if pos and pos['direction'] != direction_t:
                        pos = None
                        is_entry = False
                    else:
                        is_entry = (bias_3d_ok and macd_1h_ok and ctx_1h_ok and no_chop_1d and pos is None)
                    if is_entry and should_send(symbol, f"trend_entry_15m_{st_15m_val}", event_id=event_id, cooldown=3600):
                        SCALP_POSITIONS[pos_key] = {'direction': direction_t, 'entry_count': 1}
                        pos = SCALP_POSITIONS[pos_key]
                    else:
                        is_entry = False

                if is_entry and pos:
                    emoji = "🟢" if direction_t == "LONG" else "🔴"
                    ctx_1d_txt = ctx_1d.upper() if ctx_1d else "NEUTRE"
                    chop_warn  = "\n⚠️ ST Context 1D opposé — zone de chop possible" if not no_chop_1d else ""
                    send_telegram(
                        f"{emoji} <b>[TREND - ENTREE 15M]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_t}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ Bias 3D: {bias_3d_v.upper()}\n"
                        f"✅ MACD 1H: {macd_1h_v.upper()} (8/17/9)\n"
                        f"✅ ST Context 1H: {ctx_1h.upper()}\n"
                        f"✅ ST Context 1D: {ctx_1d_txt} (anti-chop)\n"
                        f"✅ SuperTrend AI 15m: {st_15m_val.upper()} (SIGNAL)"
                        f"{chop_warn}"
                        f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'TREND')
                    logger.info(f"[TREND] Entrée 15m: {symbol} {direction_t}")

        # ── Pyramiding : flip ST AI 1H + guard ───────────────────────────────
        if alert_type == 'supertrend' and tf == '1h':
            st_1h_val   = parse_supertrend_value(val)
            st_1h_flip  = bool(m.get('st_1h_flipped', False))
            bias_3d_v   = m.get('bias_3d')
            macd_1h_v   = m.get('macd_1h')
            ctx_1h      = m.get('st_context_1h')
            ctx_1d      = ST_CONTEXT_1D.get(symbol)
            direction_t = "LONG" if st_1h_val == 'buy' else "SHORT"
            exp_bias    = 'bull' if direction_t == 'LONG' else 'bear'
            opp_ctx     = 'sell' if direction_t == 'LONG' else 'buy'
            bias_3d_ok  = bias_3d_v == exp_bias
            macd_1h_ok  = (macd_1h_v == 'bull' and direction_t == 'LONG') or (macd_1h_v == 'bear' and direction_t == 'SHORT')
            ctx_1h_ok   = ctx_1h == st_1h_val
            no_chop_1d  = ctx_1d != opp_ctx

            pos_key = f"{symbol}_TREND"
            with STATE_LOCK:
                pos = SCALP_POSITIONS.get(pos_key)
                last_1h  = m.get('last_st_4h') if m.get('last_st_4h') else None  # réutiliser last_st_4h comme guard 1H
                opp_1h   = 'sell' if st_1h_val == 'buy' else 'buy'
                # Guard : un flip 1H opposé doit avoir eu lieu depuis le dernier add
                prev_1h_for_guard = m.get('st_1h')
                guard_ok = (prev_1h_for_guard == opp_1h) if prev_1h_for_guard else False
                is_pyra  = bool(pos and pos['direction'] == direction_t and st_1h_flip and
                                bias_3d_ok and macd_1h_ok and ctx_1h_ok and no_chop_1d and guard_ok)

            if is_pyra and should_send(symbol, f"trend_pyra_1h_{st_1h_val}", event_id=event_id, cooldown=3600):
                with STATE_LOCK:
                    pos['entry_count'] += 1
                    entry_count = pos['entry_count']
                emoji = "🟢" if direction_t == "LONG" else "🔴"
                ctx_1d_txt = ctx_1d.upper() if ctx_1d else "NEUTRE"
                send_telegram(
                    f"{emoji} <b>[TREND - PYRAMIDING #{entry_count}]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction_t}\n"
                    f"💰 Price: ${format_price(price)}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 3D: {bias_3d_v.upper()}\n"
                    f"✅ MACD 1H: {macd_1h_v.upper()} (8/17/9)\n"
                    f"✅ ST Context 1H: {ctx_1h.upper()}\n"
                    f"✅ ST Context 1D: {ctx_1d_txt} (anti-chop)\n"
                    f"✅ SuperTrend AI 1H: {st_1h_val.upper()} (PYRAMIDING)\n"
                    f"🛡️ Guard: flip opposé validé"
                    f"{get_market_context_info()}"
                )
                track_alert(symbol, 'TREND')
                logger.info(f"[TREND] Pyramiding #{entry_count}: {symbol} {direction_t}")

    # ========================================================================
    # ========================================================================
    # ========================================================================
    # LOGIQUE SWING : Bias 1D + ST Context 1D (anti-chop) → ST Context 1H + ST Context 15m + flip ST AI 15m
    # Pyramiding : ST Context 15m + flip ST AI 15m + guard
    # ========================================================================
    if strat in ['scalp', 'swing', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'supertrend' and tf == '15m':
            st_15m_val  = m.get('st_ai_15m')  # déjà mis à jour par le bloc global
            prev_15m    = m.get('last_st_15m')  # valeur précédente (guard)
            flipped_15m = (st_15m_val is not None and prev_15m is not None and st_15m_val != prev_15m)

            if flipped_15m:
                bias_1d_v   = m.get('bias_1d')
                ctx_1h      = m.get('st_context_1h')
                ctx_1d      = ST_CONTEXT_1D.get(symbol)
                ctx_15m     = ST_CONTEXT_15M.get(symbol)
                direction_s = "LONG" if st_15m_val == 'buy' else "SHORT"
                expected    = st_15m_val
                exp_bias    = 'bull' if direction_s == 'LONG' else 'bear'
                opp_ctx     = 'sell' if direction_s == 'LONG' else 'buy'
                bias_1d_ok  = bias_1d_v == exp_bias
                ctx_1h_ok   = ctx_1h == expected
                ctx_15m_ok  = ctx_15m == expected
                no_chop_1d  = ctx_1d != opp_ctx

                pos_key = f"{symbol}_SWING"
                with STATE_LOCK:
                    pos = SCALP_POSITIONS.get(pos_key)
                    if pos and pos['direction'] != direction_s:
                        pos = None
                        is_entry = False
                        is_pyra  = False
                    else:
                        is_entry = (bias_1d_ok and ctx_1h_ok and ctx_15m_ok and no_chop_1d and pos is None)
                        last_15m = m.get('last_st_15m')
                        opp_15m  = 'sell' if st_15m_val == 'buy' else 'buy'
                        guard_ok = last_15m == opp_15m
                        is_pyra  = bool(pos and pos['direction'] == direction_s and bias_1d_ok and ctx_15m_ok and guard_ok)

                    if is_entry and should_send(symbol, f"swing_entry_{st_15m_val}", event_id=event_id, cooldown=3600):
                        SCALP_POSITIONS[pos_key] = {'direction': direction_s, 'entry_count': 1}
                        pos = SCALP_POSITIONS[pos_key]

                if is_entry and pos:
                    emoji = "🟢" if direction_s == "LONG" else "🔴"
                    ctx_1d_txt = ctx_1d.upper() if ctx_1d else "NEUTRE"
                    chop_warn = "\n⚠️ ST Context 1D opposé — zone de chop possible" if not no_chop_1d else ""
                    send_telegram(
                        f"{emoji} <b>[SWING - ENTREE 15M]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_s}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ Bias 1D: {bias_1d_v.upper()}\n"
                        f"✅ ST Context 1H: {ctx_1h.upper()}\n"
                        f"✅ ST Context 15m: {ctx_15m.upper()}\n"
                        f"✅ ST Context 1D: {ctx_1d_txt}\n"
                        f"✅ SuperTrend AI 15m: {st_15m_val.upper()} (SIGNAL)"
                        f"{chop_warn}"
                    f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'SWING')
                    logger.info(f"[SWING] Entrée: {symbol} {direction_s}")
                elif is_pyra and should_send(symbol, f"swing_pyra_{st_15m_val}", event_id=event_id, cooldown=3600):
                    with STATE_LOCK:
                        pos['entry_count'] += 1
                        m['last_st_15m'] = None
                        entry_count = pos['entry_count']
                    emoji = "🟢" if direction_s == "LONG" else "🔴"
                    send_telegram(
                        f"{emoji} <b>[SWING - PYRAMIDING #{entry_count}]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_s}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ Bias 1D: {bias_1d_v.upper()}\n"
                        f"✅ ST Context 15m: {ctx_15m.upper()}\n"
                        f"✅ SuperTrend AI 15m: {st_15m_val.upper()} (PYRAMIDING)\n"
                        f"🛡️ Guard: flip opposé validé"
                    f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'SWING')
                    logger.info(f"[SWING] Pyramiding #{entry_count}: {symbol} {direction_s}")

    # ========================================================================
    # LOGIQUE SCALP : ST Context LT 1H + ST Context ST 15m → anti-chop LT 15m
    # ========================================================================
    if strat in ['scalp', 'all'] and CONFIG['SYMBOLS'].get(symbol, {}).get('scalp', False):
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'st_context' and tf == '15m':
            parsed_ctx_st = parse_st_context_value(val)
            ST_CONTEXT_15M[symbol] = parsed_ctx_st
            m['st_context_15m_ts'] = now_ts

            ctx_lt_1h   = ST_CONTEXT_LT_1H.get(symbol)
            ctx_lt_15m  = ST_CONTEXT_LT_15M.get(symbol)
            macd_1h_v   = m.get('macd_1h')
            direction_scalp = "LONG" if parsed_ctx_st == 'buy' else ("SHORT" if parsed_ctx_st == 'sell' else None)

            if direction_scalp and ctx_lt_1h == parsed_ctx_st:
                macd_1h_ok = (macd_1h_v == 'bull' and direction_scalp == 'LONG') or (macd_1h_v == 'bear' and direction_scalp == 'SHORT')
                if not macd_1h_ok:
                    logger.debug(f"[SCALP] {symbol} — annulé : MACD 1H {macd_1h_v} opposé à {direction_scalp}")
            if direction_scalp and ctx_lt_1h == parsed_ctx_st and ((macd_1h_v == 'bull' and direction_scalp == 'LONG') or (macd_1h_v == 'bear' and direction_scalp == 'SHORT')):
                if ctx_lt_15m == parsed_ctx_st:
                    logger.debug(f"[SCALP] {symbol} — annulé : ST Context LT 15m déjà en zone {parsed_ctx_st}")
                elif should_send(symbol, f"scalp_entry_{parsed_ctx_st}", event_id=event_id, cooldown=3600):
                    emoji = "🟢" if direction_scalp == "LONG" else "🔴"
                    ctx_lt_15m_txt = ctx_lt_15m.upper() if ctx_lt_15m else "NEUTRE"
                    send_telegram(
                        f"{emoji} <b>[SCALP - ENTREE 15M]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction_scalp}\n"
                        f"💰 Price: ${format_price(price)}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ ST Context LT 1H: {ctx_lt_1h.upper()} (filtre)\n"
                        f"✅ MACD 1H: {macd_1h_v.upper()} (8/17/9)\n"
                        f"✅ ST Context ST 15m: {parsed_ctx_st.upper()} (signal)\n"
                        f"✅ ST Context LT 15m: {ctx_lt_15m_txt} (anti-chop)"
                        f"{get_market_context_info()}"
                    )
                    track_alert(symbol, 'SCALP')
                    logger.info(f"[SCALP] Entrée: {symbol} {direction_scalp}")

        elif alert_type == 'st_context_lt' and tf == '15m':
            parsed_lt = parse_st_context_value(val)
            ST_CONTEXT_LT_15M[symbol] = parsed_lt
            m['st_context_lt_15m_ts'] = now_ts

        elif alert_type == 'st_context_lt' and tf == '1h':
            parsed_lt = parse_st_context_value(val)
            ST_CONTEXT_LT_1H[symbol] = parsed_lt
            m['st_context_lt_1h_ts'] = now_ts

    persist_runtime_state()
    audit_log(data, status="traité")
    return jsonify({'status': 'success', 'symbol': symbol}), 200


# ============================================================================ #
# ROUTES UTILITAIRES
# ============================================================================ #

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'running',
        'timestamp': datetime.now().isoformat(),
        'symbols_total': len(CONFIG['SYMBOLS']),
        'exchanges': {k: '✅' for k in exchanges},
        'redis': '✅ connecté' if REDIS_CLIENT else '⚠️ non disponible',
    }), 200

@app.route('/state', methods=['GET'])
def state():
    return jsonify({
          'momentum_state': MOMENTUM_STATE,
             'watchlist': CONFIG['SYMBOLS']
    }), 200

@app.route('/context_state', methods=['GET'])
def context_state_route():
    return jsonify(MOMENTUM_STATE), 200

@app.route('/weekly_stats', methods=['GET'])
def weekly_stats_route():
    total = sum(sum(s.values()) for s in WEEKLY_STATS.values())
    return jsonify({
        'week_start': WEEKLY_START.isoformat(),
        'total_alerts': total,
        'by_asset': WEEKLY_STATS
    }), 200

@app.route('/audit', methods=['GET'])
def audit_route():
    if not REDIS_CLIENT:
        return jsonify({'error': 'Redis non disponible'}), 500
    try:
        limit = int(request.args.get('limit', 100))
        symbol_filter = request.args.get('symbol', '').upper()
        type_filter = request.args.get('type', '').lower()
        logs = [json.loads(l) for l in REDIS_CLIENT.lrange('audit_trail', 0, 999)]
        if symbol_filter:
            logs = [l for l in logs if symbol_filter in str(l.get('sym', ''))]
        if type_filter:
            logs = [l for l in logs if type_filter == str(l.get('type', '')).lower()]
        return jsonify(logs[:limit]), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/test_bark', methods=['GET', 'POST'])
def test_bark():
    """Test l'envoi ntfy."""
    topic = CONFIG.get('NTFY_TOPIC', '')
    if not topic:
        return jsonify({'error': 'NTFY_TOPIC non configuré'}), 400
    send_ntfy("🤖 Test ntfy", "Si tu vois ce message, ntfy fonctionne !")
    return jsonify({'status': 'ok', 'topic': topic}), 200

@app.route('/sentiment', methods=['POST', 'GET'])
def sentiment_now():
    """Déclenche un envoi immédiat du sentiment de marché."""
    threading.Thread(target=send_market_sentiment, daemon=True).start()
    logger.info("[SENTIMENT] Envoi manuel déclenché")
    return jsonify({'status': 'ok', 'message': 'Sentiment envoyé'}), 200

@app.route('/force_context', methods=['POST'])
def force_context():
    """Injecte manuellement un état ST Context.
    Body: {"symbol": "BTC/USDT", "tf": "1h"|"4h"|"15m"|"1d"|"lt_1h"|"lt_15m", "value": "buy"|"sell"|"none"}
    Ou pour plusieurs: {"symbols": ["BTC/USDT", "ETH/USDT"], "tf": "4h", "value": "sell"}
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'no data'}), 400

    tf    = normalize_tf(data.get('tf', ''))
    value = data.get('value', '').lower()
    ctx   = None if value in ('none', 'null', '') else value
    if ctx not in ('buy', 'sell', None):
        return jsonify({'error': 'value doit être buy, sell ou none'}), 400

    symbols = data.get('symbols') or ([data.get('symbol')] if data.get('symbol') else [])
    updated = []
    for symbol in symbols:
        symbol = format_tv_symbol(symbol)
        if symbol not in CONFIG['SYMBOLS']:
            continue
        init_symbol_states(symbol)
        now_ts = datetime.now(timezone.utc).timestamp()
        with STATE_LOCK:
            m = MOMENTUM_STATE[symbol]
            if tf == '1h':
                m['st_context_1h']    = ctx
                m['st_context_1h_ts'] = now_ts
            elif tf == '4h':
                m['st_context_4h']    = ctx
                m['st_context_4h_ts'] = now_ts
            elif tf == '15m':
                ST_CONTEXT_15M[symbol] = ctx
                m['st_context_15m_ts'] = now_ts
            elif tf == '1d':
                ST_CONTEXT_1D[symbol] = ctx
                m['st_context_1d_ts'] = now_ts
            elif tf == 'lt_1h':
                ST_CONTEXT_LT_1H[symbol] = ctx
                m['st_context_lt_1h_ts'] = now_ts
            elif tf == 'lt_15m':
                ST_CONTEXT_LT_15M[symbol] = ctx
                m['st_context_lt_15m_ts'] = now_ts
        updated.append(symbol)
        logger.info(f"[FORCE_CONTEXT] {symbol} st_context_{tf} = {ctx}")

    persist_runtime_state()
    return jsonify({'status': 'ok', 'updated': updated, 'tf': tf, 'value': ctx}), 200


@app.route('/refresh', methods=['POST'])
def refresh_indicators():
    """Relance immédiatement le calcul des indicateurs OKX (Bias, MACD).
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
    """Remet tout le state à zéro."""
    MOMENTUM_STATE.clear()
    LAST_SIGNALS.clear()
    LAST_SIGNAL_EVENTS.clear()
    ST_AI_15M.clear()
    ST_CONTEXT_15M.clear()
    SCALP_POSITIONS.clear()
    ST_CONTEXT_1D.clear()
    ST_CONTEXT_LT_1H.clear()
    ST_CONTEXT_LT_15M.clear()
    persist_runtime_state()
    logger.info("🔄 State complet remis à zéro")
    return jsonify({'status': 'reset', 'message': 'État complet remis à zéro'}), 200

@app.route('/reset_state/<path:symbol>', methods=['POST'])
def reset_state_symbol(symbol):
    """Remet à zéro l'état d'un seul asset. Ex: /reset_state/CVX/USDT"""
    symbol = symbol.upper().replace('-', '/')
    if symbol not in CONFIG['SYMBOLS']:
        return jsonify({'status': 'error', 'message': f'{symbol} non trouvé dans la watchlist'}), 404
    MOMENTUM_STATE.pop(symbol, None)
   
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


def calc_bias_okx(df, ema_len=13, sma_len=30):
    """EMA13 vs SMA30 — CarréBias."""
    close   = df['close']
    ema_val = close.ewm(span=ema_len, adjust=False).mean().iloc[-1]
    sma_val = close.rolling(window=sma_len).mean().iloc[-1]
    return 'bull' if ema_val > sma_val else 'bear'

def calc_macd_okx(df, fast=12, slow=26, signal=9):
    """Retourne bull/bear selon le signe de l histogramme MACD."""
    close       = df['close']
    ema_fast    = close.ewm(span=fast,   adjust=False).mean()
    ema_slow    = close.ewm(span=slow,   adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram   = macd_line - signal_line
    val = histogram.iloc[-1]
    return 'bull' if val > 0 else 'bear'

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

def calc_macd_2d(symbol):
    """Calcule le MACD 2D en agrégeant les bougies 1D par paires."""
    try:
        df_1d = fetch_ohlcv_okx(symbol, '1d', limit=200)
        if df_1d is None or len(df_1d) < 60:
            return None
        # Agréger par paires de bougies 1D -> bougies 2D
        df_2d = df_1d.groupby(df_1d.index // 2).agg({
            'open':   'first',
            'high':   'max',
            'low':    'min',
            'close':  'last',
            'volume': 'sum'
        }).reset_index(drop=True)
        return calc_macd_okx(df_2d)
    except Exception as e:
        logger.error(f"[OKX] calc_macd_2d {symbol}: {e}")
        return None

def calc_macd_3d(symbol):
    """Calcule le MACD 3D en agrégeant les bougies 1D par groupes de 3."""
    try:
        df_1d = fetch_ohlcv_okx(symbol, '1d', limit=300)
        if df_1d is None or len(df_1d) < 90:
            return None
        df_3d = df_1d.groupby(df_1d.index // 3).agg({
            'open':   'first',
            'high':   'max',
            'low':    'min',
            'close':  'last',
            'volume': 'sum'
        }).reset_index(drop=True)
        return calc_macd_okx(df_3d, fast=12, slow=26, signal=9)
    except Exception as e:
        logger.error(f"[OKX] calc_macd_3d {symbol}: {e}")
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
        bias_1h  = calc_bias_okx(df_1h)
        bias_4h  = calc_bias_okx(df_4h)
        bias_1d  = calc_bias_okx(df_1d)
        macd_1h  = calc_macd_okx(df_1h, fast=8, slow=17, signal=9)
        macd_3d  = calc_macd_3d(symbol)
        macd_4h  = calc_macd_okx(df_4h, fast=8, slow=17, signal=9)
        macd_1d  = calc_macd_okx(df_1d, fast=8, slow=17, signal=9)
        macd_2d  = calc_macd_2d(symbol)
        bias_2d  = calc_bias_2d(symbol)
        ema200_1h = calc_ema200_okx(df_1h)

        # Bias 3D — agreger bougies 1D par triplets
        try:
            df_3d_agg = df_3d.groupby(df_3d.index // 3).agg({
                'open': 'first', 'high': 'max', 'low': 'min',
                'close': 'last', 'volume': 'sum'
            }).reset_index(drop=True)
            bias_3d = calc_bias_okx(df_3d_agg)
        except Exception:
            bias_3d = None

        # Bias 15m pour pyramiding SCALP
        try:
            df_15m_bias = fetch_ohlcv_okx(symbol, '15m', limit=50)
            if df_15m_bias is not None and len(df_15m_bias) >= 30:
                bias_15m = calc_bias_okx(df_15m_bias)
                with STATE_LOCK:
                    if symbol in MOMENTUM_STATE:
                        MOMENTUM_STATE[symbol]['bias_15m'] = bias_15m
        except Exception as e:
            logger.error(f'[OKX] bias_15m {symbol}: {e}')

        price = float(df_1h['close'].iloc[-1])

        with STATE_LOCK:
            if symbol in MOMENTUM_STATE:
                if bias_2d: MOMENTUM_STATE[symbol]['bias_2d'] = bias_2d
                if bias_3d: MOMENTUM_STATE[symbol]['bias_3d'] = bias_3d
                MOMENTUM_STATE[symbol]['bias_1d']  = bias_1d
                MOMENTUM_STATE[symbol]['bias_1h']  = bias_1h
                MOMENTUM_STATE[symbol]['bias_4h']  = bias_4h
                MOMENTUM_STATE[symbol]['macd_1h']  = macd_1h
                MOMENTUM_STATE[symbol]['macd_1d']  = macd_1d
                MOMENTUM_STATE[symbol]['macd_4h']  = macd_4h
                MOMENTUM_STATE[symbol]['macd_2d']  = macd_2d
                MOMENTUM_STATE[symbol]['macd_3d']  = macd_3d



        logger.info(f"[OKX] {symbol} mis a jour — B1H={bias_1h} B4H={bias_4h} B1D={bias_1d} B2D={bias_2d} B3D={bias_3d} M1H={macd_1h} M4H={macd_4h} M1D={macd_1d} M2D={macd_2d} M3D={macd_3d} EMA200={ema200_1h:.4f}")
        # Envoyer Bias 3D + MACD 1D au bot Tapbit
        if bias_3d and macd_1d:
            threading.Thread(target=send_to_tapbit_bot, args=(symbol, bias_3d, macd_1d), daemon=True).start()

    except Exception as e:
        logger.error(f"[OKX] update_indicators {symbol}: {e}")


def indicators_scheduler():
    """Recalcule tous les indicateurs depuis OKX toutes les heures."""
    logger.info("[OKX] Scheduler indicateurs démarré (toutes les heures)")
    # Premier calcul au démarrage après 30s
    time.sleep(30)
    while True:
        logger.info(f"[OKX] Calcul indicateurs pour {len(CONFIG['SYMBOLS'])} assets...")
        for symbol in CONFIG['SYMBOLS']:
            update_indicators_for_symbol(symbol)
            time.sleep(0.5)  # rate limit OKX
        persist_runtime_state()
        logger.info("[OKX] Mise a jour indicateurs terminée")
        # Attendre la prochaine heure pile
        now  = datetime.now(timezone.utc)
        next_hour = (now + timedelta(hours=1)).replace(minute=2, second=0, microsecond=0)
        wait = (next_hour - now).total_seconds()
        logger.info(f"[OKX] Prochain calcul dans {int(wait)}s")
        time.sleep(wait)


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
            f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
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

startup_thread = threading.Thread(target=startup, daemon=True)
startup_thread.start()

if __name__ == '__main__':
    logger.info(f"✅ Bot démarré sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)
