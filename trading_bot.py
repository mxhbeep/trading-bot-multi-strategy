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
from concurrent.futures import ThreadPoolExecutor
import redis

# ============================================================================ #
# CONFIGURATION
# ============================================================================ #

CONFIG = {
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID': os.environ.get('TELEGRAM_CHAT_ID', ''),
    
    'SYMBOLS': {
        'AAVE/USDT':   {'exchange': 'okx', 'scalp': False},
        'APT/USDT':    {'exchange': 'okx', 'scalp': True},
        'AVAX/USDT':   {'exchange': 'okx', 'scalp': False},
        'BONK/USDT':   {'exchange': 'okx', 'scalp': False},
        'BTC/USDT':    {'exchange': 'okx', 'scalp': True},
        'COMP/USDT':   {'exchange': 'okx', 'scalp': False},
        'CRV/USDT':    {'exchange': 'okx', 'scalp': True},
        'CVX/USDT':    {'exchange': 'okx', 'scalp': True},
        'DOGE/USDT':   {'exchange': 'okx', 'scalp': True},
        'ETH/USDT':    {'exchange': 'okx', 'scalp': True},
        'FARTCOIN/USDT': {'exchange': 'okx', 'scalp': True, 'okx_inst_id': 'FARTCOIN-USDT-SWAP'},
        'HYPE/USDT':   {'exchange': 'okx', 'scalp': True, 'okx_inst_id': 'HYPE-USDT-SWAP'},
        'INJ/USDT':    {'exchange': 'okx', 'scalp': False},
        'LINK/USDT':   {'exchange': 'okx', 'scalp': True},
        'PENGU/USDT':  {'exchange': 'okx', 'scalp': True},
        'PEPE/USDT':   {'exchange': 'okx', 'scalp': True},
        'LTC/USDT':    {'exchange': 'okx', 'scalp': False},
        'NEAR/USDT':   {'exchange': 'okx', 'scalp': False},
        'ONDO/USDT':   {'exchange': 'okx', 'scalp': False},
        'RENDER/USDT': {'exchange': 'okx', 'scalp': False},
        'SOL/USDT':    {'exchange': 'okx', 'scalp': False},
        'SUI/USDT':    {'exchange': 'okx', 'scalp': False},
        'TAO/USDT':    {'exchange': 'okx', 'scalp': False},  # perp-only
        'UNI/USDT':    {'exchange': 'okx', 'scalp': False},
        'USELESS/USDT': {'exchange': 'okx', 'scalp': True, 'okx_inst_id': 'USELESS-USDT-SWAP'},
        'XPL/USDT':    {'exchange': 'okx', 'scalp': True, 'okx_inst_id': 'XPL-USDT-SWAP'},
        'XRP/USDT':    {'exchange': 'okx', 'scalp': True},
        'ZEC/USDT':    {'exchange': 'okx', 'scalp': True},
    },

    # Assets suivis uniquement en radar/info. Ils ne declenchent pas les entrees trade.
    # FARTCOIN et USELESS recoivent le Bias 1D via TradingView.
    'RADAR_SYMBOLS': {
        'ARB/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'ADA/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'BCH/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'BNB/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'CHZ/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'DYDX/USDT':     {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'EIGEN/USDT':    {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'ENA/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'ETC/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'FARTBOY/USDT':  {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'FET/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'FIL/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'HBAR/USDT':     {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'LDO/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'ONT/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'SAND/USDT':     {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'SKY/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'STX/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'TIA/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'VET/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'VIRTUAL/USDT':  {'exchange': 'okx', 'bias_1d_source': 'okx'},
        'ZEN/USDT':      {'exchange': 'okx', 'bias_1d_source': 'okx'},
    },    
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,
    'HEARTBEAT_INTERVAL_SECONDS': int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", 21600)),
    'BARK_TOKEN': os.environ.get('BARK_TOKEN', ''),  # legacy
    'SCALP_BOT_TOKEN': os.environ.get('SCALP_BOT_TOKEN', ''),
    'NTFY_TOPIC': os.environ.get('NTFY_TOPIC', 'maxence-trading-3f8a72'),
    'TAPBIT_BOT_URL': os.environ.get('TAPBIT_BOT_URL', ''),  # ex: https://tapbit-bot.up.railway.app
    'JOURNAL_BOT_URL': os.environ.get('JOURNAL_BOT_URL', ''),  # ex: https://journal-bot.up.railway.app
    'WEBHOOK_PORT': int(os.environ.get("PORT", 5000)),
    'WEBHOOK_HOST': '0.0.0.0',
    'ENABLE_PULSE_LEGACY': False,
    'ENABLE_PULSE_V2': True,
    'ENABLE_CONFLUENCE_REPORT': False,
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
            'SAFE': 0, 'DAILY': 0, 'TREND2D': 0, 'PULSE': 0, 'PULSEV2': 0, 'TTI6H': 0, 'TTI1D': 0, 'MOMENTUM': 0,
        }
    if strategy not in WEEKLY_STATS[symbol]:
        WEEKLY_STATS[symbol][strategy] = 0
    WEEKLY_STATS[symbol][strategy] += 1

exchanges = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

def get_tracked_symbols():
    return set(CONFIG['SYMBOLS']) | set(CONFIG.get('RADAR_SYMBOLS', {}))

def is_trade_symbol(symbol):
    return symbol in CONFIG['SYMBOLS']

def is_radar_symbol(symbol):
    return symbol in CONFIG.get('RADAR_SYMBOLS', {})

def get_symbol_config(symbol):
    return CONFIG['SYMBOLS'].get(symbol) or CONFIG.get('RADAR_SYMBOLS', {}).get(symbol) or {}

@app.route('/')
def home():
    total_symbols = len(CONFIG['SYMBOLS'])
    radar_symbols = len(CONFIG.get('RADAR_SYMBOLS', {}))
    okx_count = sum(1 for ex in CONFIG['SYMBOLS'].values() if ex.get('exchange') == 'okx')
    return f"""
    <h1>Trading Bot Multi-Strategy</h1>
    <p>Status: Running</p>
    <p>Trade assets: {total_symbols} | Radar assets: {radar_symbols} | OKX trade: {okx_count}</p>
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
            'st_ai_30m':          dict(ST_AI_30M),
            'st_ai_1d':           dict(ST_AI_1D),
            'st_context_15m':     dict(ST_CONTEXT_15M),
            'st_context_30m':     dict(ST_CONTEXT_30M),
            'adx_state':          dict(ADX_STATE),
            'scalp_positions':    dict(SCALP_POSITIONS),
            'st_context_1d':      dict(ST_CONTEXT_1D),
            'st_context_3d':      dict(ST_CONTEXT_3D),
            'st_context_lt_1h':   dict(ST_CONTEXT_LT_1H),
            'st_context_lt_4h':   dict(ST_CONTEXT_LT_4H),
            'st_context_lt_15m':  dict(ST_CONTEXT_LT_15M),
            'st_context_lt_5m':   dict(ST_CONTEXT_LT_5M),
            'st_context_lt_10m':  dict(ST_CONTEXT_LT_10M),
            'st_context_lt_30m':  dict(ST_CONTEXT_LT_30M),
            'pyra_enabled':       dict(PYRA_ENABLED),
            'last_webhook_ts':     dict(LAST_WEBHOOK_TS),
            'last_webhook_signal_ts': dict(LAST_WEBHOOK_SIGNAL_TS),
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
        LAST_WEBHOOK_TS.update(payload.get('last_webhook_ts', {}))
        LAST_WEBHOOK_SIGNAL_TS.update(payload.get('last_webhook_signal_ts', {}))
        ST_AI_15M.update(payload.get('st_ai_15m', {}))
        ST_AI_30M.update(payload.get('st_ai_30m', {}))
        ST_AI_1D.update(payload.get('st_ai_1d', {}))
        ST_CONTEXT_15M.update(payload.get('st_context_15m', {}))
        ST_CONTEXT_30M.update(payload.get('st_context_30m', {}))
        ADX_STATE.update(payload.get('adx_state', {}))
        SCALP_POSITIONS.update(payload.get('scalp_positions', {}))
        ST_CONTEXT_1D.update(payload.get('st_context_1d', {}))
        ST_CONTEXT_3D.update(payload.get('st_context_3d', {}))
        ST_CONTEXT_LT_1H.update(payload.get('st_context_lt_1h', {}))
        ST_CONTEXT_LT_4H.update(payload.get('st_context_lt_4h', {}))
        ST_CONTEXT_LT_15M.update(payload.get('st_context_lt_15m', {}))
        ST_CONTEXT_LT_5M.update(payload.get('st_context_lt_5m', {}))
        ST_CONTEXT_LT_10M.update(payload.get('st_context_lt_10m', {}))
        ST_CONTEXT_LT_30M.update(payload.get('st_context_lt_30m', {}))
        PYRA_ENABLED.update(payload.get('pyra_enabled', {}))
        # Nettoyer les assets hors watchlist chargés depuis Redis
        stale = [s for s in list(MOMENTUM_STATE.keys()) if s not in get_tracked_symbols()]
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

def strip_html(text: str) -> str:
    """Return plain text for notification channels that do not support HTML."""
    import re
    return re.sub(r'<[^>]+>', '', str(text or '')).strip()


def notification_title_from_message(msg: str, fallback: str = "Trading Bot") -> str:
    lines = [strip_html(line).strip() for line in str(msg or '').splitlines() if strip_html(line).strip()]
    if not lines:
        return fallback
    title = lines[0].replace('[', '').replace(']', '').replace('*', '').strip()
    return title[:80] or fallback


def notification_body_for_ntfy(msg: str, max_chars: int = 700) -> str:
    body = strip_html(msg)
    if len(body) <= max_chars:
        return body
    return body[:max_chars - 3].rstrip() + "..."


def ntfy_header_value(value: str, fallback: str = "Trading Bot", max_chars: int = 120) -> str:
    clean = strip_html(value).replace('\n', ' ').strip()
    clean = clean.encode('latin-1', errors='ignore').decode('latin-1').strip()
    return (clean[:max_chars] or fallback)


def notification_tags_from_text(text: str):
    plain = strip_html(text).lower()
    if 'take profit' in plain or 'tp' in plain:
        return ['tada']
    if 'stop loss' in plain or 'sl' in plain:
        return ['warning']
    if 'short' in plain or 'sell' in plain:
        return ['chart_with_downwards_trend']
    if 'long' in plain or 'buy' in plain:
        return ['chart_with_upwards_trend']
    return ['chart_with_upwards_trend']


class NotificationChannel:
    name = 'base'

    def send(self, title: str, message: str, priority=5, tags=None, **kwargs) -> bool:
        raise NotImplementedError


class TelegramChannel(NotificationChannel):
    name = 'telegram'

    def __init__(self, token_getter, chat_getter, label='Telegram'):
        self.token_getter = token_getter
        self.chat_getter = chat_getter
        self.label = label

    def send(self, title: str, message: str, priority=5, tags=None, reply_markup=None, **kwargs) -> bool:
        token = self.token_getter()
        chat = self.chat_getter()
        if not token or not chat:
            logger.warning(f"Telegram non configure ({self.label})")
            return False

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {'chat_id': chat, 'text': message, 'parse_mode': 'HTML'}
        if reply_markup:
            payload['reply_markup'] = reply_markup

        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info(f"Message {self.label} envoye")
                return True
            if resp.status_code == 429:
                retry_after = resp.json().get('parameters', {}).get('retry_after', 30)
                logger.warning(f"Telegram rate limit ({self.label}) - retry after {retry_after}s")
                time.sleep(retry_after)
                resp = requests.post(url, json=payload, timeout=10)
                if resp.status_code == 200:
                    logger.info(f"Message {self.label} envoye apres retry")
                    return True
            if resp.status_code == 400 and "can't parse entities" in resp.text.lower():
                plain = strip_html(message) or strip_html(title) or "Trading alert"
                fallback_payload = {'chat_id': chat, 'text': plain}
                if reply_markup:
                    fallback_payload['reply_markup'] = reply_markup
                logger.warning(f"Telegram HTML invalide ({self.label}) - fallback texte brut")
                fallback_resp = requests.post(url, json=fallback_payload, timeout=10)
                if fallback_resp.status_code == 200:
                    logger.info(f"Message {self.label} envoye en texte brut")
                    return True
                logger.error(
                    f"Telegram fallback erreur HTTP {fallback_resp.status_code} "
                    f"({self.label}): {fallback_resp.text[:200]}"
                )
                return False
            logger.error(f"Telegram erreur HTTP {resp.status_code} ({self.label}): {resp.text[:200]}")
            return False
        except Exception as e:
            logger.error(f"Erreur Telegram ({self.label}): {e}")
            return False


class NtfyChannel(NotificationChannel):
    name = 'ntfy'

    def __init__(self, topic_getter):
        self.topic_getter = topic_getter

    def send(self, title: str, message: str, priority=5, tags=None, **kwargs) -> bool:
        topic = str(self.topic_getter() or '').strip()
        if not topic:
            return False
        url = topic if topic.startswith(('http://', 'https://')) else f"https://ntfy.sh/{topic}"
        headers = {
            'Title': ntfy_header_value(title, 'Trading Bot'),
            'Priority': str(priority),
        }
        if tags:
            headers['Tags'] = ntfy_header_value(','.join(tags) if isinstance(tags, (list, tuple)) else str(tags), '', max_chars=80)
        try:
            resp = requests.post(
                url,
                data=notification_body_for_ntfy(message).encode('utf-8'),
                headers=headers,
                timeout=10,
            )
            if 200 <= resp.status_code < 300:
                logger.info("ntfy envoye")
                return True
            logger.warning(f"ntfy erreur: {resp.status_code} {resp.text[:200]}")
            return False
        except Exception as e:
            logger.error(f"ntfy error: {e}")
            return False


class NotificationManager:
    def __init__(self):
        self.channels = {}

    def register(self, name: str, channel: NotificationChannel):
        self.channels[name] = channel

    def send(self, title: str, message: str, priority=5, tags=None, channels=None, **kwargs):
        results = {}
        for name in (channels or list(self.channels.keys())):
            channel = self.channels.get(name)
            if not channel:
                continue
            results[name] = channel.send(title, message, priority=priority, tags=tags, **kwargs)
        return results


NOTIFICATIONS = NotificationManager()
NOTIFICATIONS.register(
    'telegram_alerts',
    TelegramChannel(
        lambda: CONFIG.get('TELEGRAM_BOT_TOKEN', ''),
        lambda: CONFIG.get('TELEGRAM_CHAT_ID', ''),
        label='Telegram',
    ),
)
NOTIFICATIONS.register(
    'telegram_scalp',
    TelegramChannel(
        lambda: CONFIG.get('SCALP_BOT_TOKEN', '') or CONFIG.get('TELEGRAM_BOT_TOKEN', ''),
        lambda: CONFIG.get('TELEGRAM_CHAT_ID', ''),
        label='Scalp Bot',
    ),
)
NOTIFICATIONS.register('ntfy', NtfyChannel(lambda: CONFIG.get('NTFY_TOPIC', '')))


def send_notification(title: str, message: str, priority=5, tags=None,
                      telegram=True, ntfy=True, telegram_channel='telegram_alerts',
                      reply_markup=None):
    channels = []
    if telegram:
        channels.append(telegram_channel)
    if ntfy:
        channels.append('ntfy')
    if tags is None:
        tags = notification_tags_from_text(f"{title}\n{message}")
    return NOTIFICATIONS.send(title, message, priority=priority, tags=tags,
                              channels=channels, reply_markup=reply_markup)


def send_bark(title: str, body: str, group: str = "TradingBot"):
    """Legacy wrapper kept for compatibility."""
    send_ntfy(title, body)


def sanitize_scalp_notification(msg: str) -> str:
    """Retire les emojis et normalise le titre directionnel des alertes scalp."""
    import re
    cleaned = re.sub(r'[^\x00-\x7F\u00C0-\u024F\n\r\t]', '', str(msg or ''))
    lines = [re.sub(r'^\?+\s*', '', line.strip()) for line in cleaned.splitlines() if line.strip()]
    text = '\n'.join(lines)
    direction_match = re.search(r'\b(LONG|SHORT)\b', text, re.IGNORECASE)
    symbol_match = re.search(r'\b[A-Z0-9]+/USDT\b', text, re.IGNORECASE)
    if lines and direction_match:
        direction = direction_match.group(1).upper()
        symbol = f" {symbol_match.group(0).upper()}" if symbol_match else ''
        suffix = ' - PYRAMIDING' if 'PYRAMIDING' in text.upper() else ''
        lines[0] = f"<b>SCALP {direction}{suffix}</b>{symbol}"
    return '\n'.join(lines)


def send_telegram_scalp(msg):
    """Envoie une alerte sur le bot Telegram dedie SCALP + ntfy."""
    msg = sanitize_scalp_notification(msg)
    direction = 'SHORT' if 'SHORT' in msg.upper() else 'LONG'
    result = send_notification(
        f'SCALP {direction}',
        msg,
        priority=5,
        tags=[],
        telegram=True,
        ntfy=True,
        telegram_channel='telegram_scalp',
    )
    if not result.get('telegram_scalp'):
        send_telegram(msg, ntfy=False)


def send_telegram_with_buttons(msg, callback_key, token=None, chat_id=None,
                               journal_symbol=None, journal_strategy=None,
                               journal_direction=None, journal_price=None):
    """Envoie un message Telegram avec boutons Pyramiding / Ignorer / Journal + ntfy."""
    row1 = [
        {"text": "Activer pyramiding", "callback_data": f"pyra_on:{callback_key}"},
        {"text": "Ignorer",             "callback_data": f"pyra_off:{callback_key}"},
    ]
    rows = [row1]
    if journal_symbol and journal_strategy and journal_direction and journal_price is not None and CONFIG.get('JOURNAL_BOT_URL'):
        sym_safe = str(journal_symbol).replace('|', '')
        jdata = f"journal_log:{sym_safe}|{journal_strategy}|{journal_direction}|{journal_price}"
        if len(jdata.encode()) <= 64:
            rows.append([{"text": "Logger ce trade", "callback_data": jdata}])
        else:
            logger.warning(f"[JOURNAL] callback_data trop long ({len(jdata.encode())} octets), bouton ignore")

    keyboard = {"inline_keyboard": rows}
    title = notification_title_from_message(msg)

    if token or chat_id:
        temp = TelegramChannel(lambda: token or '', lambda: chat_id or '', label='Telegram custom')
        telegram_ok = temp.send(title, msg, reply_markup=keyboard)
        send_notification(title, msg, telegram=False, ntfy=True)
        return telegram_ok

    result = send_notification(
        title,
        msg,
        priority=5,
        telegram=True,
        ntfy=True,
        telegram_channel='telegram_alerts',
        reply_markup=keyboard,
    )
    return bool(result.get('telegram_alerts'))


def send_telegram_ttmtf(msg):
    """Envoie une alerte trading sur le bot Telegram principal + ntfy."""
    return send_telegram(msg)


def send_ntfy(title: str, body: str, priority=5, tags=None):
    """Envoie uniquement une notification ntfy."""
    return send_notification(title, body, priority=priority, tags=tags, telegram=False, ntfy=True)


def normalize_base_url(url):
    """Normalise une URL en ajoutant https:// si absent."""
    url = str(url or '').strip().rstrip('/')
    if url and not url.startswith(('https://', 'http://')):
        url = f'https://{url}'
    return url


def send_telegram(msg, ntfy=False):
    result = send_notification(
        notification_title_from_message(msg),
        msg,
        priority=5,
        tags=notification_tags_from_text(msg),
        telegram=True,
        ntfy=ntfy,
        telegram_channel='telegram_alerts',
    )
    return bool(result.get('telegram_alerts'))


def send_info(msg):
    """Envoie un message sur le canal info (bot secondaire)."""
    tok  = os.environ.get('INFO_BOT_TOKEN', '')
    chat = os.environ.get('INFO_CHAT_ID', '')
    if not tok or not chat:
        logger.debug("⚠️ INFO_BOT_TOKEN/INFO_CHAT_ID non configurés — message info ignoré")
        return
    try:
        url  = f"https://api.telegram.org/bot{tok}/sendMessage"
        payload = {'chat_id': chat, 'text': msg, 'parse_mode': 'HTML'}
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("✅ Message info envoyé")
        elif resp.status_code == 400 and "can't parse entities" in resp.text.lower():
            plain = strip_html(msg) or "Trading bot info"
            fallback_resp = requests.post(url, json={'chat_id': chat, 'text': plain}, timeout=10)
            if fallback_resp.status_code == 200:
                logger.info("✅ Message info envoyé en texte brut")
            else:
                logger.error(f"❌ Info bot fallback erreur {fallback_resp.status_code}: {fallback_resp.text[:100]}")
        else:
            logger.error(f"❌ Info bot erreur {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        logger.error(f"❌ Erreur info bot: {e}")


def send_start_notification():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    redis_status = "Redis connecte" if REDIS_CLIENT else "Redis non disponible"
    msg = (
        "<b>[BOT STARTED]</b>\n"
        "--------------------\n"
        f"Total Assets: {len(CONFIG['SYMBOLS'])}\n"
        f"{redis_status}\n\n"
        "<b>STRATEGIES ACTIVES</b>\n\n"
        "DAILY principale: flip ST AI 30m + ST AI 1D + Bias 6H + ST Context 30m\n"
        "DAILY secondaire: Bias 1D + ST Context 2H + ST Context 10m\n"
        "DAILY pyramiding: RF10m + ST AI 2H, bloque si ST Context 10m oppose\n\n"
        "PULSE V2 principale: RMI 6H + TTI 6H/2H + ST Context 3m\n"
        "PULSE V2 secondaire: Bias 6H + RMI 2H + TTI 6H/2H + ST Context 3m\n"
        "PULSE V2 TP: rappel si TTI oppose apparait pendant une position\n\n"
        "TTI 6H/1D: alerte zone bull/bear sur toute la watchlist, qualite si ST Context meme TF aligne\n\n"
        "CONTEXT1D: RF30m + ST Context 1D + ST Context 30m + ST AI 1D\n"
        "TREND2D: RF30m + ST Context 2H + ST Context 10m + Bias 2D, bloque si ST Context 1D oppose\n"
        "--------------------\n"
        f"{now}"
    )
    send_info(msg)


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
    total_daily      = sum(s.get('DAILY', 0)       for s in WEEKLY_STATS.values())
    total_trend      = sum(s.get('TREND2D', 0)      for s in WEEKLY_STATS.values())
    total_momentum   = sum(s.get('MOMENTUM', 0)     for s in WEEKLY_STATS.values())
    total_swing      = sum(s.get('SWING', 0)        for s in WEEKLY_STATS.values())
    total_pulse      = sum(s.get('PULSE', 0)        for s in WEEKLY_STATS.values())
    total_pulse_v2   = sum(s.get('PULSEV2', 0)      for s in WEEKLY_STATS.values())
    total_tti_6h     = sum(s.get('TTI6H', 0)        for s in WEEKLY_STATS.values())
    total_tti_1d     = sum(s.get('TTI1D', 0)        for s in WEEKLY_STATS.values())
    total_scalp      = sum(s.get('SCALP', 0)        for s in WEEKLY_STATS.values())

    msg += (
        "📋 <b>Par stratégie:</b>\n"
        f"  — CONFLUENCE: {total_confluence}\n"
        f"  — DAILY: {total_daily}\n"
        f"  — TREND: {total_trend}\n"
        f"  — SWING: {total_swing}\n"
        f"  — PULSE: {total_pulse}\n"
        f"  — PULSEV2: {total_pulse_v2}\n"
        f"  — TTI6H: {total_tti_6h}\n"
        f"  — TTI1D: {total_tti_1d}\n"
        f"  — SCALP: {total_scalp}\n"
        f"  — MOMENTUM: {total_momentum}\n\n"
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
            if stats.get('SAFE', 0):        details.append(f"S:{stats['SAFE']}")
            if stats.get('MOMENTUM', 0):    details.append(f"M:{stats['MOMENTUM']}")
            if stats.get('CONFLUENCE', 0):  details.append(f"CONF:{stats['CONFLUENCE']}")
            if stats.get('DAILY', 0):       details.append(f"D:{stats['DAILY']}")
            if stats.get('TREND2D', 0):     details.append(f"T2D:{stats['TREND2D']}")
            if stats.get('SWING', 0):       details.append(f"SW:{stats['SWING']}")
            if stats.get('PULSE', 0):       details.append(f"PL:{stats['PULSE']}")
            if stats.get('PULSEV2', 0):     details.append(f"PL2:{stats['PULSEV2']}")
            if stats.get('TTI6H', 0):       details.append(f"TTI6H:{stats['TTI6H']}")
            if stats.get('TTI1D', 0):       details.append(f"TTI1D:{stats['TTI1D']}")
            if stats.get('SCALP', 0):       details.append(f"SC:{stats['SCALP']}")
            msg += f"  —{base}: {sum(stats.values())} ({', '.join(details)})\n"
    else:
        msg += "📈 <b>Par asset:</b> Aucune alerte cette semaine\n"

    msg += f"\n⏰{now.strftime('%d/%m/%Y %H:%M')} (Taiwan)"
    send_info(msg)
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
    msg = '<b>Assets en preparation</b> - ' + now + '\n' + '-' * 20 + '\n'

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
    send_info(msg)
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
    MAX_AGE = {'3m': 10*60, '5m': 15*60, '10m': 30*60, '30m': 90*60, '2h': 4*3600, '6h': 9*3600, '1d': 36*3600}
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
                missing.append(f"  — TF {tf.upper()}: jamais reçu")
            elif (now - last_ts) > max_age:
                age_h = (now - last_ts) / 3600
                missing.append(f"  — TF {tf.upper()}: dernier reçu il y a {age_h:.1f}H")
        if missing:
            details = "\n".join(missing)
            send_info(
                "🚨 <b>[ALERTE] Webhooks TradingView manquants</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"{details}\n\n"
                "➡️ Vérifier et redémarrer les alertes sur TradingView"
            )
            logger.warning(f"[TV WATCHDOG] Alertes manquantes: {missing}")

def tv_signal_key(symbol, alert_type, tf):
    return f"{symbol}|{alert_type}|{tf}"


def track_tv_signal(symbol, alert_type, tf):
    if not symbol or not alert_type or not tf:
        return
    LAST_WEBHOOK_SIGNAL_TS[tv_signal_key(symbol, alert_type, tf)] = time.time()


def tv_required_signals():
    return [
        {
            'label': 'ST AI 6H',
            'alert_type': 'supertrend',
            'tf': '6h',
            'max_age': 9 * 3600,
            'warmup': 10 * 3600,
        },
        {
            'label': 'ST AI 1D',
            'alert_type': 'supertrend',
            'tf': '1d',
            'max_age': 36 * 3600,
            'warmup': 37 * 3600,
        },
        {
            'label': 'ST AI 30m',
            'alert_type': 'supertrend',
            'tf': '30m',
            'max_age': 90 * 60,
            'warmup': 2 * 3600,
        },
        {
            'label': 'ST Context 3m',
            'alert_type': 'st_context',
            'tf': '3m',
            'max_age': 10 * 60,
            'warmup': 20 * 60,
        },
        {
            'label': 'ST Context 10m',
            'alert_type': 'st_context',
            'tf': '10m',
            'max_age': 30 * 60,
            'warmup': 45 * 60,
        },
        {
            'label': 'ST Context 30m',
            'alert_type': 'st_context',
            'tf': '30m',
            'max_age': 90 * 60,
            'warmup': 2 * 3600,
        },
        {
            'label': 'ST Context 2H',
            'alert_type': 'st_context',
            'tf': '2h',
            'max_age': 6 * 3600,
            'warmup': 7 * 3600,
        },
        {
            'label': 'ST Context 1D',
            'alert_type': 'st_context',
            'tf': '1d',
            'max_age': 36 * 3600,
            'warmup': 37 * 3600,
        },
        {
            'label': 'TTI 1D',
            'alert_type': 'tti',
            'tf': '1d',
            'max_age': 36 * 3600,
            'warmup': 37 * 3600,
        },
        {
            'label': 'TTI 6H',
            'alert_type': 'tti',
            'tf': '6h',
            'max_age': 18 * 3600,
            'warmup': 19 * 3600,
        },
        {
            'label': 'TTI 2H',
            'alert_type': 'tti',
            'tf': '2h',
            'max_age': 6 * 3600,
            'warmup': 7 * 3600,
        },
    ]


def tv_signal_watchdog():
    """Surveille les webhooks TradingView critiques asset par asset."""
    bot_start_time = time.time()
    time.sleep(30 * 60)
    logger.info("[TV SIGNAL WATCHDOG] Demarre")
    while True:
        time.sleep(15 * 60)
        now = time.time()
        uptime = now - bot_start_time
        issues = []
        with STATE_LOCK:
            symbols = list(CONFIG['SYMBOLS'].keys())
            signal_ts = dict(LAST_WEBHOOK_SIGNAL_TS)

        for req in tv_required_signals():
            if uptime < req['warmup']:
                continue
            missing = []
            stale = []
            for symbol in symbols:
                ts = signal_ts.get(tv_signal_key(symbol, req['alert_type'], req['tf']))
                if ts is None:
                    missing.append(symbol.replace('/USDT', ''))
                elif now - float(ts) > req['max_age']:
                    stale.append((symbol.replace('/USDT', ''), (now - float(ts)) / 3600))
            if missing or stale:
                details = []
                if missing:
                    details.append("jamais recu: " + ", ".join(missing[:10]) + ("..." if len(missing) > 10 else ""))
                if stale:
                    stale_txt = ", ".join(f"{sym} {age:.1f}H" for sym, age in stale[:10])
                    details.append("perime: " + stale_txt + ("..." if len(stale) > 10 else ""))
                issues.append(f"- {req['label']}: " + " | ".join(details))

        if issues and should_send('GLOBAL', 'tv_signal_watchdog', cooldown=3600):
            send_info(
                "<b>[ALERTE] Signaux TradingView critiques manquants</b>\n"
                "--------------------\n"
                + "\n".join(issues[:8])
                + "\n\nVerifier les alertes TradingView concernees."
            )
            logger.warning(f"[TV SIGNAL WATCHDOG] Issues: {issues}")


def heartbeat_scheduler():
    logger.info("Heartbeat Telegram desactive")
    return

# ============================================================================ #
# UTILITAIRES
# ============================================================================ #

def require_admin_secret():
    """Vérifie le header X-Admin-Secret pour les endpoints d'administration."""
    expected = os.environ.get('ADMIN_SECRET', '')
    if not expected:
        logger.error("ADMIN_SECRET non défini — endpoint admin refusé")
        return False  # fail closed
    return request.headers.get('X-Admin-Secret') == expected

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
    sym_cfg = get_symbol_config(symbol)
    if not sym_cfg:
        return None
    return exchanges.get(sym_cfg.get('exchange'))

def parse_st_context_value(val, trend_level=1.96):
    """
    Convertit la valeur brute du ST Context (plot_1 = Short time context) en 'buy', 'sell' ou None.
    Accepte les strings 'buy'/'sell' (rétrocompatibilité) et les valeurs numériques
    envoyées par TradingView via {{plot_1}}.
      plot_1 > +trend_level  → zone baissière →'sell'
      plot_1 < -trend_level  → zone haussière →'buy'
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

def parse_range_filter_value(val):
    """Convertit une valeur Range Filter en 'buy' ou 'sell'."""
    s = str(val).strip().lower()
    if s in ('buy', 'long', 'bull', '1'):
        return 'buy'
    if s in ('sell', 'short', 'bear', '-1', '0'):
        return 'sell'
    logger.warning(f"[WARN] Range Filter valeur invalide: '{val}'")
    return None

def parse_directional_trend_value(val, allow_sideways=False):
    """Convertit RMI/TTI en 'bull', 'bear' ou 'sideways'."""
    s = str(val).strip().lower()
    if s in ('bull', 'buy', 'long', 'up', 'positive', 'green', '1', '2'):
        return 'bull'
    if s in ('bear', 'sell', 'short', 'down', 'negative', 'red', '-1', '-2'):
        return 'bear'
    if allow_sideways and s in ('sideways', 'sideway', 'neutral', 'range', 'chop', 'flat', '0', '0.0'):
        return 'sideways'
    try:
        numeric = float(s)
        if numeric > 0:
            return 'bull'
        if numeric < 0:
            return 'bear'
        if allow_sideways:
            return 'sideways'
    except (ValueError, TypeError):
        pass
    logger.warning(f"[WARN] Trend direction valeur invalide: '{val}'")
    return None

def parse_rmi_value(val):
    return parse_directional_trend_value(val, allow_sideways=False)

def parse_tti_value(val):
    return parse_directional_trend_value(val, allow_sideways=True)

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
        '1': '1m', '1min': '1m', '1minute': '1m',
        '3': '3m', '3min': '3m', '3minute': '3m',
        '10': '10m', '10min': '10m', '10minute': '10m',
        '15': '15m', '60': '1h', '1hr': '1h', '1hour': '1h',
        '30': '30m', '30min': '30m', '30minute': '30m',
        '120': '2h', '2hr': '2h', '2hour': '2h',
        '180': '3h', '3hr': '3h', '3hour': '3h',
        '240': '4h', '4hr': '4h', '4hour': '4h',
        '360': '6h', '6hr': '6h', '6hour': '6h',
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
        'rangefilter': 'range_filter',
        'range_filter_30m': 'range_filter',
        'trendtype': 'tti',
        'trend_type': 'tti',
        'trend_type_indicator': 'tti',
        'rmits': 'rmi',
        'rmi_trend': 'rmi',
        'rmi_trend_sniper': 'rmi',
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
        if k not in LAST_SIGNALS or (now - LAST_SIGNALS[k] > effective_cooldown):
            LAST_SIGNALS[k] = now
            if event_id:
                LAST_SIGNAL_EVENTS[k] = event_id
            return True
    return False

# États SCALP — ST AI 15min + contexte 15min
ST_AI_15M: dict = {}       # symbol -> 'buy' | 'sell' | None
ST_AI_30M: dict = {}       # symbol -> 'buy' | 'sell' | None
ST_AI_1D: dict = {}        # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_15M: dict = {}  # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_30M: dict = {}  # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_1D:  dict = {}  # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_3D:  dict = {}  # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_LT_1H:  dict = {}  # Long term context 1H
ST_CONTEXT_LT_4H:  dict = {}  # Long term context 4H (plot_2)
ADX_STATE: dict = {}  # symbol -> {adx, di_plus, di_minus, adx_rising}
PREP_STATE: dict = {}
WEBHOOK_EXECUTOR = ThreadPoolExecutor(max_workers=4)
PYRA_ENABLED: dict = {}  # f'{symbol}_{strat}' -> True si pyramiding activé  # strategy -> {'LONG': set(), 'SHORT': set()} — assets en préparation
ST_CONTEXT_LT_15M: dict = {}  # Long term context 15m
ST_CONTEXT_LT_5M:  dict = {}  # Long term context 5m (plot_2)
ST_CONTEXT_LT_10M: dict = {}  # Long term context 10m (plot_2)
ST_CONTEXT_LT_30M: dict = {}  # Long term context 30m (plot_2)
RANGE_FILTER_30M: dict = {}  # symbol -> 'buy' | 'sell' | None

# Timestamps derniers webhooks TradingView par tf (pour heartbeat)
LAST_WEBHOOK_TS: dict = {}  # tf -> timestamp
LAST_WEBHOOK_SIGNAL_TS: dict = {}  # "symbol|type|tf" -> timestamp

# Positions SCALP
SCALP_POSITIONS: dict = {}      # pos_key -> position dict

def init_symbol_states(symbol):
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'bias_1d': None, 'bias_1d_ts': None, 'bias_2d': None, 'bias_2d_ts': None, 'bias_3d': None, 'bias_3d_ts': None,
            'st_context_1h': None, 'st_context_4h': None,
            'st_context_1h_ts': None, 'st_context_2h_ts': None, 'st_context_3m_ts': None, 'st_context_4h_ts': None, 'st_context_6h_ts': None, 'st_context_10m_ts': None, 'st_context_15m_ts': None, 'st_context_30m_ts': None, 'st_context_1d_ts': None, 'st_context_3d_ts': None, 'st_context_lt_1h_ts': None, 'st_context_lt_10m_ts': None, 'st_context_lt_15m_ts': None, 'st_context_lt_30m_ts': None, 'st_context_lt_4h_ts': None, 'st_context_5m_ts': None, 'last_st_context_5m_dir': None, 'last_st_context_5m_ts': None,
            'st_ai_5m': None, 'last_st_5m': None, 'st_context_5m': None, 'bias_5m': None,
            'st_1h': None, 'st_1h_ts': None, 'st_4h': None, 'st_6h': None,
            'last_st_4h': None,   # dernier flip 4H (guard pyramiding)
            'last_st_6h': None,   # dernier flip 6H
            'last_st_15m': None,  # dernier flip 15min (guard pyramiding)
            'last_st_30m': None,  # dernier flip 30min (guard pyramiding PULSE)
            # Nouveaux états pour CONTEXT v2 et SCALP
            'bias_1h': None, 'bias_1h_ts': None, 'bias_2h': None, 'bias_2h_ts': None, 'bias_4h': None, 'bias_6h': None, 'bias_6h_ts': None, 'bias_30m': None, 'bias_30m_ts': None, 'bias_15m': None, 'st_ai_15m': None, 'st_ai_30m': None, 'st_ai_30m_ts': None, 'st_ai_1d': None, 'st_ai_1d_ts': None, 'st_6h_ts': None,
            'daily_st_ai_30m_flip_dir': None, 'daily_st_ai_30m_flip_ts': None, 'daily_st_ai_30m_flip_event_id': None,
            'williams_1d': None, 'williams_1d_ts': None, 'williams_2d': None, 'williams_2d_ts': None, 'williams_2h': None, 'williams_2h_ts': None, 'williams_6h': None, 'williams_6h_ts': None,
            'st_context_2h': None,
            'st_context_6h': None,
            'st_context_3m': None,
            'st_context_10m': None, 'st_context_lt_10m': None,
            'rmi_6h': None, 'rmi_6h_ts': None, 'rmi_2h': None, 'rmi_2h_ts': None,
            'tti_1d': None, 'tti_1d_ts': None, 'tti_6h': None, 'tti_6h_ts': None, 'tti_2h': None, 'tti_2h_ts': None,
            'range_filter_10m': None, 'range_filter_10m_ts': None, 'last_range_filter_10m_signal_ts': None,
            'range_filter_30m': None, 'range_filter_30m_ts': None, 'last_range_filter_30m_signal_ts': None,
        }


# ============================================================================ #
# WEBHOOK HANDLER
# ============================================================================ #

def send_close_alert(symbol, strategy, direction, price, reason):
    """Envoie une alerte de clôture, supprime la position et persiste l'état."""
    pos_key = f"{symbol}_{strategy}"
    with STATE_LOCK:
        pos = SCALP_POSITIONS.pop(pos_key, None)
        PYRA_ENABLED.pop(pos_key, None)
    if pos:
        emoji = "🔴" if direction == "LONG" else "🟢"
        send_telegram(
            f"{emoji} <b>[{strategy} - CLÔTURE RAPPEL]</b> {symbol}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 Direction était: {direction}\n"
            f"💰 Price: ${format_price(price)}\n"
            f"⏰{datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
            f"📋 Raison: {reason}"
        )
        persist_runtime_state()
        logger.info(f"[{strategy}] Position clôturée: {symbol} {direction} —{reason}")


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        logger.warning("⚠️ Webhook sans données")
        return jsonify({'status': 'no_data'}), 400
    # Répondre immédiatement — traitement asynchrone pour éviter timeout TV
    WEBHOOK_EXECUTOR.submit(run_webhook_job, data)
    return jsonify({'status': 'ok'}), 200


def run_webhook_job(data):
    """Wrapper avec contexte Flask pour l'exécuteur."""
    try:
        with app.app_context():
            process_webhook(data)
    except Exception:
        logger.exception("[WEBHOOK] Erreur non geree dans le job async")


def process_webhook(data):
    """Traitement asynchrone du webhook — appelé dans un thread séparé."""
    try:

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
        # Defaut de securite : toujours defini, meme si le bloc de mise a jour
        # ST AI 15m (gate par strat) ne s'execute pas pour cette alerte.
        st_ai_15m_flipped_this_call = False
        st_ai_30m_flipped_this_call = False
        ctx30m_zone_changed_this_call = False

        if symbol not in get_tracked_symbols():
            logger.info(f"⚠️ {symbol} non dans la watchlist")
            audit_log(data, status="ignoré_watchlist")
            return jsonify({'status': 'ignored', 'reason': 'not_in_watchlist'}), 200

        trade_symbol = is_trade_symbol(symbol)
        radar_only = is_radar_symbol(symbol) and not trade_symbol
        exchange_name = get_symbol_config(symbol).get('exchange', 'okx')
        init_symbol_states(symbol)
        track_tv_signal(symbol, alert_type, tf)

        # Mise à jour globale des contextes (indépendante de la stratégie du webhook)
        m = MOMENTUM_STATE[symbol]
        now_ts = datetime.now(timezone.utc).timestamp()
        if alert_type == 'bias':
            bias_val = val.lower() if isinstance(val, str) else None
            if bias_val in ('bull', 'bear', 'neutral'):
                if tf == '4h':
                    prev_bias_4h = m.get('bias_4h')
                    m['bias_4h'] = bias_val if bias_val != 'neutral' else None
                    logger.info(f"[BIAS TV] {symbol} bias_4h = {bias_val}")
                elif tf == '6h':
                    prev_bias_6h = m.get('bias_6h')
                    m['bias_6h'] = bias_val if bias_val != 'neutral' else None
                    m['bias_6h_ts'] = now_ts
                    logger.info(f"[BIAS TV] {symbol} bias_6h = {bias_val}")
                    # Clôture PULSE si Bias 6H inversé (via alerte TV)
                    pos_pulse = SCALP_POSITIONS.get(f'{symbol}_PULSE')
                    if pos_pulse and prev_bias_6h and bias_val != prev_bias_6h and bias_val != 'neutral':
                        dir_p = pos_pulse['direction']
                        exp_bias = 'bull' if dir_p == 'LONG' else 'bear'
                        if bias_val != exp_bias:
                            send_close_alert(symbol, 'PULSE', dir_p, price, 'Bias 6H inversé')
                elif tf == '1d':
                    prev_bias_1d = m.get('bias_1d')
                    m['bias_1d'] = bias_val if bias_val != 'neutral' else None
                    m['bias_1d_ts'] = now_ts
                    logger.info(f"[BIAS TV] {symbol} bias_1d = {bias_val}")
                    logger.info(f"[BIAS TV] {symbol} bias_1d = {bias_val}")
                elif tf == '2d':
                    m['bias_2d'] = bias_val if bias_val != 'neutral' else None
                    logger.info(f"[BIAS TV] {symbol} bias_2d = {bias_val}")
                elif tf == '2h':
                    m['bias_2h'] = bias_val if bias_val != 'neutral' else None
                    m['bias_2h_ts'] = now_ts
                    logger.info(f"[BIAS TV] {symbol} bias_2h = {bias_val}")
                elif tf == '30m':
                    m['bias_30m'] = bias_val if bias_val != 'neutral' else None
                    m['bias_30m_ts'] = now_ts
                    logger.info(f"[BIAS TV] {symbol} bias_30m = {bias_val}")
                elif tf == '1h':
                    m['bias_1h'] = bias_val if bias_val != 'neutral' else None
                    m['bias_1h_ts'] = now_ts
                    logger.info(f"[BIAS TV] {symbol} bias_1h = {bias_val}")
                elif tf == '15m':
                    m['bias_15m'] = bias_val if bias_val != 'neutral' else None
                    logger.info(f"[BIAS TV] {symbol} bias_15m = {bias_val}")

        if alert_type == 'supertrend' and tf == '2d':
            st_2d_val  = parse_supertrend_value(val)
            prev_2d    = m.get('st_2d')
            flipped_2d = (st_2d_val is not None and prev_2d is not None and st_2d_val != prev_2d)
            m['st_2d'] = st_2d_val
            if flipped_2d:
                direction_2d = "LONG" if st_2d_val == 'buy' else "SHORT"
                emoji = "🟢" if direction_2d == "LONG" else "🔴"
                send_telegram(
                    f"{emoji} <b>[ST AI 2D - FLIP]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction_2d}\n"
                    f"💰 Price: ${format_price(price)}\n"
                    f"⏰{datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                    f"📊 SuperTrend AI 2D: {st_2d_val.upper()}"
                )
                logger.info(f"[ST2D] Flip: {symbol} →{direction_2d}")



        if alert_type == 'st_context_lt' and tf == '5m':
            parsed_lt5m = parse_st_context_value(val)
            ST_CONTEXT_LT_5M[symbol] = parsed_lt5m
            m['st_context_lt_5m'] = parsed_lt5m
            m['st_context_lt_5m_ts'] = now_ts

        if alert_type == 'st_context_lt' and tf == '10m':
            parsed_lt10m = parse_st_context_value(val)
            ST_CONTEXT_LT_10M[symbol] = parsed_lt10m
            m['st_context_lt_10m'] = parsed_lt10m
            m['st_context_lt_10m_ts'] = now_ts

        if alert_type == 'st_context':
            parsed_ctx = parse_st_context_value(val)
            if tf == '1h':
                m['st_context_1h'] = parsed_ctx
                m['st_context_1h_ts'] = now_ts
                logger.info(f"[CTX 1H] symbol={symbol} raw={val} parsed={parsed_ctx} ts={now_ts}")
            elif tf == '2h':
                m['st_context_2h'] = parsed_ctx
                m['st_context_2h_ts'] = now_ts
                logger.info(f"[CTX 2H] symbol={symbol} raw={val} parsed={parsed_ctx} ts={now_ts}")
            elif tf == '3m':
                m['st_context_3m'] = parsed_ctx
                m['st_context_3m_ts'] = now_ts
                logger.info(f"[CTX 3M] symbol={symbol} raw={val} parsed={parsed_ctx} ts={now_ts}")
            elif tf == '4h':
                prev_ctx_4h = m.get('st_context_4h')
                m['st_context_4h'] = parsed_ctx
                m['st_context_4h_ts'] = now_ts
                # Clôture CONTEXT4H si ST Context 4H opposé
                pos_c4h = SCALP_POSITIONS.get(f'{symbol}_CONTEXT4H')
                if False and pos_c4h and parsed_ctx and parsed_ctx != prev_ctx_4h:
                    dir_c4h = pos_c4h['direction']
                    exp_ctx = 'buy' if dir_c4h == 'LONG' else 'sell'
                    if parsed_ctx != exp_ctx:
                        send_close_alert(symbol, 'CONTEXT4H', dir_c4h, price, 'ST Context 4H opposé')
            elif tf == '6h':
                m['st_context_6h'] = parsed_ctx
                m['st_context_6h_ts'] = now_ts
            elif tf == '15m':
                prev_ctx_15m_global = ST_CONTEXT_15M.get(symbol)
                ctx15m_zone_changed_this_call = (parsed_ctx is not None and parsed_ctx != prev_ctx_15m_global)
                ST_CONTEXT_15M[symbol] = parsed_ctx
                m['st_context_15m_ts'] = now_ts
            elif tf == '30m':
                prev_ctx_30m_global = ST_CONTEXT_30M.get(symbol)
                ctx30m_zone_changed_this_call = (parsed_ctx is not None and parsed_ctx != prev_ctx_30m_global)
                ST_CONTEXT_30M[symbol] = parsed_ctx
                m['st_context_30m_ts'] = now_ts
            elif tf == '10m':
                m['st_context_10m'] = parsed_ctx
                m['st_context_10m_ts'] = now_ts
            elif tf == '5m':
                m['st_context_5m'] = parsed_ctx
                m['st_context_5m_ts'] = now_ts
                if parsed_ctx in ('buy', 'sell'):
                    m['last_st_context_5m_dir'] = parsed_ctx
                    m['last_st_context_5m_ts'] = now_ts
            elif tf == '1d':
                ST_CONTEXT_1D[symbol] = parsed_ctx
                m['st_context_1d_ts'] = now_ts
            elif tf == '3d':
                prev_ctx_3d = ST_CONTEXT_3D.get(symbol)
                ST_CONTEXT_3D[symbol] = parsed_ctx
                m['st_context_3d_ts'] = now_ts

        if alert_type == 'st_context_lt':
            parsed_ctx_lt = parse_st_context_value(val)
            if tf == '1h':
                ST_CONTEXT_LT_1H[symbol] = parsed_ctx_lt
                m['st_context_lt_1h_ts'] = now_ts
            elif tf == '30m':
                ST_CONTEXT_LT_30M[symbol] = parsed_ctx_lt
                m['st_context_lt_30m'] = parsed_ctx_lt
                m['st_context_lt_30m_ts'] = now_ts
            elif tf == '15m':
                ST_CONTEXT_LT_15M[symbol] = parsed_ctx_lt
                m['st_context_lt_15m_ts'] = now_ts
            elif tf == '10m':
                ST_CONTEXT_LT_10M[symbol] = parsed_ctx_lt
                m['st_context_lt_10m_ts'] = now_ts
            elif tf in ('4h', 'lt_4h'):
                ST_CONTEXT_LT_4H[symbol] = parsed_ctx_lt
                m['st_context_lt_4h_ts'] = now_ts

        if alert_type == 'rmi':
            parsed_rmi = parse_rmi_value(val)
            if parsed_rmi in ('bull', 'bear'):
                if tf == '6h':
                    m['rmi_6h'] = parsed_rmi
                    m['rmi_6h_ts'] = now_ts
                    logger.info(f"[RMI 6H] {symbol} = {parsed_rmi}")
                elif tf == '2h':
                    m['rmi_2h'] = parsed_rmi
                    m['rmi_2h_ts'] = now_ts
                    logger.info(f"[RMI 2H] {symbol} = {parsed_rmi}")

        if alert_type == 'tti':
            parsed_tti = parse_tti_value(val)
            if parsed_tti in ('bull', 'bear', 'sideways'):
                if tf == '6h':
                    m['tti_6h'] = parsed_tti
                    m['tti_6h_ts'] = now_ts
                    logger.info(f"[TTI 6H] {symbol} = {parsed_tti}")
                    evaluate_tti_zone_alert(
                        symbol,
                        '6h',
                        parsed_tti,
                        price=price,
                        exchange_name=exchange_name,
                        event_id=event_id,
                    )
                elif tf == '2h':
                    m['tti_2h'] = parsed_tti
                    m['tti_2h_ts'] = now_ts
                    logger.info(f"[TTI 2H] {symbol} = {parsed_tti}")
                elif tf == '1d':
                    m['tti_1d'] = parsed_tti
                    m['tti_1d_ts'] = now_ts
                    logger.info(f"[TTI 1D] {symbol} = {parsed_tti}")
                    evaluate_tti_zone_alert(
                        symbol,
                        '1d',
                        parsed_tti,
                        price=price,
                        exchange_name=exchange_name,
                        event_id=event_id,
                    )
                evaluate_pulse_v2_take_profit_reminder(
                    symbol,
                    price=price,
                    exchange_name=exchange_name,
                    event_id=event_id,
                )

        if radar_only:
            check_daily_radar_report()
            persist_runtime_state()
            return jsonify({'status': 'ok', 'mode': 'radar_only'}), 200

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
        if strat in ['momentum', 'context', 'scalp', 'pulse', 'daily', 'trend2d', 'all']:
            m = MOMENTUM_STATE[symbol]

            if alert_type == 'supertrend' and tf == '1h':
                prev_1h = m.get('st_1h')
                m['st_1h'] = parse_supertrend_value(val)
                m['st_1h_ts'] = now_ts
                m['st_1h_flipped'] = bool(prev_1h is not None and m['st_1h'] is not None and m['st_1h'] != prev_1h)
                if m['st_1h_flipped'] and prev_1h:
                    m['last_st_1h'] = prev_1h  # guard pyramiding CONTEXT4H
            if alert_type == 'supertrend' and tf == '2h':
                prev_2h = m.get('st_2h')
                m['st_2h'] = parse_supertrend_value(val)
                m['st_ai_2h'] = m['st_2h']
                m['st_ai_2h_ts'] = now_ts
                m['st_2h_flipped'] = bool(prev_2h is not None and m['st_2h'] is not None and m['st_2h'] != prev_2h)
                if m['st_2h_flipped'] and prev_2h:
                    m['last_st_2h'] = prev_2h
            if alert_type == 'supertrend' and tf == '4h':
                prev_4h = m.get('st_4h')
                m['prev_st_4h'] = prev_4h  # sauvegarder avant mise à jour
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
            if alert_type == 'supertrend' and tf == '6h':
                prev_6h = m.get('st_6h')
                m['prev_st_6h'] = prev_6h
                m['st_6h'] = parse_supertrend_value(val)
                m['st_6h_ts'] = now_ts
                m['st_6h_flipped'] = bool(prev_6h is not None and m['st_6h'] is not None and m['st_6h'] != prev_6h)
                if m['st_6h_flipped']:
                    m['last_st_6h'] = prev_6h
            if alert_type == 'supertrend' and tf == '1d':
                st_1d_val = parse_supertrend_value(val)
                m['st_ai_1d'] = st_1d_val
                m['st_ai_1d_ts'] = now_ts
                ST_AI_1D[symbol] = st_1d_val
            if alert_type == 'supertrend' and tf == '15m':
                prev_15m = m.get('st_ai_15m')
                st_15m_val = parse_supertrend_value(val)
                m['st_ai_15m'] = st_15m_val
                st_ai_15m_flipped_this_call = bool(prev_15m and st_15m_val and st_15m_val != prev_15m)
                if st_ai_15m_flipped_this_call:
                    m['last_st_15m'] = prev_15m  # garde la valeur précédente pour le guard
                ST_AI_15M[symbol] = st_15m_val
            if alert_type == 'supertrend' and tf == '30m':
                prev_30m = m.get('st_ai_30m')
                st_30m_val = parse_supertrend_value(val)
                m['st_ai_30m'] = st_30m_val
                m['st_ai_30m_ts'] = now_ts
                st_ai_30m_flipped_this_call = bool(prev_30m and st_30m_val and st_30m_val != prev_30m)
                if st_ai_30m_flipped_this_call:
                    m['last_st_30m'] = prev_30m
                    m['daily_st_ai_30m_flip_dir'] = st_30m_val
                    m['daily_st_ai_30m_flip_ts'] = now_ts
                    m['daily_st_ai_30m_flip_event_id'] = event_id or f"st_ai_30m_flip_{symbol}_{int(now_ts)}_{st_30m_val}"
                ST_AI_30M[symbol] = st_30m_val

        # ========================================================================
        # ========================================================================
        # LOGIQUE CONFLUENCE : ST Context 3D + ST Context 4H aligné → flip ST AI 4H
        # Anti-chop : ST Context 3D opposé OU ADX 1D DI opposé dominant → annulé
        # ========================================================================
        # LOGIQUE CONTEXT4H - SUPPRIMEE :
        # Remplacee par DAILY. Bloc garde en reference mais desactive.
        # ST Context 4H + Bias 1H + Zone ST Context 5m
        # Bonus qualite non-bloquant : Zone ST Context 10m alignee
        # ========================================================================
        if False and strat in ['context', 'context4h', 'all']:
            m = MOMENTUM_STATE[symbol]

            if alert_type in ('st_context', 'bias') and tf in ('4h', '1h', '5m', '10m'):
                ctx_4h_c = m.get('st_context_4h')
                bias_1h_c = m.get('bias_1h')
                ctx_5m_c = m.get('st_context_5m')
                ctx_10m_c = m.get('st_context_10m')

                if ctx_4h_c is not None and ctx_5m_c is not None:
                    direction_c = 'LONG' if ctx_5m_c == 'buy' else 'SHORT'
                    exp_ctx_c = 'buy' if direction_c == 'LONG' else 'sell'
                    exp_bias_c = 'bull' if direction_c == 'LONG' else 'bear'
                    ctx10m_bonus_c = ctx_10m_c == exp_ctx_c

                    principal_ok_c = (
                        ctx_4h_c == exp_ctx_c
                        and bias_1h_c == exp_bias_c
                        and ctx_5m_c == exp_ctx_c
                    )

                    logger.info(
                        f"[CONTEXT4H CHECK] {symbol} dir={direction_c} "
                        f"ctx4h={ctx_4h_c} bias1h={bias_1h_c} ctx5m={ctx_5m_c} "
                        f"ctx10m={ctx_10m_c} ctx10m_bonus={ctx10m_bonus_c} principal={principal_ok_c}"
                    )

                    pos_key_c = f"{symbol}_CONTEXT4H"
                    with STATE_LOCK:
                        pos_c = SCALP_POSITIONS.get(pos_key_c)
                        if pos_c and pos_c['direction'] != direction_c:
                            SCALP_POSITIONS.pop(pos_key_c, None)
                            PYRA_ENABLED.pop(pos_key_c, None)
                            pos_c = None
                        is_entry_c = bool(principal_ok_c and (pos_c is None or pos_c.get('signal_type') != 'principal_ctx4h_bias1h_ctx5m'))
                        if is_entry_c and should_send(symbol, f"context4h_principal_{exp_ctx_c}", event_id=event_id, cooldown=14400):
                            SCALP_POSITIONS[pos_key_c] = {
                                'direction': direction_c,
                                'entry_count': 1,
                                'signal_type': 'principal_ctx4h_bias1h_ctx5m',
                            }
                            PYRA_ENABLED.pop(pos_key_c, None)
                            pos_c = SCALP_POSITIONS[pos_key_c]
                        else:
                            is_entry_c = False

                    if is_entry_c and pos_c:
                        emoji = "\U0001f7e2" if direction_c == "LONG" else "\U0001f534"
                        bonus_10m_txt_c = (
                            "\u2b50 <b>BONUS 10M ALIGNÉ</b>\n"
                            if ctx10m_bonus_c else ""
                        )
                        send_telegram_with_buttons(
                            f"{emoji} <b>[CONTEXT4H - ENTREE]</b> {symbol}\n"
                            f"--------------------\n"
                            f"Direction: {direction_c}\n"
                            f"Price: ${format_price(price)}\n"
                            f"Exchange: {exchange_name.upper()}\n"
                            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                            f"{bonus_10m_txt_c}"
                            f"[OK] ST Context 4H: {(ctx_4h_c or 'N/A').upper()}\n"
                            f"[OK] Bias 1H: {(bias_1h_c or 'N/A').upper()}\n"
                            f"[OK] Zone ST Context 5m: {(ctx_5m_c or 'N/A').upper()}\n"
                            f"[BONUS] Zone ST Context 10m: {(ctx_10m_c or 'NEUTRE').upper()}\n"
                            f"{get_market_context_info()}",
                            f"{symbol}_CONTEXT4H",
                            journal_symbol=symbol, journal_strategy='CONTEXT4H',
                            journal_direction=direction_c, journal_price=price
                        )
                        track_alert(symbol, 'CONTEXT4H')
                        logger.info(f"[CONTEXT4H] Entree principale: {symbol} {direction_c}")

            # LOGIQUE CONTEXT4H - ENTREE SECONDAIRE :
            # ST Context 4H CT aligne + ST Context 30m aligne + ST Context LT 1H aligne
            # + ST Context CT 1H neutre -> flip ST AI 1H
            # ================================================================
            if alert_type == 'supertrend' and tf == '1h' and m.get('st_1h_flipped'):
                st_1h_val_c2 = m.get('st_1h')
                if st_1h_val_c2 is not None:
                    direction_c2 = 'LONG' if st_1h_val_c2 == 'buy' else 'SHORT'
                    exp_c2 = 'buy' if direction_c2 == 'LONG' else 'sell'

                    ctx_4h_ct_c2  = m.get('st_context_4h')
                    ctx_30m_c2    = ST_CONTEXT_30M.get(symbol)
                    ctx_lt_1h_c2  = ST_CONTEXT_LT_1H.get(symbol)
                    ctx_ct_1h_c2  = m.get('st_context_1h')

                    ctx4h_fresh_c2  = bool(ctx_4h_ct_c2) and is_signal_fresh(m.get('st_context_4h_ts'), 12 * 3600)
                    ctx30m_fresh_c2 = bool(ctx_30m_c2) and is_signal_fresh(m.get('st_context_30m_ts'), 90 * 60)
                    lt1h_fresh_c2   = bool(ctx_lt_1h_c2) and is_signal_fresh(m.get('st_context_lt_1h_ts'), 3 * 3600)
                    ctx1h_fresh_c2  = is_signal_fresh(m.get('st_context_1h_ts'), 3 * 3600)

                    ct_4h_ok_c2      = ctx4h_fresh_c2 and ctx_4h_ct_c2 == exp_c2
                    ctx_30m_ok_c2    = ctx30m_fresh_c2 and ctx_30m_c2 == exp_c2
                    lt_1h_ok_c2      = lt1h_fresh_c2 and ctx_lt_1h_c2 == exp_c2
                    # "CT 1H neutre" doit etre une lecture recente qui vaut None, pas une
                    # absence/valeur perimee confondue avec un vrai neutre.
                    ct_1h_neutral_c2 = ctx1h_fresh_c2 and ctx_ct_1h_c2 is None

                    secondary_ok_c2 = ct_4h_ok_c2 and ctx_30m_ok_c2 and lt_1h_ok_c2 and ct_1h_neutral_c2

                    logger.info(
                        f"[CONTEXT4H CHECK SECONDAIRE] {symbol} dir={direction_c2} "
                        f"ctx4h={ctx_4h_ct_c2}/{exp_c2} fresh={ctx4h_fresh_c2} "
                        f"ctx30m={ctx_30m_c2}/{exp_c2} fresh={ctx30m_fresh_c2} "
                        f"lt1h={ctx_lt_1h_c2}/{exp_c2} fresh={lt1h_fresh_c2} "
                        f"ctx1h={ctx_ct_1h_c2} fresh={ctx1h_fresh_c2} neutral={ct_1h_neutral_c2} "
                        f"secondary={secondary_ok_c2}"
                    )

                    pos_key_c2 = f"{symbol}_CONTEXT4H"
                    with STATE_LOCK:
                        pos_c2 = SCALP_POSITIONS.get(pos_key_c2)
                        if pos_c2 and pos_c2['direction'] != direction_c2:
                            SCALP_POSITIONS.pop(pos_key_c2, None)
                            PYRA_ENABLED.pop(pos_key_c2, None)
                            pos_c2 = None
                        is_entry_c2 = bool(secondary_ok_c2 and (pos_c2 is None or pos_c2.get('signal_type') != 'secondary_ctx4h_30m_lt1h_neutral1h'))
                        if is_entry_c2 and should_send(symbol, f"context4h_secondary_{exp_c2}", event_id=event_id, cooldown=14400):
                            SCALP_POSITIONS[pos_key_c2] = {
                                'direction': direction_c2,
                                'entry_count': 1,
                                'signal_type': 'secondary_ctx4h_30m_lt1h_neutral1h',
                            }
                            PYRA_ENABLED.pop(pos_key_c2, None)
                            pos_c2 = SCALP_POSITIONS[pos_key_c2]
                        else:
                            is_entry_c2 = False

                    if is_entry_c2 and pos_c2:
                        emoji = "\U0001f7e2" if direction_c2 == "LONG" else "\U0001f534"
                        send_telegram_with_buttons(
                            f"{emoji} <b>[CONTEXT4H - ENTREE SECONDAIRE]</b> {symbol}\n"
                            f"--------------------\n"
                            f"Direction: {direction_c2}\n"
                            f"Price: ${format_price(price)}\n"
                            f"Exchange: {exchange_name.upper()}\n"
                            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                            f"[OK] ST Context 4H CT: {(ctx_4h_ct_c2 or 'N/A').upper()}\n"
                            f"[OK] ST Context 30m: {(ctx_30m_c2 or 'N/A').upper()}\n"
                            f"[OK] ST Context LT 1H: {(ctx_lt_1h_c2 or 'N/A').upper()}\n"
                            f"[OK] ST Context CT 1H: NEUTRE\n"
                            f"[OK] Flip ST AI 1H: {st_1h_val_c2.upper()}\n"
                            f"{get_market_context_info()}",
                            f"{symbol}_CONTEXT4H",
                            journal_symbol=symbol, journal_strategy='CONTEXT4H',
                            journal_direction=direction_c2, journal_price=price
                        )
                        track_alert(symbol, 'CONTEXT4H')
                        logger.info(f"[CONTEXT4H] Entree secondaire: {symbol} {direction_c2}")

            # LOGIQUE CONTEXT4H - PYRAMIDING :
            # Nouvelle zone ST Context 30m dans le sens de la position ouverte
            # Cooldown 4H, necessite activation manuelle (bouton Telegram)
            # ================================================================
            if alert_type == 'st_context' and tf == '30m':
                ctx_30m_pyra = ST_CONTEXT_30M.get(symbol)
                if ctx_30m_pyra is not None:
                    direction_pyra_c4h = 'LONG' if ctx_30m_pyra == 'buy' else 'SHORT'
                    pos_key_c3 = f"{symbol}_CONTEXT4H"
                    with STATE_LOCK:
                        pos_c3 = SCALP_POSITIONS.get(pos_key_c3)
                        is_pyra_c4h = bool(
                            pos_c3 and pos_c3['direction'] == direction_pyra_c4h
                            and ctx30m_zone_changed_this_call
                            and PYRA_ENABLED.get(pos_key_c3, False)
                        )
                        if is_pyra_c4h and should_send(symbol, f"context4h_pyra_{ctx_30m_pyra}", event_id=event_id, cooldown=14400):
                            pos_c3['entry_count'] += 1
                            entry_count_c4h = pos_c3['entry_count']
                        else:
                            is_pyra_c4h = False

                    if is_pyra_c4h:
                        emoji = "\U0001f7e2" if direction_pyra_c4h == "LONG" else "\U0001f534"
                        send_telegram_ttmtf(
                            f"{emoji} <b>[CONTEXT4H - PYRAMIDING #{entry_count_c4h}]</b> {symbol}\n"
                            f"--------------------\n"
                            f"Direction: {direction_pyra_c4h}\n"
                            f"Price: ${format_price(price)}\n"
                            f"Exchange: {exchange_name.upper()}\n"
                            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                            f"[OK] Nouvelle zone ST Context 30m: {ctx_30m_pyra.upper()}\n"
                            f"{get_market_context_info()}"
                        )
                        track_alert(symbol, 'CONTEXT4H')
                        logger.info(f"[CONTEXT4H] Pyramiding #{entry_count_c4h}: {symbol} {direction_pyra_c4h}")

        # ========================================================================
        # LOGIQUE DAILY :
        # Bias 1D + ST AI 1D alignes + Zone ST Context 30m
        # Signal : flip ST AI 30m
        # Bonus tres haute qualite : ST Context 1D aligne
        # Anti-chop : ST Context LT 30m meme sens => bloque
        # ========================================================================
        if strat in ['daily', 'all']:
            m = MOMENTUM_STATE[symbol]

            # Remplacee par l'entree DAILY sur Range Filter 30m.
            if False and alert_type == 'supertrend' and tf == '30m' and st_ai_30m_flipped_this_call:
                st_30m_d = m.get('st_ai_30m')
                if st_30m_d is not None:
                    direction_d = 'LONG' if st_30m_d == 'buy' else 'SHORT'
                    exp_ctx_d = 'buy' if direction_d == 'LONG' else 'sell'
                    opp_ctx_d = 'sell' if direction_d == 'LONG' else 'buy'
                    exp_bias_d = 'bull' if direction_d == 'LONG' else 'bear'

                    bias_1d_d = m.get('bias_1d')
                    st_1d_d = m.get('st_ai_1d') or ST_AI_1D.get(symbol)
                    ctx_2h_d = m.get('st_context_2h')
                    ctx_30m_d = ST_CONTEXT_30M.get(symbol)
                    ctx_lt_30m_d = ST_CONTEXT_LT_30M.get(symbol)
                    ctx_1d_d = ST_CONTEXT_1D.get(symbol)

                    ctx_2h_fresh_d = bool(ctx_2h_d) and is_signal_fresh(m.get('st_context_2h_ts'), 6 * 3600)
                    ctx_30m_fresh_d = bool(ctx_30m_d) and is_signal_fresh(m.get('st_context_30m_ts'), 90 * 60)
                    ctx_lt_30m_fresh_d = bool(ctx_lt_30m_d) and is_signal_fresh(m.get('st_context_lt_30m_ts'), 90 * 60)
                    ctx_1d_fresh_d = bool(ctx_1d_d) and is_signal_fresh(m.get('st_context_1d_ts'), 36 * 3600)
                    st_1d_fresh_d = bool(st_1d_d) and is_signal_fresh(m.get('st_ai_1d_ts'), 36 * 3600)

                    bias_1d_ok_d = bias_1d_d == exp_bias_d
                    st_1d_ok_d = st_1d_fresh_d and st_1d_d == exp_ctx_d
                    ctx_30m_ok_d = ctx_30m_fresh_d and ctx_30m_d == exp_ctx_d
                    lt_30m_block_d = ctx_lt_30m_fresh_d and ctx_lt_30m_d == exp_ctx_d
                    ctx_2h_warning_d = ctx_2h_fresh_d and ctx_2h_d == opp_ctx_d
                    daily_plus_d = ctx_1d_fresh_d and ctx_1d_d == exp_ctx_d
                    daily_ok_d = bias_1d_ok_d and st_1d_ok_d and ctx_30m_ok_d and not lt_30m_block_d
                    signal_type_d = 'daily_plus' if daily_plus_d else 'daily'

                    logger.info(
                        f"[DAILY CHECK] {symbol} dir={direction_d} "
                        f"bias1d={bias_1d_d}/{exp_bias_d} st1d={st_1d_d}/{exp_ctx_d} st1d_fresh={st_1d_fresh_d} "
                        f"ctx30m={ctx_30m_d}/{exp_ctx_d} fresh={ctx_30m_fresh_d} "
                        f"ctx2h={ctx_2h_d}/{opp_ctx_d} fresh={ctx_2h_fresh_d} warning={ctx_2h_warning_d} "
                        f"lt30m={ctx_lt_30m_d} fresh={ctx_lt_30m_fresh_d} block={lt_30m_block_d} "
                        f"ctx1d_bonus={ctx_1d_d} fresh={ctx_1d_fresh_d} daily_plus={daily_plus_d} ok={daily_ok_d}"
                    )

                    pos_key_d = f"{symbol}_DAILY"
                    with STATE_LOCK:
                        pos_d = SCALP_POSITIONS.get(pos_key_d)
                        if pos_d and pos_d['direction'] != direction_d:
                            SCALP_POSITIONS.pop(pos_key_d, None)
                            PYRA_ENABLED.pop(pos_key_d, None)
                            pos_d = None
                        is_entry_d = bool(daily_ok_d and (pos_d is None or pos_d.get('signal_type') != signal_type_d))
                        if is_entry_d and should_send(symbol, f"daily_entry_{signal_type_d}_{exp_ctx_d}", event_id=event_id, cooldown=14400):
                            SCALP_POSITIONS[pos_key_d] = {
                                'direction': direction_d,
                                'entry_count': 1,
                                'signal_type': signal_type_d,
                            }
                            PYRA_ENABLED.pop(pos_key_d, None)
                            pos_d = SCALP_POSITIONS[pos_key_d]
                        else:
                            is_entry_d = False

                    if is_entry_d and pos_d:
                        emoji = "\U0001f7e2" if direction_d == "LONG" else "\U0001f534"
                        title_d = "[DAILY++ - ENTREE]" if daily_plus_d else "[DAILY - ENTREE]"
                        plus_txt_d = (
                            "\u2b50 <b>DAILY++ / TRES HAUTE QUALITE</b> (ST Context 1D aligne)\n\n"
                            if daily_plus_d else ""
                        )
                        warning_2h_txt_d = (
                            "\u26a0\ufe0f <b>WARNING NON BLOQUANT</b> : ST Context 2H oppose\n"
                            if ctx_2h_warning_d else ""
                        )
                        send_telegram_with_buttons(
                            f"{emoji} <b>{title_d}</b> {symbol}\n"
                            f"--------------------\n"
                            f"{plus_txt_d}"
                            f"Direction: {direction_d}\n"
                            f"Price: ${format_price(price)}\n"
                            f"Exchange: {exchange_name.upper()}\n"
                            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                            f"[OK] Bias 1D: {(bias_1d_d or 'N/A').upper()} (EMA17/SMA40)\n"
                            f"[OK] ST AI 1D: {(st_1d_d or 'N/A').upper()}\n"
                            f"[OK] Zone ST Context 30m: {(ctx_30m_d or 'N/A').upper()}\n"
                            f"[OK] Flip ST AI 30m: {st_30m_d.upper()}\n"
                            f"{warning_2h_txt_d}"
                            f"[ANTI-CHOP] LT 30m: {(ctx_lt_30m_d or 'NEUTRE').upper()}\n"
                            f"[BONUS] ST Context 1D: {(ctx_1d_d or 'NEUTRE').upper()}\n"
                            f"{get_market_context_info()}",
                            f"{symbol}_DAILY",
                            journal_symbol=symbol, journal_strategy='DAILY',
                            journal_direction=direction_d, journal_price=price
                        )
                        track_alert(symbol, 'DAILY')
                        logger.info(f"[DAILY] Entree {signal_type_d}: {symbol} {direction_d}")

            # Entree secondaire DAILY :
            # Bias 1D + ST Context 2H + ST Context 10m alignes. Williams 2D en bonus.
            if (
                (alert_type == 'st_context' and tf in ('2h', '10m'))
                or (alert_type == 'bias' and tf == '1d')
            ):
                bias_1d_s = m.get('bias_1d')
                ctx_2h_s = m.get('st_context_2h')
                ctx_10m_s = m.get('st_context_10m')

                bias_1d_fresh_s = bool(bias_1d_s) and is_signal_fresh(m.get('bias_1d_ts'), 36 * 3600)
                ctx_2h_fresh_s = bool(ctx_2h_s) and is_signal_fresh(m.get('st_context_2h_ts'), 6 * 3600)
                ctx_10m_fresh_s = bool(ctx_10m_s) and is_signal_fresh(m.get('st_context_10m_ts'), 30 * 60)

                direction_s = None
                if bias_1d_s == 'bull' and ctx_2h_s == 'buy' and ctx_10m_s == 'buy':
                    direction_s = 'LONG'
                elif bias_1d_s == 'bear' and ctx_2h_s == 'sell' and ctx_10m_s == 'sell':
                    direction_s = 'SHORT'

                williams_2d_s = get_williams_filter(symbol, '2d', direction_s, 72 * 3600) if direction_s else {
                    'value': None, 'ema': None, 'trend': None, 'fresh': False, 'ok': False
                }
                daily_secondary_ok = bool(direction_s and bias_1d_fresh_s and ctx_2h_fresh_s and ctx_10m_fresh_s)

                logger.info(
                    f"[DAILY CHECK SECONDAIRE] {symbol} dir={direction_s} "
                    f"bias1d={bias_1d_s} fresh={bias_1d_fresh_s} "
                    f"ctx2h={ctx_2h_s} fresh={ctx_2h_fresh_s} "
                    f"ctx10m={ctx_10m_s} fresh={ctx_10m_fresh_s} "
                    f"will2d={williams_2d_s['value']}/{williams_2d_s['ema']} trend={williams_2d_s['trend']} fresh={williams_2d_s['fresh']} ok={williams_2d_s['ok']} "
                    f"ok={daily_secondary_ok}"
                )

                if daily_secondary_ok:
                    exp_ctx_s = 'buy' if direction_s == 'LONG' else 'sell'
                    pos_key_s = f"{symbol}_DAILY"
                    with STATE_LOCK:
                        pos_s = SCALP_POSITIONS.get(pos_key_s)
                        if pos_s and pos_s['direction'] != direction_s:
                            SCALP_POSITIONS.pop(pos_key_s, None)
                            PYRA_ENABLED.pop(pos_key_s, None)
                            pos_s = None
                        is_entry_s = bool(pos_s is None or pos_s.get('signal_type') != 'daily_secondaire')
                        if is_entry_s and should_send(symbol, f"daily_entry_secondaire_{exp_ctx_s}", event_id=event_id, cooldown=14400):
                            SCALP_POSITIONS[pos_key_s] = {
                                'direction': direction_s,
                                'entry_count': 1,
                                'signal_type': 'daily_secondaire',
                            }
                            PYRA_ENABLED.pop(pos_key_s, None)
                            pos_s = SCALP_POSITIONS[pos_key_s]
                        else:
                            is_entry_s = False

                    if is_entry_s and pos_s:
                        emoji = "\U0001f7e2" if direction_s == "LONG" else "\U0001f534"
                        send_telegram_with_buttons(
                            f"{emoji} <b>[DAILY - ENTREE SECONDAIRE]</b> {symbol}\n"
                            f"--------------------\n"
                            f"Direction: {direction_s}\n"
                            f"Price: ${format_price(price)}\n"
                            f"Exchange: {exchange_name.upper()}\n"
                            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                            f"[OK] Bias 1D: {(bias_1d_s or 'N/A').upper()} (EMA17/SMA40)\n"
                            f"{format_optional_williams_line('2D', williams_2d_s)}\n"
                            f"[OK] Zone ST Context 2H: {(ctx_2h_s or 'N/A').upper()}\n"
                            f"[OK] Zone ST Context 10m: {(ctx_10m_s or 'N/A').upper()}\n"
                            f"{get_market_context_info()}",
                            f"{symbol}_DAILY",
                            journal_symbol=symbol, journal_strategy='DAILY',
                            journal_direction=direction_s, journal_price=price
                        )
                        track_alert(symbol, 'DAILY')
                        logger.info(f"[DAILY] Entree secondaire: {symbol} {direction_s}")

        # ========================================================================
        # ========================================================================
        # LOGIQUE PULSE :
        # Entree : flip ST AI 30m + ST AI 6H + Zone ST Context 30m
        # Jackpot : ST Context 30m + ST Context 5m alignes
        # Qualite : ST AI 1D aligne
        # Pyramiding : ST AI 6H + Bias 2H + ST Context 5m
        # ========================================================================
        if False and (strat in ['pulse', 'all'] or (alert_type == 'range_filter' and tf == '30m')):
            m = MOMENTUM_STATE[symbol]

            if alert_type == 'supertrend' and tf == '30m' and st_ai_30m_flipped_this_call:
                st_30m_signal_p = m.get('st_ai_30m')
                ctx_5m_p = m.get('st_context_5m')
                if st_30m_signal_p is not None:
                    direction_p = 'LONG' if st_30m_signal_p == 'buy' else 'SHORT'
                    exp_ctx = 'buy' if direction_p == 'LONG' else 'sell'
                    opp_ctx = 'sell' if direction_p == 'LONG' else 'buy'
                    exp_bias = 'bull' if direction_p == 'LONG' else 'bear'

                    bias_2h_v = m.get('bias_2h')
                    st_6h_v = m.get('st_6h') or m.get('st_ai_6h')
                    st_1d_v = m.get('st_ai_1d') or ST_AI_1D.get(symbol)
                    ctx_6h_p = m.get('st_context_6h')
                    ctx_30m_p = ST_CONTEXT_30M.get(symbol)

                    ctx_5m_ok = ctx_5m_p == exp_ctx
                    ctx30m_fresh_p = bool(ctx_30m_p) and is_signal_fresh(m.get('st_context_30m_ts'), 90 * 60)
                    ctx6h_fresh_p = bool(ctx_6h_p) and is_signal_fresh(m.get('st_context_6h_ts'), 18 * 3600)
                    st_1d_fresh_p = bool(st_1d_v) and is_signal_fresh(m.get('st_ai_1d_ts'), 36 * 3600)
                    ctx30m_ok_p = ctx30m_fresh_p and ctx_30m_p == exp_ctx
                    jackpot_p = ctx30m_ok_p and ctx_5m_ok
                    daily_quality_p = st_1d_fresh_p and st_1d_v == exp_ctx
                    st_6h_fresh = bool(st_6h_v) and is_signal_fresh(m.get('st_6h_ts'), 9 * 3600)
                    st_6h_ok = st_6h_fresh and st_6h_v == exp_ctx

                    primary_ok = st_6h_ok and ctx30m_ok_p
                    all_ok = primary_ok
                    signal_type_p = 'principal' if primary_ok else 'blocked'

                    logger.info(
                        f"[PULSE CHECK] {symbol} dir={direction_p} "
                        f"st30m_flip={st_30m_signal_p}/{exp_ctx} "
                        f"ctx30m={ctx_30m_p}/{exp_ctx} fresh={ctx30m_fresh_p} "
                        f"ctx5m={ctx_5m_p}/{exp_ctx} jackpot={jackpot_p} "
                        f"ctx6h={ctx_6h_p} fresh={ctx6h_fresh_p} "
                        f"st6h={st_6h_v} fresh={st_6h_fresh} "
                        f"st1d={st_1d_v} fresh={st_1d_fresh_p} daily_quality={daily_quality_p} "
                        f"primary={primary_ok}"
                    )
                    if not all_ok:
                        logger.info(
                            f"[PULSE BLOCKED] {symbol} raison=no_entry "
                            f"st6h:{st_6h_ok},ctx30m:{ctx30m_ok_p}"
                        )

                    pos_key_p = f"{symbol}_PULSE"
                    with STATE_LOCK:
                        pos_p = SCALP_POSITIONS.get(pos_key_p)
                        if pos_p and pos_p['direction'] != direction_p:
                            SCALP_POSITIONS.pop(pos_key_p, None)
                            PYRA_ENABLED.pop(pos_key_p, None)
                            pos_p = None
                        is_entry_p = bool(all_ok and (pos_p is None or pos_p.get('signal_type') != signal_type_p))
                        if is_entry_p and should_send(symbol, f"pulse_entry_{signal_type_p}_{exp_ctx}", event_id=event_id, cooldown=3600):
                            SCALP_POSITIONS[pos_key_p] = {
                                'direction': direction_p,
                                'entry_count': 1,
                                'signal_type': signal_type_p,
                            }
                            PYRA_ENABLED.pop(pos_key_p, None)
                            pos_p = SCALP_POSITIONS[pos_key_p]
                        else:
                            is_entry_p = False

                    if is_entry_p and pos_p:
                        emoji = "\U0001f7e2" if direction_p == "LONG" else "\U0001f534"
                        title = "[PULSE - ENTREE]"
                        quality_txt = (
                            ("\u2b50 <b>JACKPOT</b> (ST Context 30m + 5m alignes)\n" if jackpot_p else "")
                            + ("\u2b50 <b>QUALITE DAILY</b> (ST AI 1D aligne)\n" if daily_quality_p else "")
                        )
                        if quality_txt:
                            quality_txt += "\n"
                        send_telegram_with_buttons(
                            f"{emoji} <b>{title}</b> {symbol}\n"
                            f"--------------------\n"
                            f"{quality_txt}"
                            f"Direction: {direction_p}\n"
                            f"Price: ${format_price(price)}\n"
                            f"Exchange: {exchange_name.upper()}\n"
                            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                            f"[OK] Flip ST AI 30m: {(st_30m_signal_p or 'N/A').upper()}\n"
                            f"[OK] ST AI 6H: {(st_6h_v or 'N/A').upper()}\n"
                            f"[OK] Zone ST Context 30m: {(ctx_30m_p or 'N/A').upper()}\n"
                            f"[INFO] Zone ST Context 5m: {(ctx_5m_p or 'NEUTRE').upper()}\n"
                            f"[INFO] Derniere zone ST Context 6H: {(ctx_6h_p or 'NEUTRE').upper()}\n"
                            f"[INFO] ST AI 1D: {(st_1d_v or 'N/A').upper()}\n"
                            f"{get_market_context_info()}",
                            f"{symbol}_PULSE",
                            journal_symbol=symbol, journal_strategy='PULSE',
                            journal_direction=direction_p, journal_price=price
                        )
                        track_alert(symbol, 'PULSE')
                        logger.info(f"[PULSE] Entree {signal_type_p}: {symbol} {direction_p}")

            # Pyramiding PULSE :
            # Position deja ouverte + bouton active + flip ST AI 30m dans le sens,
            # avec ST AI 6H + Bias 2H + ST Context 5m alignes.
            if alert_type == 'supertrend' and tf == '30m':
                pos_key_pu = f"{symbol}_PULSE"
                with STATE_LOCK:
                    pos_pu = SCALP_POSITIONS.get(pos_key_pu)

                if pos_pu:
                    direction_pu = pos_pu.get('direction')
                    exp_ctx_pu = 'buy' if direction_pu == 'LONG' else 'sell'
                    exp_bias_pu = 'bull' if direction_pu == 'LONG' else 'bear'

                    st_30m_pu = m.get('st_ai_30m')
                    last_st_30m_pu = m.get('last_st_30m')
                    st_6h_pu = m.get('st_6h') or m.get('st_ai_6h')
                    st_1d_pu = m.get('st_ai_1d') or ST_AI_1D.get(symbol)
                    bias_2h_pu = m.get('bias_2h')
                    ctx_5m_pu = m.get('st_context_5m')
                    last_ctx_5m_pu = m.get('last_st_context_5m_dir')
                    last_ctx_5m_ts_pu = m.get('last_st_context_5m_ts')
                    ctx_6h_pu = m.get('st_context_6h')

                    flip_30m_pu = bool(last_st_30m_pu and st_30m_pu == exp_ctx_pu and last_st_30m_pu != st_30m_pu)
                    st_6h_fresh_pu = bool(st_6h_pu) and is_signal_fresh(m.get('st_6h_ts'), 9 * 3600)
                    st_6h_ok_pu = st_6h_fresh_pu and st_6h_pu == exp_ctx_pu
                    bias_2h_ok_pu = bias_2h_pu == exp_bias_pu
                    recent_ctx_5m_ok_pu = (
                        last_ctx_5m_pu == exp_ctx_pu
                        and is_signal_fresh(last_ctx_5m_ts_pu, 3600)
                    )
                    ctx_5m_ok_pu = ctx_5m_pu == exp_ctx_pu or recent_ctx_5m_ok_pu
                    st_1d_fresh_pu = bool(st_1d_pu) and is_signal_fresh(m.get('st_ai_1d_ts'), 36 * 3600)
                    daily_quality_pu = st_1d_fresh_pu and st_1d_pu == exp_ctx_pu

                    logger.info(
                        f"[PULSE PYRA CHECK] {symbol} dir={direction_pu} "
                        f"st30m={st_30m_pu} last_st30m={last_st_30m_pu} "
                        f"st6h={st_6h_pu} fresh={st_6h_fresh_pu} "
                        f"bias2h={bias_2h_pu} ctx5m={ctx_5m_pu} recent_ctx5m={last_ctx_5m_pu} recent_ok={recent_ctx_5m_ok_pu} "
                        f"ctx6h={ctx_6h_pu} st1d={st_1d_pu} daily_quality={daily_quality_pu} "
                        f"flip={flip_30m_pu}"
                    )

                    can_pyra_pu = (
                        flip_30m_pu
                        and st_6h_ok_pu
                        and bias_2h_ok_pu
                        and ctx_5m_ok_pu
                        and PYRA_ENABLED.get(pos_key_pu, False)
                    )

                    if can_pyra_pu and should_send(symbol, f"pulse_pyra_{exp_ctx_pu}", event_id=event_id, cooldown=1800):
                        with STATE_LOCK:
                            pos_pu = SCALP_POSITIONS.get(pos_key_pu)
                            if pos_pu and pos_pu.get('direction') == direction_pu:
                                pos_pu['entry_count'] = int(pos_pu.get('entry_count', 1)) + 1
                                entry_count_pu = pos_pu['entry_count']
                            else:
                                entry_count_pu = None

                        if entry_count_pu:
                            emoji = "\U0001f7e2" if direction_pu == "LONG" else "\U0001f534"
                            quality_pu_txt = (
                                "\u2b50 <b>QUALITE DAILY</b> (ST AI 1D aligne)\n\n"
                                if daily_quality_pu else ""
                            )
                            send_telegram(
                                f"{emoji} <b>[PULSE - PYRAMIDING #{entry_count_pu}]</b> {symbol}\n"
                                f"--------------------\n"
                                f"{quality_pu_txt}"
                                f"Direction: {direction_pu}\n"
                                f"Price: ${format_price(price)}\n"
                                f"Exchange: {exchange_name.upper()}\n"
                                f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                                f"[OK] Flip ST AI 30m: {(st_30m_pu or 'N/A').upper()}\n"
                                f"[OK] ST AI 6H: {(st_6h_pu or 'N/A').upper()}\n"
                                f"[OK] Bias 2H: {(bias_2h_pu or 'N/A').upper()}\n"
                                f"[OK] Zone ST Context 5m recente: {(last_ctx_5m_pu or ctx_5m_pu or 'N/A').upper()}\n"
                                f"[INFO] Derniere zone ST Context 6H: {(ctx_6h_pu or 'NEUTRE').upper()}\n"
                                f"[INFO] ST AI 1D: {(st_1d_pu or 'N/A').upper()}\n"
                                f"{get_market_context_info()}",
                                ntfy=True,
                            )
                            logger.info(f"[PULSE] Pyramiding #{entry_count_pu}: {symbol} {direction_pu}")
                            # Consomme le guard seulement une fois le pyramiding reellement declenche :
                            # tant qu'il est refuse (cooldown/bouton/bias/anti-chop), le flip reste disponible.
                            m['last_st_30m'] = st_30m_pu

            # Range Filter 30m : stockage uniquement.
            # Les entrees actives passent par le gestionnaire range_filter plus bas.
            if alert_type == 'range_filter' and tf == '30m':
                range_30m_dir = parse_range_filter_value(val)
                if range_30m_dir is not None:
                    m['range_filter_30m'] = range_30m_dir
                    m['range_filter_30m_ts'] = now_ts
                    RANGE_FILTER_30M[symbol] = range_30m_dir

            # Ancienne entree troisieme PULSE supprimee :
            # remplacee par la nouvelle strategie DAILY.
            # ================================================================
            if False and alert_type == 'supertrend' and tf == '30m' and st_ai_30m_flipped_this_call:
                st_30m_val_p3 = m.get('st_ai_30m')
                if st_30m_val_p3 is not None:
                    direction_p3 = 'LONG' if st_30m_val_p3 == 'buy' else 'SHORT'
                    exp_p3 = 'buy' if direction_p3 == 'LONG' else 'sell'
                    exp_bias_p3 = 'bull' if direction_p3 == 'LONG' else 'bear'

                    bias_6h_p3 = m.get('bias_6h')
                    st_6h_p3 = m.get('st_6h') or m.get('st_ai_6h')
                    ctx_30m_p3 = ST_CONTEXT_30M.get(symbol)
                    ctx_30m_fresh_p3 = bool(ctx_30m_p3) and is_signal_fresh(m.get('st_context_30m_ts'), 90 * 60)

                    bias_6h_ok_p3 = bias_6h_p3 == exp_bias_p3
                    st_6h_info_p3 = st_6h_p3 == exp_p3
                    ctx_30m_ok_p3 = ctx_30m_fresh_p3 and ctx_30m_p3 == exp_p3
                    third_ok = bias_6h_ok_p3 and ctx_30m_ok_p3

                    logger.info(
                        f"[PULSE CHECK TROISIEME] {symbol} dir={direction_p3} "
                        f"bias6h={bias_6h_p3}/{exp_bias_p3} st6h_info={st_6h_p3}/{exp_p3} st6h_aligned={st_6h_info_p3} "
                        f"ctx30m={ctx_30m_p3}/{exp_p3} fresh={ctx_30m_fresh_p3} third_ok={third_ok}"
                    )

                    pos_key_p3 = f"{symbol}_PULSE"
                    with STATE_LOCK:
                        pos_p3 = SCALP_POSITIONS.get(pos_key_p3)
                        if pos_p3 and pos_p3['direction'] != direction_p3:
                            SCALP_POSITIONS.pop(pos_key_p3, None)
                            PYRA_ENABLED.pop(pos_key_p3, None)
                            pos_p3 = None
                        is_entry_p3 = bool(third_ok and (pos_p3 is None or pos_p3.get('signal_type') != 'troisieme'))
                        if is_entry_p3 and should_send(symbol, f"pulse_entry_troisieme_{exp_p3}", event_id=event_id, cooldown=3600):
                            SCALP_POSITIONS[pos_key_p3] = {
                                'direction': direction_p3,
                                'entry_count': 1,
                                'signal_type': 'troisieme',
                            }
                            PYRA_ENABLED.pop(pos_key_p3, None)
                            pos_p3 = SCALP_POSITIONS[pos_key_p3]
                        else:
                            is_entry_p3 = False

                    if is_entry_p3 and pos_p3:
                        emoji = "\U0001f7e2" if direction_p3 == "LONG" else "\U0001f534"
                        send_telegram_with_buttons(
                            f"{emoji} <b>[PULSE - ENTREE TROISIEME]</b> {symbol}\n"
                            f"--------------------\n"
                            f"Direction: {direction_p3}\n"
                            f"Price: ${format_price(price)}\n"
                            f"Exchange: {exchange_name.upper()}\n"
                            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                            f"[OK] Bias 6H: {(bias_6h_p3 or 'N/A').upper()}\n"
                            f"[OK] Zone ST Context 30m: {(ctx_30m_p3 or 'N/A').upper()}\n"
                            f"[OK] Flip ST AI 30m: {st_30m_val_p3.upper()}\n"
                            f"[INFO] ST AI 6H: {(st_6h_p3 or 'N/A').upper()}\n"
                            f"{get_market_context_info()}",
                            f"{symbol}_PULSE",
                            journal_symbol=symbol, journal_strategy='PULSE',
                            journal_direction=direction_p3, journal_price=price
                        )
                        track_alert(symbol, 'PULSE')
                        logger.info(f"[PULSE] Entree troisieme: {symbol} {direction_p3}")

        # Stocker ST AI 4H pour sync_scalp
        if alert_type == 'supertrend' and tf == '4h':
            st_4h_val = parse_supertrend_value(val)
            if st_4h_val is not None:
                with STATE_LOCK:
                    m = MOMENTUM_STATE.get(symbol, {})
                    m['st_4h'] = st_4h_val
                    MOMENTUM_STATE[symbol] = m
        # Stocker ST AI 6H pour PULSE
        if alert_type == 'supertrend' and tf == '6h':
            st_6h_val = parse_supertrend_value(val)
            if st_6h_val is not None:
                with STATE_LOCK:
                    m = MOMENTUM_STATE.get(symbol, {})
                    m['st_6h'] = st_6h_val
                    m['st_6h_ts'] = time.time()
                    MOMENTUM_STATE[symbol] = m

        # Support optionnel des alertes TradingView Range Filter. Le calcul OKX
        # local reste la source principale, mais ce chemin couvre notamment les
        # actifs sans bougies OKX directes.
        if alert_type == 'range_filter' and tf in ('10m', '30m'):
            range_dir = parse_range_filter_value(val)
            if range_dir is not None:
                range_event = data.get('event_id') or event_id
                if tf == '10m':
                    evaluate_range_filter_10m(
                        symbol, range_dir, range_event, price, exchange_name,
                        event_id=event_id,
                    )
                else:
                    evaluate_pulse_range_filter_30m(
                        symbol, range_dir, range_event, price, exchange_name,
                        event_id=event_id,
                    )
                    evaluate_context_1d_range_filter_30m(
                        symbol, range_dir, range_event, price, exchange_name,
                        event_id=event_id,
                    )
                    evaluate_trend_2d_range_filter_30m(
                        symbol, range_dir, range_event, price, exchange_name,
                        event_id=event_id,
                    )

        if (
            strat in ['daily', 'all']
            and alert_type == 'supertrend'
            and tf == '30m'
            and st_ai_30m_flipped_this_call
        ):
            evaluate_daily_primary_confluence(
                symbol,
                price=price,
                exchange_name=exchange_name,
                event_id=event_id,
                source="st_ai_30m_flip",
            )

        if (
            alert_type in ('st_context', 'st_context_lt', 'bias', 'supertrend')
            and tf in ('1d', '2d', '2h', '10m', '30m', '6h')
        ):
            evaluate_daily_primary_after_okx(
                symbol,
                price=price,
                exchange_name=exchange_name,
            )
            replay_recent_range_filter_30m(
                symbol,
                price=price,
                exchange_name=exchange_name,
                event_id=f"webhook_rf30_replay_{symbol}_{tf}_{alert_type}_{event_id}",
                source=f"webhook_{alert_type}_{tf}",
            )

        if (
            CONFIG.get('ENABLE_PULSE_V2', True)
            and strat in ['pulse', 'pulse_v2', 'all']
            and (
                (alert_type == 'st_context' and tf == '3m')
                or (alert_type in ('rmi', 'tti') and tf in ('2h', '6h'))
                or (alert_type == 'bias' and tf == '6h')
            )
        ):
            evaluate_pulse_v2(
                symbol,
                price=price,
                exchange_name=exchange_name,
                event_id=event_id,
                source=f"{alert_type}_{tf}",
            )

        if CONFIG.get('ENABLE_PULSE_LEGACY', False) and strat in ['pulse', 'all'] and (
            (alert_type == 'st_context' and tf == '10m')
            or (alert_type == 'supertrend' and tf == '6h')
            or (alert_type == 'bias' and tf == '2h')
        ):
            evaluate_pulse_context_10m_alert(
                symbol,
                price=price,
                exchange_name=exchange_name,
                event_id=event_id,
            )

        persist_runtime_state()
        # ━━ Relay vers le Scalping Bot ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        scalp_url = normalize_base_url(os.environ.get('SCALP_BOT_URL', ''))
        should_relay_scalp = (
            (alert_type == 'supertrend' and tf in ('1h', '2h', '30m'))
            or (alert_type == 'st_context' and tf in ('1m', '3m', '5m', '10m', '1h', '30m'))
            or (alert_type == 'st_context_lt' and tf in ('1m', '5m'))
            or (alert_type == 'bias' and tf == '2h')
            or (alert_type in ('rmi', 'tti') and tf == '30m')
        )
        if scalp_url and should_relay_scalp:
            scalp_symbols = {s for s, cfg in CONFIG['SYMBOLS'].items() if cfg.get('scalp')}
            if symbol in scalp_symbols:
                try:
                    # Payload normalisé — symbole et tf déjà normalisés par le bot principal
                    relay_payload = {
                        'symbol':   symbol,
                        'strategy': 'scalp',
                        'tf':       tf,
                        'type':     alert_type,
                        'value':    val,
                        'price':    price,
                        'event_id': event_id,
                    }
                    try:
                        resp = requests.post(
                            f"{scalp_url}/webhook",
                            json=relay_payload,
                            timeout=6
                        )
                    except requests.exceptions.Timeout:
                        # Un seul retry court : le scalpbot peut avoir un cold start
                        # ou un pic de charge ponctuel, un deuxieme essai suffit souvent.
                        logger.warning(f"[RELAY] {symbol} {tf} timeout, retry...")
                        resp = requests.post(
                            f"{scalp_url}/webhook",
                            json=relay_payload,
                            timeout=6
                        )
                    if 200 <= resp.status_code < 300:
                        try:
                            relay_result = resp.json()
                        except ValueError:
                            relay_result = {}
                        if relay_result.get('status') == 'ignored':
                            logger.warning(f"[RELAY] {symbol} {tf} ignoré par scalpbot: {relay_result.get('reason', 'raison inconnue')}")
                        else:
                            logger.info(f"[RELAY] {symbol} {tf} → scalpbot OK")
                    else:
                        logger.warning(f"[RELAY] scalpbot HTTP {resp.status_code}: {resp.text[:200]}")
                except Exception as e:
                    logger.warning(f"[RELAY] Erreur: {e}")


    except Exception:
        logger.exception("[WEBHOOK] Erreur traitement")

@app.route('/telegram_callback', methods=['POST'])
def telegram_callback():
    tg_secret = os.environ.get('TELEGRAM_WEBHOOK_SECRET', '')
    if tg_secret:
        provided = request.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
        if provided != tg_secret:
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
                            logger.info(f"[JOURNAL] Relai log_entry →{resp.status_code}")
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
        if symbol not in get_tracked_symbols():
            return jsonify({'error': f'{symbol} non dans la watchlist'}), 404
        symbols = [symbol]
    else:
        symbols = list(CONFIG['SYMBOLS'].keys()) + list(CONFIG.get('RADAR_SYMBOLS', {}).keys())

    def _run():
        logger.info(f"[REFRESH] Calcul forcé pour {len(symbols)} assets...")
        for sym in symbols:
            try:
                if is_radar_symbol(sym) and not is_trade_symbol(sym):
                    update_daily_radar_bias(sym)
                else:
                    update_indicators_for_symbol(sym)
            except Exception as e:
                logger.error(f"[REFRESH] {sym}: {e}")
        persist_runtime_state()
        check_daily_radar_report()
        logger.info("[REFRESH] Terminé")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'status': 'ok', 'message': f'Refresh lancé pour {len(symbols)} assets'}), 200


@app.route('/sync_scalp', methods=['POST'])
def sync_scalp():
    """Force l'envoi des etats utiles au scalpbot."""
    if not require_admin_secret():
        return jsonify({'error': 'unauthorized'}), 401

    scalp_url = normalize_base_url(os.environ.get('SCALP_BOT_URL', ''))
    if not scalp_url:
        return jsonify({'error': 'SCALP_BOT_URL non defini'}), 400

    scalp_symbols = {s for s, cfg in CONFIG['SYMBOLS'].items() if cfg.get('scalp')}
    sent = []
    errors = []

    with STATE_LOCK:
        state_copy = dict(MOMENTUM_STATE)

    def ctx_to_sync_value(ctx):
        if ctx == 'buy':
            return '-2.0'
        if ctx == 'sell':
            return '2.0'
        return '0'

    for symbol in sorted(scalp_symbols):
        m = state_copy.get(symbol, {})
        symbol_sent = []

        st_2h = m.get('st_ai_2h') or m.get('st_2h')
        if st_2h in ('buy', 'sell'):
            try:
                payload = {
                    'symbol':   symbol,
                    'strategy': 'scalp',
                    'tf':       '2h',
                    'type':     'supertrend',
                    'value':    '1' if st_2h == 'buy' else '0',
                    'price':    0,
                    'event_id': f"sync_scalp_st2h_{symbol}_{int(time.time())}",
                }
                resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
                if resp.status_code == 200:
                    symbol_sent.append('st2h')
                else:
                    errors.append(f"{symbol}: ST2H HTTP {resp.status_code}")
            except Exception as e:
                errors.append(f"{symbol}: ST2H {e}")
        else:
            errors.append(f"{symbol}: etat ST AI 2H absent/invalide ({st_2h!r})")

        st_30m = m.get('st_ai_30m')
        if st_30m in ('buy', 'sell'):
            try:
                payload = {
                    'symbol':   symbol,
                    'strategy': 'scalp',
                    'tf':       '30m',
                    'type':     'supertrend',
                    'value':    '1' if st_30m == 'buy' else '0',
                    'price':    0,
                    'event_id': f"sync_scalp_st30m_{symbol}_{int(time.time())}",
                }
                resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
                if resp.status_code == 200:
                    symbol_sent.append('st30m')
                else:
                    errors.append(f"{symbol}: ST30M HTTP {resp.status_code}")
            except Exception as e:
                errors.append(f"{symbol}: ST30M {e}")
        else:
            errors.append(f"{symbol}: etat ST AI 30M absent/invalide ({st_30m!r})")

        bias_2h = m.get('bias_2h')
        if bias_2h in ('bull', 'bear', 'neutral', None):
            try:
                payload = {
                    'symbol':   symbol,
                    'strategy': 'scalp',
                    'tf':       '2h',
                    'type':     'bias',
                    'value':    bias_2h or 'neutral',
                    'price':    0,
                    'event_id': f"sync_scalp_bias2h_{symbol}_{int(time.time())}",
                }
                resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
                if resp.status_code == 200:
                    symbol_sent.append('bias2h')
                else:
                    errors.append(f"{symbol}: Bias2H HTTP {resp.status_code}")
            except Exception as e:
                errors.append(f"{symbol}: Bias2H {e}")

        ctx_1h = m.get('st_context_1h')
        try:
            payload = {
                'symbol':   symbol,
                'strategy': 'scalp',
                'tf':       '1h',
                'type':     'st_context',
                'value':    ctx_to_sync_value(ctx_1h),
                'price':    0,
                'event_id': f"sync_scalp_ctx1h_{symbol}_{int(time.time())}",
            }
            resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
            if resp.status_code == 200:
                symbol_sent.append('ctx1h')
            else:
                errors.append(f"{symbol}: CTX1H HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"{symbol}: CTX1H {e}")

        ctx_5m = m.get('st_context_5m')
        try:
            payload = {
                'symbol':   symbol,
                'strategy': 'scalp',
                'tf':       '5m',
                'type':     'st_context',
                'value':    ctx_to_sync_value(ctx_5m),
                'price':    0,
                'event_id': f"sync_scalp_ctx5m_{symbol}_{int(time.time())}",
            }
            resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
            if resp.status_code == 200:
                symbol_sent.append('ctx5m')
            else:
                errors.append(f"{symbol}: CTX5M HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"{symbol}: CTX5M {e}")

        ctx_10m = m.get('st_context_10m')
        try:
            payload = {
                'symbol':   symbol,
                'strategy': 'scalp',
                'tf':       '10m',
                'type':     'st_context',
                'value':    ctx_to_sync_value(ctx_10m),
                'price':    0,
                'event_id': f"sync_scalp_ctx10m_{symbol}_{int(time.time())}",
            }
            resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
            if resp.status_code == 200:
                symbol_sent.append('ctx10m')
            else:
                errors.append(f"{symbol}: CTX10M HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"{symbol}: CTX10M {e}")

        lt_5m = m.get('st_context_lt_5m') or ST_CONTEXT_LT_5M.get(symbol)
        try:
            payload = {
                'symbol':   symbol,
                'strategy': 'scalp',
                'tf':       '5m',
                'type':     'st_context_lt',
                'value':    ctx_to_sync_value(lt_5m),
                'price':    0,
                'event_id': f"sync_scalp_lt5m_{symbol}_{int(time.time())}",
            }
            resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
            if resp.status_code == 200:
                symbol_sent.append('lt5m')
            else:
                errors.append(f"{symbol}: LT5M HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"{symbol}: LT5M {e}")

        if symbol_sent:
            sent.append(f"{symbol}:{','.join(symbol_sent)}")

    logger.info(f"[SYNC_SCALP] Envoye: {len(sent)} assets, erreurs: {len(errors)}")
    return jsonify({'sent': sent, 'errors': errors}), 200


@app.route('/reset_state', methods=['POST'])
def reset_state_all():
    if not require_admin_secret():
        return jsonify({'error': 'unauthorized'}), 401
    """Remet tout le state à zéro."""
    with STATE_LOCK:
        MOMENTUM_STATE.clear()
        LAST_SIGNALS.clear()
        LAST_SIGNAL_EVENTS.clear()
        LAST_WEBHOOK_SIGNAL_TS.clear()
        ST_AI_15M.clear()
        ST_AI_30M.clear()
        ST_AI_1D.clear()
        ST_CONTEXT_15M.clear()
        ST_CONTEXT_30M.clear()
        SCALP_POSITIONS.clear()
        ST_CONTEXT_1D.clear()
        ST_CONTEXT_LT_1H.clear()
        ST_CONTEXT_LT_4H.clear()
        ST_CONTEXT_3D.clear()
        ST_CONTEXT_LT_15M.clear()
        ST_CONTEXT_LT_5M.clear()
        ST_CONTEXT_LT_10M.clear()
        ST_CONTEXT_LT_30M.clear()
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
    if symbol not in get_tracked_symbols():
        return jsonify({'status': 'error', 'message': f'{symbol} non trouvé dans la watchlist'}), 404
    with STATE_LOCK:
        MOMENTUM_STATE.pop(symbol, None)
        ST_AI_15M.pop(symbol, None)
        ST_AI_30M.pop(symbol, None)
        ST_AI_1D.pop(symbol, None)
        ST_CONTEXT_15M.pop(symbol, None)
        ST_CONTEXT_30M.pop(symbol, None)
        ST_CONTEXT_1D.pop(symbol, None)
        ST_CONTEXT_3D.pop(symbol, None)
        ST_CONTEXT_LT_1H.pop(symbol, None)
        ST_CONTEXT_LT_4H.pop(symbol, None)
        ST_CONTEXT_LT_15M.pop(symbol, None)
        ST_CONTEXT_LT_5M.pop(symbol, None)
        ST_CONTEXT_LT_10M.pop(symbol, None)
        ST_CONTEXT_LT_30M.pop(symbol, None)

        for k in ['', '_1h', '_4h', '_1d']:
            ADX_STATE.pop(f'{symbol}{k}', None)

        for strat in ['PULSE', 'DAILY', 'CONTEXT4H', 'TREND2D']:
            PYRA_ENABLED.pop(f'{symbol}_{strat}', None)
            SCALP_POSITIONS.pop(f'{symbol}_{strat}', None)

        keys_to_remove = [k for k in LAST_SIGNALS if k.startswith(f"{symbol}:")]
        for k in keys_to_remove:
            LAST_SIGNALS.pop(k, None)
            LAST_SIGNAL_EVENTS.pop(k, None)
        signal_keys_to_remove = [k for k in LAST_WEBHOOK_SIGNAL_TS if k.startswith(f"{symbol}|")]
        for k in signal_keys_to_remove:
            LAST_WEBHOOK_SIGNAL_TS.pop(k, None)
    persist_runtime_state()
    logger.info(f"🔄 State remis à zéro pour {symbol}")
    return jsonify({'status': 'reset', 'symbol': symbol, 'message': f'État de {symbol} remis à zéro'}), 200





def fetch_ohlcv_okx(symbol, timeframe, limit=250):
    """Fetch OHLCV depuis l API publique OKX (sans cle API)."""
    try:
        cfg = get_symbol_config(symbol)
        inst_id = cfg.get('okx_inst_id') or symbol.replace('/', '-')
        tf_map = {'5m': '5m', '10m': '10m', '15m': '15m', '30m': '30m', '1h': '1H', '2h': '2H', '3h': '3H', '4h': '4H', '6h': '6H', '1d': '1D'}
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


def calc_williams_ema(df, length=14, ema_length=14):
    """Calcule Williams %R et son EMA."""
    try:
        if df is None or len(df) < length + ema_length:
            return None
        high = df['high'].astype(float)
        low = df['low'].astype(float)
        close = df['close'].astype(float)
        upper = high.rolling(length).max()
        lower = low.rolling(length).min()
        spread = upper - lower
        williams = 100.0 * (close - upper) / spread.where(spread != 0)
        williams_ema = williams.ewm(span=ema_length, adjust=False).mean()
        value = float(williams.iloc[-1])
        ema_value = float(williams_ema.iloc[-1])
        if pd.isna(value) or pd.isna(ema_value):
            return None
        return {
            'value': value,
            'ema': ema_value,
            'trend': 'bull' if value > ema_value else 'bear' if value < ema_value else 'neutral',
        }
    except Exception as e:
        logger.debug(f"[WILLIAMS] Calcul impossible: {e}")
        return None


def calc_rmi_trend(df, length=10, positive_above=62, negative_below=34):
    """Reproduit l'etat RMI Trend Sniper: RSI/MFI moyen + pente EMA5."""
    try:
        if df is None or len(df) < length + 10:
            return None
        close = df['close'].astype(float).reset_index(drop=True)
        high = df['high'].astype(float).reset_index(drop=True)
        low = df['low'].astype(float).reset_index(drop=True)
        volume = df['volume'].astype(float).reset_index(drop=True)

        change = close.diff()
        up = change.clip(lower=0).ewm(alpha=1 / length, adjust=False).mean()
        down = (-change.clip(upper=0)).ewm(alpha=1 / length, adjust=False).mean()
        rs = up / down.where(down != 0)
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.where(down != 0, 100).where(up != 0, 0)

        typical = (high + low + close) / 3
        money_flow = typical * volume
        typical_change = typical.diff()
        pos_flow = money_flow.where(typical_change > 0, 0.0).rolling(length).sum()
        neg_flow = money_flow.where(typical_change < 0, 0.0).rolling(length).sum()
        mfi_ratio = pos_flow / neg_flow.where(neg_flow != 0)
        mfi = 100 - (100 / (1 + mfi_ratio))
        mfi = mfi.where(neg_flow != 0, 100).where(pos_flow != 0, 0)

        rsi_mfi = ((rsi + mfi) / 2).astype(float)
        ema5 = close.ewm(span=5, adjust=False).mean()

        state = None
        for i in range(1, len(close)):
            current = rsi_mfi.iloc[i]
            previous = rsi_mfi.iloc[i - 1]
            if pd.isna(current) or pd.isna(previous):
                continue
            p_mom = (
                previous < positive_above
                and current > positive_above
                and current > negative_below
                and (ema5.iloc[i] - ema5.iloc[i - 1]) > 0
            )
            n_mom = current < negative_below and (ema5.iloc[i] - ema5.iloc[i - 1]) < 0
            if p_mom:
                state = 'bull'
            elif n_mom:
                state = 'bear'
        return state
    except Exception as e:
        logger.debug(f"[RMI] Calcul impossible: {e}")
        return None


def get_williams_filter(symbol, timeframe, direction, max_age_seconds):
    """Retourne l'etat du filtre Williams pour une direction donnee."""
    normalized_direction = str(direction or '').upper()
    expected_trend = 'bull' if normalized_direction == 'LONG' else 'bear'
    key = f'williams_{timeframe}'
    ts_key = f'{key}_ts'
    with STATE_LOCK:
        data = dict(MOMENTUM_STATE.get(symbol, {}).get(key) or {})
        ts = MOMENTUM_STATE.get(symbol, {}).get(ts_key)
    fresh = bool(data) and is_signal_fresh(ts, max_age_seconds)
    trend = data.get('trend')
    ok = fresh and trend == expected_trend
    return {
        'data': data,
        'fresh': fresh,
        'ok': ok,
        'trend': trend,
        'value': data.get('value'),
        'ema': data.get('ema'),
    }


def format_williams_filter_line(label, williams_filter):
    """Formate Williams pour les messages Telegram."""
    value = williams_filter.get('value')
    ema_value = williams_filter.get('ema')
    trend = williams_filter.get('trend')
    if value is None or ema_value is None:
        return f"[OK] Williams {label}: N/A"
    relation = ">" if trend == 'bull' else "<" if trend == 'bear' else "="
    return f"[OK] Williams {label}: W%R {value:.2f} {relation} EMA14 {ema_value:.2f}"


def format_optional_williams_line(label, williams_filter):
    """Formate un Williams optionnel : qualite si aligne, warning sinon."""
    base = format_williams_filter_line(label, williams_filter).replace("[OK]", "").strip()
    if williams_filter.get('ok'):
        return f"[QUALITE] {base}"
    return f"[WARNING NON BLOQUANT] {base} pas aligne"


def fmt_sig(value):
    if value in ('buy', 'bull'):
        return 'BUY'
    if value in ('sell', 'bear'):
        return 'SELL'
    return 'NEUTRE'


def evaluate_tti_zone_alert(symbol, tf, tti_value, price=0.0, exchange_name=None, event_id=None):
    """Alerte info quand TTI 6H/1D passe en zone bull/bear sur toute la watchlist."""
    if symbol not in all_known_symbols():
        return False
    tf = normalize_tf(tf)
    if tf not in ('6h', '1d'):
        return False
    if tti_value not in ('bull', 'bear'):
        return False

    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    direction = 'LONG' if tti_value == 'bull' else 'SHORT'
    exp_ctx = 'buy' if direction == 'LONG' else 'sell'
    tf_label = '6H' if tf == '6h' else '1D'
    stat_key = 'TTI6H' if tf == '6h' else 'TTI1D'
    max_age = 18 * 3600 if tf == '6h' else 36 * 3600
    cooldown = 3 * 3600 if tf == '6h' else 12 * 3600
    if tf == '6h':
        ctx_value = m.get('st_context_6h')
        ctx_ts = m.get('st_context_6h_ts')
    else:
        ctx_value = ST_CONTEXT_1D.get(symbol)
        ctx_ts = m.get('st_context_1d_ts')
    ctx_fresh = is_signal_fresh(ctx_ts, max_age)
    ctx_quality = ctx_fresh and ctx_value == exp_ctx
    event_key = event_id or f"tti_{tf}_zone_{symbol}_{int(time.time())}_{tti_value}"

    logger.info(
        f"[TTI {tf_label} ALERT CHECK] {symbol} dir={direction} "
        f"tti={tti_value} ctx={ctx_value}/{exp_ctx} fresh={ctx_fresh} quality={ctx_quality}"
    )

    if not should_send(symbol, f"tti_{tf}_zone_{tti_value}", event_id=event_key, cooldown=cooldown):
        return False

    emoji = "\U0001f7e2" if direction == 'LONG' else "\U0001f534"
    quality_line = (
        f"\u2b50 <b>ALERTE HAUTE QUALITE: ST Context {tf_label} aligne</b>\n\n"
        if ctx_quality else ""
    )
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    send_telegram(
        f"{emoji} <b>[TTI {tf_label} - ZONE]</b> {symbol}\n"
        f"--------------------\n"
        f"{quality_line}"
        f"Direction: {direction}\n"
        f"Price: ${format_price(price)}\n"
        f"Exchange: {exchange_name.upper()}\n"
        f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        f"[OK] TTI {tf_label}: {tti_value.upper()}\n"
        f"[QUALITE] ST Context {tf_label}: {(ctx_value or 'NEUTRE').upper()}\n"
        f"{get_market_context_info()}",
        ntfy=True,
    )
    track_alert(symbol, stat_key)
    return True


def calc_range_filter_signal(df, per=100, mult=2.0):
    """Reproduit le Range Filter Pine et retourne le dernier signal confirme."""
    try:
        # 100 periodes suffisent pour amorcer les deux EMA. Le scheduler 10m
        # agrege 300 bougies 5m, soit environ 150 bougies 10m confirmees.
        if df is None or len(df) < (per + 5):
            return None

        close = df['close'].astype(float).reset_index(drop=True)
        wper = per * 2 - 1
        avrng = close.diff().abs().ewm(span=per, adjust=False).mean()
        smrng = avrng.ewm(span=wper, adjust=False).mean() * mult

        filt = []
        for i, x in enumerate(close):
            prev = x if i == 0 else filt[-1]
            r = smrng.iloc[i]
            if pd.isna(r):
                filt.append(prev)
            elif x > prev:
                filt.append(prev if x - r < prev else x - r)
            else:
                filt.append(prev if x + r > prev else x + r)
        filt = pd.Series(filt)

        upward = []
        downward = []
        for i, value in enumerate(filt):
            if i == 0:
                upward.append(0.0)
                downward.append(0.0)
                continue
            prev_up = upward[-1]
            prev_down = downward[-1]
            prev_filt = filt.iloc[i - 1]
            upward.append(prev_up + 1 if value > prev_filt else 0.0 if value < prev_filt else prev_up)
            downward.append(prev_down + 1 if value < prev_filt else 0.0 if value > prev_filt else prev_down)

        long_cond = (close > filt) & (pd.Series(upward) > 0)
        short_cond = (close < filt) & (pd.Series(downward) > 0)

        cond_ini = []
        for long_ok, short_ok in zip(long_cond, short_cond):
            prev = cond_ini[-1] if cond_ini else 0
            cond_ini.append(1 if long_ok else -1 if short_ok else prev)

        idx = len(close) - 1
        prev_cond = cond_ini[idx - 1]
        direction = None
        if bool(long_cond.iloc[idx]) and prev_cond == -1:
            direction = 'buy'
        elif bool(short_cond.iloc[idx]) and prev_cond == 1:
            direction = 'sell'

        if direction is None:
            return None

        return {
            'direction': direction,
            'ts': str(df['ts'].iloc[idx]),
            'price': float(close.iloc[idx]),
        }
    except Exception as e:
        logger.info(f"[RANGE30M] calc failed: {e}")
        logger.debug("[RANGE30M] calc exception", exc_info=True)
        return None


def keep_confirmed_candles(df, timeframe_minutes):
    """Retourne uniquement les bougies dont la cloture est deja passee."""
    if df is None or df.empty:
        return None
    duration_ms = int(timeframe_minutes * 60 * 1000)
    now_ms = int(time.time() * 1000)
    confirmed = df[df['ts'].astype('int64') + duration_ms <= now_ms].copy()
    if confirmed.empty:
        return None
    return confirmed.reset_index(drop=True)


def evaluate_strategy_2h_range_filter_30m(symbol, range_dir, signal_ts, price=0.0, exchange_name=None, event_id=None, source='okx'):
    """Entree STRATEGIE 2H: Range Filter 30m + ST AI 2H + Bias 2H."""
    if range_dir not in ('buy', 'sell'):
        return False
    if not is_trade_symbol(symbol):
        return False

    init_symbol_states(symbol)
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    direction = 'LONG' if range_dir == 'buy' else 'SHORT'
    exp_ctx = 'buy' if direction == 'LONG' else 'sell'
    opp_ctx = 'sell' if direction == 'LONG' else 'buy'
    exp_bias = 'bull' if direction == 'LONG' else 'bear'

    m = MOMENTUM_STATE[symbol]
    now_ts = datetime.now(timezone.utc).timestamp()
    st_2h_v = m.get('st_ai_2h') or m.get('st_2h')
    bias_2h_v = m.get('bias_2h')
    ctx_30m_v = ST_CONTEXT_30M.get(symbol)
    st_2h_ok = st_2h_v == exp_ctx
    bias_2h_ok = bias_2h_v == exp_bias
    ctx30m_fresh = bool(ctx_30m_v) and is_signal_fresh(m.get('st_context_30m_ts'), 90 * 60)
    ctx30m_opp_block = ctx30m_fresh and ctx_30m_v == opp_ctx
    entry_ok = st_2h_ok and bias_2h_ok and not ctx30m_opp_block

    logger.info(
        f"[STRATEGIE 2H CHECK] {symbol} dir={direction} source={source} "
        f"range30m={range_dir} signal_ts={signal_ts} "
        f"st2h={st_2h_v}/{exp_ctx} "
        f"bias2h={bias_2h_v}/{exp_bias} "
        f"ctx30m={ctx_30m_v}/{opp_ctx} fresh={ctx30m_fresh} block={ctx30m_opp_block} "
        f"ok={entry_ok}"
    )
    if not entry_ok:
        logger.info(
            f"[STRATEGIE 2H BLOCKED] {symbol} "
            f"st2h:{st_2h_ok},bias2h:{bias_2h_ok},ctx30m_opp:{ctx30m_opp_block}"
        )
        return False

    signal_type = 'range30m'
    pos_key = f"{symbol}_2H"
    event_key = event_id or f"range30m_{symbol}_{signal_ts}_{range_dir}"
    with STATE_LOCK:
        pos = SCALP_POSITIONS.get(pos_key)
        if pos and pos.get('direction') != direction:
            SCALP_POSITIONS.pop(pos_key, None)
            PYRA_ENABLED.pop(pos_key, None)
            pos = None
        is_entry = bool(pos is None or pos.get('signal_type') != signal_type)
        if is_entry and should_send(symbol, f"strategy_2h_entry_{signal_type}_{exp_ctx}", event_id=event_key, cooldown=3600):
            SCALP_POSITIONS[pos_key] = {
                'direction': direction,
                'entry_count': 1,
                'signal_type': signal_type,
            }
            PYRA_ENABLED.pop(pos_key, None)
            m['range_filter_30m'] = range_dir
            m['range_filter_30m_ts'] = now_ts
            RANGE_FILTER_30M[symbol] = range_dir
        else:
            is_entry = False

    if not is_entry:
        return False

    emoji = "\U0001f7e2" if direction == "LONG" else "\U0001f534"
    send_telegram_with_buttons(
        f"{emoji} <b>[STRATEGIE 2H - ENTREE]</b> {symbol}\n"
        f"--------------------\n"
        f"Direction: {direction}\n"
        f"Price: ${format_price(price)}\n"
        f"Exchange: {exchange_name.upper()}\n"
        f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        f"[OK] Flip Range Filter 30m: {range_dir.upper()}\n"
        f"[OK] ST AI 2H: {(st_2h_v or 'N/A').upper()}\n"
        f"[OK] Bias 2H: {(bias_2h_v or 'N/A').upper()}\n"
        f"[ANTI-CHOP] ST Context 30m oppose: {(ctx_30m_v or 'NEUTRE').upper()}\n"
        f"{get_market_context_info()}",
        pos_key,
        journal_symbol=symbol, journal_strategy='2H',
        journal_direction=direction, journal_price=price
    )
    track_alert(symbol, '2H')
    logger.info(f"[STRATEGIE 2H] Entree Range 30m: {symbol} {direction}")
    return True


def evaluate_daily_range_filter_30m(symbol, range_dir, signal_ts, price=0.0, exchange_name=None, event_id=None, source='okx'):
    """Entree DAILY declenchee par Range Filter 30m, avec les filtres DAILY existants."""
    if range_dir not in ('buy', 'sell') or not is_trade_symbol(symbol):
        return False

    init_symbol_states(symbol)
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    direction = 'LONG' if range_dir == 'buy' else 'SHORT'
    exp_ctx = 'buy' if direction == 'LONG' else 'sell'
    opp_ctx = 'sell' if direction == 'LONG' else 'buy'
    exp_bias = 'bull' if direction == 'LONG' else 'bear'

    m = MOMENTUM_STATE[symbol]
    bias_1d = m.get('bias_1d')
    st_1d = m.get('st_ai_1d') or ST_AI_1D.get(symbol)
    ctx_2h = m.get('st_context_2h')
    ctx_30m = ST_CONTEXT_30M.get(symbol)
    ctx_lt_30m = ST_CONTEXT_LT_30M.get(symbol)
    ctx_1d = ST_CONTEXT_1D.get(symbol)
    williams_1d = get_williams_filter(symbol, '1d', direction, 36 * 3600)
    williams_2d = get_williams_filter(symbol, '2d', direction, 72 * 3600)

    bias_1d_fresh = bool(bias_1d) and is_signal_fresh(m.get('bias_1d_ts'), 36 * 3600)
    st_1d_fresh = bool(st_1d) and is_signal_fresh(m.get('st_ai_1d_ts'), 36 * 3600)
    ctx_2h_fresh = bool(ctx_2h) and is_signal_fresh(m.get('st_context_2h_ts'), 6 * 3600)
    ctx_30m_fresh = bool(ctx_30m) and is_signal_fresh(m.get('st_context_30m_ts'), 90 * 60)
    ctx_lt_30m_fresh = bool(ctx_lt_30m) and is_signal_fresh(m.get('st_context_lt_30m_ts'), 90 * 60)
    ctx_1d_fresh = bool(ctx_1d) and is_signal_fresh(m.get('st_context_1d_ts'), 36 * 3600)

    # Conserve exactement le filtre DAILY precedent : le Bias 1D doit etre
    # aligne, sans ajouter une nouvelle contrainte de fraicheur bloquante.
    bias_1d_ok = bias_1d == exp_bias
    st_1d_ok = st_1d_fresh and st_1d == exp_ctx
    ctx_30m_ok = ctx_30m_fresh and ctx_30m == exp_ctx
    lt_30m_block = ctx_lt_30m_fresh and ctx_lt_30m == exp_ctx
    ctx_2h_warning = ctx_2h_fresh and ctx_2h == opp_ctx
    daily_plus = ctx_1d_fresh and ctx_1d == exp_ctx
    entry_ok = bias_1d_ok and st_1d_ok and ctx_30m_ok and not lt_30m_block
    signal_type = 'daily_plus_range30m' if daily_plus else 'daily_range30m'

    logger.info(
        f"[DAILY RANGE30M CHECK] {symbol} dir={direction} source={source} "
        f"range30m={range_dir} signal_ts={signal_ts} "
        f"bias1d={bias_1d}/{exp_bias} fresh={bias_1d_fresh} "
        f"st1d={st_1d}/{exp_ctx} fresh={st_1d_fresh} "
        f"will1d={williams_1d['value']}/{williams_1d['ema']} trend={williams_1d['trend']} fresh={williams_1d['fresh']} ok={williams_1d['ok']} "
        f"will2d={williams_2d['value']}/{williams_2d['ema']} trend={williams_2d['trend']} fresh={williams_2d['fresh']} ok={williams_2d['ok']} "
        f"ctx30m={ctx_30m}/{exp_ctx} fresh={ctx_30m_fresh} "
        f"lt30m={ctx_lt_30m} fresh={ctx_lt_30m_fresh} block={lt_30m_block} "
        f"daily_plus={daily_plus} ok={entry_ok}"
    )
    if not entry_ok:
        return False

    pos_key = f"{symbol}_DAILY"
    event_key = event_id or f"range30m_daily_{symbol}_{signal_ts}_{range_dir}"
    with STATE_LOCK:
        pos = SCALP_POSITIONS.get(pos_key)
        if pos and pos.get('direction') != direction:
            SCALP_POSITIONS.pop(pos_key, None)
            PYRA_ENABLED.pop(pos_key, None)
            pos = None
        is_entry = bool(pos is None or pos.get('signal_type') != signal_type)
        if is_entry and should_send(symbol, f"daily_entry_{signal_type}_{exp_ctx}", event_id=event_key, cooldown=14400):
            SCALP_POSITIONS[pos_key] = {
                'direction': direction,
                'entry_count': 1,
                'signal_type': signal_type,
            }
            PYRA_ENABLED.pop(pos_key, None)
        else:
            is_entry = False

    if not is_entry:
        return False

    emoji = "\U0001f7e2" if direction == "LONG" else "\U0001f534"
    title = "[DAILY++ - ENTREE RANGE 30M]" if daily_plus else "[DAILY - ENTREE RANGE 30M]"
    plus_txt = "\u2b50 <b>DAILY++ / TRES HAUTE QUALITE</b> (ST Context 1D aligne)\n\n" if daily_plus else ""
    warning_txt = "\u26a0\ufe0f <b>WARNING NON BLOQUANT</b> : ST Context 2H oppose\n" if ctx_2h_warning else ""
    send_telegram_with_buttons(
        f"{emoji} <b>{title}</b> {symbol}\n"
        f"--------------------\n"
        f"{plus_txt}"
        f"Direction: {direction}\n"
        f"Price: ${format_price(price)}\n"
        f"Exchange: {exchange_name.upper()}\n"
        f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        f"[OK] Range Filter 30m: {range_dir.upper()}\n"
        f"[OK] Bias 1D: {(bias_1d or 'N/A').upper()} (EMA17/SMA40)\n"
        f"[OK] ST AI 1D: {(st_1d or 'N/A').upper()}\n"
        f"{format_williams_filter_line('1D', williams_1d)}\n"
        f"{format_optional_williams_line('2D', williams_2d)}\n"
        f"[OK] Zone ST Context 30m: {(ctx_30m or 'N/A').upper()}\n"
        f"{warning_txt}"
        f"[ANTI-CHOP] LT 30m: {(ctx_lt_30m or 'NEUTRE').upper()}\n"
        f"[BONUS] ST Context 1D: {(ctx_1d or 'NEUTRE').upper()}\n"
        f"{get_market_context_info()}",
        pos_key,
        journal_symbol=symbol, journal_strategy='DAILY',
        journal_direction=direction, journal_price=price,
    )
    track_alert(symbol, 'DAILY')
    logger.info(f"[DAILY] Entree Range 30m: {symbol} {direction}")
    return True


def evaluate_range_filter_10m(symbol, range_dir, signal_ts, price=0.0, exchange_name=None, event_id=None):
    """Evalue le pyramiding DAILY sur un nouveau flip Range Filter 10m."""
    if range_dir not in ('buy', 'sell') or not is_trade_symbol(symbol):
        return
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    direction = 'LONG' if range_dir == 'buy' else 'SHORT'
    exp_ctx = range_dir
    exp_bias = 'bull' if direction == 'LONG' else 'bear'
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    event_key = event_id or f"range10m_{symbol}_{signal_ts}_{range_dir}"

    logger.info(f"[DAILY RANGE10M CHECK] {symbol} dir={direction} rf10={range_dir} entry_disabled=True")
    _evaluate_daily_range10m_pyramiding(
        symbol, range_dir, signal_ts, price, exchange_name, event_key,
    )


def _open_strategy_entry(symbol, strategy, direction, signal_type, event_id, price, exchange_name, detail_lines, cooldown=3600):
    """Cree une entree unique et envoie l'alerte Telegram correspondante."""
    pos_key = f"{symbol}_{strategy}"
    exp_ctx = 'buy' if direction == 'LONG' else 'sell'
    with STATE_LOCK:
        pos = SCALP_POSITIONS.get(pos_key)
        if pos and pos.get('direction') != direction:
            SCALP_POSITIONS.pop(pos_key, None)
            PYRA_ENABLED.pop(pos_key, None)
            pos = None
        if pos is not None or not should_send(
            symbol, f"{strategy.lower()}_entry_{signal_type}_{exp_ctx}",
            event_id=event_id, cooldown=cooldown,
        ):
            return False
        SCALP_POSITIONS[pos_key] = {
            'direction': direction, 'entry_count': 1, 'signal_type': signal_type,
        }
        PYRA_ENABLED.pop(pos_key, None)
    emoji = "\U0001f7e2" if direction == 'LONG' else "\U0001f534"
    send_telegram_with_buttons(
        f"{emoji} <b>[{strategy} - ENTREE]</b> {symbol}\n"
        f"--------------------\nDirection: {direction}\n"
        f"Price: ${format_price(price)}\nExchange: {exchange_name.upper()}\n"
        f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        + "\n".join(detail_lines) + "\n" + get_market_context_info(),
        pos_key, journal_symbol=symbol, journal_strategy=strategy,
        journal_direction=direction, journal_price=price,
    )
    track_alert(symbol, strategy)
    persist_runtime_state()
    logger.info(f"[{strategy}] Entree {signal_type}: {symbol} {direction}")
    return True


def _bias_to_trade_direction(bias_value):
    if bias_value == 'bull':
        return 'LONG'
    if bias_value == 'bear':
        return 'SHORT'
    return None


def _trade_direction_to_ctx(direction):
    return 'buy' if direction == 'LONG' else 'sell'


def _trade_direction_to_bias(direction):
    return 'bull' if direction == 'LONG' else 'bear'


def _pulse_v2_tti_state(m, exp_bias):
    tti_6h = m.get('tti_6h')
    tti_2h = m.get('tti_2h')
    tti_6h_fresh = is_signal_fresh(m.get('tti_6h_ts'), 18 * 3600)
    tti_2h_fresh = is_signal_fresh(m.get('tti_2h_ts'), 6 * 3600)
    tti_6h_ok = tti_6h_fresh and tti_6h == exp_bias
    tti_2h_ok = tti_2h_fresh and tti_2h == exp_bias
    return {
        'tti_6h': tti_6h, 'tti_6h_fresh': tti_6h_fresh, 'tti_6h_ok': tti_6h_ok,
        'tti_2h': tti_2h, 'tti_2h_fresh': tti_2h_fresh, 'tti_2h_ok': tti_2h_ok,
        'any_ok': tti_6h_ok or tti_2h_ok,
        'both_ok': tti_6h_ok and tti_2h_ok,
    }


def evaluate_pulse_v2_take_profit_reminder(symbol, price=0.0, exchange_name=None, event_id=None):
    """Rappel de prise de profit si une zone TTI opposee apparait pendant un PULSE V2."""
    pos_key = f"{symbol}_PULSEV2"
    with STATE_LOCK:
        pos = SCALP_POSITIONS.get(pos_key)
    if not pos:
        return False

    direction = pos.get('direction')
    if direction not in ('LONG', 'SHORT'):
        return False

    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    opp_bias = 'bear' if direction == 'LONG' else 'bull'
    tti_6h_opp = is_signal_fresh(m.get('tti_6h_ts'), 18 * 3600) and m.get('tti_6h') == opp_bias
    tti_2h_opp = is_signal_fresh(m.get('tti_2h_ts'), 6 * 3600) and m.get('tti_2h') == opp_bias
    if not (tti_6h_opp or tti_2h_opp):
        return False

    strength = 'double' if tti_6h_opp and tti_2h_opp else 'single'
    if not should_send(symbol, f"pulsev2_tp_tti_{opp_bias}_{strength}", event_id=event_id, cooldown=3600):
        return False

    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    emoji = "\u26a0\ufe0f" if strength == 'single' else "\U0001f6a8"
    send_telegram(
        f"{emoji} <b>[PULSE V2 - RAPPEL TAKE PROFIT]</b> {symbol}\n"
        f"--------------------\n"
        f"Position suivie: {direction}\n"
        f"Price: ${format_price(price)}\n"
        f"Exchange: {exchange_name.upper()}\n"
        f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        f"<b>Zone TTI opposee detectee: penser a securiser / prendre des profits.</b>\n"
        f"[INFO] TTI 6H: {(m.get('tti_6h') or 'N/A').upper()}\n"
        f"[INFO] TTI 2H: {(m.get('tti_2h') or 'N/A').upper()}\n"
        f"{get_market_context_info()}",
        ntfy=True,
    )
    logger.info(f"[PULSEV2 TP] {symbol} position={direction} tti_6h_opp={tti_6h_opp} tti_2h_opp={tti_2h_opp}")
    return True


def evaluate_pulse_v2(symbol, price=0.0, exchange_name=None, event_id=None, source='webhook'):
    """PULSE V2: RMI/Bias HTF + TTI 6H/2H, entree sur zone ST Context 3m."""
    if not CONFIG.get('ENABLE_PULSE_V2', True) or not is_trade_symbol(symbol):
        return False

    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')

    ctx_3m = m.get('st_context_3m')
    ctx_3m_fresh = is_signal_fresh(m.get('st_context_3m_ts'), 10 * 60)

    rmi_6h = m.get('rmi_6h')
    rmi_6h_fresh = is_signal_fresh(m.get('rmi_6h_ts'), 18 * 3600)
    primary_direction = _bias_to_trade_direction(rmi_6h) if rmi_6h_fresh else None

    bias_6h = m.get('bias_6h')
    bias_6h_fresh = is_signal_fresh(m.get('bias_6h_ts'), 18 * 3600)
    rmi_2h = m.get('rmi_2h')
    rmi_2h_fresh = is_signal_fresh(m.get('rmi_2h_ts'), 6 * 3600)
    secondary_direction = _bias_to_trade_direction(bias_6h) if bias_6h_fresh else None

    candidates = []
    if primary_direction:
        exp_bias = _trade_direction_to_bias(primary_direction)
        tti = _pulse_v2_tti_state(m, exp_bias)
        candidates.append({
            'name': 'principal_rmi6h',
            'label': 'PULSE V2 PRINCIPALE',
            'direction': primary_direction,
            'exp_ctx': _trade_direction_to_ctx(primary_direction),
            'ready': tti['any_ok'],
            'quality': tti['both_ok'],
            'lines': [
                f"[OK] RMI 6H: {rmi_6h.upper()}",
                f"[OK] TTI 6H: {(tti['tti_6h'] or 'N/A').upper()}" if tti['tti_6h_ok'] else f"[INFO] TTI 6H: {(tti['tti_6h'] or 'N/A').upper()}",
                f"[OK] TTI 2H: {(tti['tti_2h'] or 'N/A').upper()}" if tti['tti_2h_ok'] else f"[INFO] TTI 2H: {(tti['tti_2h'] or 'N/A').upper()}",
            ],
            'log': f"rmi6h={rmi_6h} fresh={rmi_6h_fresh} tti6h={tti['tti_6h']} ok={tti['tti_6h_ok']} tti2h={tti['tti_2h']} ok={tti['tti_2h_ok']}",
        })

    if secondary_direction and rmi_2h_fresh and rmi_2h == _trade_direction_to_bias(secondary_direction):
        exp_bias = _trade_direction_to_bias(secondary_direction)
        tti = _pulse_v2_tti_state(m, exp_bias)
        candidates.append({
            'name': 'secondaire_bias6h_rmi2h',
            'label': 'PULSE V2 SECONDAIRE',
            'direction': secondary_direction,
            'exp_ctx': _trade_direction_to_ctx(secondary_direction),
            'ready': tti['any_ok'],
            'quality': tti['both_ok'],
            'lines': [
                f"[OK] Bias 6H: {(bias_6h or 'N/A').upper()} (EMA17/SMA40)",
                f"[OK] RMI 2H: {(rmi_2h or 'N/A').upper()}",
                f"[OK] TTI 6H: {(tti['tti_6h'] or 'N/A').upper()}" if tti['tti_6h_ok'] else f"[INFO] TTI 6H: {(tti['tti_6h'] or 'N/A').upper()}",
                f"[OK] TTI 2H: {(tti['tti_2h'] or 'N/A').upper()}" if tti['tti_2h_ok'] else f"[INFO] TTI 2H: {(tti['tti_2h'] or 'N/A').upper()}",
            ],
            'log': f"bias6h={bias_6h} fresh={bias_6h_fresh} rmi2h={rmi_2h} fresh={rmi_2h_fresh} tti6h={tti['tti_6h']} ok={tti['tti_6h_ok']} tti2h={tti['tti_2h']} ok={tti['tti_2h_ok']}",
        })

    logger.info(
        f"[PULSEV2 CHECK] {symbol} source={source} ctx3m={ctx_3m} fresh={ctx_3m_fresh} "
        f"rmi6h={rmi_6h} fresh={rmi_6h_fresh} bias6h={bias_6h} fresh={bias_6h_fresh} "
        f"rmi2h={rmi_2h} fresh={rmi_2h_fresh} candidates={len(candidates)}"
    )

    ready_candidates = [c for c in candidates if c['ready']]
    for candidate in ready_candidates:
        if should_send(
            symbol,
            f"pulsev2_prep_{candidate['name']}_{candidate['direction']}",
            cooldown=3600,
        ):
            send_info(
                f"<b>[PREP {candidate['label']}]</b> {symbol}\n"
                f"--------------------\n"
                f"Direction: {candidate['direction']}\n"
                f"Price: ${format_price(price)}\n\n"
                + "\n".join(candidate['lines']) + "\n"
                f"[WAIT] Zone ST Context 3m: {(ctx_3m or 'NEUTRE').upper()}"
            )

    entry_candidates = [
        c for c in ready_candidates
        if ctx_3m_fresh and ctx_3m == c['exp_ctx']
    ]
    if not entry_candidates:
        for candidate in candidates:
            logger.info(
                f"[PULSEV2 WAITING] {symbol} {candidate['name']} dir={candidate['direction']} "
                f"ready={candidate['ready']} {candidate['log']} ctx3m={ctx_3m}/{candidate['exp_ctx']} fresh={ctx_3m_fresh}"
            )
        return False

    directions = {c['direction'] for c in entry_candidates}
    if len(directions) > 1:
        logger.warning(f"[PULSEV2 BLOCKED] {symbol} directions contradictoires: {directions}")
        return False

    selected = next((c for c in entry_candidates if c['name'] == 'principal_rmi6h'), entry_candidates[0])
    quality = selected['quality'] or len(entry_candidates) > 1
    quality_line = "\u2b50 <b>PULSE V2 HAUTE QUALITE: TTI 6H + TTI 2H alignes</b>" if selected['quality'] else None
    if len(entry_candidates) > 1:
        quality_line = "\u2b50 <b>PULSE V2 HAUTE QUALITE: principale + secondaire alignees</b>"

    detail_lines = []
    if quality_line:
        detail_lines.append(quality_line)
    detail_lines.extend(selected['lines'])
    detail_lines.append(f"[OK] Zone ST Context 3m: {ctx_3m.upper()}")
    if len(entry_candidates) > 1:
        detail_lines.append("[BONUS] Confirmation secondaire: OK")

    event_key = event_id or f"pulsev2_{selected['name']}_{symbol}_{int(time.time())}_{selected['exp_ctx']}"
    opened = _open_strategy_entry(
        symbol,
        'PULSEV2',
        selected['direction'],
        selected['name'],
        event_key,
        price,
        exchange_name,
        detail_lines,
        cooldown=3600,
    )
    if opened:
        logger.info(f"[PULSEV2] Entree {selected['name']}: {symbol} {selected['direction']}")
    return opened


def _evaluate_daily_range10m_pyramiding(symbol, range_dir, signal_ts, price, exchange_name, event_id):
    """Pyramiding DAILY: nouveau RF10 + ST AI 2H, bloque par Context 10m oppose."""
    return False  # DAILY pyramiding desactive (demande utilisateur) — code garde en reference
    m = MOMENTUM_STATE.get(symbol, {})
    direction = 'LONG' if range_dir == 'buy' else 'SHORT'
    exp_ctx = range_dir
    opp_ctx = 'sell' if range_dir == 'buy' else 'buy'
    st_2h = m.get('st_2h') or m.get('st_ai_2h')
    ctx_10m = m.get('st_context_10m')
    williams_2d = get_williams_filter(symbol, '2d', direction, 72 * 3600)
    antichop = ctx_10m == opp_ctx and is_signal_fresh(m.get('st_context_10m_ts'), 30 * 60)
    pos_key = f"{symbol}_DAILY"
    with STATE_LOCK:
        pos = SCALP_POSITIONS.get(pos_key)
        can_pyra = bool(
            pos and pos.get('direction') == direction
            and PYRA_ENABLED.get(pos_key, False)
            and st_2h == exp_ctx and not antichop
        )
        if not can_pyra or not should_send(
            symbol, f"daily_pyra_range10m_{exp_ctx}", event_id=event_id, cooldown=1800,
        ):
            return False
        pos['entry_count'] = int(pos.get('entry_count', 1)) + 1
        count = pos['entry_count']
    send_telegram(
        f"<b>[DAILY - PYRAMIDING #{count}]</b> {symbol}\n--------------------\n"
        f"Direction: {direction}\nPrice: ${format_price(price)}\nExchange: {exchange_name.upper()}\n"
        f"[OK] Flip Range Filter 10m (100/2.00): {range_dir.upper()}\n"
        f"[OK] ST AI 2H: {st_2h.upper()}\n"
        f"{format_optional_williams_line('2D', williams_2d)}\n"
        f"[ANTI-CHOP] ST Context 10m oppose: {antichop}\n{get_market_context_info()}",
        ntfy=True,
    )
    persist_runtime_state()
    logger.info(f"[DAILY] Pyramiding Range10m #{count}: {symbol} {direction}")
    return True


def evaluate_daily_primary_confluence(symbol, price=0.0, exchange_name=None, event_id=None, source='webhook'):
    """Entree DAILY principale: flip ST AI 30m + ST AI 1D + Bias 6H + Context 30m."""
    if not is_trade_symbol(symbol):
        return False
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')

    st_30m = m.get('st_ai_30m')
    if st_30m not in ('buy', 'sell'):
        return False

    direction = 'LONG' if st_30m == 'buy' else 'SHORT'
    exp_ctx = st_30m
    exp_bias = 'bull' if direction == 'LONG' else 'bear'
    st_1d = m.get('st_ai_1d') or ST_AI_1D.get(symbol)
    bias_6h = m.get('bias_6h')
    ctx_30m = ST_CONTEXT_30M.get(symbol)
    ctx_10m = m.get('st_context_10m')
    williams_1d = get_williams_filter(symbol, '1d', direction, 36 * 3600)
    williams_2d = get_williams_filter(symbol, '2d', direction, 72 * 3600)
    williams_2h = get_williams_filter(symbol, '2h', direction, 6 * 3600)

    st_1d_ok = st_1d == exp_ctx and is_signal_fresh(m.get('st_ai_1d_ts'), 36 * 3600)
    bias_6h_ok = bias_6h == exp_bias
    ctx_30m_ok = ctx_30m == exp_ctx and is_signal_fresh(m.get('st_context_30m_ts'), 90 * 60)
    ctx_10m_quality = ctx_10m == exp_ctx and is_signal_fresh(m.get('st_context_10m_ts'), 30 * 60)
    daily_ok = st_1d_ok and bias_6h_ok and ctx_30m_ok

    logger.info(
        f"[DAILY PRIMARY CHECK] {symbol} dir={direction} source={source} "
        f"st1d={st_1d}/{exp_ctx} ok={st_1d_ok} "
        f"trigger_st30m={st_30m}/{exp_ctx} "
        f"bias6h={bias_6h}/{exp_bias} ok={bias_6h_ok} "
        f"ctx30m={ctx_30m}/{exp_ctx} ok={ctx_30m_ok} "
        f"will1d={williams_1d['trend']} fresh={williams_1d['fresh']} ok={williams_1d['ok']} "
        f"will2d={williams_2d['trend']} fresh={williams_2d['fresh']} ok={williams_2d['ok']} "
        f"will2h={williams_2h['trend']} fresh={williams_2h['fresh']} ok={williams_2h['ok']} "
        f"ctx10m_quality={ctx_10m}/{exp_ctx} ok={ctx_10m_quality} "
        f"ok={daily_ok}"
    )
    if not daily_ok:
        return False

    event_key = event_id or f"daily_primary_{symbol}_{int(time.time())}_{exp_ctx}"
    opened = _open_strategy_entry(
        symbol, 'DAILY', direction, 'principal_st30m', event_key, price, exchange_name,
        [
            f"[QUALITE] ST Context 10m: {(ctx_10m or 'NEUTRE').upper()}" if ctx_10m_quality else f"[INFO] ST Context 10m: {(ctx_10m or 'NEUTRE').upper()}",
            f"[OK] ST AI 1D: {(st_1d or 'N/A').upper()}",
            f"[OK] Bias 6H: {(bias_6h or 'N/A').upper()} (EMA17/SMA40)",
            f"[OK] ST Context 30m: {(ctx_30m or 'N/A').upper()}",
            f"[OK] Flip ST AI 30m: {(st_30m or 'N/A').upper()}",
            format_williams_filter_line('1D', williams_1d),
            format_optional_williams_line('2D', williams_2d),
            format_williams_filter_line('2H', williams_2h),
        ],
        cooldown=14400,
    )
    if opened:
        logger.info(f"[DAILY] Entree principale confluence: {symbol} {direction}")
    return opened


def evaluate_daily_primary_after_okx(symbol, price=0.0, exchange_name=None):
    """Rejoue DAILY apres recalcul OKX seulement si un vrai flip ST AI 30m est encore recent."""
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    flip_dir = m.get('daily_st_ai_30m_flip_dir')
    flip_ts = m.get('daily_st_ai_30m_flip_ts')
    flip_event_id = m.get('daily_st_ai_30m_flip_event_id')

    if flip_dir not in ('buy', 'sell') or not is_signal_fresh(flip_ts, 90 * 60):
        return False
    if m.get('st_ai_30m') != flip_dir:
        return False

    event_key = flip_event_id or f"daily_primary_st30m_flip_{symbol}_{int(flip_ts)}_{flip_dir}"
    return evaluate_daily_primary_confluence(
        symbol,
        price=price,
        exchange_name=exchange_name,
        event_id=event_key,
        source='okx_post_recalc_st_ai_30m_flip',
    )


def evaluate_pulse_range_filter_30m(symbol, range_dir, signal_ts, price=0.0, exchange_name=None, event_id=None):
    """Evalue uniquement le pyramiding PULSE sur un nouveau flip Range Filter 30m."""
    return False  # PULSE pyramiding desactive (demande utilisateur) — code garde en reference, independant du flag ENABLE_PULSE_LEGACY
    if not CONFIG.get('ENABLE_PULSE_LEGACY', False):
        return False
    if range_dir not in ('buy', 'sell'):
        return False
    m = MOMENTUM_STATE.get(symbol, {})
    direction = 'LONG' if range_dir == 'buy' else 'SHORT'
    exp_ctx = range_dir
    exp_bias = 'bull' if direction == 'LONG' else 'bear'
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    event_key = event_id or f"range30m_pulse_{symbol}_{signal_ts}_{range_dir}"

    st_6h = m.get('st_6h') or m.get('st_ai_6h')
    bias_2h = m.get('bias_2h')
    ctx_10m = m.get('st_context_10m')
    williams_6h = get_williams_filter(symbol, '6h', direction, 18 * 3600)
    williams_1d = get_williams_filter(symbol, '1d', direction, 36 * 3600)

    st_6h_ok = (
        st_6h == exp_ctx
        and is_signal_fresh(m.get('st_6h_ts'), 9 * 3600)
    )
    bias_2h_ok = bias_2h == exp_bias

    logger.info(
        f"[PULSE RANGE30M CHECK] {symbol} dir={direction} rf30={range_dir} "
        f"st6h={st_6h}/{exp_ctx} ok={st_6h_ok} "
        f"bias2h={bias_2h}/{exp_bias} ok={bias_2h_ok} "
        f"will6h={williams_6h['trend']} fresh={williams_6h['fresh']} ok={williams_6h['ok']} "
        f"will1d={williams_1d['trend']} fresh={williams_1d['fresh']} ok={williams_1d['ok']} "
        f"ctx10m_info={ctx_10m}/{exp_ctx} entry_main=False"
    )

    opened = False
    pos_key = f"{symbol}_PULSE"
    st_2h = m.get('st_2h') or m.get('st_ai_2h')
    ctx10m_opp_block = (
        ctx_10m == ('sell' if direction == 'LONG' else 'buy')
        and is_signal_fresh(m.get('st_context_10m_ts'), 30 * 60)
    )
    st_2h_ok = (
        st_2h == exp_ctx
        and is_signal_fresh(m.get('st_ai_2h_ts'), 6 * 3600)
    )

    with STATE_LOCK:
        pos = SCALP_POSITIONS.get(pos_key)
        can_pyra = bool(
            not opened
            and pos
            and pos.get('direction') == direction
            and PYRA_ENABLED.get(pos_key, False)
            and st_2h_ok
            and not ctx10m_opp_block
        )
        if can_pyra and should_send(
            symbol, f"pulse_pyra_range30m_{exp_ctx}",
            event_id=event_key, cooldown=1800,
        ):
            pos['entry_count'] = int(pos.get('entry_count', 1)) + 1
            entry_count = pos['entry_count']
        else:
            entry_count = None

    if entry_count:
        emoji = "\U0001f7e2" if direction == "LONG" else "\U0001f534"
        send_telegram(
            f"{emoji} <b>[PULSE - PYRAMIDING #{entry_count}]</b> {symbol}\n"
            f"--------------------\n"
            f"Direction: {direction}\n"
            f"Price: ${format_price(price)}\n"
            f"Exchange: {exchange_name.upper()}\n"
            f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
            f"[OK] Flip Range Filter 30m (100/2.00): {range_dir.upper()}\n"
            f"[OK] ST AI 2H: {(st_2h or 'N/A').upper()}\n"
            f"{format_optional_williams_line('1D', williams_1d)}\n"
            f"[ANTI-CHOP] ST Context 10m oppose: {ctx10m_opp_block}\n"
            f"{get_market_context_info()}",
            ntfy=True,
        )
        persist_runtime_state()
        logger.info(f"[PULSE] Pyramiding Range30m #{entry_count}: {symbol} {direction}")
        return True

    if not opened:
        logger.info(
            f"[PULSE RANGE30M BLOCKED] {symbol} dir={direction} "
            f"entry_main=False "
            f"pyra_pos={bool(SCALP_POSITIONS.get(pos_key))} "
            f"pyra_enabled={PYRA_ENABLED.get(pos_key, False)} "
            f"st2h={st_2h}/{exp_ctx} ok={st_2h_ok} "
            f"ctx10m_opp_block={ctx10m_opp_block}"
        )
    return opened


def evaluate_pulse_context_10m_alert(symbol, price=0.0, exchange_name=None, event_id=None):
    """Alerte PULSE quand ST AI 6H + Bias 2H + ST Context 10m sont alignes. Williams en bonus."""
    if not CONFIG.get('ENABLE_PULSE_LEGACY', False):
        return False
    if not is_trade_symbol(symbol):
        return False
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    ctx_10m = m.get('st_context_10m')
    if ctx_10m not in ('buy', 'sell') or not is_signal_fresh(m.get('st_context_10m_ts'), 30 * 60):
        return False

    direction = 'LONG' if ctx_10m == 'buy' else 'SHORT'
    exp_ctx = ctx_10m
    exp_bias = 'bull' if direction == 'LONG' else 'bear'
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    st_6h = m.get('st_6h') or m.get('st_ai_6h')
    bias_2h = m.get('bias_2h')
    williams_6h = get_williams_filter(symbol, '6h', direction, 18 * 3600)
    williams_1d = get_williams_filter(symbol, '1d', direction, 36 * 3600)

    st_6h_ok = st_6h == exp_ctx and is_signal_fresh(m.get('st_6h_ts'), 9 * 3600)
    bias_2h_ok = bias_2h == exp_bias
    entry_ok = st_6h_ok and bias_2h_ok

    logger.info(
        f"[PULSE CONTEXT10M CHECK] {symbol} dir={direction} "
        f"st6h={st_6h}/{exp_ctx} ok={st_6h_ok} "
        f"bias2h={bias_2h}/{exp_bias} ok={bias_2h_ok} "
        f"will6h={williams_6h['trend']} fresh={williams_6h['fresh']} ok={williams_6h['ok']} "
        f"will1d={williams_1d['trend']} fresh={williams_1d['fresh']} ok={williams_1d['ok']} "
        f"ctx10m={ctx_10m}/{exp_ctx} ok=True entry={entry_ok}"
    )
    if not entry_ok:
        return False

    event_key = event_id or f"pulse_context10m_{symbol}_{int(time.time())}_{exp_ctx}"
    opened = _open_strategy_entry(
        symbol, 'PULSE', direction, 'context10m_st6h_bias2h', event_key, price, exchange_name,
        [
            f"[OK] ST Context 10m: {ctx_10m.upper()}",
            f"[OK] ST AI 6H: {(st_6h or 'N/A').upper()}",
            f"[OK] Bias 2H: {(bias_2h or 'N/A').upper()} (EMA17/SMA40)",
            format_williams_filter_line('6H', williams_6h),
            format_optional_williams_line('1D', williams_1d),
        ],
    )
    if opened:
        logger.info(f"[PULSE] Entree Context10m: {symbol} {direction}")
    return opened


def replay_recent_range_filter_30m(symbol, price=0.0, exchange_name=None, event_id=None, source='state_refresh'):
    """Rejoue le dernier RF30 frais quand un filtre arrive apres le flip."""
    if not is_trade_symbol(symbol):
        return False
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    range_dir = m.get('range_filter_30m') or RANGE_FILTER_30M.get(symbol)
    range_ts = m.get('range_filter_30m_ts')
    signal_ts = m.get('last_range_filter_30m_signal_ts') or int(range_ts or time.time())

    if range_dir not in ('buy', 'sell') or not is_signal_fresh(range_ts, 90 * 60):
        return False

    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    event_base = event_id or f"{source}_rf30_replay_{symbol}_{signal_ts}_{range_dir}"
    logger.info(
        f"[RF30 REPLAY] {symbol} source={source} rf30={range_dir} "
        f"signal_ts={signal_ts} age_ok=True"
    )
    opened = False
    opened = evaluate_pulse_range_filter_30m(
        symbol, range_dir, signal_ts, price, exchange_name,
        event_id=f"{event_base}_pulse",
    ) or opened
    opened = evaluate_context_1d_range_filter_30m(
        symbol, range_dir, signal_ts, price, exchange_name,
        event_id=f"{event_base}_context1d",
    ) or opened
    opened = evaluate_trend_2d_range_filter_30m(
        symbol, range_dir, signal_ts, price, exchange_name,
        event_id=f"{event_base}_trend2d",
    ) or opened
    return opened


def evaluate_context_1d_range_filter_30m(symbol, range_dir, signal_ts, price=0.0, exchange_name=None, event_id=None):
    """CONTEXT1D: flip RF30m + ST Context 1D + ST Context 30m + ST AI 1D alignes."""
    return False  # CONTEXT1D desactivee (demande utilisateur) — code garde en reference
    if not is_trade_symbol(symbol):
        return False
    if range_dir not in ('buy', 'sell'):
        return False
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    ctx_1d = ST_CONTEXT_1D.get(symbol)
    ctx_30m = ST_CONTEXT_30M.get(symbol)
    st_1d = m.get('st_ai_1d') or ST_AI_1D.get(symbol)
    direction = 'LONG' if range_dir == 'buy' else 'SHORT'
    exp_ctx = range_dir
    ctx_1d_fresh = is_signal_fresh(m.get('st_context_1d_ts'), 36 * 3600)
    ctx_30m_fresh = is_signal_fresh(m.get('st_context_30m_ts'), 90 * 60)
    st_1d_fresh = is_signal_fresh(m.get('st_ai_1d_ts'), 36 * 3600)
    all_ok = (
        ctx_1d_fresh
        and ctx_30m_fresh
        and st_1d_fresh
        and ctx_1d == exp_ctx
        and ctx_30m == exp_ctx
        and st_1d == exp_ctx
    )
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')

    logger.info(
        f"[CONTEXT1D RF30M CHECK] {symbol} dir={direction} rf30={range_dir} signal_ts={signal_ts} "
        f"ctx1d={ctx_1d}/{exp_ctx} fresh={ctx_1d_fresh} "
        f"ctx30m={ctx_30m}/{exp_ctx} fresh={ctx_30m_fresh} "
        f"st1d={st_1d}/{exp_ctx} fresh={st_1d_fresh} ok={all_ok}"
    )
    if not all_ok:
        return False

    event_key = event_id or f"context1d_rf30m_{symbol}_{signal_ts}_{exp_ctx}"
    opened = _open_strategy_entry(
        symbol, 'CONTEXT1D', direction, 'context_1d_rf30m', event_key, price, exchange_name,
        [
            f"[OK] Flip Range Filter 30m: {range_dir.upper()}",
            f"[OK] ST Context 1D: {ctx_1d.upper()}",
            f"[OK] ST Context 30m: {ctx_30m.upper()}",
            f"[OK] ST AI 1D: {st_1d.upper()}",
        ],
        cooldown=14400,
    )
    if opened:
        logger.info(f"[CONTEXT1D] Entree RF30m: {symbol} {direction}")
    return opened


def evaluate_trend_2d_range_filter_30m(symbol, range_dir, signal_ts, price=0.0, exchange_name=None, event_id=None):
    """TREND2D: flip RF30m + Context 2H + Context 10m + Bias 2D, bloque par Context 1D oppose."""
    if not is_trade_symbol(symbol):
        return False
    if range_dir not in ('buy', 'sell'):
        return False
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    ctx_2h = m.get('st_context_2h')
    ctx_10m = m.get('st_context_10m')
    ctx_1d = ST_CONTEXT_1D.get(symbol)
    bias_2d = m.get('bias_2d')
    direction = 'LONG' if range_dir == 'buy' else 'SHORT'
    exp_ctx = range_dir
    exp_bias = 'bull' if direction == 'LONG' else 'bear'
    opp_ctx = 'sell' if direction == 'LONG' else 'buy'
    ctx_2h_fresh = is_signal_fresh(m.get('st_context_2h_ts'), 6 * 3600)
    ctx_10m_fresh = is_signal_fresh(m.get('st_context_10m_ts'), 30 * 60)
    ctx_1d_fresh = is_signal_fresh(m.get('st_context_1d_ts'), 36 * 3600)
    bias_2d_fresh = is_signal_fresh(m.get('bias_2d_ts'), 5 * 3600)
    ctx_1d_opp_block = ctx_1d_fresh and ctx_1d == opp_ctx
    all_ok = (
        ctx_2h_fresh
        and ctx_10m_fresh
        and bias_2d_fresh
        and ctx_2h == exp_ctx
        and ctx_10m == exp_ctx
        and bias_2d == exp_bias
        and not ctx_1d_opp_block
    )
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')

    logger.info(
        f"[TREND2D RF30M CHECK] {symbol} dir={direction} rf30={range_dir} signal_ts={signal_ts} "
        f"ctx2h={ctx_2h}/{exp_ctx} fresh={ctx_2h_fresh} "
        f"ctx10m={ctx_10m}/{exp_ctx} fresh={ctx_10m_fresh} "
        f"bias2d={bias_2d}/{exp_bias} fresh={bias_2d_fresh} "
        f"ctx1d_antichop={ctx_1d}/{opp_ctx} fresh={ctx_1d_fresh} block={ctx_1d_opp_block} ok={all_ok}"
    )
    if not all_ok:
        return False

    event_key = event_id or f"trend2d_rf30m_{symbol}_{signal_ts}_{exp_ctx}"
    opened = _open_strategy_entry(
        symbol, 'TREND2D', direction, 'trend_2d_rf30m', event_key, price, exchange_name,
        [
            f"[OK] Flip Range Filter 30m: {range_dir.upper()}",
            f"[OK] ST Context 2H: {ctx_2h.upper()}",
            f"[OK] ST Context 10m: {ctx_10m.upper()}",
            f"[OK] Bias 2D: {bias_2d.upper()} (EMA17/SMA40)",
            f"[ANTI-CHOP] ST Context 1D oppose: {fmt_sig(ctx_1d)}",
        ],
        cooldown=3600,
    )
    if opened:
        logger.info(f"[TREND2D] Entree RF30m: {symbol} {direction}")
    return opened


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

def calc_bias_okx(df, ema_len=17, sma_len=40):
    """EMA17 vs SMA40 — CarreBias uniforme."""
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


def relay_scalp_bias_1h(symbol, bias_1h, price=0):
    """Envoie au scalpbot le Bias 1H calcule par le bot principal."""
    if bias_1h not in ('bull', 'bear', 'neutral'):
        return False
    if not CONFIG['SYMBOLS'].get(symbol, {}).get('scalp'):
        return False
    scalp_url = normalize_base_url(os.environ.get('SCALP_BOT_URL', ''))
    if not scalp_url:
        return False
    payload = {
        'symbol':   symbol,
        'strategy': 'scalp',
        'tf':       '1h',
        'type':     'bias',
        'value':    bias_1h,
        'price':    price,
        'event_id': f"okx_bias_1h_{symbol}_{int(time.time())}",
    }
    try:
        resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
        if resp.status_code == 200:
            logger.info(f"[RELAY] {symbol} bias 1H -> scalpbot OK ({bias_1h})")
            return True
        logger.warning(f"[RELAY] {symbol} bias 1H -> scalpbot HTTP {resp.status_code}: {resp.text[:120]}")
    except Exception as e:
        logger.warning(f"[RELAY] {symbol} bias 1H -> scalpbot erreur: {e}")
    return False


def relay_scalp_bias_2h(symbol, bias_2h, price=0):
    """Envoie au scalpbot le Bias 2H calcule par le bot principal."""
    if bias_2h not in ('bull', 'bear', 'neutral'):
        return False
    if not CONFIG['SYMBOLS'].get(symbol, {}).get('scalp'):
        return False
    scalp_url = normalize_base_url(os.environ.get('SCALP_BOT_URL', ''))
    if not scalp_url:
        return False
    payload = {
        'symbol':   symbol,
        'strategy': 'scalp',
        'tf':       '2h',
        'type':     'bias',
        'value':    bias_2h,
        'price':    price,
        'event_id': f"okx_bias_2h_{symbol}_{int(time.time())}",
    }
    try:
        resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
        if resp.status_code == 200:
            logger.info(f"[RELAY] {symbol} bias 2H -> scalpbot OK ({bias_2h})")
            return True
        logger.warning(f"[RELAY] {symbol} bias 2H -> scalpbot HTTP {resp.status_code}: {resp.text[:120]}")
    except Exception as e:
        logger.warning(f"[RELAY] {symbol} bias 2H -> scalpbot erreur: {e}")
    return False


def update_indicators_for_symbol(symbol):
    """Met a jour tous les indicateurs calculables pour un asset."""
    # Assets sans données OKX directes — indicateurs via webhooks TV uniquement
    OKX_SKIP = {'TAO/USDT'}
    if symbol in OKX_SKIP:
        return
    try:
        # Fetch bougies
        df_1h  = fetch_ohlcv_okx(symbol, '1h',  limit=250)
        df_4h  = fetch_ohlcv_okx(symbol, '4h',  limit=200)
        df_6h  = fetch_ohlcv_okx(symbol, '6h',  limit=200)
        df_30m = fetch_ohlcv_okx(symbol, '30m', limit=100)
        df_1d  = fetch_ohlcv_okx(symbol, '1d',  limit=100)
        df_3d  = fetch_ohlcv_okx(symbol, '1d',  limit=200)  # aggregate pour 3D

        if df_1h is None or df_4h is None or df_1d is None:
            return

        # Calculs
        bias_1h  = calc_bias_okx(df_1h, ema_len=13, sma_len=30)
        df_2h    = fetch_ohlcv_okx(symbol, '2h', limit=150)
        bias_2h  = calc_bias_okx(df_2h, ema_len=17, sma_len=40) if df_2h is not None else None
        bias_4h  = calc_bias_okx(df_4h, ema_len=17, sma_len=40)
        bias_6h  = calc_bias_okx(df_6h, ema_len=17, sma_len=40) if df_6h is not None else None
        bias_30m = calc_bias_okx(df_30m, ema_len=13, sma_len=30) if df_30m is not None and len(df_30m) >= 30 else None
        rmi_2h = calc_rmi_trend(df_2h, length=10, positive_above=62, negative_below=34) if df_2h is not None else None
        rmi_6h = calc_rmi_trend(df_6h, length=10, positive_above=62, negative_below=34) if df_6h is not None else None
        williams_2h = calc_williams_ema(df_2h, length=14, ema_length=14) if df_2h is not None else None
        williams_6h = calc_williams_ema(df_6h, length=14, ema_length=14) if df_6h is not None else None
        williams_1d = calc_williams_ema(df_1d, length=14, ema_length=14)
        try:
            df_2d_w = df_1d.groupby(df_1d.index // 2).agg({
                'open': 'first', 'high': 'max', 'low': 'min',
                'close': 'last', 'volume': 'sum'
            }).reset_index(drop=True)
            williams_2d = calc_williams_ema(df_2d_w, length=14, ema_length=14)
        except Exception:
            williams_2d = None
        bias_1d  = calc_bias_okx(df_1d, ema_len=17, sma_len=40)
        bias_2d  = calc_bias_2d(symbol)
        ema200_1h = calc_ema200_okx(df_1h)

        # Bias 3D — agreger bougies 1D par triplets
        try:
            df_3d_agg = df_3d.groupby(df_3d.index // 3).agg({
                'open': 'first', 'high': 'max', 'low': 'min',
                'close': 'last', 'volume': 'sum'
            }).reset_index(drop=True)
            bias_3d = calc_bias_okx(df_3d_agg, ema_len=17, sma_len=40)
        except Exception:
            bias_3d = None

        # Bias 15m pour pyramiding SCALP
        try:
            df_15m_bias = fetch_ohlcv_okx(symbol, '15m', limit=50)
            if df_15m_bias is not None and len(df_15m_bias) >= 30:
                bias_15m = calc_bias_okx(df_15m_bias, ema_len=17, sma_len=40)
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
                if bias_2d:
                    MOMENTUM_STATE[symbol]['bias_2d'] = bias_2d
                    MOMENTUM_STATE[symbol]['bias_2d_ts'] = datetime.now(timezone.utc).timestamp()
                if bias_3d:
                    MOMENTUM_STATE[symbol]['bias_3d'] = bias_3d
                    MOMENTUM_STATE[symbol]['bias_3d_ts'] = datetime.now(timezone.utc).timestamp()
                MOMENTUM_STATE[symbol]['bias_1d']  = bias_1d
                MOMENTUM_STATE[symbol]['bias_1d_ts'] = datetime.now(timezone.utc).timestamp()
                MOMENTUM_STATE[symbol]['bias_1h']  = bias_1h
                MOMENTUM_STATE[symbol]['bias_1h_ts'] = datetime.now(timezone.utc).timestamp()
                MOMENTUM_STATE[symbol]['bias_4h']  = bias_4h
                if bias_6h is not None:
                    MOMENTUM_STATE[symbol]['bias_6h'] = bias_6h
                    MOMENTUM_STATE[symbol]['bias_6h_ts'] = datetime.now(timezone.utc).timestamp()
                if bias_2h is not None:
                    MOMENTUM_STATE[symbol]['bias_2h'] = bias_2h
                    MOMENTUM_STATE[symbol]['bias_2h_ts'] = datetime.now(timezone.utc).timestamp()
                if bias_30m is not None:
                    MOMENTUM_STATE[symbol]['bias_30m'] = bias_30m
                    MOMENTUM_STATE[symbol]['bias_30m_ts'] = datetime.now(timezone.utc).timestamp()
                if williams_1d is not None:
                    MOMENTUM_STATE[symbol]['williams_1d'] = williams_1d
                    MOMENTUM_STATE[symbol]['williams_1d_ts'] = datetime.now(timezone.utc).timestamp()
                if williams_2d is not None:
                    MOMENTUM_STATE[symbol]['williams_2d'] = williams_2d
                    MOMENTUM_STATE[symbol]['williams_2d_ts'] = datetime.now(timezone.utc).timestamp()
                if williams_2h is not None:
                    MOMENTUM_STATE[symbol]['williams_2h'] = williams_2h
                    MOMENTUM_STATE[symbol]['williams_2h_ts'] = datetime.now(timezone.utc).timestamp()
                if williams_6h is not None:
                    MOMENTUM_STATE[symbol]['williams_6h'] = williams_6h
                    MOMENTUM_STATE[symbol]['williams_6h_ts'] = datetime.now(timezone.utc).timestamp()
                if rmi_2h is not None:
                    MOMENTUM_STATE[symbol]['rmi_2h'] = rmi_2h
                    MOMENTUM_STATE[symbol]['rmi_2h_ts'] = datetime.now(timezone.utc).timestamp()
                if rmi_6h is not None:
                    MOMENTUM_STATE[symbol]['rmi_6h'] = rmi_6h
                    MOMENTUM_STATE[symbol]['rmi_6h_ts'] = datetime.now(timezone.utc).timestamp()


        logger.info(f"[OKX] {symbol} mis a jour — B1H={bias_1h} B2H={bias_2h} B4H={bias_4h} B6H={bias_6h} RMI2H={rmi_2h} RMI6H={rmi_6h} B1D={bias_1d} B2D={bias_2d} B3D={bias_3d} EMA200={ema200_1h:.4f}")
        evaluate_pulse_context_10m_alert(
            symbol,
            price=price,
            exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
            event_id=f"okx_pulse_ctx10m_{symbol}_{int(time.time())}",
        )
        evaluate_pulse_v2(
            symbol,
            price=price,
            exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
            event_id=f"okx_pulsev2_{symbol}_{int(time.time())}",
            source='okx_post_recalc',
        )
        evaluate_daily_primary_after_okx(
            symbol,
            price=price,
            exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
        )
        replay_recent_range_filter_30m(
            symbol,
            price=price,
            exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
            event_id=f"okx_rf30_replay_{symbol}_{int(time.time())}",
            source='okx_post_recalc',
        )
        relay_scalp_bias_1h(symbol, bias_1h, price)
        relay_scalp_bias_2h(symbol, bias_2h, price)
    except Exception as e:
        logger.error(f"[OKX] update_indicators {symbol}: {e}")


def update_daily_radar_bias(symbol):
    """Met a jour uniquement le Bias 1D des assets radar suivis en info."""
    cfg = CONFIG.get('RADAR_SYMBOLS', {}).get(symbol, {})
    if cfg.get('bias_1d_source') == 'tv':
        return
    try:
        df_1d = fetch_ohlcv_okx(symbol, '1d', limit=100)
        if df_1d is None:
            logger.info(f"[RADAR] {symbol} bias1d=None reason=fetch_failed")
            return
        bias_1d = calc_bias_okx(df_1d, ema_len=17, sma_len=40)
        with STATE_LOCK:
            init_symbol_states(symbol)
            MOMENTUM_STATE[symbol]['bias_1d'] = bias_1d
            MOMENTUM_STATE[symbol]['bias_1d_ts'] = datetime.now(timezone.utc).timestamp()
        logger.info(f"[RADAR] {symbol} bias1d={bias_1d}")
    except Exception as e:
        logger.error(f"[RADAR] update_daily_radar_bias {symbol}: {e}")


def check_daily_radar_report():
    """Rapport daily/intraday desactive."""
    return

def check_prep_alerts():
    """Envoie alertes PREP CONTEXT4H et PULSE quand les conditions sont réunies."""
    global PREP_STATE
    with STATE_LOCK:
        state_copy  = dict(MOMENTUM_STATE)
        symbols_conf = CONFIG['SYMBOLS']

    # ━━ PREP CONTEXT4H ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    new_prep_c4h = {'LONG': set(), 'SHORT': set()}

    for symbol, m in state_copy.items():
        if symbol not in symbols_conf:
            continue
        ctx_4h_ct = m.get('st_context_4h')
        ctx_30m   = ST_CONTEXT_30M.get(symbol)
        ctx_lt_1h = ST_CONTEXT_LT_1H.get(symbol)
        ctx_ct_1h = m.get('st_context_1h')

        for direction in ('LONG', 'SHORT'):
            exp = 'buy' if direction == 'LONG' else 'sell'
            opp = 'sell' if direction == 'LONG' else 'buy'
            ct_4h_ok     = ctx_4h_ct  == exp
            ctx_30m_ok   = ctx_30m    == exp
            lt_1h_ok     = ctx_lt_1h  == exp
            ct_1h_neutral = ctx_ct_1h != exp and ctx_ct_1h != opp
            if ct_4h_ok and ctx_30m_ok and lt_1h_ok and ct_1h_neutral:
                new_prep_c4h[direction].add(symbol)

    old_c4h   = PREP_STATE.get('CONTEXT4H', {'LONG': set(), 'SHORT': set()})
    new_long  = new_prep_c4h['LONG']
    new_short = new_prep_c4h['SHORT']
    if False and (new_long != old_c4h.get('LONG', set()) or new_short != old_c4h.get('SHORT', set())):
        lines = ["⏰<b>[PREP CONTEXT4H]</b>"]
        if new_long:
            lines.append("🟢 LONG  : " + "  ".join(sorted(s.replace('/USDT','') for s in new_long)))
        if new_short:
            lines.append("🔴 SHORT : " + "  ".join(sorted(s.replace('/USDT','') for s in new_short)))
        if not new_long and not new_short:
            lines.append("— Aucun asset en préparation")
        lines.append(f"⏰{datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%H:%M (Shanghai)')}")
        send_info("\n".join(lines))
        logger.info("[PREP] CONTEXT4H envoyé")
    PREP_STATE['CONTEXT4H'] = {'LONG': set(), 'SHORT': set()}

    # PREP DAILY
    # Condition : tout est pret sauf le flip Range Filter 10m.
    new_prep_daily = {'LONG': set(), 'SHORT': set()}

    for symbol, m in state_copy.items():
        if symbol not in symbols_conf:
            continue
        st_1d = m.get('st_ai_1d') or ST_AI_1D.get(symbol)
        bias_6h = m.get('bias_6h')
        ctx_30m = ST_CONTEXT_30M.get(symbol)

        for direction in ('LONG', 'SHORT'):
            exp_ctx = 'buy' if direction == 'LONG' else 'sell'
            exp_bias = 'bull' if direction == 'LONG' else 'bear'
            williams_1d = get_williams_filter(symbol, '1d', direction, 36 * 3600)
            prep_ok = (
                st_1d == exp_ctx
                and bias_6h == exp_bias
                and ctx_30m == exp_ctx
                and is_signal_fresh(m.get('st_ai_1d_ts'), 36 * 3600)
                and is_signal_fresh(m.get('st_context_30m_ts'), 90 * 60)
            )
            if prep_ok:
                new_prep_daily[direction].add(symbol)

    old_daily = PREP_STATE.get('DAILY', {'LONG': set(), 'SHORT': set()})
    new_d_long = new_prep_daily['LONG']
    new_d_short = new_prep_daily['SHORT']
    if new_d_long != old_daily.get('LONG', set()) or new_d_short != old_daily.get('SHORT', set()):
        lines = ["⏰<b>[PREP DAILY]</b>"]
        if new_d_long:
            lines.append("🟢 LONG  : " + "  ".join(sorted(s.replace('/USDT','') for s in new_d_long)))
        if new_d_short:
            lines.append("🔴 SHORT : " + "  ".join(sorted(s.replace('/USDT','') for s in new_d_short)))
        if not new_d_long and not new_d_short:
            lines.append("— Aucun asset en préparation")
        lines.append(f"⏰ {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%H:%M (Shanghai)')}")
        send_info("\n".join(lines))
        logger.info("[PREP] DAILY envoye")
    PREP_STATE['DAILY'] = {'LONG': new_d_long, 'SHORT': new_d_short}


def bias4h_report_scheduler():
    """Envoie toutes les 4H le rapport des confluences conservees."""
    if not CONFIG.get('ENABLE_CONFLUENCE_REPORT', False):
        logger.info("[CONFLUENCES] Rapport desactive")
        return
    logger.info("📊 Scheduler rapport confluences demarre (toutes les 4H)")
    # Attendre 10 minutes après démarrage pour que les données soient chargées
    time.sleep(600)
    while True:
        try:
            with STATE_LOCK:
                state_copy  = dict(MOMENTUM_STATE)
                ai2h_bias2h_long = sorted([
                    s.replace('/USDT', '') for s, m in state_copy.items()
                    if (m.get('st_2h') or m.get('st_ai_2h')) == 'buy' and m.get('bias_2h') == 'bull'
                ])
                ai2h_bias2h_short = sorted([
                    s.replace('/USDT', '') for s, m in state_copy.items()
                    if (m.get('st_2h') or m.get('st_ai_2h')) == 'sell' and m.get('bias_2h') == 'bear'
                ])
                ai6h_bias2h_long = sorted([
                    s.replace('/USDT', '') for s, m in state_copy.items()
                    if (m.get('st_6h') or m.get('st_ai_6h')) == 'buy' and m.get('bias_2h') == 'bull'
                ])
                ai6h_bias2h_short = sorted([
                    s.replace('/USDT', '') for s, m in state_copy.items()
                    if (m.get('st_6h') or m.get('st_ai_6h')) == 'sell' and m.get('bias_2h') == 'bear'
                ])
                ai1d_bias6h_long = sorted([
                    s.replace('/USDT', '') for s, m in state_copy.items()
                    if (m.get('st_ai_1d') or ST_AI_1D.get(s)) == 'buy' and m.get('bias_6h') == 'bull'
                ])
                ai1d_bias6h_short = sorted([
                    s.replace('/USDT', '') for s, m in state_copy.items()
                    if (m.get('st_ai_1d') or ST_AI_1D.get(s)) == 'sell' and m.get('bias_6h') == 'bear'
                ])

            ai2h_bias2h_long_str = "  ".join(ai2h_bias2h_long) if ai2h_bias2h_long else "-"
            ai2h_bias2h_short_str = "  ".join(ai2h_bias2h_short) if ai2h_bias2h_short else "-"
            ai6h_bias2h_long_str = "  ".join(ai6h_bias2h_long) if ai6h_bias2h_long else "-"
            ai6h_bias2h_short_str = "  ".join(ai6h_bias2h_short) if ai6h_bias2h_short else "-"
            ai1d_bias6h_long_str = "  ".join(ai1d_bias6h_long) if ai1d_bias6h_long else "-"
            ai1d_bias6h_short_str = "  ".join(ai1d_bias6h_short) if ai1d_bias6h_short else "-"

            msg = (
                f"📊 <b>[CONFLUENCES — {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%H:%M (Shanghai)')}]</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📈 <b>[ST AI 2H + BIAS 2H]</b>\n"
                f"🟢 <b>LONG ({len(ai2h_bias2h_long)})</b> : {ai2h_bias2h_long_str}\n"
                f"🔴 <b>SHORT ({len(ai2h_bias2h_short)})</b> : {ai2h_bias2h_short_str}\n\n"
                f"📈 <b>[ST AI 6H + BIAS 2H]</b>\n"
                f"🟢 <b>LONG ({len(ai6h_bias2h_long)})</b> : {ai6h_bias2h_long_str}\n"
                f"🔴 <b>SHORT ({len(ai6h_bias2h_short)})</b> : {ai6h_bias2h_short_str}\n\n"
                f"📈 <b>[ST AI 1D + BIAS 6H]</b>\n"
                f"🟢 <b>LONG ({len(ai1d_bias6h_long)})</b> : {ai1d_bias6h_long_str}\n"
                f"🔴 <b>SHORT ({len(ai1d_bias6h_short)})</b> : {ai1d_bias6h_short_str}"
            )
            send_info(msg)
            logger.info(
                f"[CONFLUENCES] Rapport envoye "
                f"ai2h_bias2h={len(ai2h_bias2h_long) + len(ai2h_bias2h_short)} "
                f"ai6h_bias2h={len(ai6h_bias2h_long) + len(ai6h_bias2h_short)} "
                f"ai1d_bias6h={len(ai1d_bias6h_long) + len(ai1d_bias6h_short)}"
            )
        except Exception as e:
            logger.error(f"[CONFLUENCES] Erreur rapport: {e}")

        # Attendre la prochaine heure multiple de 4
        now = datetime.now(timezone.utc)
        hours_to_next = 4 - (now.hour % 4)
        next_4h = now.replace(minute=5, second=0, microsecond=0) + timedelta(hours=hours_to_next)
        wait = (next_4h - now).total_seconds()
        time.sleep(max(300, wait))

def range_filter_30m_scheduler():
    """Calcule Range Filter 30m pour les entrees PULSE."""
    logger.info("[RANGE30M] Scheduler demarre (per=100, mult=2.0)")
    OKX_SKIP = {'TAO/USDT'}
    time.sleep(45)
    while True:
        checked_count = 0
        skipped_count = 0
        fetch_ok_count = 0
        signal_count = 0
        new_signal_count = 0
        error_count = 0
        try:
            for symbol in CONFIG['SYMBOLS']:
                if symbol in OKX_SKIP:
                    skipped_count += 1
                    continue
                checked_count += 1
                try:
                    df = fetch_ohlcv_okx(symbol, '30m', limit=260)
                    df_confirmed = keep_confirmed_candles(df, 30)
                    if df_confirmed is not None:
                        fetch_ok_count += 1
                    signal = calc_range_filter_signal(df_confirmed, per=100, mult=2.0)
                    if signal is None:
                        continue
                    signal_count += 1

                    range_dir = signal['direction']
                    signal_ts = signal['ts']
                    signal_price = signal['price']
                    should_process = False

                    with STATE_LOCK:
                        init_symbol_states(symbol)
                        m = MOMENTUM_STATE[symbol]
                        if m.get('last_range_filter_30m_signal_ts') != signal_ts:
                            m['last_range_filter_30m_signal_ts'] = signal_ts
                            m['range_filter_30m'] = range_dir
                            m['range_filter_30m_ts'] = datetime.now(timezone.utc).timestamp()
                            RANGE_FILTER_30M[symbol] = range_dir
                            should_process = True

                    if not should_process:
                        continue
                    new_signal_count += 1

                    logger.info(
                        f"[RANGE30M] Nouveau signal {symbol} "
                        f"dir={range_dir} ts={signal_ts} price={signal_price}"
                    )
                    evaluate_pulse_range_filter_30m(
                        symbol,
                        range_dir,
                        signal_ts,
                        price=signal_price,
                        exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
                        event_id=f"range30m_{symbol}_{signal_ts}_{range_dir}",
                    )
                    evaluate_context_1d_range_filter_30m(
                        symbol,
                        range_dir,
                        signal_ts,
                        price=signal_price,
                        exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
                        event_id=f"range30m_context1d_{symbol}_{signal_ts}_{range_dir}",
                    )
                    evaluate_trend_2d_range_filter_30m(
                        symbol,
                        range_dir,
                        signal_ts,
                        price=signal_price,
                        exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
                        event_id=f"range30m_trend2d_{symbol}_{signal_ts}_{range_dir}",
                    )
                except Exception as e:
                    error_count += 1
                    logger.error(f"[RANGE30M] {symbol}: {e}")
                time.sleep(0.3)
        except Exception as e:
            logger.error(f"[RANGE30M] Scheduler erreur: {e}")
        logger.info(
            f"[RANGE30M] Cycle termine: checked={checked_count}, skipped={skipped_count}, "
            f"fetch_ok={fetch_ok_count}, signals={signal_count}, "
            f"new_signals={new_signal_count}, errors={error_count}"
        )
        now = datetime.now(timezone.utc)
        minutes_to_next = 30 - (now.minute % 30)
        next_30m = now + timedelta(minutes=minutes_to_next)
        next_30m = next_30m.replace(second=20, microsecond=0)
        time.sleep(max(60, (next_30m - now).total_seconds()))


def build_confirmed_10m_candles(df_5m):
    """Agrege les bougies 5m confirmees par paires en bougies 10m."""
    df_5m = keep_confirmed_candles(df_5m, 5)
    if df_5m is None or len(df_5m) < 2:
        return None
    df = df_5m.copy().sort_values('ts').reset_index(drop=True)
    bucket_ms = 10 * 60 * 1000
    df['bucket'] = (df['ts'].astype('int64') // bucket_ms) * bucket_ms
    counts = df.groupby('bucket').size()
    complete = counts[counts >= 2].index
    df = df[df['bucket'].isin(complete)]
    if df.empty:
        return None
    return df.groupby('bucket', as_index=False).agg(
        ts=('bucket', 'first'), open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'), volume=('volume', 'sum'),
    )


def range_filter_10m_scheduler():
    """Calcule RF10 (Length 100, Multiplier 2.00) depuis les bougies OKX 5m."""
    logger.info("[RANGE10M] Scheduler demarre (per=100, mult=2.0, source=5m agrege)")
    time.sleep(50)
    while True:
        try:
            for symbol in CONFIG['SYMBOLS']:
                if symbol == 'TAO/USDT':
                    continue
                try:
                    df_5m = fetch_ohlcv_okx(symbol, '5m', limit=300)
                    df_10m = build_confirmed_10m_candles(df_5m)
                    signal = calc_range_filter_signal(df_10m, per=100, mult=2.0)
                    if signal is None:
                        continue
                    range_dir, signal_ts, signal_price = signal['direction'], signal['ts'], signal['price']
                    with STATE_LOCK:
                        init_symbol_states(symbol)
                        m = MOMENTUM_STATE[symbol]
                        if m.get('last_range_filter_10m_signal_ts') == signal_ts:
                            continue
                        m['last_range_filter_10m_signal_ts'] = signal_ts
                        m['range_filter_10m'] = range_dir
                        m['range_filter_10m_ts'] = datetime.now(timezone.utc).timestamp()
                    logger.info(f"[RANGE10M] Nouveau signal {symbol} dir={range_dir} ts={signal_ts}")
                    evaluate_range_filter_10m(
                        symbol, range_dir, signal_ts, price=signal_price,
                        exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
                        event_id=f"range10m_{symbol}_{signal_ts}_{range_dir}",
                    )
                except Exception as e:
                    logger.error(f"[RANGE10M] {symbol}: {e}")
                time.sleep(0.3)
        except Exception as e:
            logger.error(f"[RANGE10M] Scheduler erreur: {e}")
        now = datetime.now(timezone.utc)
        minutes_to_next = 10 - (now.minute % 10)
        next_run = (now + timedelta(minutes=minutes_to_next)).replace(second=20, microsecond=0)
        time.sleep(max(60, (next_run - now).total_seconds()))


def indicators_scheduler():
    """Recalcule tous les indicateurs depuis OKX toutes les heures."""
    logger.info("[OKX] Scheduler indicateurs démarré (toutes les 15 minutes)")
    # Premier calcul au démarrage après 30s
    time.sleep(30)
    while True:
        radar_symbols = CONFIG.get('RADAR_SYMBOLS', {})
        logger.info(f"[OKX] Calcul indicateurs pour {len(CONFIG['SYMBOLS'])} assets trade + {len(radar_symbols)} assets radar...")
        for symbol in CONFIG['SYMBOLS']:
            update_indicators_for_symbol(symbol)
            time.sleep(0.5)  # rate limit OKX
        for symbol in radar_symbols:
            update_daily_radar_bias(symbol)
            time.sleep(0.5)  # rate limit OKX
        persist_runtime_state()
        check_prep_alerts()
        check_daily_radar_report()
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
            f"   {bulls_2d} bulls / {bears_2d} bears —{pct_2d}%\n\n"
            f"⚡<b>Court terme (4H)</b> : {label_4h}\n"
            f"   {bulls_4h} bulls / {bears_4h} bears —{pct_4h}%\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰{datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}"
        )
        send_info(msg)
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

        logger.info("Heartbeat Telegram desactive")
        # Configurer le webhook Telegram pour les boutons inline
        try:
            tok = CONFIG.get('TELEGRAM_BOT_TOKEN', '')
            base_url = os.environ.get('PUBLIC_BASE_URL', '').rstrip('/')
            if tok and base_url:
                if not base_url.startswith(('https://', 'http://')):
                    base_url = f'https://{base_url}'
                wh_url = f'{base_url}/telegram_callback'
                wh_payload = {'url': wh_url}
                tg_secret = os.environ.get('TELEGRAM_WEBHOOK_SECRET', '')
                if tg_secret:
                    wh_payload['secret_token'] = tg_secret
                resp_wh = requests.post(f'https://api.telegram.org/bot{tok}/setWebhook', json=wh_payload, timeout=10)
                telegram_result = resp_wh.json()
                if resp_wh.status_code != 200 or not telegram_result.get('ok'):
                    raise RuntimeError(f"Telegram setWebhook HTTP {resp_wh.status_code}: {resp_wh.text[:200]}")
                logger.info(f'✅ Telegram webhook configuré: {wh_url}')
            elif tok and not base_url:
                logger.warning('⚠️ PUBLIC_BASE_URL non défini — webhook Telegram non configuré')
                logger.warning('⚠️ Les boutons Telegram (pyramiding, journal) ne fonctionneront PAS')
                # Envoyer un avertissement sur Telegram
                send_info('⚠️ <b>Bot démarré sans webhook Telegram.</b>\nLes boutons inline (pyramiding, journal) sont désactivés.\nConfigurer PUBLIC_BASE_URL sur Railway.')
        except Exception as e:
            logger.warning(f'⚠️ Telegram webhook setup: {e}')
        if CONFIG.get('ENABLE_CONFLUENCE_REPORT', False):
            bias4h_thread = threading.Thread(target=bias4h_report_scheduler, daemon=True)
            bias4h_thread.start()
        else:
            logger.info("[CONFLUENCES] Scheduler non demarre: rapport desactive")

        prep_thread = threading.Thread(target=prep_report_scheduler, daemon=True)
        prep_thread.start()

        indicators_thread = threading.Thread(target=indicators_scheduler, daemon=True)
        indicators_thread.start()

        range30m_thread = threading.Thread(target=range_filter_30m_scheduler, daemon=True)
        range30m_thread.start()

        range10m_thread = threading.Thread(target=range_filter_10m_scheduler, daemon=True)
        range10m_thread.start()

        sentiment_thread = threading.Thread(target=sentiment_scheduler, daemon=True)
        sentiment_thread.start()

        watchdog_thread = threading.Thread(target=tv_alert_watchdog, daemon=True)
        watchdog_thread.start()
        signal_watchdog_thread = threading.Thread(target=tv_signal_watchdog, daemon=True)
        signal_watchdog_thread.start()

        scalp_url_check = os.environ.get('SCALP_BOT_URL', '')
        if not scalp_url_check:
            logger.warning('⚠️ SCALP_BOT_URL non défini — relay scalpbot désactivé')
        else:
            logger.info(f'✅ Relay scalpbot activé →{scalp_url_check}')
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
