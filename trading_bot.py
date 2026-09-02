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
        'ADA/USDT':    {'exchange': 'okx', 'scalp': False},
        'APT/USDT':    {'exchange': 'okx', 'scalp': True},
        'ARB/USDT':    {'exchange': 'okx', 'scalp': False},
        'AVAX/USDT':   {'exchange': 'okx', 'scalp': False},
        'BCH/USDT':    {'exchange': 'okx', 'scalp': False},
        'BNB/USDT':    {'exchange': 'okx', 'scalp': False},
        'BONK/USDT':   {'exchange': 'okx', 'scalp': False},
        'BTC/USDT':    {'exchange': 'okx', 'scalp': True},
        'CHZ/USDT':    {'exchange': 'okx', 'scalp': False},
        'COMP/USDT':   {'exchange': 'okx', 'scalp': False},
        'CRV/USDT':    {'exchange': 'okx', 'scalp': True},
        'CVX/USDT':    {'exchange': 'okx', 'scalp': True},
        'DOGE/USDT':   {'exchange': 'okx', 'scalp': True},
        'DYDX/USDT':   {'exchange': 'okx', 'scalp': False},
        'EIGEN/USDT':  {'exchange': 'okx', 'scalp': False},
        'ENA/USDT':    {'exchange': 'okx', 'scalp': False},
        'ETC/USDT':    {'exchange': 'okx', 'scalp': False},
        'ETH/USDT':    {'exchange': 'okx', 'scalp': True},
        'FARTCOIN/USDT': {'exchange': 'okx', 'scalp': True, 'okx_inst_id': 'FARTCOIN-USDT-SWAP'},
        'FET/USDT':    {'exchange': 'okx', 'scalp': False},
        'FIL/USDT':    {'exchange': 'okx', 'scalp': False},
        'HBAR/USDT':   {'exchange': 'okx', 'scalp': False},
        'HYPE/USDT':   {'exchange': 'okx', 'scalp': True, 'okx_inst_id': 'HYPE-USDT-SWAP'},
        'INJ/USDT':    {'exchange': 'okx', 'scalp': False},
        'LDO/USDT':    {'exchange': 'okx', 'scalp': False},
        'LINK/USDT':   {'exchange': 'okx', 'scalp': True},
        'ONT/USDT':    {'exchange': 'okx', 'scalp': False},
        'PENGU/USDT':  {'exchange': 'okx', 'scalp': True},
        'PEPE/USDT':   {'exchange': 'okx', 'scalp': True},
        'LTC/USDT':    {'exchange': 'okx', 'scalp': False},
        'NEAR/USDT':   {'exchange': 'okx', 'scalp': False},
        'ONDO/USDT':   {'exchange': 'okx', 'scalp': False},
        'RENDER/USDT': {'exchange': 'okx', 'scalp': False},
        'SAND/USDT':   {'exchange': 'okx', 'scalp': False},
        'SKY/USDT':    {'exchange': 'okx', 'scalp': False},
        'SOL/USDT':    {'exchange': 'okx', 'scalp': False},
        'STX/USDT':    {'exchange': 'okx', 'scalp': False},
        'SUI/USDT':    {'exchange': 'okx', 'scalp': False},
        'TAO/USDT':    {'exchange': 'okx', 'scalp': False},  # perp-only
        'TIA/USDT':    {'exchange': 'okx', 'scalp': False},
        'UNI/USDT':    {'exchange': 'okx', 'scalp': False},
        'USELESS/USDT': {'exchange': 'okx', 'scalp': True, 'okx_inst_id': 'USELESS-USDT-SWAP'},
        'VET/USDT':    {'exchange': 'okx', 'scalp': False},
        'VIRTUAL/USDT': {'exchange': 'okx', 'scalp': False},
        'XPL/USDT':    {'exchange': 'okx', 'scalp': True, 'okx_inst_id': 'XPL-USDT-SWAP'},
        'XRP/USDT':    {'exchange': 'okx', 'scalp': True},
        'ZEC/USDT':    {'exchange': 'okx', 'scalp': True},
        'ZEN/USDT':    {'exchange': 'okx', 'scalp': False},
    },

    # Assets suivis uniquement en radar/info. Ils ne declenchent pas les entrees trade.
    # Certains symboles ne sont pas disponibles en spot OKX: ils restent suivis via TradingView.
    'RADAR_SYMBOLS': {
        'FARTBOY/USDT':  {'exchange': 'okx', 'bias_1d_source': 'tv'},
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
    'ENABLE_PULSE_V4': True,
    'ENABLE_DAILY': True,
    'ENABLE_SCALP_RELAY': True,
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
            'SAFE': 0, 'DAILY': 0, 'TREND2D': 0, 'PULSE': 0, 'PULSEV4': 0,
            'RPZ': 0, 'MOMENTUM': 0,
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
    # Redis : etat DAILY + PULSE V4.
    # Relay scalp V3 = ZALT 1m/10m/30m + ST Context 1m/3m + RPZ 30m (avec signal=trend_flip).
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
        "2 DAILY principale: ZALT 2D + ST Context 2H + flip ZALT 2H\n"
        "2 DAILY secondaire: RPZ 2D + ZALT 1D + ST Context 2H + flip ZALT 2H\n"
        "PULSE V4 principale: ZALT 6H + ST Context 30m + ST Context 10m + flip ZALT 10m\n"
        "PULSE V4 secondaire: ZALT 2H + RPZ 6H + ST Context 30m + ST Context 10m + flip ZALT 10m\n"
        "PULSE V4 info: flip ZALT 30m si ZALT 6H ou ZALT 2H + RPZ 6H aligne\n\n"
        "SCALP V3: gere par le scalpbot actif\n"
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
    total_momentum   = sum(s.get('MOMENTUM', 0)     for s in WEEKLY_STATS.values())
    total_swing      = sum(s.get('SWING', 0)        for s in WEEKLY_STATS.values())
    total_pulse      = sum(s.get('PULSE', 0)        for s in WEEKLY_STATS.values())
    total_pulse_v4   = sum(s.get('PULSEV4', 0)      for s in WEEKLY_STATS.values())
    total_rpz        = sum(s.get('RPZ', 0)          for s in WEEKLY_STATS.values())
    total_scalp      = sum(s.get('SCALP', 0)        for s in WEEKLY_STATS.values())

    msg += (
        "📋 <b>Par stratégie:</b>\n"
        f"  — CONFLUENCE: {total_confluence}\n"
        f"  — DAILY: {total_daily}\n"
        f"  — SWING: {total_swing}\n"
        f"  — PULSE: {total_pulse}\n"
        f"  — PULSEV4: {total_pulse_v4}\n"
        f"  — RPZ: {total_rpz}\n"
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
            if stats.get('SWING', 0):       details.append(f"SW:{stats['SWING']}")
            if stats.get('PULSE', 0):       details.append(f"PL:{stats['PULSE']}")
            if stats.get('PULSEV4', 0):     details.append(f"PL4:{stats['PULSEV4']}")
            if stats.get('RPZ', 0):         details.append(f"RPZ:{stats['RPZ']}")
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
            'label': 'ZALT 2D',
            'alert_type': 'zalt',
            'tf': '2d',
            'max_age': 5 * 24 * 3600,
            'warmup': 5 * 24 * 3600,
            'scope': 'all',
        },
        {
            'label': 'RPZ 2D',
            'alert_type': 'rpz',
            'tf': '2d',
            'max_age': 5 * 24 * 3600,
            'warmup': 5 * 24 * 3600,
            'scope': 'all',
        },
        {
            'label': 'ZALT 1D',
            'alert_type': 'zalt',
            'tf': '1d',
            'max_age': 3 * 24 * 3600,
            'warmup': 3 * 24 * 3600,
            'scope': 'all',
        },
        {
            'label': 'ZALT 6H',
            'alert_type': 'zalt',
            'tf': '6h',
            'max_age': 18 * 3600,
            'warmup': 19 * 3600,
            'scope': 'active',
        },
        {
            'label': 'RPZ 6H',
            'alert_type': 'rpz',
            'tf': '6h',
            'max_age': 18 * 3600,
            'warmup': 19 * 3600,
            'scope': 'active',
        },
        {
            'label': 'ST Context 2H',
            'alert_type': 'st_context',
            'tf': '2h',
            'max_age': 6 * 3600,
            'warmup': 7 * 3600,
            'scope': 'active',
        },
        {
            'label': 'ZALT 2H',
            'alert_type': 'zalt',
            'tf': '2h',
            'max_age': 6 * 3600,
            'warmup': 7 * 3600,
            'scope': 'active',
        },
        {
            'label': 'ST Context 10m',
            'alert_type': 'st_context',
            'tf': '10m',
            'max_age': 2 * 3600,
            'warmup': 3 * 3600,
            'scope': 'active',
            'symbol_max_age': {
                'CVX/USDT': 6 * 3600,
                'CRV/USDT': 6 * 3600,
            },
            'symbol_warmup': {
                'CVX/USDT': 6 * 3600,
                'CRV/USDT': 6 * 3600,
            },
        },
        {
            'label': 'ZALT 10m',
            'alert_type': 'zalt',
            'tf': '10m',
            'max_age': 2 * 3600,
            'warmup': 3 * 3600,
            'scope': 'active',
            'symbol_max_age': {
                'CVX/USDT': 6 * 3600,
                'CRV/USDT': 6 * 3600,
            },
            'symbol_warmup': {
                'CVX/USDT': 6 * 3600,
                'CRV/USDT': 6 * 3600,
            },
        },
        {
            'label': 'ZALT 30m',
            'alert_type': 'zalt',
            'tf': '30m',
            'max_age': 4 * 3600,
            'warmup': 4 * 3600,
            'scope': 'active',
        },
        {
            'label': 'ST Context 30m',
            'alert_type': 'st_context',
            'tf': '30m',
            'max_age': 4 * 3600,
            'warmup': 4 * 3600,
            'scope': 'active',
        },
    ]


def tv_watchdog_symbols(req):
    if req.get('scope') == 'all':
        return sorted(get_tracked_symbols())
    return sorted(s for s, cfg in CONFIG['SYMBOLS'].items() if cfg.get('scalp'))


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
            signal_ts = dict(LAST_WEBHOOK_SIGNAL_TS)

        for req in tv_required_signals():
            if uptime < req['warmup']:
                continue
            missing = []
            stale = []
            for symbol in tv_watchdog_symbols(req):
                ts = signal_ts.get(tv_signal_key(symbol, req['alert_type'], req['tf']))
                max_age = req.get('symbol_max_age', {}).get(symbol, req['max_age'])
                warmup = req.get('symbol_warmup', {}).get(symbol, req['warmup'])
                if ts is None:
                    if uptime >= warmup:
                        missing.append(symbol.replace('/USDT', ''))
                elif now - float(ts) > max_age:
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

def parse_directional_trend_value(val, allow_sideways=False):
    """Convertit une valeur directionnelle brute (ex. RPZ) en 'bull', 'bear' ou 'sideways'."""
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



def parse_rpz_value(val):
    parsed = parse_directional_trend_value(val, allow_sideways=False)
    if parsed == 'bull':
        return 'buy'
    if parsed == 'bear':
        return 'sell'
    return None

def parse_zalt_value(val):
    parsed = parse_directional_trend_value(val, allow_sideways=True)
    if parsed == 'bull':
        return 'buy'
    if parsed == 'bear':
        return 'sell'
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
        'reversal_probability_zone': 'rpz',
        'reversal_probability': 'rpz',
        'rpz_zone': 'rpz',
        'zerolagtrendsignal': 'zalt',
        'zerolagtrendsignals': 'zalt',
        'zero_lag_trend_signal': 'zalt',
        'zero_lag_trend_signals': 'zalt',
        'zls': 'zalt',
        'zalt': 'zalt',
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
            'bias_1h': None, 'bias_1h_ts': None, 'bias_2h': None, 'bias_2h_ts': None, 'bias_4h': None, 'bias_6h': None, 'bias_6h_ts': None, 'bias_30m': None, 'bias_30m_ts': None, 'st_ai_15m': None, 'st_ai_30m': None, 'st_ai_30m_ts': None, 'st_ai_1d': None, 'st_ai_1d_ts': None, 'st_6h_ts': None,
            'daily_st_ai_30m_flip_dir': None, 'daily_st_ai_30m_flip_ts': None, 'daily_st_ai_30m_flip_event_id': None,
            'st_context_2h': None,
            'st_context_6h': None,
            'st_context_3m': None,
            'st_context_10m': None, 'st_context_lt_10m': None,
            'rpz_6h': None, 'rpz_6h_ts': None, 'rpz_2h': None, 'rpz_2h_ts': None, 'rpz_30m': None, 'rpz_30m_ts': None, 'rpz_2d': None, 'rpz_2d_ts': None,
            'zalt_1m': None, 'zalt_1m_ts': None, 'last_zalt_1m_signal_ts': None,
            'zalt_10m': None, 'zalt_10m_ts': None, 'last_zalt_10m_signal_ts': None,
            'zalt_30m': None, 'zalt_30m_ts': None, 'last_zalt_30m_signal_ts': None,
            'zalt_2h': None, 'zalt_2h_ts': None, 'last_zalt_2h_signal_ts': None,
            'zalt_6h': None, 'zalt_6h_ts': None, 'last_zalt_6h_signal_ts': None,
            'zalt_1d': None, 'zalt_1d_ts': None, 'last_zalt_1d_signal_ts': None,
            'zalt_2d': None, 'zalt_2d_ts': None, 'last_zalt_2d_signal_ts': None,
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
            if tf == '1m':
                m['st_context_1m'] = parsed_ctx
                m['st_context_1m_ts'] = now_ts
                logger.info(f"[CTX 1M] symbol={symbol} raw={val} parsed={parsed_ctx} ts={now_ts}")
            elif tf == '1h':
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



        if alert_type == 'rpz':
            parsed_rpz = parse_rpz_value(val)
            if parsed_rpz in ('buy', 'sell'):
                if tf in ('2d', '6h', '2h', '30m'):
                    m[f'rpz_{tf}'] = parsed_rpz
                    m[f'rpz_{tf}_ts'] = now_ts
                    logger.info(f"[RPZ {tf.upper()}] {symbol} = {parsed_rpz}")
                else:
                    logger.info(f"[RPZ] {symbol} tf={tf} ignore: timeframe non utilise")
            else:
                logger.warning(f"[WARN] RPZ valeur invalide pour {symbol}: '{val}'")

        if alert_type == 'zalt':
            parsed_zalt = parse_zalt_value(val)
            zalt_signal = str(data.get('signal') or data.get('event') or '').strip().lower()
            if parsed_zalt in ('buy', 'sell'):
                if tf in ('1m', '10m', '30m', '2h', '6h', '1d', '2d'):
                    m[f'zalt_{tf}'] = parsed_zalt
                    m[f'zalt_{tf}_ts'] = now_ts
                    if zalt_signal in ('trend_flip', 'flip'):
                        m[f'last_zalt_{tf}_signal_ts'] = now_ts
                    logger.info(f"[ZALT {tf.upper()}] {symbol} = {parsed_zalt} signal={zalt_signal or 'state'}")
                else:
                    logger.info(f"[ZALT] {symbol} tf={tf} ignore: timeframe non utilise")
            else:
                logger.warning(f"[WARN] ZALT valeur invalide pour {symbol}: '{val}'")

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
        # STRATEGIES ACTIVES
        # 2 DAILY principale : ZALT 2D + ST Context 2H + flip ZALT 2H
        # 2 DAILY secondaire : RPZ 2D + ZALT 1D + ST Context 2H + flip ZALT 2H
        # PULSE V4 principale : ZALT 6H + ST Context 30m + ST Context 10m + flip ZALT 10m
        # PULSE V4 secondaire : ZALT 2H + RPZ 6H + ST Context 30m + ST Context 10m + flip ZALT 10m
        # PULSE V4 info : flip ZALT 30m si ZALT 6H ou ZALT 2H + RPZ 6H aligne
        # ========================================================================
        # Stocker ST AI 4H pour sync_scalp
        if alert_type == 'supertrend' and tf == '4h':
            st_4h_val = parse_supertrend_value(val)
            if st_4h_val is not None:
                with STATE_LOCK:
                    m = MOMENTUM_STATE.get(symbol, {})
                    m['st_4h'] = st_4h_val
                    MOMENTUM_STATE[symbol] = m
        # Stocker ST AI 6H pour les reliquats de suivi et diagnostics.
        if alert_type == 'supertrend' and tf == '6h':
            st_6h_val = parse_supertrend_value(val)
            if st_6h_val is not None:
                with STATE_LOCK:
                    m = MOMENTUM_STATE.get(symbol, {})
                    m['st_6h'] = st_6h_val
                    m['st_6h_ts'] = time.time()
                    MOMENTUM_STATE[symbol] = m

        # Support optionnel des alertes TradingView Range Filter. RF10/RF30
        # peuvent aussi venir des schedulers OKX, RF1/RF3 viennent de TradingView.
        if CONFIG.get('ENABLE_DAILY', True) and is_trade_symbol(symbol) and (
            (alert_type == 'rpz' and tf == '2d')
            or (alert_type == 'zalt' and tf in ('2d', '1d', '2h'))
            or (alert_type == 'st_context' and tf == '2h')
        ):
            zalt_signal = str(data.get('signal') or data.get('event') or '').strip().lower()
            trigger_dir = parse_zalt_value(val) if alert_type == 'zalt' and tf == '2h' and zalt_signal in ('trend_flip', 'flip') else None
            evaluate_daily_rpz(
                symbol,
                trigger_dir=trigger_dir,
                price=price,
                exchange_name=exchange_name,
                event_id=event_id,
                source=f"{alert_type}_{tf}",
            )
            init_symbol_states(symbol)
            m = MOMENTUM_STATE[symbol]
            zalt2h = m.get('zalt_2h')
            if trigger_dir is None and zalt2h in ('buy', 'sell') and is_signal_fresh(m.get('last_zalt_2h_signal_ts'), 6 * 3600):
                evaluate_daily_rpz(
                    symbol,
                    trigger_dir=zalt2h,
                    price=price,
                    exchange_name=exchange_name,
                    event_id=f"daily_replay_zalt2h_{symbol}_{tf}_{alert_type}_{event_id}",
                    source=f"{alert_type}_{tf}_zalt2h_replay",
                )

        if CONFIG.get('ENABLE_PULSE_V4', True) and is_trade_symbol(symbol) and (
            (alert_type == 'rpz' and tf == '6h')
            or (alert_type == 'zalt' and tf in ('2h', '6h', '10m', '30m'))
            or (alert_type == 'st_context' and tf in ('10m', '30m'))
        ):
            zalt_signal = str(data.get('signal') or data.get('event') or '').strip().lower()
            trigger_tf = tf if alert_type == 'zalt' and zalt_signal in ('trend_flip', 'flip') else None
            trigger_dir = parse_zalt_value(val) if trigger_tf in ('10m', '30m') else None
            evaluate_pulse_v3(
                symbol,
                trigger_dir=trigger_dir,
                trigger_tf=trigger_tf,
                price=price,
                exchange_name=exchange_name,
                event_id=event_id,
                source=f"{alert_type}_{tf}",
            )




        persist_runtime_state()
        # ━━ Relay vers le Scalping Bot ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        scalp_url = normalize_base_url(os.environ.get('SCALP_BOT_URL', ''))
        should_relay_scalp = (
            CONFIG.get('ENABLE_SCALP_RELAY', False)
            and (
                (alert_type == 'zalt' and tf in ('1m', '10m', '30m'))
                or (alert_type == 'st_context' and tf in ('1m', '3m'))
                or (alert_type == 'rpz' and tf == '30m')
            )
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
                    sig = str(data.get('signal') or data.get('event') or '').strip().lower()
                    if sig:
                        relay_payload['signal'] = sig
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
    """Rechauffe le scalpbot V3 : ZALT 30m/10m + ST Context 1m/3m + RPZ 30m. Pas de faux flip 1m."""
    if not require_admin_secret():
        return jsonify({'error': 'unauthorized'}), 401
    if not CONFIG.get('ENABLE_SCALP_RELAY', False):
        return jsonify({'status': 'disabled', 'reason': 'scalp relay paused'}), 200
    scalp_url = normalize_base_url(os.environ.get('SCALP_BOT_URL', ''))
    if not scalp_url:
        return jsonify({'error': 'SCALP_BOT_URL non defini'}), 400

    scalp_symbols = {s for s, cfg in CONFIG['SYMBOLS'].items() if cfg.get('scalp')}
    sent, errors = [], []

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

        zalt30 = m.get('zalt_30m')
        if zalt30 in ('buy', 'sell'):
            try:
                payload = {
                    'symbol':   symbol,
                    'strategy': 'scalp',
                    'tf':       '30m',
                    'type':     'zalt',
                    'value':    zalt30,
                    'price':    0,
                    'event_id': f"sync_scalp_zalt30_{symbol}_{int(time.time())}",
                }
                resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
                if resp.status_code == 200:
                    symbol_sent.append('zalt30')
                else:
                    errors.append(f"{symbol}: ZALT30 HTTP {resp.status_code}")
            except Exception as e:
                errors.append(f"{symbol}: ZALT30 {e}")
        else:
            errors.append(f"{symbol}: ZALT 30m absent/invalide ({zalt30!r})")

        zalt10 = m.get('zalt_10m')
        if zalt10 in ('buy', 'sell'):
            try:
                payload = {
                    'symbol':   symbol,
                    'strategy': 'scalp',
                    'tf':       '10m',
                    'type':     'zalt',
                    'value':    zalt10,
                    'price':    0,
                    'event_id': f"sync_scalp_zalt10_{symbol}_{int(time.time())}",
                }
                resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
                if resp.status_code == 200:
                    symbol_sent.append('zalt10')
                else:
                    errors.append(f"{symbol}: ZALT10 HTTP {resp.status_code}")
            except Exception as e:
                errors.append(f"{symbol}: ZALT10 {e}")
        else:
            errors.append(f"{symbol}: ZALT 10m absent/invalide ({zalt10!r})")

        rpz30 = m.get('rpz_30m')
        if rpz30 in ('buy', 'sell'):
            try:
                payload = {
                    'symbol':   symbol,
                    'strategy': 'scalp',
                    'tf':       '30m',
                    'type':     'rpz',
                    'value':    rpz30,
                    'price':    0,
                    'event_id': f"sync_scalp_rpz30_{symbol}_{int(time.time())}",
                }
                resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
                if resp.status_code == 200:
                    symbol_sent.append('rpz30')
                else:
                    errors.append(f"{symbol}: RPZ30 HTTP {resp.status_code}")
            except Exception as e:
                errors.append(f"{symbol}: RPZ30 {e}")
        else:
            errors.append(f"{symbol}: RPZ 30m absent/invalide ({rpz30!r})")

        ctx1 = m.get('st_context_1m')
        try:
            payload = {
                'symbol':   symbol,
                'strategy': 'scalp',
                'tf':       '1m',
                'type':     'st_context',
                'value':    ctx_to_sync_value(ctx1),
                'price':    0,
                'event_id': f"sync_scalp_ctx1_{symbol}_{int(time.time())}",
            }
            resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
            if resp.status_code == 200:
                symbol_sent.append('ctx1m')
            else:
                errors.append(f"{symbol}: CTX1M HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"{symbol}: CTX1M {e}")

        ctx3 = m.get('st_context_3m')
        try:
            payload = {
                'symbol':   symbol,
                'strategy': 'scalp',
                'tf':       '3m',
                'type':     'st_context',
                'value':    ctx_to_sync_value(ctx3),
                'price':    0,
                'event_id': f"sync_scalp_ctx3_{symbol}_{int(time.time())}",
            }
            resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
            if resp.status_code == 200:
                symbol_sent.append('ctx3m')
            else:
                errors.append(f"{symbol}: CTX3M HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"{symbol}: CTX3M {e}")

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












def fmt_sig(value):
    if value in ('buy', 'bull'):
        return 'BUY'
    if value in ('sell', 'bear'):
        return 'SELL'
    return 'NEUTRE'






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


ZALT_HTF_SETTINGS = {
    '2h': {'length': 50, 'mult': 1.2},
    '6h': {'length': 50, 'mult': 1.2},
    '1d': {'length': 50, 'mult': 1.2},
}


def _rma(series, length):
    return series.ewm(alpha=1 / length, adjust=False).mean()




def calc_zalt_from_ohlcv(df, length=50, mult=1.2):
    if df is None or len(df) < length * 3 + 5:
        return None
    d = df.copy().reset_index(drop=True)
    close = d['close']
    high = d['high']
    low = d['low']
    lag = int((length - 1) // 2)
    src = close + (close - close.shift(lag))
    zlema = src.ewm(span=length, adjust=False).mean()
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = _rma(tr, length)
    vol = atr.rolling(length * 3).max() * mult
    upper = zlema + vol
    lower = zlema - vol

    trend = 0
    trends = []
    for i in range(len(d)):
        c = close.iloc[i]
        if pd.isna(zlema.iloc[i]) or pd.isna(vol.iloc[i]):
            trends.append(trend)
            continue
        prev_c = close.iloc[i - 1] if i else c
        prev_up = upper.iloc[i - 1] if i else upper.iloc[i]
        prev_lo = lower.iloc[i - 1] if i else lower.iloc[i]
        if i and prev_c <= prev_up and c > upper.iloc[i]:
            trend = 1
        elif i and prev_c >= prev_lo and c < lower.iloc[i]:
            trend = -1
        trends.append(trend)

    last = trends[-1]
    prev = trends[-2] if len(trends) > 1 else 0
    if last == 1:
        direction = 'buy'
    elif last == -1:
        direction = 'sell'
    else:
        return None
    flip = (prev <= 0 and last > 0) or (prev >= 0 and last < 0)
    return {'trend': direction, 'flip': flip, 'close': float(close.iloc[-1])}




def update_okx_zalt_htf(symbol):
    """ZALT 2H/6H/1D calcules en interne depuis OKX. ZALT 2D reste sur alerte TradingView."""
    if not is_trade_symbol(symbol):
        return
    computed = {}
    for tf, minutes in (('2h', 120), ('6h', 360), ('1d', 1440)):
        cfg = ZALT_HTF_SETTINGS[tf]
        df = keep_confirmed_candles(fetch_ohlcv_okx(symbol, tf, limit=300), minutes)
        computed[tf] = calc_zalt_from_ohlcv(df, length=cfg['length'], mult=cfg['mult'])

    flipped_2h = False
    flip_dir = None
    price = 0.0
    now_ts = time.time()
    with STATE_LOCK:
        init_symbol_states(symbol)
        m = MOMENTUM_STATE[symbol]
        for tf, payload in computed.items():
            if not payload:
                logger.info(f"[ZALT OKX] {symbol} {tf}=None")
                continue
            old = m.get(f'zalt_{tf}')
            m[f'zalt_{tf}'] = payload['trend']
            m[f'zalt_{tf}_ts'] = now_ts
            if payload['flip'] and old in ('buy', 'sell', None) and old != payload['trend']:
                m[f'last_zalt_{tf}_signal_ts'] = now_ts
                logger.info(f"[ZALT OKX] {symbol} {tf}={payload['trend']} FLIP")
                if tf == '2h':
                    flipped_2h = True
                    flip_dir = payload['trend']
                    price = payload['close']
            else:
                logger.info(f"[ZALT OKX] {symbol} {tf}={payload['trend']}")
        persist_runtime_state()

    if flipped_2h and flip_dir in ('buy', 'sell'):
        evaluate_daily_rpz(
            symbol,
            trigger_dir=flip_dir,
            price=price,
            exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
            event_id=f"okx_zalt_2h_flip_{symbol}_{int(now_ts)}",
            source='okx_zalt_2h_flip',
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


def _state_signal(m, field, max_age):
    value = m.get(field)
    fresh = is_signal_fresh(m.get(f'{field}_ts'), max_age)
    return value, fresh


def _ctx_label(value):
    return (value or 'NEUTRE').upper()


def _rpz_condition(m, tf, exp_ctx):
    max_age = {'2d': 5 * 24 * 3600, '6h': 18 * 3600, '2h': 6 * 3600, '30m': 90 * 60}.get(tf, 0)
    value, fresh = _state_signal(m, f'rpz_{tf}', max_age)
    return value, fresh, bool(fresh and value == exp_ctx)


def _zalt_condition(m, tf, exp_ctx):
    max_age = {
        '2d': 5 * 24 * 3600,
        '1d': 3 * 24 * 3600,
        '6h': 18 * 3600,
        '2h': 6 * 3600,
        '30m': 90 * 60,
        '10m': 45 * 60,
        '1m': 5 * 60,
    }.get(tf, 0)
    value, fresh = _state_signal(m, f'zalt_{tf}', max_age)
    return value, fresh, bool(fresh and value == exp_ctx)


def _st_context_condition(m, tf, exp_ctx):
    max_age = {'1m': 5 * 60, '3m': 10 * 60, '10m': 30 * 60, '30m': 90 * 60, '2h': 6 * 3600}.get(tf, 0)
    value, fresh = _state_signal(m, f'st_context_{tf}', max_age)
    return value, fresh, bool(fresh and value == exp_ctx)




def _send_strategy_pyramiding(symbol, strategy, direction, signal_type, event_id, price, exchange_name, detail_lines, cooldown=1800):
    pos_key = f"{symbol}_{strategy}"
    with STATE_LOCK:
        pos = SCALP_POSITIONS.get(pos_key)
        if not (
            pos
            and pos.get('direction') == direction
            and PYRA_ENABLED.get(pos_key, False)
            and should_send(symbol, f"{strategy.lower()}_pyra_{signal_type}_{direction}", event_id=event_id, cooldown=cooldown)
        ):
            return False
        pos['entry_count'] = int(pos.get('entry_count', 1)) + 1
        count = pos['entry_count']

    emoji = "\U0001f7e2" if direction == 'LONG' else "\U0001f534"
    send_telegram(
        f"{emoji} <b>[{strategy} - PYRAMIDING #{count}]</b> {symbol}\n"
        f"--------------------\n"
        f"Direction: {direction}\n"
        f"Price: ${format_price(price)}\n"
        f"Exchange: {exchange_name.upper()}\n"
        f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        + "\n".join(detail_lines) + "\n" + get_market_context_info(),
        ntfy=True,
    )
    persist_runtime_state()
    logger.info(f"[{strategy}] Pyramiding {signal_type} #{count}: {symbol} {direction}")
    return True


def evaluate_daily_rpz(symbol, trigger_dir=None, price=0.0, exchange_name=None, event_id=None, source='state_refresh'):
    """2 DAILY: entree principale ZALT 2D, entree secondaire RPZ 2D + ZALT 1D."""
    if not CONFIG.get('ENABLE_DAILY', True) or not is_trade_symbol(symbol):
        return False
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')

    directions = [trigger_dir] if trigger_dir in ('buy', 'sell') else ['buy', 'sell']
    opened = False
    for exp_ctx in directions:
        direction = 'LONG' if exp_ctx == 'buy' else 'SHORT'
        zalt2d, zalt2d_fresh, zalt2d_ok = _zalt_condition(m, '2d', exp_ctx)
        zalt1d, zalt1d_fresh, zalt1d_ok = _zalt_condition(m, '1d', exp_ctx)
        rpz2d, rpz2d_fresh, rpz2d_ok = _rpz_condition(m, '2d', exp_ctx)
        zalt2h, zalt2h_fresh, zalt2h_ok = _zalt_condition(m, '2h', exp_ctx)
        ctx2h, ctx2h_fresh, ctx2h_ok = _st_context_condition(m, '2h', exp_ctx)
        zalt2h_flip_fresh = is_signal_fresh(m.get('last_zalt_2h_signal_ts'), 6 * 3600)
        trigger_ok = zalt2h_ok and zalt2h_flip_fresh and (trigger_dir is None or trigger_dir == exp_ctx)
        primary_ok = zalt2d_ok and ctx2h_ok and trigger_ok
        secondary_ok = rpz2d_ok and zalt1d_ok and ctx2h_ok and trigger_ok

        logger.info(
            f"[2DAILY CHECK] {symbol} source={source} dir={direction} "
            f"zalt2d={zalt2d}/{exp_ctx} fresh={zalt2d_fresh} ok={zalt2d_ok} "
            f"zalt1d={zalt1d}/{exp_ctx} fresh={zalt1d_fresh} ok={zalt1d_ok} "
            f"rpz2d={rpz2d}/{exp_ctx} fresh={rpz2d_fresh} ok={rpz2d_ok} "
            f"ctx2h={ctx2h}/{exp_ctx} fresh={ctx2h_fresh} ok={ctx2h_ok} "
            f"zalt2h={zalt2h}/{exp_ctx} fresh={zalt2h_fresh} flip_fresh={zalt2h_flip_fresh} "
            f"ok={trigger_ok} primary={primary_ok} secondary={secondary_ok}"
        )

        if primary_ok:
            event_key = event_id or f"2daily_{symbol}_{int(time.time())}_{exp_ctx}"
            opened = _open_strategy_entry(
                symbol,
                'DAILY',
                direction,
                'zalt2d_ctx2h_zalt2h_flip',
                event_key,
                price,
                exchange_name,
                [
                    "[OK] Entree principale",
                    f"[OK] ZALT 2D: {_ctx_label(zalt2d)}",
                    f"[OK] ST Context 2H: {_ctx_label(ctx2h)}",
                    f"[OK] Flip ZALT 2H: {_ctx_label(zalt2h)}",
                ],
                cooldown=14400,
            ) or opened

        if secondary_ok:
            event_key = event_id or f"2daily_secondary_{symbol}_{int(time.time())}_{exp_ctx}"
            opened = _open_strategy_entry(
                symbol,
                'DAILY_SECONDARY',
                direction,
                'rpz2d_zalt1d_ctx2h_zalt2h_flip',
                event_key,
                price,
                exchange_name,
                [
                    "[OK] Entree secondaire",
                    f"[OK] RPZ 2D: {_ctx_label(rpz2d)}",
                    f"[OK] ZALT 1D: {_ctx_label(zalt1d)}",
                    f"[OK] ST Context 2H: {_ctx_label(ctx2h)}",
                    f"[OK] Flip ZALT 2H: {_ctx_label(zalt2h)}",
                ],
                cooldown=14400,
            ) or opened
    return opened






def evaluate_pulse_v3(symbol, trigger_dir=None, trigger_tf=None, price=0.0, exchange_name=None, event_id=None, source='state_refresh'):
    """PULSE V4: entrees 6H/2H+RPZ6H sur flip ZALT10, info sur flip ZALT30."""
    if not CONFIG.get('ENABLE_PULSE_V4', True) or not is_trade_symbol(symbol):
        return False
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    directions = [trigger_dir] if trigger_dir in ('buy', 'sell') else ['buy', 'sell']
    opened = False
    for exp_ctx in directions:
        direction = 'LONG' if exp_ctx == 'buy' else 'SHORT'
        zalt6, zalt6_fresh, zalt6_ok = _zalt_condition(m, '6h', exp_ctx)
        zalt2h, zalt2h_fresh, zalt2h_ok = _zalt_condition(m, '2h', exp_ctx)
        rpz6, rpz6_fresh, rpz6_ok = _rpz_condition(m, '6h', exp_ctx)
        ctx30, ctx30_fresh, ctx30_ok = _st_context_condition(m, '30m', exp_ctx)
        ctx10, ctx10_fresh, ctx10_ok = _st_context_condition(m, '10m', exp_ctx)
        zalt10, zalt10_fresh, zalt10_ok = _zalt_condition(m, '10m', exp_ctx)
        zalt30, zalt30_fresh, zalt30_ok = _zalt_condition(m, '30m', exp_ctx)
        zalt10_flip_fresh = is_signal_fresh(m.get('last_zalt_10m_signal_ts'), 45 * 60)
        zalt30_flip_fresh = is_signal_fresh(m.get('last_zalt_30m_signal_ts'), 90 * 60)
        primary_trigger_ok = zalt10_ok and zalt10_flip_fresh and (trigger_tf is None or trigger_tf == '10m') and (trigger_dir is None or trigger_dir == exp_ctx)
        primary_ok = zalt6_ok and ctx30_ok and ctx10_ok and primary_trigger_ok
        secondary_ok = zalt2h_ok and rpz6_ok and ctx30_ok and ctx10_ok and primary_trigger_ok
        info_context_ok = zalt6_ok or (zalt2h_ok and rpz6_ok)
        info_ok = (
            trigger_tf == '30m'
            and trigger_dir == exp_ctx
            and zalt30_ok
            and zalt30_flip_fresh
            and info_context_ok
        )

        logger.info(
            f"[PULSEV4 CHECK] {symbol} source={source} dir={direction} "
            f"zalt6={zalt6}/{exp_ctx} fresh={zalt6_fresh} ok={zalt6_ok} "
            f"zalt2h={zalt2h}/{exp_ctx} fresh={zalt2h_fresh} ok={zalt2h_ok} "
            f"rpz6={rpz6}/{exp_ctx} fresh={rpz6_fresh} ok={rpz6_ok} "
            f"ctx30={ctx30}/{exp_ctx} fresh={ctx30_fresh} ok={ctx30_ok} "
            f"ctx10={ctx10}/{exp_ctx} fresh={ctx10_fresh} ok={ctx10_ok} "
            f"zalt10={zalt10}/{exp_ctx} fresh={zalt10_fresh} ok={zalt10_ok} flip_fresh={zalt10_flip_fresh} "
            f"zalt30={zalt30}/{exp_ctx} fresh={zalt30_fresh} ok={zalt30_ok} flip_fresh={zalt30_flip_fresh} "
            f"primary={primary_ok} secondary={secondary_ok} info={info_ok}"
        )

        if primary_ok:
            event_key = event_id or f"pulsev4_primary_{symbol}_{int(time.time())}_{exp_ctx}"
            opened = _open_strategy_entry(
                symbol,
                'PULSEV4',
                direction,
                'ctx30_ctx10_zalt10_flip',
                event_key,
                price,
                exchange_name,
                [
                    "[OK] Entree principale",
                    f"[OK] ZALT 6H: {_ctx_label(zalt6)}",
                    f"[OK] ST Context 30m: {_ctx_label(ctx30)}",
                    f"[OK] ST Context 10m: {_ctx_label(ctx10)}",
                    f"[OK] Flip ZALT 10m: {_ctx_label(zalt10)}",
                ],
                cooldown=1800,
            ) or opened

        if secondary_ok:
            event_key = event_id or f"pulsev4_secondary_{symbol}_{int(time.time())}_{exp_ctx}"
            opened = _open_strategy_entry(
                symbol,
                'PULSEV4_SECONDARY',
                direction,
                'zalt2h_rpz6_ctx30_ctx10_zalt10_flip',
                event_key,
                price,
                exchange_name,
                [
                    "[OK] Entree secondaire",
                    f"[OK] ZALT 2H: {_ctx_label(zalt2h)}",
                    f"[OK] RPZ 6H: {_ctx_label(rpz6)}",
                    f"[OK] ST Context 30m: {_ctx_label(ctx30)}",
                    f"[OK] ST Context 10m: {_ctx_label(ctx10)}",
                    f"[OK] Flip ZALT 10m: {_ctx_label(zalt10)}",
                ],
                cooldown=1800,
            ) or opened

        if info_ok and should_send(symbol, f"pulsev4_info_zalt30_{exp_ctx}", event_id=event_id, cooldown=1800):
            quality = zalt6_ok and zalt2h_ok and rpz6_ok
            quality_line = "[QUALITE] ZALT 6H + ZALT 2H + RPZ 6H alignes" if quality else "[INFO] Contexte 6H valide"
            send_telegram(
                f"<b>[PULSE V4 - INFO ZALT 30m]</b> {symbol}\n"
                f"--------------------\n"
                f"Direction: {direction}\n"
                f"Price: ${format_price(price)}\n"
                f"Exchange: {exchange_name.upper()}\n"
                f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
                f"{quality_line}\n"
                f"[OK] Flip ZALT 30m: {_ctx_label(zalt30)}\n"
                f"[INFO] ZALT 6H: {_ctx_label(zalt6)}\n"
                f"[INFO] ZALT 2H: {_ctx_label(zalt2h)}\n"
                f"[INFO] RPZ 6H: {_ctx_label(rpz6)}\n"
                f"{get_market_context_info()}",
                ntfy=True,
            )
            logger.info(f"[PULSEV4 INFO] ZALT30 {symbol} {direction}")
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

        # ADX 15m
        try:
            df_15m_bias = fetch_ohlcv_okx(symbol, '15m', limit=50)
            if df_15m_bias is not None and len(df_15m_bias) >= 30:
                adx_data = calc_adx_okx(df_15m_bias)
                if adx_data:
                    ADX_STATE[symbol] = adx_data
        except Exception as e:
            logger.error(f'[OKX] adx_15m {symbol}: {e}')
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

        logger.info(f"[OKX] {symbol} mis a jour — B1H={bias_1h} B2H={bias_2h} B4H={bias_4h} B6H={bias_6h} B1D={bias_1d} B2D={bias_2d} B3D={bias_3d} EMA200={ema200_1h:.4f}")
        update_okx_zalt_htf(symbol)
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

        indicators_thread = threading.Thread(target=indicators_scheduler, daemon=True)
        indicators_thread.start()

        watchdog_thread = threading.Thread(target=tv_alert_watchdog, daemon=True)
        watchdog_thread.start()
        signal_watchdog_thread = threading.Thread(target=tv_signal_watchdog, daemon=True)
        signal_watchdog_thread.start()

        scalp_url_check = os.environ.get('SCALP_BOT_URL', '')
        if not CONFIG.get('ENABLE_SCALP_RELAY', False):
            logger.info('Relay scalpbot en pause — strategie scalp en retravail')
        elif not scalp_url_check:
            logger.warning('⚠️ SCALP_BOT_URL non défini — relay scalpbot désactivé')
        else:
            logger.info(f'✅ Relay scalpbot activé →{scalp_url_check}')
        logger.info("⏰ Schedulers démarrés (rapport hebdo + heartbeat + prep report + indicateurs OKX + TV watchdog)")
    except Exception as e:
        logger.error(f"❌ Erreur au démarrage: {e}")

# Démarrer les schedulers seulement dans le worker principal
if os.environ.get('ENABLE_SCHEDULERS', '1') == '1':
    startup_thread = threading.Thread(target=startup, daemon=True)
    startup_thread.start()

if __name__ == '__main__':
    logger.info(f"✅ Bot démarré sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)
