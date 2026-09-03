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
        'AAVE/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'ADA/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'APT/USDT':    {'exchange': 'okx', 'scalp': True, 'pulse': True},
        'ARB/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'AVAX/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'BCH/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'BNB/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'BONK/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'BTC/USDT':    {'exchange': 'okx', 'scalp': True, 'pulse': True},
        'CHZ/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'COMP/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'CRV/USDT':    {'exchange': 'okx', 'scalp': True, 'pulse': True},
        'CVX/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'DOGE/USDT':   {'exchange': 'okx', 'scalp': True, 'pulse': True},
        'DYDX/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'EIGEN/USDT':  {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'ENA/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'ETC/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'ETH/USDT':    {'exchange': 'okx', 'scalp': True, 'pulse': True},
        'FARTCOIN/USDT': {'exchange': 'okx', 'scalp': False, 'pulse': True, 'okx_inst_id': 'FARTCOIN-USDT-SWAP'},
        'FET/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'FIL/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'HBAR/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'HYPE/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': True, 'okx_inst_id': 'HYPE-USDT-SWAP'},
        'INJ/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'LDO/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'LINK/USDT':   {'exchange': 'okx', 'scalp': True, 'pulse': True},
        'ONT/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'PENGU/USDT':  {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'PEPE/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'LTC/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'NEAR/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'ONDO/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'RENDER/USDT': {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'SAND/USDT':   {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'SKY/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'SOL/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'STX/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'SUI/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'TAO/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': True},  # perp-only
        'TIA/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'UNI/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'USELESS/USDT': {'exchange': 'okx', 'scalp': False, 'pulse': True, 'okx_inst_id': 'USELESS-USDT-SWAP'},
        'VET/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'VIRTUAL/USDT': {'exchange': 'okx', 'scalp': False, 'pulse': False},
        'XPL/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': True, 'okx_inst_id': 'XPL-USDT-SWAP'},
        'XRP/USDT':    {'exchange': 'okx', 'scalp': True, 'pulse': True},
        'ZEC/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': True},
        'ZEN/USDT':    {'exchange': 'okx', 'scalp': False, 'pulse': False},
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
    return set(CONFIG['SYMBOLS'])

def is_trade_symbol(symbol):
    return symbol in CONFIG['SYMBOLS']

def is_pulse_symbol(symbol):
    return symbol in CONFIG['SYMBOLS'] and CONFIG['SYMBOLS'][symbol].get('pulse', False)

def get_symbol_config(symbol):
    return CONFIG['SYMBOLS'].get(symbol) or {}

@app.route('/')
def home():
    total_symbols = len(CONFIG['SYMBOLS'])
    okx_count = sum(1 for ex in CONFIG['SYMBOLS'].values() if ex.get('exchange') == 'okx')
    return f"""
    <h1>Trading Bot Multi-Strategy</h1>
    <p>Status: Running</p>
    <p>Trade assets: {total_symbols} | OKX trade: {okx_count}</p>
    <p>Strategies: DAILY / PULSE / SCALP</p>
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
    # Relay scalp = ZALT 5m + RPZ 30m + ST Context 5m/15m/30m (avec signal=trend_flip).
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
            'scalp_positions':    dict(SCALP_POSITIONS),
            'st_context_1d':      dict(ST_CONTEXT_1D),
            'st_context_3d':      dict(ST_CONTEXT_3D),
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
        SCALP_POSITIONS.update(payload.get('scalp_positions', {}))
        ST_CONTEXT_1D.update(payload.get('st_context_1d', {}))
        ST_CONTEXT_3D.update(payload.get('st_context_3d', {}))
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


def send_telegram_with_buttons(msg, token=None, chat_id=None,
                               journal_symbol=None, journal_strategy=None,
                               journal_direction=None, journal_price=None):
    """Envoie un message Telegram avec bouton Journal (optionnel) + ntfy."""
    rows = []
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
        "DAILY A: RPZ 1D + flip ZALT 4H (OKX) + veto ST Context 12H oppose\n"
        "DAILY B: ST Context 12H + ST Context 4H alignes + flip ZALT 4H (OKX)\n"
        "PULSE A: RPZ 6H + flip ZALT 15m + veto ST Context 2H oppose\n"
        "PULSE B: ST Context 2H + ST Context 15m alignes + flip ZALT 15m\n"
        "SCALP: gere par le scalpbot actif (7 assets)\n"
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
    total_daily      = sum(s.get('DAILY', 0)       for s in WEEKLY_STATS.values())
    total_pulse_v4   = sum(s.get('PULSEV4', 0)      for s in WEEKLY_STATS.values())

    msg += (
        "📋 <b>Par stratégie:</b>\n"
        f"  — DAILY: {total_daily}\n"
        f"  — PULSEV4: {total_pulse_v4}\n\n"
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
            if stats.get('DAILY', 0):       details.append(f"D:{stats['DAILY']}")
            if stats.get('PULSEV4', 0):     details.append(f"PL4:{stats['PULSEV4']}")
            msg += f"  —{base}: {sum(stats.values())} ({', '.join(details)})\n"
    else:
        msg += "📈 <b>Par asset:</b> Aucune alerte cette semaine\n"

    msg += f"\n⏰{now.strftime('%d/%m/%Y %H:%M')} (Taiwan)"
    send_info(msg)
    logger.info("📊 Rapport hebdomadaire envoyé")

    WEEKLY_STATS.clear()
    WEEKLY_START = datetime.now(timezone.utc)

    persist_runtime_state()



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
        '720': '12h', '12hr': '12h', '12hour': '12h',
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

ST_CONTEXT_1D:  dict = {}  # symbol -> 'buy' | 'sell' | None
ST_CONTEXT_3D:  dict = {}  # symbol -> 'buy' | 'sell' | None
PREP_STATE: dict = {}
WEBHOOK_EXECUTOR = ThreadPoolExecutor(max_workers=4)

# Timestamps derniers webhooks TradingView par tf (pour heartbeat)
LAST_WEBHOOK_TS: dict = {}  # tf -> timestamp
LAST_WEBHOOK_SIGNAL_TS: dict = {}  # "symbol|type|tf" -> timestamp

# Positions SCALP
SCALP_POSITIONS: dict = {}      # pos_key -> position dict

def init_symbol_states(symbol):
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'st_context_1h': None, 'st_context_4h': None, 'st_context_12h': None, 'st_context_15m': None, 'st_context_30m': None,
            'st_context_1h_ts': None, 'st_context_2h_ts': None, 'st_context_4h_ts': None, 'st_context_12h_ts': None, 'st_context_6h_ts': None, 'st_context_10m_ts': None, 'st_context_15m_ts': None, 'st_context_30m_ts': None, 'st_context_1d_ts': None, 'st_context_3d_ts': None, 'st_context_5m_ts': None, 'last_st_context_5m_dir': None, 'last_st_context_5m_ts': None,
            'st_context_5m': None,
            'st_1h': None, 'st_1h_ts': None, 'st_4h': None, 'st_6h': None,
            'last_st_6h': None,   # dernier flip 6H
            # Nouveaux états pour CONTEXT v2 et SCALP
            'st_6h_ts': None,
            'st_context_2h': None,
            'st_context_6h': None,
            'st_context_10m': None,
            'rpz_1d': None, 'rpz_1d_ts': None, 'rpz_6h': None, 'rpz_6h_ts': None, 'rpz_2h': None, 'rpz_2h_ts': None, 'rpz_30m': None, 'rpz_30m_ts': None, 'rpz_2d': None, 'rpz_2d_ts': None,
            'zalt_1m': None, 'zalt_1m_ts': None, 'last_zalt_1m_signal_ts': None,
            'zalt_10m': None, 'zalt_10m_ts': None, 'last_zalt_10m_signal_ts': None,
            'zalt_30m': None, 'zalt_30m_ts': None, 'last_zalt_30m_signal_ts': None,
            'zalt_2h': None, 'zalt_2h_ts': None, 'last_zalt_2h_signal_ts': None,
            'zalt_4h': None, 'zalt_4h_ts': None, 'last_zalt_4h_signal_ts': None,
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
        val         = str(val_raw).strip().lower()
        try:
            price = float(data.get('price', 0) or 0)
        except (TypeError, ValueError):
            price = 0.0

        logger.info(f"📥 Webhook: {symbol} | strat={strat} | tf={tf} | type={alert_type} | val={val} | price={price}")
        # Tracker le dernier webhook reçu par tf
        LAST_WEBHOOK_TS[tf] = time.time()
        audit_log(data, status="reçu")
        event_id = build_event_id(data, symbol, strat, tf, alert_type, val)

        if symbol not in get_tracked_symbols():
            logger.info(f"⚠️ {symbol} non dans la watchlist")
            audit_log(data, status="ignoré_watchlist")
            return jsonify({'status': 'ignored', 'reason': 'not_in_watchlist'}), 200

        exchange_name = get_symbol_config(symbol).get('exchange', 'okx')
        init_symbol_states(symbol)
        track_tv_signal(symbol, alert_type, tf)

        # Mise à jour globale des contextes (indépendante de la stratégie du webhook)
        m = MOMENTUM_STATE[symbol]
        now_ts = datetime.now(timezone.utc).timestamp()
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
            elif tf == '4h':
                m['st_context_4h'] = parsed_ctx
                m['st_context_4h_ts'] = now_ts
            elif tf == '6h':
                m['st_context_6h'] = parsed_ctx
                m['st_context_6h_ts'] = now_ts
            elif tf == '12h':
                m['st_context_12h'] = parsed_ctx
                m['st_context_12h_ts'] = now_ts
            elif tf == '15m':
                m['st_context_15m'] = parsed_ctx
                m['st_context_15m_ts'] = now_ts
            elif tf == '30m':
                m['st_context_30m'] = parsed_ctx
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
                ST_CONTEXT_3D[symbol] = parsed_ctx
                m['st_context_3d_ts'] = now_ts



        if alert_type == 'rpz':
            parsed_rpz = parse_rpz_value(val)
            if parsed_rpz in ('buy', 'sell'):
                if tf in ('1d', '2d', '6h', '2h', '30m'):
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
                if tf in ('1m', '10m', '15m', '30m', '2h', '4h', '6h', '1d', '2d'):
                    m[f'zalt_{tf}'] = parsed_zalt
                    m[f'zalt_{tf}_ts'] = now_ts
                    if zalt_signal in ('trend_flip', 'flip'):
                        m[f'last_zalt_{tf}_signal_ts'] = now_ts
                    logger.info(f"[ZALT {tf.upper()}] {symbol} = {parsed_zalt} signal={zalt_signal or 'state'}")
                else:
                    logger.info(f"[ZALT] {symbol} tf={tf} ignore: timeframe non utilise")
            else:
                logger.warning(f"[WARN] ZALT valeur invalide pour {symbol}: '{val}'")

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
            if alert_type == 'supertrend' and tf == '2h':
                prev_2h = m.get('st_2h')
                m['st_2h'] = parse_supertrend_value(val)
                m['st_2h_flipped'] = bool(prev_2h is not None and m['st_2h'] is not None and m['st_2h'] != prev_2h)
                if m['st_2h_flipped'] and prev_2h:
                    m['last_st_2h'] = prev_2h
            if alert_type == 'supertrend' and tf == '4h':
                prev_4h = m.get('st_4h')
                m['prev_st_4h'] = prev_4h  # sauvegarder avant mise à jour
                m['st_4h'] = parse_supertrend_value(val)
                m['st_4h_flipped'] = bool(prev_4h is not None and m['st_4h'] is not None and m['st_4h'] != prev_4h)
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

        # ========================================================================
        # STRATEGIES ACTIVES
        # DAILY A : RPZ 1D + flip ZALT 4H (OKX) + veto ST Context 12H oppose
        # DAILY B : ST Context 12H + ST Context 4H alignes + flip ZALT 4H (OKX)
        # PULSE A : RPZ 6H + flip ZALT 15m + veto ST Context 30m oppose
        # PULSE B : ST Context 30m + ST Context 15m alignes + flip ZALT 15m
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

        # DAILY: trigger = flip ZALT 4H OKX (via update_okx_zalt_htf). Le webhook ne fait
        # que rafraichir sur Context 4H/12H (A ou B) ou RPZ 1D (A) pour retester si le flip 4H est encore frais.
        if CONFIG.get('ENABLE_DAILY', True) and is_trade_symbol(symbol) and (
            (alert_type == 'st_context' and tf in ('4h', '12h'))
            or (alert_type == 'rpz' and tf == '1d')
        ):
            evaluate_daily_rpz(
                symbol,
                trigger_dir=None,
                price=price,
                exchange_name=exchange_name,
                event_id=event_id,
                source=f"{alert_type}_{tf}",
            )

        # PULSE: trigger = flip ZALT 15m (TV). Rafraichi aussi sur Context 2H/15m (A ou B)
        # ou RPZ 6H (A) pour retester si le flip 15m est encore frais.
        if CONFIG.get('ENABLE_PULSE_V4', True) and is_pulse_symbol(symbol) and (
            (alert_type == 'zalt' and tf == '15m')
            or (alert_type == 'st_context' and tf in ('15m', '2h'))
            or (alert_type == 'rpz' and tf == '6h')
        ):
            zalt_signal = str(data.get('signal') or data.get('event') or '').strip().lower()
            trigger_dir = parse_zalt_value(val) if alert_type == 'zalt' and tf == '15m' and zalt_signal in ('trend_flip', 'flip') else None
            evaluate_pulse_v3(
                symbol,
                trigger_dir=trigger_dir,
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
                (alert_type == 'zalt' and tf == '5m')
                or (alert_type == 'rpz' and tf == '30m')
                or (alert_type == 'st_context' and tf in ('5m', '15m', '30m'))
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

        if callback_data.startswith('journal_log:'):
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
    """Route conservee pour compatibilite. Plus de logique PREP active (CONTEXT4H retiree)."""
    return jsonify({'status': 'ok', 'message': 'Aucune alerte PREP active'}), 200


@app.route('/refresh', methods=['POST'])
def refresh_indicators():
    if not require_admin_secret():
        return jsonify({'error': 'unauthorized'}), 401
    """Relance immédiatement le calcul des indicateurs OKX (ZALT HTF).
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
        symbols = list(CONFIG['SYMBOLS'].keys())

    def _run():
        logger.info(f"[REFRESH] Calcul forcé pour {len(symbols)} assets...")
        for sym in symbols:
            try:
                update_indicators_for_symbol(sym)
            except Exception as e:
                logger.error(f"[REFRESH] {sym}: {e}")
        persist_runtime_state()
        logger.info("[REFRESH] Terminé")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'status': 'ok', 'message': f'Refresh lancé pour {len(symbols)} assets'}), 200


@app.route('/sync_scalp', methods=['POST'])
def sync_scalp():
    """Rechauffe le scalpbot : RPZ 30m (TV, relaye) + ST Context 15m/30m/5m.
    ZALT 5m (trigger) reste TradingView uniquement — n'est jamais stocke par le bot
    principal, il arrivera via le prochain webhook TradingView normal (relaye directement
    par should_relay_scalp)."""
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

        ctx15 = m.get('st_context_15m')
        try:
            payload = {
                'symbol':   symbol,
                'strategy': 'scalp',
                'tf':       '15m',
                'type':     'st_context',
                'value':    ctx_to_sync_value(ctx15),
                'price':    0,
                'event_id': f"sync_scalp_ctx15_{symbol}_{int(time.time())}",
            }
            resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
            if resp.status_code == 200:
                symbol_sent.append('ctx15m')
            else:
                errors.append(f"{symbol}: CTX15M HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"{symbol}: CTX15M {e}")

        ctx30 = m.get('st_context_30m')
        try:
            payload = {
                'symbol':   symbol,
                'strategy': 'scalp',
                'tf':       '30m',
                'type':     'st_context',
                'value':    ctx_to_sync_value(ctx30),
                'price':    0,
                'event_id': f"sync_scalp_ctx30_{symbol}_{int(time.time())}",
            }
            resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
            if resp.status_code == 200:
                symbol_sent.append('ctx30m')
            else:
                errors.append(f"{symbol}: CTX30M HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"{symbol}: CTX30M {e}")

        ctx5 = m.get('st_context_5m')
        try:
            payload = {
                'symbol':   symbol,
                'strategy': 'scalp',
                'tf':       '5m',
                'type':     'st_context',
                'value':    ctx_to_sync_value(ctx5),
                'price':    0,
                'event_id': f"sync_scalp_ctx5_{symbol}_{int(time.time())}",
            }
            resp = requests.post(f"{scalp_url}/webhook", json=payload, timeout=5)
            if resp.status_code == 200:
                symbol_sent.append('ctx5m')
            else:
                errors.append(f"{symbol}: CTX5M HTTP {resp.status_code}")
        except Exception as e:
            errors.append(f"{symbol}: CTX5M {e}")

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
        SCALP_POSITIONS.clear()
        ST_CONTEXT_1D.clear()
        ST_CONTEXT_3D.clear()
        PREP_STATE.clear()
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
        ST_CONTEXT_1D.pop(symbol, None)
        ST_CONTEXT_3D.pop(symbol, None)

        for strat in ['PULSE', 'DAILY', 'TREND2D']:
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
    '30m': {'length': 50, 'mult': 1.2},
    '4h':  {'length': 50, 'mult': 1.2},
    '6h':  {'length': 50, 'mult': 1.2},
    '1d':  {'length': 50, 'mult': 1.3},
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
    """ZALT 30m/4H/6H/1D calcules en interne depuis OKX. ZALT 2D reste sur alerte TradingView.
    Seul le flip 4H declenche evaluate_daily_rpz — 30m/6H/1D ne declenchent jamais
    Daily/Pulse/Scalp evaluate, et ne sont plus exiges par ces strategies (RPZ TV a la place)
    ni relayes au scalpbot (RPZ 30m TV = tendance A scalp desormais)."""
    if not is_trade_symbol(symbol):
        return
    computed = {}
    for tf, minutes in (('30m', 30), ('4h', 240), ('6h', 360), ('1d', 1440)):
        cfg = ZALT_HTF_SETTINGS[tf]
        df = keep_confirmed_candles(fetch_ohlcv_okx(symbol, tf, limit=300), minutes)
        computed[tf] = calc_zalt_from_ohlcv(df, length=cfg['length'], mult=cfg['mult'])

    flipped_4h = False
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
                if tf == '4h':
                    flipped_4h = True
                    flip_dir = payload['trend']
                    price = payload['close']
            else:
                logger.info(f"[ZALT OKX] {symbol} {tf}={payload['trend']}")
        persist_runtime_state()

    if flipped_4h and flip_dir in ('buy', 'sell'):
        evaluate_daily_rpz(
            symbol,
            trigger_dir=flip_dir,
            price=price,
            exchange_name=get_symbol_config(symbol).get('exchange', 'okx'),
            event_id=f"okx_zalt_4h_flip_{symbol}_{int(now_ts)}",
            source='okx_zalt_4h_flip',
        )






def _open_strategy_entry(symbol, strategy, direction, signal_type, event_id, price, exchange_name, detail_lines, cooldown=3600):
    """Cree une entree unique et envoie l'alerte Telegram correspondante."""
    pos_key = f"{symbol}_{strategy}"
    exp_ctx = 'buy' if direction == 'LONG' else 'sell'
    with STATE_LOCK:
        pos = SCALP_POSITIONS.get(pos_key)
        if pos and pos.get('direction') != direction:
            SCALP_POSITIONS.pop(pos_key, None)
            pos = None
        if pos is not None or not should_send(
            symbol, f"{strategy.lower()}_entry_{signal_type}_{exp_ctx}",
            event_id=event_id, cooldown=cooldown,
        ):
            return False
        SCALP_POSITIONS[pos_key] = {
            'direction': direction, 'entry_count': 1, 'signal_type': signal_type,
        }
    emoji = "\U0001f7e2" if direction == 'LONG' else "\U0001f534"
    send_telegram_with_buttons(
        f"{emoji} <b>[{strategy} - ENTREE]</b> {symbol}\n"
        f"--------------------\nDirection: {direction}\n"
        f"Price: ${format_price(price)}\nExchange: {exchange_name.upper()}\n"
        f"Time: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M (Shanghai)')}\n\n"
        + "\n".join(detail_lines) + "\n" + get_market_context_info(),
        journal_symbol=symbol, journal_strategy=strategy,
        journal_direction=direction, journal_price=price,
    )
    track_alert(symbol, strategy)
    persist_runtime_state()
    logger.info(f"[{strategy}] Entree {signal_type}: {symbol} {direction}")
    return True


def _trade_direction_to_ctx(direction):
    return 'buy' if direction == 'LONG' else 'sell'


def _state_signal(m, field, max_age):
    value = m.get(field)
    fresh = is_signal_fresh(m.get(f'{field}_ts'), max_age)
    return value, fresh


def _ctx_label(value):
    return (value or 'NEUTRE').upper()


def _rpz_condition(m, tf, exp_ctx):
    max_age = {'1d': 3 * 24 * 3600, '2d': 5 * 24 * 3600, '6h': 18 * 3600, '2h': 6 * 3600, '30m': 90 * 60}.get(tf, 0)
    value, fresh = _state_signal(m, f'rpz_{tf}', max_age)
    return value, fresh, bool(fresh and value == exp_ctx)


def _zalt_condition(m, tf, exp_ctx):
    max_age = {
        '2d': 5 * 24 * 3600,
        '1d': 3 * 24 * 3600,
        '6h': 18 * 3600,
        '4h': 12 * 3600,
        '2h': 6 * 3600,
        '30m': 90 * 60,
        '15m': 45 * 60,
        '10m': 45 * 60,
        '1m': 5 * 60,
    }.get(tf, 0)
    value, fresh = _state_signal(m, f'zalt_{tf}', max_age)
    return value, fresh, bool(fresh and value == exp_ctx)


def _st_context_condition(m, tf, exp_ctx):
    max_age = {'1m': 5 * 60, '3m': 10 * 60, '10m': 30 * 60, '15m': 45 * 60, '30m': 90 * 60, '2h': 6 * 3600, '4h': 12 * 3600, '12h': 24 * 3600}.get(tf, 0)
    value, fresh = _state_signal(m, f'st_context_{tf}', max_age)
    return value, fresh, bool(fresh and value == exp_ctx)


def _st_context_veto(m, tf, exp_ctx):
    """Veto seulement si le contexte est OPPOSE et frais. Neutre/None/perime = on passe (pas de veto)."""
    max_age = {'15m': 45 * 60, '30m': 90 * 60, '2h': 6 * 3600, '4h': 12 * 3600, '12h': 24 * 3600}.get(tf, 0)
    value, fresh = _state_signal(m, f'st_context_{tf}', max_age)
    opp = 'sell' if exp_ctx == 'buy' else 'buy'
    veto = bool(fresh and value == opp)
    return value, fresh, veto



def evaluate_daily_rpz(symbol, trigger_dir=None, price=0.0, exchange_name=None, event_id=None, source='state_refresh'):
    """DAILY porte A/B: trigger flip ZALT 4H (OKX) commun.
    A: RPZ 1D + veto Context 12H oppose.
    B: Context 12H + Context 4H alignes (pas de RPZ 1D)."""
    if not CONFIG.get('ENABLE_DAILY', True) or not is_trade_symbol(symbol):
        return False
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')

    directions = [trigger_dir] if trigger_dir in ('buy', 'sell') else ['buy', 'sell']
    opened = False
    for exp_ctx in directions:
        direction = 'LONG' if exp_ctx == 'buy' else 'SHORT'
        rpz1d, rpz1d_fresh, rpz1d_ok = _rpz_condition(m, '1d', exp_ctx)
        zalt4h, zalt4h_fresh, zalt4h_ok = _zalt_condition(m, '4h', exp_ctx)
        zalt4h_flip_fresh = is_signal_fresh(m.get('last_zalt_4h_signal_ts'), 12 * 3600)
        trigger_ok = zalt4h_ok and zalt4h_flip_fresh and (trigger_dir is None or trigger_dir == exp_ctx)

        ctx12h_veto_val, ctx12h_veto_fresh, ctx12h_veto = _st_context_veto(m, '12h', exp_ctx)
        ctx12h, ctx12h_fresh, ctx12h_ok = _st_context_condition(m, '12h', exp_ctx)
        ctx4h, ctx4h_fresh, ctx4h_ok = _st_context_condition(m, '4h', exp_ctx)

        entry_a_ok = rpz1d_ok and trigger_ok and not ctx12h_veto
        entry_b_ok = ctx12h_ok and ctx4h_ok and trigger_ok
        entry_ok = entry_a_ok or entry_b_ok

        logger.info(
            f"[DAILY CHECK] {symbol} source={source} dir={direction} "
            f"rpz1d={rpz1d}/{exp_ctx} fresh={rpz1d_fresh} ok={rpz1d_ok} "
            f"zalt4h={zalt4h}/{exp_ctx} fresh={zalt4h_fresh} flip_fresh={zalt4h_flip_fresh} trig={trigger_ok} "
            f"ctx12h={ctx12h_veto_val} veto={ctx12h_veto} align_ok={ctx12h_ok} "
            f"ctx4h={ctx4h} align_ok={ctx4h_ok} "
            f"A={entry_a_ok} B={entry_b_ok} entry={entry_ok}"
        )

        if entry_ok:
            signal_type = 'daily_a_rpz1d' if entry_a_ok else 'daily_b_ctx12_ctx4'
            event_key = event_id or f"daily_{symbol}_{int(time.time())}_{exp_ctx}"
            detail_lines = ["[OK] Entree DAILY"]
            if entry_a_ok:
                detail_lines += [
                    "[VOIE] A: RPZ 1D + veto Context 12H",
                    f"[OK] RPZ 1D: {_ctx_label(rpz1d)}",
                    f"[INFO] ST Context 12H (veto si oppose): {_ctx_label(ctx12h_veto_val)}",
                ]
            else:
                detail_lines += [
                    "[VOIE] B: Context 12H + Context 4H alignes",
                    f"[OK] ST Context 12H: {_ctx_label(ctx12h)}",
                    f"[OK] ST Context 4H: {_ctx_label(ctx4h)}",
                ]
            detail_lines.append(f"[OK] Flip ZALT 4H: {_ctx_label(zalt4h)}")
            opened = _open_strategy_entry(
                symbol,
                'DAILY',
                direction,
                signal_type,
                event_key,
                price,
                exchange_name,
                detail_lines,
                cooldown=14400,
            ) or opened
    return opened






def evaluate_pulse_v3(symbol, trigger_dir=None, price=0.0, exchange_name=None, event_id=None, source='state_refresh'):
    """PULSE porte A/B: trigger flip ZALT 15m (TV) commun.
    A: RPZ 6H + veto Context 2H oppose.
    B: Context 2H + Context 15m alignes (pas de RPZ 6H)."""
    if not CONFIG.get('ENABLE_PULSE_V4', True) or not is_pulse_symbol(symbol):
        return False
    init_symbol_states(symbol)
    m = MOMENTUM_STATE[symbol]
    exchange_name = exchange_name or get_symbol_config(symbol).get('exchange', 'okx')
    directions = [trigger_dir] if trigger_dir in ('buy', 'sell') else ['buy', 'sell']
    opened = False
    for exp_ctx in directions:
        direction = 'LONG' if exp_ctx == 'buy' else 'SHORT'
        rpz6, rpz6_fresh, rpz6_ok = _rpz_condition(m, '6h', exp_ctx)
        zalt15, zalt15_fresh, zalt15_ok = _zalt_condition(m, '15m', exp_ctx)
        zalt15_flip_fresh = is_signal_fresh(m.get('last_zalt_15m_signal_ts'), 45 * 60)
        trigger_ok = zalt15_ok and zalt15_flip_fresh and (trigger_dir is None or trigger_dir == exp_ctx)

        ctx2h_veto_val, ctx2h_veto_fresh, ctx2h_veto = _st_context_veto(m, '2h', exp_ctx)
        ctx2h, ctx2h_fresh, ctx2h_ok = _st_context_condition(m, '2h', exp_ctx)
        ctx15, ctx15_fresh, ctx15_ok = _st_context_condition(m, '15m', exp_ctx)

        entry_a_ok = rpz6_ok and trigger_ok and not ctx2h_veto
        entry_b_ok = ctx2h_ok and ctx15_ok and trigger_ok
        entry_ok = entry_a_ok or entry_b_ok

        logger.info(
            f"[PULSEV4 CHECK] {symbol} source={source} dir={direction} "
            f"rpz6={rpz6}/{exp_ctx} fresh={rpz6_fresh} ok={rpz6_ok} "
            f"zalt15={zalt15}/{exp_ctx} fresh={zalt15_fresh} flip_fresh={zalt15_flip_fresh} trig={trigger_ok} "
            f"ctx2h={ctx2h_veto_val} veto={ctx2h_veto} align_ok={ctx2h_ok} "
            f"ctx15={ctx15} align_ok={ctx15_ok} "
            f"A={entry_a_ok} B={entry_b_ok} entry={entry_ok}"
        )

        if entry_ok:
            signal_type = 'pulse_a_rpz6h' if entry_a_ok else 'pulse_b_ctx2h_ctx15'
            event_key = event_id or f"pulsev4_{symbol}_{int(time.time())}_{exp_ctx}"
            detail_lines = ["[OK] Entree PULSE"]
            if entry_a_ok:
                detail_lines += [
                    "[VOIE] A: RPZ 6H + veto Context 2H",
                    f"[OK] RPZ 6H: {_ctx_label(rpz6)}",
                    f"[INFO] ST Context 2H (veto si oppose): {_ctx_label(ctx2h_veto_val)}",
                ]
            else:
                detail_lines += [
                    "[VOIE] B: Context 2H + Context 15m alignes",
                    f"[OK] ST Context 2H: {_ctx_label(ctx2h)}",
                    f"[OK] ST Context 15m: {_ctx_label(ctx15)}",
                ]
            detail_lines.append(f"[OK] Flip ZALT 15m: {_ctx_label(zalt15)}")
            opened = _open_strategy_entry(
                symbol,
                'PULSEV4',
                direction,
                signal_type,
                event_key,
                price,
                exchange_name,
                detail_lines,
                cooldown=1800,
            ) or opened
    return opened


def update_indicators_for_symbol(symbol):
    """Calcule les ZALT HTF (30m/4H/6H/1D) via update_okx_zalt_htf, qui fait son propre fetch OHLCV."""
    # Assets sans données OKX directes — indicateurs via webhooks TV uniquement
    OKX_SKIP = {'TAO/USDT'}
    if symbol in OKX_SKIP:
        return
    try:
        update_okx_zalt_htf(symbol)
    except Exception as e:
        logger.error(f"[OKX] update_indicators {symbol}: {e}")







def indicators_scheduler():
    """Recalcule tous les indicateurs depuis OKX toutes les heures."""
    logger.info("[OKX] Scheduler indicateurs démarré (toutes les 15 minutes)")
    # Premier calcul au démarrage après 30s
    time.sleep(30)
    while True:
        logger.info(f"[OKX] Calcul indicateurs pour {len(CONFIG['SYMBOLS'])} assets trade...")
        for symbol in CONFIG['SYMBOLS']:
            update_indicators_for_symbol(symbol)
            time.sleep(0.5)  # rate limit OKX
        persist_runtime_state()
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
                logger.warning('⚠️ Le bouton Telegram (journal) ne fonctionnera PAS')
                # Envoyer un avertissement sur Telegram
                send_info('⚠️ <b>Bot démarré sans webhook Telegram.</b>\nLe bouton inline (journal) est désactivé.\nConfigurer PUBLIC_BASE_URL sur Railway.')
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
