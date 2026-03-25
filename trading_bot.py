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
        'AAVE/USDT': 'okx',
        'APT/USDT': 'okx',
        'ARB/USDT': 'okx',
        'ATOM/USDT': 'okx',
        'AVAX/USDT': 'okx',
        'AXS/USDT': 'okx',
        'BNB/USDT': 'okx',
        'BONK/USDT': 'okx',
        'BTC/USDT': 'okx',
        'CRV/USDT': 'okx',
        'CVX/USDT': 'okx',
        'DOGE/USDT': 'okx',
        'DOT/USDT': 'okx',
        'ENA/USDT': 'okx',
        'ETH/USDT': 'okx',
        'FET/USDT': 'okx',
        'FIL/USDT': 'okx',
        'FLOKI/USDT': 'okx',
        'GALA/USDT': 'okx',
        'HBAR/USDT': 'okx',
        'HYPE/USDT': 'okx',
        'IMX/USDT': 'okx',
        'INJ/USDT': 'okx',
        'JTO/USDT': 'okx',
        'JUP/USDT': 'okx',
        'LINK/USDT': 'okx',
        'LTC/USDT': 'okx',
        'MANA/USDT': 'okx',
        'MOVE/USDT': 'okx',
        'NEAR/USDT': 'okx',
        'ONDO/USDT': 'okx',
        'OP/USDT': 'okx',
        'PENDLE/USDT': 'okx',
        'PENGU/USDT': 'okx',
        'PEPE/USDT': 'okx',
        'PYTH/USDT': 'okx',
        'RAY/USDT': 'okx',
        'RENDER/USDT': 'okx',
        'SAND/USDT': 'okx',
        'SEI/USDT': 'okx',
        'SOL/USDT': 'okx',
        'STX/USDT': 'okx',
        'SUI/USDT': 'okx',
        'TIA/USDT': 'okx',
        'TON/USDT': 'okx',
        'VIRTUAL/USDT': 'okx',
        'WIF/USDT': 'okx',
        'WLD/USDT': 'okx',
        'XRP/USDT': 'okx',
        'ZK/USDT': 'okx',
        'ZRO/USDT': 'okx',
        'UNI/USDT': 'okx',
        'SHIB/USDT': 'okx',
        'GRT/USDT': 'okx',
        'ENJ/USDT': 'okx',
        'APE/USDT': 'okx',
        'CORE/USDT': 'okx',
        'TURBO/USDT': 'okx',
        'MEW/USDT': 'okx',
        'NEIRO/USDT': 'okx',
        'STRK/USDT': 'okx',
        'BERA/USDT': 'okx',
        'SONIC/USDT': 'okx',
    },
    
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,
    'HEARTBEAT_INTERVAL_SECONDS': int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", 21600)),
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
STATE_LOCK = threading.Lock()

def track_alert(symbol, strategy):
    if symbol not in WEEKLY_STATS:
        WEEKLY_STATS[symbol] = {
            'SAFE': 0, 'MOMENTUM': 0, 'CONTEXT': 0,
            'CONTEXT_A': 0, 'CONTEXT_B': 0, 'CONTEXT_B+': 0,
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
    okx_count = sum(1 for ex in CONFIG['SYMBOLS'].values() if ex == 'okx')
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
            requests.post(url, json=payload, timeout=10)
            logger.info("✅ Message Telegram envoyé (après retry)")
        else:
            logger.error(f"❌ Telegram erreur HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"❌ Erreur Telegram: {e}")


def send_start_notification():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    redis_status = "✅ Redis connecté" if REDIS_CLIENT else "⚠️ Redis non disponible"
    msg = (
        "🤖 <b>[BOT STARTED]</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Total Assets: {len(CONFIG['SYMBOLS'])}\n"
        f"💾 {redis_status}\n\n"
        "📋 <b>STRATEGIES:</b>\n\n"
        "1️⃣ <b>MOMENTUM</b>\n"
        "   • Bias 2D + ST Context 1H\n"
        "   • Signal: Flip ST AI 1H ou 4H\n\n"
        "2️⃣ <b>CONTEXT</b>\n"
        "   • Zone: ST Context 4H\n"
        "   • Signal: Flip ST AI 1H\n\n"
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

    total_safe      = sum(s.get('SAFE', 0)       for s in WEEKLY_STATS.values())
    total_momentum  = sum(s.get('MOMENTUM', 0)   for s in WEEKLY_STATS.values())
    total_context   = sum(s.get('CONTEXT', 0)    for s in WEEKLY_STATS.values())
    total_ctx_a     = sum(s.get('CONTEXT_A', 0)  for s in WEEKLY_STATS.values())
    total_ctx_b     = sum(s.get('CONTEXT_B', 0)  for s in WEEKLY_STATS.values())
    total_ctx_bplus = sum(s.get('CONTEXT_B+', 0) for s in WEEKLY_STATS.values())

    msg += (
        "📋 <b>Par stratégie:</b>\n"
        f"  • SAFE: {total_safe}\n"
        f"  • MOMENTUM: {total_momentum}\n"
        f"  • CONTEXT: {total_context}\n"
        f"  • CONTEXT A: {total_ctx_a}\n"
        f"  • CONTEXT B: {total_ctx_b}\n"
        f"  • CONTEXT B+: {total_ctx_bplus}\n\n"
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
    exchange_name = CONFIG['SYMBOLS'].get(symbol)
    if not exchange_name:
        return None
    return exchanges.get(exchange_name)

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

def should_send(symbol, key, event_id=None):
    now = time.time()
    k = f"{symbol}:{key}"
    if event_id:
        previous_event = LAST_SIGNAL_EVENTS.get(k)
        if previous_event == event_id:
            return False
        LAST_SIGNAL_EVENTS[k] = event_id
    if k not in LAST_SIGNALS or (now - LAST_SIGNALS[k] > CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']):
        LAST_SIGNALS[k] = now
        return True
    return False

def init_symbol_states(symbol):
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'bias_2d': None, 'st_context_1h': None,
            'st_context_4h': None, 'st_1h': None, 'st_4h': None,
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
    audit_log(data, status="reçu")
    event_id = build_event_id(data, symbol, strat, tf, alert_type, val)

    if symbol not in CONFIG['SYMBOLS']:
        logger.info(f"⏭️ {symbol} non dans la watchlist")
        audit_log(data, status="ignoré_watchlist")
        return jsonify({'status': 'ignored', 'reason': 'not_in_watchlist'}), 200

    exchange_name = CONFIG['SYMBOLS'][symbol]
    init_symbol_states(symbol)

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
    # LOGIQUE MOMENTUM : Bias 2D + ST Context 1H → ST AI 1H ou ST AI 4H
    # ========================================================================
    if strat in ['momentum', 'all']:
        m = MOMENTUM_STATE[symbol]

        # Mise a jour des etats
        if alert_type == 'st_context' and tf == '1h': m['st_context_1h'] = parse_st_context_value(val)
        if alert_type == 'supertrend' and tf == '1h': m['st_1h'] = val
        if alert_type == 'supertrend' and tf == '4h': m['st_4h'] = val

        bias_2d_val = m.get('bias_2d')
        direction = None
        if bias_2d_val == 'bull':   direction = "LONG"
        elif bias_2d_val == 'bear': direction = "SHORT"

        if direction:
            st_expected  = 'buy'  if direction == "LONG" else 'sell'
            ctx_expected = 'buy'  if direction == "LONG" else 'sell'
            ctx_ok = m.get('st_context_1h') == ctx_expected

            # PREP : Bias 2D + ST Context 1H (sans signal ST)
            if ctx_ok and alert_type == 'st_context' and tf == '1h' and should_send(symbol, "momentum_prep", event_id=event_id):
                with STATE_LOCK:
                    PREP_BUFFER.append({'strat': 'MOMENTUM', 'dir': direction, 'sym': symbol, 'price': price})
                logger.info(f"[PREP] MOMENTUM {direction} {symbol} ajouté au buffer")

            # SIGNAL : ST AI 1H flip
            if ctx_ok and alert_type == 'supertrend' and tf == '1h' and val == st_expected and should_send(symbol, "momentum_entry_1h", event_id=event_id):
                emoji = "🟢" if direction == "LONG" else "🔴"
                send_telegram(
                    f"{emoji} <b>[MOMENTUM - ENTREE 1H]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 2D: {bias_2d_val.upper()}\n"
                    f"✅ ST Context 1H: {m['st_context_1h'].upper()}\n"
                    f"✅ SuperTrend AI 1H: {val.upper()} (SIGNAL)"
                )
                track_alert(symbol, 'MOMENTUM')

            # SIGNAL : ST AI 4H flip
            if ctx_ok and alert_type == 'supertrend' and tf == '4h' and val == st_expected and should_send(symbol, "momentum_entry_4h", event_id=event_id):
                emoji = "🟢" if direction == "LONG" else "🔴"
                send_telegram(
                    f"{emoji} <b>[MOMENTUM - ENTREE 4H]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 2D: {bias_2d_val.upper()}\n"
                    f"✅ ST Context 1H: {m['st_context_1h'].upper()}\n"
                    f"✅ SuperTrend AI 4H: {val.upper()} (SIGNAL)"
                )
                track_alert(symbol, 'MOMENTUM')

    # ========================================================================
    # LOGIQUE CONTEXT : ST Context 4H → ST AI 1H
    # ========================================================================
    if strat in ['context', 'momentum_context', 'all']:
        m = MOMENTUM_STATE[symbol]
        if alert_type == 'st_context' and tf == '4h':
            old_val = m.get('st_context_4h')
            m['st_context_4h'] = parse_st_context_value(val)
            logger.info(f"[CONTEXT] {symbol} ST Context 4H: {old_val} → {m['st_context_4h']}")

            # PREP : envoi alerte zone dès réception ST Context 4H
            if m['st_context_4h'] is not None and should_send(symbol, f"context_prep_{m['st_context_4h']}", event_id=event_id):
                direction = "LONG" if m['st_context_4h'] == 'buy' else "SHORT"
                with STATE_LOCK:
                    PREP_BUFFER.append({'strat': 'CONTEXT', 'dir': direction, 'sym': symbol, 'price': price})
                logger.info(f"[PREP] CONTEXT {direction} {symbol} zone 4H active")

        # SIGNAL : ST AI 1H flip dans le sens du context 4H
        if alert_type == 'supertrend' and tf == '1h':
            ctx_4h = m.get('st_context_4h')
            if ctx_4h == val and should_send(symbol, f"context_entry_1h_{val}", event_id=event_id):
                direction = "LONG" if val == 'buy' else "SHORT"
                emoji = "🟢" if direction == "LONG" else "🔴"
                send_telegram(
                    f"{emoji} <b>[CONTEXT - ENTREE 1H]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ ST Context 4H: {ctx_4h.upper()} (zone active)\n"
                    f"✅ SuperTrend AI 1H: {val.upper()} (SIGNAL)"
                )
                track_alert(symbol, 'CONTEXT')
                logger.info(f"[CONTEXT] Alerte envoyée: {symbol} {direction}")

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

@app.route('/reset_state', methods=['POST'])
def reset_state_all():
    """Remet tout le state à zéro."""
    MOMENTUM_STATE.clear()
    LAST_SIGNALS.clear()
    LAST_SIGNAL_EVENTS.clear()
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




def supertrend_ai(df, atr_len=6, min_mult=1.0, max_mult=2.0, step=1.0,
                  perf_alpha=100, from_cluster='Best', max_iter=100):
    """SuperTrend AI — reproduction exacte du Pine Script (ATR 6, factors 1-2, Best cluster)."""
    high  = df['high'].values
    low   = df['low'].values
    close = df['close'].values
    n     = len(close)
    tr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
    atr = pd.Series(tr).ewm(alpha=1/atr_len, adjust=False).mean().values
    hl2 = (high + low) / 2.0
    factors, f = [], min_mult
    while f <= max_mult + 1e-9:
        factors.append(round(f, 10))
        f += step
    nf = len(factors)
    upper_arr  = np.full((n, nf), hl2[0])
    lower_arr  = np.full((n, nf), hl2[0])
    trend_arr  = np.zeros((n, nf), dtype=int)
    output_arr = np.full((n, nf), hl2[0])
    perf_arr   = np.zeros((n, nf))
    alpha_perf = 2.0 / (perf_alpha + 1)
    for i in range(1, n):
        for k, factor in enumerate(factors):
            up = hl2[i] + atr[i] * factor
            dn = hl2[i] - atr[i] * factor
            if close[i] > upper_arr[i-1, k]:   trend_arr[i, k] = 1
            elif close[i] < lower_arr[i-1, k]: trend_arr[i, k] = 0
            else:                               trend_arr[i, k] = trend_arr[i-1, k]
            upper_arr[i, k] = min(up, upper_arr[i-1, k]) if close[i-1] < upper_arr[i-1, k] else up
            lower_arr[i, k] = max(dn, lower_arr[i-1, k]) if close[i-1] > lower_arr[i-1, k] else dn
            output_arr[i, k] = lower_arr[i, k] if trend_arr[i, k] == 1 else upper_arr[i, k]
            diff = np.sign(close[i-1] - output_arr[i-1, k]) if output_arr[i-1, k] != 0 else 0
            perf_arr[i, k] = perf_arr[i-1, k] + alpha_perf * ((close[i] - close[i-1]) * diff - perf_arr[i-1, k])
    perf_final   = perf_arr[-1]
    factor_final = np.array(factors)
    centroids    = np.percentile(perf_final, [25, 50, 75])
    clusters_p = [[], [], []]
    clusters_f = [[], [], []]
    for _ in range(max_iter):
        clusters_p = [[], [], []]
        clusters_f = [[], [], []]
        for j, val in enumerate(perf_final):
            idx = int(np.argmin([abs(val - c) for c in centroids]))
            clusters_p[idx].append(val)
            clusters_f[idx].append(factor_final[j])
        new_c = [np.mean(cp) if cp else 0.0 for cp in clusters_p]
        if np.max(np.abs(np.array(new_c) - centroids)) < 0.0001:
            centroids = np.array(new_c)
            break
        centroids = np.array(new_c)
    from_idx = {'Best': 2, 'Average': 1, 'Worst': 0}.get(from_cluster, 2)
    sorted_idx = np.argsort(centroids)
    target_idx = sorted_idx[from_idx]
    target_factor = np.mean(clusters_f[target_idx]) if clusters_f[target_idx] else factors[0]
    upper_f = lower_f = hl2[0]
    os_f = 0
    direction = pd.Series('', index=df.index, dtype=str)
    for i in range(1, n):
        up = hl2[i] + atr[i] * target_factor
        dn = hl2[i] - atr[i] * target_factor
        upper_f = min(up, upper_f) if close[i-1] < upper_f else up
        lower_f = max(dn, lower_f) if close[i-1] > lower_f else dn
        if close[i] > upper_f:   os_f = 1
        elif close[i] < lower_f: os_f = 0
        direction.iloc[i] = 'buy' if os_f == 1 else 'sell'
    return direction

# ============================================================================ #
# CALCUL AUTOMATIQUE DES INDICATEURS DEPUIS OKX
# ============================================================================ #

def fetch_ohlcv_okx(symbol, timeframe, limit=250):
    """Fetch OHLCV depuis l API publique OKX (sans cle API)."""
    try:
        # Convertir BTC/USDT -> BTC-USDT
        inst_id = symbol.replace('/', '-')
        # Map timeframe
        tf_map = {'1h': '1H', '4h': '4H', '1d': '1D', '2h': '2H', '3h': '3H'}
        bar = tf_map.get(timeframe, timeframe.upper())
        url = f'https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}'
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data.get('code') != '0' or not data.get('data'):
            logger.error(f"[OKX] API error {symbol} {timeframe}: {data.get('msg', 'no data')}")
            return None
        # OKX retourne [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
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
    val = histogram.iloc[-2]
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

def update_indicators_for_symbol(symbol):
    """Met a jour tous les indicateurs calculables pour un asset."""
    try:
        # Fetch bougies
        df_1h  = fetch_ohlcv_okx(symbol, '1h',  limit=250)
        df_4h  = fetch_ohlcv_okx(symbol, '4h',  limit=200)
        df_1d  = fetch_ohlcv_okx(symbol, '1d',  limit=100)
        df_3d  = fetch_ohlcv_okx(symbol, '1d',  limit=200)  # aggregate pour 3D

        df_1h_st = fetch_ohlcv_okx(symbol, '1h',  limit=500)  # plus de bougies pour ST AI
        if df_1h is None or df_4h is None or df_1d is None or df_1h_st is None:
            return

        # Calculs
        bias_1h  = calc_bias_okx(df_1h)
        bias_4h  = calc_bias_okx(df_4h)
        bias_1d  = calc_bias_okx(df_1d)
        macd_4h  = calc_macd_okx(df_4h)
        macd_1d  = calc_macd_okx(df_1d)
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

        # SuperTrend AI 1H
        try:
            st_1h_series = supertrend_ai(df_1h_st)
            st_1h_val    = st_1h_series.iloc[-2]  # derniere bougie fermee
        except Exception as e:
            logger.error(f'[OKX] ST AI {symbol}: {e}')
            st_1h_val = None

        price = float(df_1h['close'].iloc[-1])

        old_st_1h = None
        with STATE_LOCK:
            if symbol in MOMENTUM_STATE:
                old_st_1h = MOMENTUM_STATE[symbol].get('st_1h')
                if bias_2d:   MOMENTUM_STATE[symbol]['bias_2d'] = bias_2d
                if st_1h_val: MOMENTUM_STATE[symbol]['st_1h']   = st_1h_val

        # Détection flip ST AI 1H
        if st_1h_val and old_st_1h and st_1h_val != old_st_1h:
            m = MOMENTUM_STATE.get(symbol, {})
            ctx_4h    = m.get('st_context_4h')
            ctx_1h    = m.get('st_context_1h')
            bias_2d_v = m.get('bias_2d')
            st_expected = st_1h_val  # 'buy' ou 'sell'
            direction   = "LONG" if st_1h_val == 'buy' else "SHORT"
            emoji       = "🟢" if direction == "LONG" else "🔴"

            # CONTEXT : ST Context 4H + flip ST AI 1H
            if ctx_4h == st_expected and should_send(symbol, f"context_entry_1h_{st_expected}"):
                send_telegram(
                    f"{emoji} <b>[CONTEXT - ENTREE 1H]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ ST Context 4H: {ctx_4h.upper()} (zone active)\n"
                    f"✅ SuperTrend AI 1H: {st_1h_val.upper()} (SIGNAL)"
                )
                track_alert(symbol, 'CONTEXT')
                logger.info(f"[CONTEXT] Flip ST AI 1H: {symbol} {direction}")

            # MOMENTUM : Bias 2D + ST Context 1H + flip ST AI 1H
            if bias_2d_v and ctx_1h == st_expected and (
                (direction == "LONG"  and bias_2d_v == 'bull') or
                (direction == "SHORT" and bias_2d_v == 'bear')
            ) and should_send(symbol, f"momentum_entry_1h_{st_expected}"):
                send_telegram(
                    f"{emoji} <b>[MOMENTUM - ENTREE 1H]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 2D: {bias_2d_v.upper()}\n"
                    f"✅ ST Context 1H: {ctx_1h.upper()}\n"
                    f"✅ SuperTrend AI 1H: {st_1h_val.upper()} (SIGNAL)"
                )
                track_alert(symbol, 'MOMENTUM')
                logger.info(f"[MOMENTUM] Flip ST AI 1H: {symbol} {direction}")



        logger.info(f"[OKX] {symbol} mis a jour — B1H={bias_1h} B4H={bias_4h} B1D={bias_1d} B2D={bias_2d} B3D={bias_3d} M4H={macd_4h} M1D={macd_1d} M2D={macd_2d} EMA200={ema200_1h:.4f}")

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

        logger.info("⏰ Schedulers démarrés (rapport hebdo + heartbeat + prep report + indicateurs OKX)")
    except Exception as e:
        logger.error(f"❌ Erreur au démarrage: {e}")

startup_thread = threading.Thread(target=startup, daemon=True)
startup_thread.start()

if __name__ == '__main__':
    logger.info(f"✅ Bot démarré sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)
