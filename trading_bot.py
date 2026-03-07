#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ccxt
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
    'TELEGRAM_BOT_TOKEN': '8110041550:AAHJKAWxIG1ZBjZ8fRfFMKq-4iTeo5v4-Hw',
    'TELEGRAM_CHAT_ID': '6473214015',
    
    'SYMBOLS': {
        # Tier 1 - Majors
        'BTC/USDT': 'okx',
        'ETH/USDT': 'okx',
        'SOL/USDT': 'okx',
        'XRP/USDT': 'okx',
        'LINK/USDT': 'okx',
        'TIA/USDT': 'okx',

        # Tier 2 - IA & Tech
        'TAO/USDT': 'okx',
        'FET/USDT': 'okx',
        'RENDER/USDT': 'okx',
        'ZK/USDT': 'okx',

        # Tier 3 - DeFi & RWA
        'ONDO/USDT': 'okx',
        'PENDLE/USDT': 'okx',
        'CRV/USDT': 'okx',
        'CVX/USDT': 'okx',

        # Tier 4 - Memes
        'PEPE/USDT': 'okx',
        'WIF/USDT': 'okx',
        'DOGE/USDT': 'okx',
        'BONK/USDT': 'okx',

        # Tier 5 - Wildcard
        'VIRTUAL/USDT': 'okx',
        'HYPE/USDT': 'okx',

        # Tier 6 - Expansions
        'AAVE/USDT': 'okx',
        'NEAR/USDT': 'okx',
        'PYTH/USDT': 'okx',
        'STX/USDT': 'okx',
        'ZRO/USDT': 'okx',

        # Tier 7 - Nouveaux
        'SUI/USDT': 'okx',
        'ENA/USDT': 'okx',
        'ARB/USDT': 'okx',
        'AVAX/USDT': 'okx',
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
SAFE_STATE = {}
MOMENTUM_STATE = {}
CONTEXT_STATE = {}

# ============================================================================ #
# STATISTIQUES HEBDOMADAIRES
# ============================================================================ #

WEEKLY_STATS = {}
WEEKLY_START = datetime.now(timezone.utc)
STATE_LOCK = threading.Lock()

def track_alert(symbol, strategy):
    if symbol not in WEEKLY_STATS:
        WEEKLY_STATS[symbol] = {
            'SAFE': 0, 'MOMENTUM': 0,
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
            'safe_state':         SAFE_STATE,
            'momentum_state':     MOMENTUM_STATE,
            'context_state':      CONTEXT_STATE,
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
    global SAFE_STATE, MOMENTUM_STATE, CONTEXT_STATE, WEEKLY_STATS, WEEKLY_START, LAST_SIGNALS, LAST_SIGNAL_EVENTS
    if not REDIS_CLIENT:
        logger.info("ℹ️ Redis non disponible — démarrage à froid")
        return
    try:
        raw = REDIS_CLIENT.get('bot_state')
        if not raw:
            logger.info("ℹ️ Aucun état persistant trouvé dans Redis — démarrage à froid")
            return

        payload = json.loads(raw)
        SAFE_STATE          = payload.get('safe_state', {})
        MOMENTUM_STATE      = payload.get('momentum_state', {})
        CONTEXT_STATE       = payload.get('context_state', {})
        WEEKLY_STATS        = payload.get('weekly_stats', {})
        LAST_SIGNALS        = payload.get('last_signals', {})
        LAST_SIGNAL_EVENTS  = payload.get('last_signal_events', {})

        weekly_start_raw = payload.get('weekly_start')
        if weekly_start_raw:
            WEEKLY_START = datetime.fromisoformat(weekly_start_raw)

        logger.info(
            f"✅ État restauré depuis Redis | "
            f"safe={len(SAFE_STATE)} momentum={len(MOMENTUM_STATE)} context={len(CONTEXT_STATE)}"
        )
    except Exception as e:
        logger.error(f"❌ Redis load error: {e}")

# ============================================================================ #
# INITIALISATION EXCHANGES
# ============================================================================ #

def init_exchanges():
    global exchanges
    try:
        exchanges['okx'] = ccxt.okx({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        for name, exchange in exchanges.items():
            try:
                exchange.load_markets()
                logger.info(f"✅ {name.upper()} - Markets chargés")
            except Exception as e:
                logger.error(f"❌ {name.upper()} - Erreur: {e}")
    except Exception as e:
        logger.error(f"❌ Erreur initialisation exchanges: {e}")

# ============================================================================ #
# FONCTIONS TELEGRAM
# ============================================================================ #

def send_telegram(msg):
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
        "1️⃣ <b>SAFE</b>\n"
        "   • Bias 3D + MACD 4H + Bias 1H + ST 1H\n\n"
        "2️⃣ <b>MOMENTUM</b>\n"
        "   • Bias 1D + EMA 200 1H + ST 1H\n\n"
        "3️⃣ <b>CONTEXT</b>\n"
        "   • Alerte A: ST Context 4H + Flip ST AI 1H\n"
        "   • Alerte B: MACD 2D + EMA 200 1H + ST Context 1H + Flip ST AI 1H\n\n"
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
    total_ctx_a     = sum(s.get('CONTEXT_A', 0)  for s in WEEKLY_STATS.values())
    total_ctx_b     = sum(s.get('CONTEXT_B', 0)  for s in WEEKLY_STATS.values())
    total_ctx_bplus = sum(s.get('CONTEXT_B+', 0) for s in WEEKLY_STATS.values())

    msg += (
        "📋 <b>Par stratégie:</b>\n"
        f"  • SAFE: {total_safe}\n"
        f"  • MOMENTUM: {total_momentum}\n"
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
            f"🧠 State safe/momentum/context: {len(SAFE_STATE)}/{len(MOMENTUM_STATE)}/{len(CONTEXT_STATE)}\n"
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
    if symbol not in SAFE_STATE:
        SAFE_STATE[symbol] = {
            'bias_3d': None, 'macd_4h': None, 'bias_1h': None,
            'st_1h': None, 'bias_4h': None, 'macd_1d': None,
            'macd_2d': None,
        }
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'bias_1d': None, 'macd_4h': None, 'ema200_1h': None,
            'st_1h': None, 'macd_1d': None, 'macd_2d': None,
            'st_context_4h': None, 'st_context_1h': None,
        }
    if symbol not in CONTEXT_STATE:
        CONTEXT_STATE[symbol] = {
            'st_context_4h': None,
            'ema200_1h': None,
            'st_context_1h': None,
            'macd_2d': None,
            'bias_1d': None,
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
    price       = float(data.get('price', 0))

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
    # MACD 2D — sortie partielle sur toutes les stratégies
    # ========================================================================
    if alert_type == 'macd' and tf == '2d':
        if symbol in SAFE_STATE:
            old = SAFE_STATE[symbol].get('macd_2d')
            SAFE_STATE[symbol]['macd_2d'] = val
            if old and old != val:
                emoji = "🟢" if val == 'bull' else "🔴"
                send_telegram(
                    f"{emoji} <b>[SAFE - TP PARTIEL]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Trigger: MACD 2D Inversion\n"
                    f"📈 New Direction: {'BULLISH' if val == 'bull' else 'BEARISH'}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"💡 Action: Take partial profits (30-50%)"
                )

        if symbol in MOMENTUM_STATE:
            old = MOMENTUM_STATE[symbol].get('macd_2d')
            MOMENTUM_STATE[symbol]['macd_2d'] = val
            if old and old != val:
                emoji = "🟢" if val == 'bull' else "🔴"
                send_telegram(
                    f"{emoji} <b>[MOMENTUM - TP PARTIEL]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Trigger: MACD 2D Inversion\n"
                    f"📈 New Direction: {'BULLISH' if val == 'bull' else 'BEARISH'}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"💡 Action: Take partial profits (30-50%)"
                )

        if symbol in CONTEXT_STATE:
            old = CONTEXT_STATE[symbol].get('macd_2d')
            CONTEXT_STATE[symbol]['macd_2d'] = val
            if old and old != val:
                emoji = "🟢" if val == 'bull' else "🔴"
                send_telegram(
                    f"{emoji} <b>[CONTEXT - TP PARTIEL]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Trigger: MACD 2D Inversion\n"
                    f"📈 New Direction: {'BULLISH' if val == 'bull' else 'BEARISH'}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"💡 Action: Take partial profits (30-50%)"
                )

        persist_runtime_state()
        return jsonify({'status': 'success', 'symbol': symbol}), 200

    # ========================================================================
    # LOGIQUE SAFE
    # ========================================================================
    if strat in ['safe', 'all']:
        s = SAFE_STATE[symbol]

        if alert_type == 'bias' and tf == '3d':       s['bias_3d'] = val
        if alert_type == 'macd' and tf == '4h':       s['macd_4h'] = val
        if alert_type == 'bias' and tf == '1h':       s['bias_1h'] = val
        if alert_type == 'supertrend' and tf == '1h': s['st_1h'] = val
        if alert_type == 'bias_9_26' and tf == '4h':  s['bias_4h'] = val
        if alert_type == 'macd' and tf == '1d':       s['macd_1d'] = val

        if alert_type == 'macd' and tf == '1d':
            emoji = "🟢" if val == 'bull' else "🔴"
            send_telegram(
                f"{emoji} <b>[SAFE - TP PARTIEL]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Trigger: MACD 1D Inversion\n"
                f"📈 New Direction: {'BULLISH' if val == 'bull' else 'BEARISH'}\n"
                f"💰 Price: ${price:.4f}\n"
                f"🏦 Exchange: {exchange_name.upper()}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"💡 Action: Take partial profits (30-50%)"
            )

        if alert_type == 'macd' and tf == '3d':
            emoji = "🟢" if val == 'bull' else "🔴"
            send_telegram(
                f"🚪 <b>[SAFE - EXIT COMPLET]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Trigger: MACD 3D Change\n"
                f"📈 New Direction: {'BULLISH' if val == 'bull' else 'BEARISH'}\n"
                f"💰 Price: ${price:.4f}\n"
                f"🏦 Exchange: {exchange_name.upper()}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"❌ Action: EXIT ALL POSITIONS NOW"
            )

        direction = None
        if s['bias_3d'] == 'bull' and s['macd_4h'] == 'bull':   direction = "LONG"
        elif s['bias_3d'] == 'bear' and s['macd_4h'] == 'bear': direction = "SHORT"

        if direction:
            stars = 2
            expected    = 'bull' if direction == "LONG" else 'bear'
            st_expected = 'buy'  if direction == "LONG" else 'sell'

            if s['bias_1h'] == expected:    stars = 3
            if s['st_1h'] == st_expected:   stars = 4
            if s['bias_4h'] == expected:    stars = 5

            if stars >= 3 and alert_type == 'bias' and tf == '1h' and val == expected and should_send(symbol, f"safe_prep_{stars}*", event_id=event_id):
                emoji = "🟡" if direction == "LONG" else "🟠"
                send_telegram(
                    f"{emoji} <b>[SAFE {stars}⭐ - PREPARATION]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 3D: {s['bias_3d']}\n"
                    f"✅ MACD 4H: {s['macd_4h']}\n"
                    f"✅ Bias 1H: {s['bias_1h']}\n"
                    f"⏳ SuperTrend 1H: En attente...\n"
                    f"{'✅' if stars >= 5 else '❌'} Bias 4H: {s['bias_4h']}\n\n"
                    f"💡 Préparez-vous si SuperTrend 1H confirme"
                )
                track_alert(symbol, 'SAFE')

            if stars >= 2 and alert_type == 'supertrend' and tf == '1h' and should_send(symbol, f"safe_{stars}*", event_id=event_id):
                emoji  = "🟢" if direction == "LONG" else "🔴"
                action = "ENTREE MAINTENANT" if stars >= 4 else "POSITION POSSIBLE"
                send_telegram(
                    f"{emoji} <b>[SAFE {stars}⭐ - {action}]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 3D: {s['bias_3d']}\n"
                    f"✅ MACD 4H: {s['macd_4h']}\n"
                    f"{'✅' if stars >= 3 else '❌'} Bias 1H: {s['bias_1h']}\n"
                    f"✅ SuperTrend 1H: {s['st_1h']} (CONFIRME)\n"
                    f"{'✅' if stars == 5 else '❌'} Bias 4H: {s['bias_4h']}"
                )
                track_alert(symbol, 'SAFE')

    # ========================================================================
    # LOGIQUE MOMENTUM
    # ========================================================================
    if strat in ['momentum', 'momentum_context', 'all']:
        m = MOMENTUM_STATE[symbol]
        old_bias_1d = m.get('bias_1d')

        if alert_type == 'bias' and tf == '1d':       m['bias_1d'] = val
        if alert_type == 'macd' and tf == '4h':       m['macd_4h'] = val
        if alert_type == 'ema200' and tf == '1h':
            if ema200_value is not None:              m['ema200_1h'] = ema200_value
        if alert_type == 'supertrend' and tf == '1h': m['st_1h'] = val
        if alert_type == 'macd' and tf == '1d':       m['macd_1d'] = val
        if alert_type == 'st_context' and tf == '4h': m['st_context_4h'] = parse_st_context_value(val)
        if alert_type == 'st_context' and tf == '1h': m['st_context_1h'] = parse_st_context_value(val)

        if alert_type == 'macd' and tf == '1d':
            emoji = "🟢" if val == 'bull' else "🔴"
            send_telegram(
                f"{emoji} <b>[MOMENTUM - TP PARTIEL]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Trigger: MACD 1D Inversion\n"
                f"📈 New Direction: {'BULLISH' if val == 'bull' else 'BEARISH'}\n"
                f"💰 Price: ${price:.4f}\n"
                f"🏦 Exchange: {exchange_name.upper()}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"💡 Action: Take partial profits (40-60%)"
            )

        if alert_type == 'bias' and tf == '1d' and old_bias_1d and old_bias_1d != val:
            send_telegram(
                f"🚪 <b>[MOMENTUM - EXIT COMPLET]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Trigger: Bias 1D Inversion\n"
                f"📈 New Bias: {'BULLISH' if val == 'bull' else 'BEARISH'}\n"
                f"💰 Price: ${price:.4f}\n"
                f"🏦 Exchange: {exchange_name.upper()}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"❌ Action: EXIT ALL POSITIONS NOW"
            )

        direction = None
        if m['bias_1d'] == 'bull':   direction = "LONG"
        elif m['bias_1d'] == 'bear': direction = "SHORT"

        if direction:
            ema_ok = False
            ema_status = "N/A"
            if m['ema200_1h']:
                if direction == "LONG" and price < m['ema200_1h']:
                    ema_ok = True
                    ema_status = f"✅ ${price:.2f} < EMA200 ${m['ema200_1h']:.2f}"
                elif direction == "SHORT" and price > m['ema200_1h']:
                    ema_ok = True
                    ema_status = f"✅ ${price:.2f} > EMA200 ${m['ema200_1h']:.2f}"
                else:
                    ema_status = f"❌ Prix: ${price:.2f} | EMA200: ${m['ema200_1h']:.2f}"

            if ema_ok and alert_type == 'ema200' and tf == '1h' and should_send(symbol, "momentum_prep", event_id=event_id):
                emoji = "🟡" if direction == "LONG" else "🟠"
                send_telegram(
                    f"{emoji} <b>[MOMENTUM - PREPARATION]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 1D: {m['bias_1d']}\n"
                    f"✅ EMA200 1H: {ema_status}\n"
                    f"⏳ SuperTrend 1H: En attente...\n\n"
                    f"💡 Préparez-vous si SuperTrend 1H confirme"
                )

            if ema_ok and alert_type == 'supertrend' and tf == '1h':
                st_expected = 'buy' if direction == "LONG" else 'sell'
                if val == st_expected and should_send(symbol, "momentum_entry", event_id=event_id):
                    stars = 3
                    if m['st_context_1h'] == st_expected:
                        stars = 4
                        if m['st_context_4h'] == st_expected:
                            stars = 5

                    emoji = "🟢" if direction == "LONG" else "🔴"
                    msg = (
                        f"{emoji} <b>[MOMENTUM {stars}⭐ - ENTREE MAINTENANT]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction}\n"
                        f"💰 Price: ${price:.4f}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ Bias 1D: {m['bias_1d']}\n"
                        f"✅ EMA200 1H: {ema_status}\n"
                        f"✅ SuperTrend AI 1H: {val} (CONFIRME)\n"
                    )
                    if m['st_context_1h']:
                        icon = "✅" if m['st_context_1h'] == st_expected else "❌"
                        msg += f"{icon} ST Context 1H: {m['st_context_1h']}\n"
                    if m['st_context_4h']:
                        icon = "✅" if m['st_context_4h'] == st_expected else "❌"
                        msg += f"{icon} ST Context 4H: {m['st_context_4h']}\n"
                    msg += "\n🎯 <b>Position Size: "
                    if stars == 5:   msg += "70-80% (SETUP PARFAIT)"
                    elif stars == 4: msg += "60-70% (BONUS ST CONTEXT 1H)"
                    else:            msg += "50-60%"
                    msg += "</b>"
                    send_telegram(msg)
                    track_alert(symbol, 'MOMENTUM')

    # ========================================================================
    # LOGIQUE CONTEXT
    # ========================================================================
    if strat in ['context', 'momentum_context', 'all']:
        c = CONTEXT_STATE[symbol]

        if alert_type == 'st_context' and tf == '4h':
            old_val = c['st_context_4h']
            c['st_context_4h'] = parse_st_context_value(val)
            logger.info(f"[CONTEXT] {symbol} - ST Context 4H: {old_val} → {c['st_context_4h']} (CT={val})")

        if alert_type == 'bias' and tf == '1d':
            c['bias_1d'] = val
            logger.info(f"[CONTEXT] {symbol} - Bias 1D: {val}")

        if alert_type == 'ema200' and tf == '1h':
            if ema200_value is not None:
                c['ema200_1h'] = ema200_value
                logger.info(f"[CONTEXT] {symbol} - EMA200 1H: {ema200_value}")

        if alert_type == 'st_context' and tf == '1h':
            old_val = c['st_context_1h']
            c['st_context_1h'] = parse_st_context_value(val)
            logger.info(f"[CONTEXT] {symbol} - ST Context 1H: {old_val} → {c['st_context_1h']} (CT={val})")

        if alert_type == 'supertrend' and tf == '1h':
            direction = "LONG" if val == 'buy' else "SHORT"
            emoji = "🟢" if val == 'buy' else "🔴"
            macd_2d_expected = 'bull' if val == 'buy' else 'bear'
            bias_1d_expected = 'bull' if val == 'buy' else 'bear'

            # ALERTE A — ST Context 4H + Flip ST AI 1H (pas de filtre MACD 2D)
            if c['st_context_4h'] == val and should_send(symbol, f"context_A_{val}", event_id=event_id):
                macd_2d_status = ""
                if c['macd_2d']:
                    icon = "✅" if c['macd_2d'] == macd_2d_expected else "❌"
                    macd_2d_status = f"\n{icon} MACD 2D: {c['macd_2d'].upper()} (info)"
                send_telegram(
                    f"{emoji} <b>[CONTEXT A - ALERTE {direction}]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ ST Context 4H: {c['st_context_4h'].upper()} (zone active)\n"
                    f"✅ Flip SuperTrend AI 1H: {val.upper()} (signal)"
                    f"{macd_2d_status}\n\n"
                    f"💡 Zone 4H alignée avec signal 1H\n"
                    f"🛑 SL: {'Sous dernier swing low' if direction == 'LONG' else 'Au-dessus dernier swing high'}"
                )
                track_alert(symbol, 'CONTEXT_A')
                logger.info(f"[CONTEXT A] Alerte envoyée: {symbol} {direction}")

            # ALERTE B — MACD 2D + Bias 1D + EMA200 (prix < EMA pour LONG, > pour SHORT) + ST Context 1H + Flip ST AI 1H
            ema_trend_ok = False
            ema_status = "N/A"
            if c['ema200_1h']:
                if val == 'buy' and price < c['ema200_1h']:
                    ema_trend_ok = True
                    ema_status = f"✅ ${price:.4f} < EMA200 ${c['ema200_1h']:.4f} (zone de value)"
                elif val == 'sell' and price > c['ema200_1h']:
                    ema_trend_ok = True
                    ema_status = f"✅ ${price:.4f} > EMA200 ${c['ema200_1h']:.4f} (zone de value)"
                else:
                    ema_status = f"❌ Hors zone EMA200 (prix: ${price:.4f} | EMA200: ${c['ema200_1h']:.4f})"

            macd_2d_ok = c['macd_2d'] == macd_2d_expected
            bias_1d_ok = c.get('bias_1d') == bias_1d_expected

            if ema_trend_ok and macd_2d_ok and bias_1d_ok and c['st_context_1h'] == val and should_send(symbol, f"context_B_{val}", event_id=event_id):
                send_telegram(
                    f"{emoji} <b>[CONTEXT B - ALERTE {direction}]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ MACD 2D: {c['macd_2d'].upper()} (aligné)\n"
                    f"✅ Bias 1D: {c['bias_1d'].upper()} (aligné)\n"
                    f"✅ EMA200 1H: {ema_status}\n"
                    f"✅ ST Context 1H: {c['st_context_1h'].upper()} (zone active)\n"
                    f"✅ Flip SuperTrend AI 1H: {val.upper()} (signal)\n\n"
                    f"💡 Zone de value + Tendance + Signal alignés\n"
                    f"🛑 SL: {'Sous dernier swing low' if direction == 'LONG' else 'Au-dessus dernier swing high'}"
                )
                track_alert(symbol, 'CONTEXT_B')
                logger.info(f"[CONTEXT B] Alerte envoyée: {symbol} {direction}")

            # ALERTE B+ — tout aligné + ST Context 4H
            if ema_trend_ok and macd_2d_ok and bias_1d_ok and c['st_context_1h'] == val and c['st_context_4h'] == val and should_send(symbol, f"context_B_plus_{val}", event_id=event_id):
                send_telegram(
                    f"{emoji} <b>[CONTEXT B+ - SETUP COMPLET {direction}]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔥 <b>CONFLUENCE MAXIMALE — TOUS LES FILTRES ALIGNES</b>\n\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ MACD 2D: {c['macd_2d'].upper()} (aligné)\n"
                    f"✅ Bias 1D: {c['bias_1d'].upper()} (aligné)\n"
                    f"✅ EMA200 1H: {ema_status}\n"
                    f"✅ ST Context 4H: {c['st_context_4h'].upper()} (zone active)\n"
                    f"✅ ST Context 1H: {c['st_context_1h'].upper()} (zone active)\n"
                    f"✅ Flip SuperTrend AI 1H: {val.upper()} (signal)\n\n"
                    f"🎯 <b>Position Size: 70-80% (SETUP PARFAIT)</b>\n"
                    f"🛑 SL: {'Sous dernier swing low' if direction == 'LONG' else 'Au-dessus dernier swing high'}"
                )
                track_alert(symbol, 'CONTEXT_B+')
                logger.info(f"[CONTEXT B+] Alerte MAXIMALE envoyée: {symbol} {direction}")

    persist_runtime_state()
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
        'safe_state': SAFE_STATE,
        'momentum_state': MOMENTUM_STATE,
        'context_state': CONTEXT_STATE,
        'watchlist': CONFIG['SYMBOLS']
    }), 200

@app.route('/context_state', methods=['GET'])
def context_state_route():
    return jsonify(CONTEXT_STATE), 200

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
    SAFE_STATE.clear()
    MOMENTUM_STATE.clear()
    CONTEXT_STATE.clear()
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
    SAFE_STATE.pop(symbol, None)
    MOMENTUM_STATE.pop(symbol, None)
    CONTEXT_STATE.pop(symbol, None)
    keys_to_remove = [k for k in LAST_SIGNALS if k.startswith(f"{symbol}:")]
    for k in keys_to_remove:
        LAST_SIGNALS.pop(k, None)
        LAST_SIGNAL_EVENTS.pop(k, None)
    persist_runtime_state()
    logger.info(f"🔄 State remis à zéro pour {symbol}")
    return jsonify({'status': 'reset', 'symbol': symbol, 'message': f'État de {symbol} remis à zéro'}), 200


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

        logger.info("⏰ Schedulers démarrés (rapport hebdo + heartbeat)")
    except Exception as e:
        logger.error(f"❌ Erreur au démarrage: {e}")

startup_thread = threading.Thread(target=startup, daemon=True)
startup_thread.start()

if __name__ == '__main__':
    logger.info(f"✅ Bot démarré sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)
