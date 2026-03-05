#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
    'TELEGRAM_BOT_TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', '8110041550:AAHJKAWxIG1ZBjZ8fRfFMKq-4iTeo5v4-Hw'),
    'TELEGRAM_CHAT_ID': os.environ.get('TELEGRAM_CHAT_ID', '6473214015'),
    'SYMBOLS': {
        'BTC/USDT': 'okx', 'ETH/USDT': 'okx', 'SOL/USDT': 'okx',
        'XRP/USDT': 'okx', 'LINK/USDT': 'okx', 'TIA/USDT': 'okx',
        'TAO/USDT': 'okx', 'FET/USDT': 'okx', 'RENDER/USDT': 'okx',
        'ZK/USDT': 'okx', 'ONDO/USDT': 'okx', 'PENDLE/USDT': 'okx',
        'CRV/USDT': 'okx', 'CVX/USDT': 'okx', 'PEPE/USDT': 'okx',
        'WIF/USDT': 'okx', 'DOGE/USDT': 'okx', 'VIRTUAL/USDT': 'okx',
        'HYPE/USDT': 'okx', 'AAVE/USDT': 'okx', 'NEAR/USDT': 'okx',
        'PYTH/USDT': 'okx', 'STX/USDT': 'okx', 'ZEC/USDT': 'okx',
        'ZRO/USDT': 'okx', 'SUI/USDT': 'okx', 'ENA/USDT': 'okx',
        'ARB/USDT': 'okx', 'AVAX/USDT': 'okx',
    },
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,
    'WEBHOOK_PORT': int(os.environ.get("PORT", 8080)),
}

# ============================================================================ #
# ETAT & PERSISTANCE REDIS
# ============================================================================ #

LAST_SIGNALS = {}
CONTEXT_STATE = {}
REDIS_CLIENT = None
STATE_LOCK = threading.Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
app = Flask(__name__)

def init_redis():
    global REDIS_CLIENT
    redis_url = os.environ.get('REDIS_URL')
    if redis_url:
        try:
            REDIS_CLIENT = redis.from_url(redis_url, decode_responses=True)
            logger.info("✅ Redis connecté")
        except Exception as e:
            logger.error(f"❌ Erreur Redis: {e}")

def persist_state():
    if not REDIS_CLIENT: return
    with STATE_LOCK:
        try:
            state = {'context': CONTEXT_STATE, 'last_signals': LAST_SIGNALS}
            REDIS_CLIENT.set('bot_state_v4', json.dumps(state))
        except: pass

def load_state():
    global CONTEXT_STATE, LAST_SIGNALS
    if not REDIS_CLIENT: return
    try:
        raw = REDIS_CLIENT.get('bot_state_v4')
        if raw:
            p = json.loads(raw)
            CONTEXT_STATE.update(p.get('context', {}))
            LAST_SIGNALS.update(p.get('last_signals', {}))
            logger.info("✅ État restauré depuis Redis")
    except: pass

def audit_log_redis(data, status="received"):
    if not REDIS_CLIENT: return
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "sym": data.get('symbol'),
        "type": data.get('type'),
        "val": data.get('value'),
        "price": data.get('price'),
        "status": status
    }
    try:
        REDIS_CLIENT.lpush('audit_alerts', json.dumps(entry))
        REDIS_CLIENT.ltrim('audit_alerts', 0, 499)
    except: pass

# ============================================================================ #
# LOGIQUE WEBHOOK
# ============================================================================ #

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
    try: requests.post(url, json={'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'parse_mode': 'HTML'}, timeout=10)
    except: pass

def normalize_tf(tf):
    return {'60': '1h', '240': '4h', '1440': '1d'}.get(str(tf), str(tf))

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data: return jsonify({'status': 'no_data'}), 400

    audit_log_redis(data)
    
    symbol = data.get('symbol', '').replace('OKX:', '').replace('.P', '')
    if '/' not in symbol and 'USDT' in symbol: symbol = symbol.replace('USDT', '/USDT')
    if symbol not in CONFIG['SYMBOLS']: return jsonify({'status': 'ignored'}), 200

    tf = normalize_tf(data.get('tf', ''))
    alert_type = str(data.get('type', '')).lower()
    price = float(data.get('price', 0))
    val = str(data.get('value', '')).strip().lower()

    if symbol not in CONTEXT_STATE: CONTEXT_STATE[symbol] = {}

    # MAJ EMA 200
    if ('ema' in alert_type) and tf == '1h':
        for k in ('value', 'ema200', 'indicator_value'):
            if k in data:
                CONTEXT_STATE[symbol]['ema200_1h'] = float(data[k])
                break

    # MAJ MACD 2D
    if alert_type == 'macd' and tf == '2d':
        sig = str(data.get('signal', '')).lower() or val
        if sig in ('bull', 'bear'):
            CONTEXT_STATE[symbol]['macd_2d'] = sig

    # Signal Supertrend
    if alert_type == 'supertrend' and tf == '1h':
        c = CONTEXT_STATE[symbol]
        macd_2d = c.get('macd_2d') 
        ema_1h = c.get('ema200_1h')

        if macd_2d == 'bull' and ema_1h and price > ema_1h and val == 'buy':
            now = time.time()
            last_ts = LAST_SIGNALS.get(f"{symbol}:buy", 0)
            if now - float(last_ts) > CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']:
                LAST_SIGNALS[f"{symbol}:buy"] = now
                send_telegram(f"🚀 <b>ACHAT</b> {symbol} @ {price}")
                persist_state()

    return jsonify({'status': 'success'}), 200

@app.route('/audit')
def get_audit():
    if not REDIS_CLIENT: return "Redis non connecté", 500
    logs = REDIS_CLIENT.lrange('audit_alerts', 0, -1)
    return jsonify([json.loads(l) for l in logs])

@app.route('/')
def health(): return "Bot is alive", 200

def startup():
    init_redis()
    load_state()
    time.sleep(2)
    send_telegram("🤖 <b>Bot Trading Online</b>\nAudit: /audit")

if __name__ == '__main__':
    threading.Thread(target=startup, daemon=True).start()
    app.run(host='0.0.0.0', port=CONFIG['WEBHOOK_PORT'])