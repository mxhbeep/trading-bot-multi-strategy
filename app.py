#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ccxt
import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime
import logging
from flask import Flask, request, jsonify
import os

# ============================================================================ #
# CONFIGURATION
# ============================================================================ #

CONFIG = {
    'TELEGRAM_BOT_TOKEN': '8110041550:AAHJKAWxIG1ZBjZ8fRfFMKq-4iTeo5v4-Hw',
    'TELEGRAM_CHAT_ID': '6473214015',
    
    'SYMBOLS': [
        'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'CRV/USDT',
        'PEPE/USDT', 'DOGE/USDT', 'WIF/USDT', 'BONK/USDT', 'CVX/USDT'
    ],
    
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,
    'WEBHOOK_PORT': int(os.environ.get("PORT", 5000)),
    'WEBHOOK_HOST': '0.0.0.0',
}

# ============================================================================ #
# ETAT GLOBAL DES STRATÉGIES
# ============================================================================ #

LAST_SIGNALS = {}
SAFE_STATE = {}
AGGRESSIVE_STATE = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot trading OK - Monitoring actif"

# ============================================================================ #
# FONCTIONS TELEGRAM & NOTIFICATIONS
# ============================================================================ #

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
    payload = {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'parse_mode': 'HTML'}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Erreur Telegram: {e}")

def send_start_notification():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        "🤖 <b>[BOT STARTED & UPDATED]</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📊 <b>Mode:</b> Multi-Strategy Webhook\n"
        "🎯 <b>Monitoring:</b> 10 assets\n\n"
        "📋 <b>ACTIVE STRATEGIES:</b>\n\n"
        "1️⃣ <b>SAFE</b>\n"
        "   • Entry: System 2★ to 5★\n"
        "   • TP Partiel: MACD 1D inversion\n"
        "   • Exit: MACD 3D inversion\n\n"
        "2️⃣ <b>AGGRESSIVE</b>\n"
        "   • <b>Filter:</b> EMA 200 4H (Mean Reversion)\n"
        "   • <b>Entry:</b> Stars 3★ to 5★\n"
        "   • TP Partiel: MACD 1D inversion\n"
        "   • Exit Complet: Biais 1D inversion\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "✅ <b>Ready for TradingView</b>\n"
        f"⏰ {now}"
    )
    send_telegram(msg)

# ============================================================================ #
# UTILITAIRES LOGIQUE
# ============================================================================ #

def format_tv_symbol(s):
    if ':' in s: s = s.split(':')[-1]
    for q in ['USDT', 'USDC']:
        if s.endswith(q): return f"{s.replace(q, '')}/{q}"
    return s

def should_send(symbol, key):
    now = time.time()
    k = f"{symbol}:{key}"
    if k not in LAST_SIGNALS or (now - LAST_SIGNALS[k] > CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']):
        LAST_SIGNALS[k] = now
        return True
    return False

# ============================================================================ #
# HANDLER WEBHOOK
# ============================================================================ #

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data: return jsonify({'status': 'no_data'}), 400
    
    symbol = format_tv_symbol(data.get('symbol', ''))
    strat = data.get('strategy', '').lower()
    tf = data.get('tf', '').lower()
    alert_type = data.get('type', '').lower()
    val = str(data.get('value', '')).lower()
    price = float(data.get('price', 0))

    if symbol not in CONFIG['SYMBOLS']: return jsonify({'status': 'ignored'}), 200

    # Init états
    if symbol not in SAFE_STATE:
        SAFE_STATE[symbol] = {'bias_3d':None, 'macd_4h':None, 'bias_1h':None, 'st_1h':None, 'bias_4h':None, 'macd_1d':None}
    if symbol not in AGGRESSIVE_STATE:
        AGGRESSIVE_STATE[symbol] = {'st_context_4h':None, 'st_context_1h':None, 'macd_4h':None, 'bias_1h':None, 'bias_4h':None, 'bias_1d':None, 'macd_1d':None, 'ema200_4h':None}

    # --- 1. LOGIQUE SAFE ---
    if strat in ['safe', 'both']:
        s = SAFE_STATE[symbol]
        if alert_type == 'bias' and tf == '3d': s['bias_3d'] = val
        if alert_type == 'macd' and tf == '4h': s['macd_4h'] = val
        if alert_type == 'bias' and tf == '1h': s['bias_1h'] = val
        if alert_type == 'supertrend' and tf == '1h': s['st_1h'] = val
        if alert_type == 'bias_9_26' and tf == '4h': s['bias_4h'] = val
        if alert_type == 'macd' and tf == '1d': s['macd_1d'] = val

        # Sorties SAFE
        if alert_type == 'macd' and tf == '1d':
            send_telegram(f"🛡️ <b>[SAFE - TP PARTIEL]</b> {symbol}\nInversion MACD 1D.")
        if alert_type == 'macd_exit' and tf == '3d':
            send_telegram(f"🛡️ <b>[SAFE - EXIT COMPLET]</b> {symbol}\nInversion MACD 3D.")

        # Entrées SAFE
        direction = "LONG" if s['bias_3d'] == 'bull' and s['macd_4h'] == 'bull' else "SHORT" if s['bias_3d'] == 'bear' and s['macd_4h'] == 'bear' else None
        if direction:
            stars = 2
            expected = 'bull' if direction == "LONG" else 'bear'
            st_expected = 'buy' if direction == "LONG" else 'sell'
            if s['bias_1h'] == expected: stars = 3
            if s['st_1h'] == st_expected: stars = 4
            if s['bias_4h'] == expected: stars = 5
            
            if stars >= 2 and alert_type == 'supertrend' and tf == '1h' and should_send(symbol, f"safe_{stars}*"):
                send_telegram(f"🛡️ <b>[SAFE {stars}⭐]</b> {symbol}\nDir: {direction}\nPrix: {price}")

    # --- 2. LOGIQUE AGGRESSIVE ---
    if strat in ['aggressive', 'both']:
        a = AGGRESSIVE_STATE[symbol]
        if alert_type == 'st_context' and tf == '4h': a['st_context_4h'] = val
        if alert_type == 'st_context' and tf == '1h': a['st_context_1h'] = val
        if alert_type == 'macd' and tf == '4h': a['macd_4h'] = val
        if alert_type == 'bias' and tf == '1h': a['bias_1h'] = val
        if alert_type == 'bias' and tf == '4h': a['bias_4h'] = val
        if alert_type == 'bias' and tf == '1d': a['bias_1d'] = val
        if alert_type == 'macd' and tf == '1d': a['macd_1d'] = val
        if alert_type == 'ema200' and tf == '4h': a['ema200_4h'] = float(val)

        # Sorties AGGRESSIVE
        if alert_type == 'macd' and tf == '1d':
            send_telegram(f"🔥 <b>[AGGRESSIVE - TP PARTIEL]</b> {symbol}\nInversion MACD 1D.")
        if alert_type == 'bias' and tf == '1d':
            send_telegram(f"🔥 <b>[AGGRESSIVE - EXIT COMPLET]</b> {symbol}\nInversion Biais 1D.")

        # Entrées AGGRESSIVE
        if alert_type == 'supertrend' and tf == '1h':
            direction = "LONG" if val == 'buy' else "SHORT"
            expected = 'bull' if direction == "LONG" else 'bear'
            
            # Filtre EMA 200
            ema_ok = False
            if a['ema200_4h']:
                if direction == "LONG" and price < a['ema200_4h']: ema_ok = True
                if direction == "SHORT" and price > a['ema200_4h']: ema_ok = True

            if ema_ok:
                stars = 0
                if a['st_context_4h'] == val and a['st_context_1h'] == val and a['macd_4h'] == expected:
                    stars = 3
                    if a['bias_1h'] == expected: stars = 4
                    if a['bias_4h'] == expected: stars = 5
                
                # Check condition 4* alternative (Biais D)
                if stars < 4 and a['bias_1d'] == expected and a['st_context_1h'] == val and a['macd_4h'] == expected:
                    stars = 4

                if stars >= 3 and should_send(symbol, f"agg_{stars}*"):
                    send_telegram(f"🔥 <b>[AGGRESSIVE {stars}⭐]</b> {symbol}\nDir: {direction}\nPrix: {price}\nEMA: OK")

    return jsonify({'status': 'success'}), 200

if __name__ == '__main__':
    send_start_notification()
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'])