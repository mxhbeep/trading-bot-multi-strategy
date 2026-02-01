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
        logger.info(f"✅ Message Telegram envoyé")
    except Exception as e:
        logger.error(f"❌ Erreur Telegram: {e}")

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
        f"⏰ {now}\n\n"
        "🆕 <b>NEW:</b> Alertes de sortie améliorées avec direction"
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
    if not data: 
        logger.warning("⚠️ Webhook sans données")
        return jsonify({'status': 'no_data'}), 400
    
    symbol = format_tv_symbol(data.get('symbol', ''))
    strat = data.get('strategy', '').lower()
    tf = data.get('tf', '').lower()
    alert_type = data.get('type', '').lower()
    val = str(data.get('value', '')).lower()
    price = float(data.get('price', 0))

    logger.info(f"📥 Webhook reçu: {symbol} | {strat} | {tf} | {alert_type} | {val}")

    if symbol not in CONFIG['SYMBOLS']: 
        logger.info(f"⏭️ Symbole {symbol} non surveillé")
        return jsonify({'status': 'ignored'}), 200

    # Init états
    if symbol not in SAFE_STATE:
        SAFE_STATE[symbol] = {
            'bias_3d': None, 
            'macd_4h': None, 
            'bias_1h': None, 
            'st_1h': None, 
            'bias_4h': None, 
            'macd_1d': None
        }
    if symbol not in AGGRESSIVE_STATE:
        AGGRESSIVE_STATE[symbol] = {
            'st_context_4h': None, 
            'st_context_1h': None, 
            'macd_4h': None, 
            'bias_1h': None, 
            'bias_4h': None, 
            'bias_1d': None,
            'macd_1d': None, 
            'ema200_4h': None
        }

    # ========================================================================
    # 1. LOGIQUE SAFE
    # ========================================================================
    if strat in ['safe', 'both']:
        s = SAFE_STATE[symbol]
        
        # Mise à jour des états
        if alert_type == 'bias' and tf == '3d': 
            s['bias_3d'] = val
            logger.info(f"[SAFE] {symbol} - Bias 3D: {val}")
        if alert_type == 'macd' and tf == '4h': 
            s['macd_4h'] = val
            logger.info(f"[SAFE] {symbol} - MACD 4H: {val}")
        if alert_type == 'bias' and tf == '1h': 
            s['bias_1h'] = val
            logger.info(f"[SAFE] {symbol} - Bias 1H: {val}")
        if alert_type == 'supertrend' and tf == '1h': 
            s['st_1h'] = val
            logger.info(f"[SAFE] {symbol} - SuperTrend 1H: {val}")
        if alert_type == 'bias_9_26' and tf == '4h': 
            s['bias_4h'] = val
            logger.info(f"[SAFE] {symbol} - Bias 4H (9/26): {val}")
        if alert_type == 'macd' and tf == '1d': 
            s['macd_1d'] = val
            logger.info(f"[SAFE] {symbol} - MACD 1D: {val}")

        # ===== SORTIES SAFE (AMÉLIORÉES) =====
        
        # TP Partiel MACD 1D
        if alert_type == 'macd' and tf == '1d':
            direction_macd = "BULLISH" if val == 'bull' else "BEARISH"
            emoji = "🟢" if val == 'bull' else "🔴"
            
            msg = (
                f"{emoji} <b>[SAFE - TP PARTIEL]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Trigger:</b> MACD 1D Inversion\n"
                f"📈 <b>New Direction:</b> {direction_macd}\n"
                f"💰 <b>Price:</b> ${price:.4f}\n"
                f"⏰ <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"💡 <b>Action:</b> Consider taking partial profits (30-50%)\n"
                f"🎯 <b>Position:</b> Keep remaining for MACD 3D exit"
            )
            send_telegram(msg)
            logger.info(f"✅ [SAFE] TP Partiel envoyé pour {symbol}")
        
        # Exit Complet MACD 3D
        if alert_type == 'macd_exit' and tf == '3d':
            direction_macd = "BULLISH" if val == 'bull' else "BEARISH"
            emoji = "🟢" if val == 'bull' else "🔴"
            exit_emoji = "🚪"
            
            msg = (
                f"{exit_emoji} <b>[SAFE - EXIT COMPLET]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Trigger:</b> MACD 3D Opposite Cross\n"
                f"📈 <b>New Direction:</b> {direction_macd}\n"
                f"💰 <b>Price:</b> ${price:.4f}\n"
                f"⏰ <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"❌ <b>Action:</b> EXIT ALL POSITIONS NOW\n"
                f"🛡️ <b>Strategy:</b> SAFE - Full exit required"
            )
            send_telegram(msg)
            logger.info(f"✅ [SAFE] Exit complet envoyé pour {symbol}")

        # ===== ENTRÉES SAFE =====
        direction = None
        if s['bias_3d'] == 'bull' and s['macd_4h'] == 'bull':
            direction = "LONG"
        elif s['bias_3d'] == 'bear' and s['macd_4h'] == 'bear':
            direction = "SHORT"
        
        if direction:
            stars = 2
            expected = 'bull' if direction == "LONG" else 'bear'
            st_expected = 'buy' if direction == "LONG" else 'sell'
            
            if s['bias_1h'] == expected: stars = 3
            if s['st_1h'] == st_expected: stars = 4
            if s['bias_4h'] == expected: stars = 5
            
            if stars >= 2 and alert_type == 'supertrend' and tf == '1h' and should_send(symbol, f"safe_{stars}*"):
                emoji = "🟢" if direction == "LONG" else "🔴"
                msg = (
                    f"{emoji} <b>[SAFE {stars}⭐]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 <b>Direction:</b> {direction}\n"
                    f"💰 <b>Price:</b> ${price:.4f}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 3D: {s['bias_3d']}\n"
                    f"✅ MACD 4H: {s['macd_4h']}\n"
                    f"{'✅' if stars >= 3 else '❌'} Bias 1H: {s['bias_1h']}\n"
                    f"{'✅' if stars >= 4 else '❌'} SuperTrend 1H: {s['st_1h']}\n"
                    f"{'✅' if stars == 5 else '❌'} Bias 4H (9/26): {s['bias_4h']}"
                )
                send_telegram(msg)
                logger.info(f"✅ [SAFE] Signal {stars}★ envoyé pour {symbol}")

    # ========================================================================
    # 2. LOGIQUE AGGRESSIVE
    # ========================================================================
    if strat in ['aggressive', 'both']:
        a = AGGRESSIVE_STATE[symbol]
        
        # Mise à jour des états
        if alert_type == 'st_context' and tf == '4h': 
            a['st_context_4h'] = val
            logger.info(f"[AGGRESSIVE] {symbol} - ST Context 4H: {val}")
        if alert_type == 'st_context' and tf == '1h': 
            a['st_context_1h'] = val
            logger.info(f"[AGGRESSIVE] {symbol} - ST Context 1H: {val}")
        if alert_type == 'macd' and tf == '4h': 
            a['macd_4h'] = val
            logger.info(f"[AGGRESSIVE] {symbol} - MACD 4H: {val}")
        if alert_type == 'bias' and tf == '1h': 
            a['bias_1h'] = val
            logger.info(f"[AGGRESSIVE] {symbol} - Bias 1H: {val}")
        if alert_type == 'bias' and tf == '4h': 
            a['bias_4h'] = val
            logger.info(f"[AGGRESSIVE] {symbol} - Bias 4H: {val}")
        if alert_type == 'bias' and tf == '1d': 
            a['bias_1d'] = val
            logger.info(f"[AGGRESSIVE] {symbol} - Bias 1D: {val}")
        if alert_type == 'macd' and tf == '1d': 
            a['macd_1d'] = val
            logger.info(f"[AGGRESSIVE] {symbol} - MACD 1D: {val}")
        if alert_type == 'ema200' and tf == '4h': 
            a['ema200_4h'] = float(val)
            logger.info(f"[AGGRESSIVE] {symbol} - EMA200 4H: {val}")

        # ===== SORTIES AGGRESSIVE (AMÉLIORÉES) =====
        
        # TP Partiel MACD 1D
        if alert_type == 'macd' and tf == '1d':
            direction_macd = "BULLISH" if val == 'bull' else "BEARISH"
            emoji = "🟢" if val == 'bull' else "🔴"
            
            msg = (
                f"{emoji} <b>[AGGRESSIVE - TP PARTIEL]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Trigger:</b> MACD 1D Inversion\n"
                f"📈 <b>New Direction:</b> {direction_macd}\n"
                f"💰 <b>Price:</b> ${price:.4f}\n"
                f"⏰ <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"💡 <b>Action:</b> Consider taking partial profits (50-70%)\n"
                f"🎯 <b>Position:</b> Keep remaining for Bias 1D exit"
            )
            send_telegram(msg)
            logger.info(f"✅ [AGGRESSIVE] TP Partiel envoyé pour {symbol}")
        
        # Exit Complet Bias 1D (AMÉLIORÉ - Option 2)
        if alert_type == 'bias' and tf == '1d':
            # Déterminer la nouvelle direction du biais
            new_bias_direction = "BULLISH" if val == 'bull' else "BEARISH"
            emoji = "🟢" if val == 'bull' else "🔴"
            exit_emoji = "🚪"
            
            # Message enrichi avec toutes les informations
            msg = (
                f"{exit_emoji} <b>[AGGRESSIVE - EXIT COMPLET]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Trigger:</b> Bias 1D Inversion\n"
                f"📈 <b>New Bias Direction:</b> {new_bias_direction}\n"
                f"💰 <b>Current Price:</b> ${price:.4f}\n"
                f"⏰ <b>Alert Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"❌ <b>Action:</b> EXIT ALL REMAINING POSITIONS\n"
                f"🔥 <b>Strategy:</b> AGGRESSIVE - Full exit required\n\n"
                f"📋 <b>Context:</b>\n"
                f"   • Old Bias 1D: {a.get('bias_1d', 'N/A')} → New: {val}\n"
                f"   • Bias 4H: {a.get('bias_4h', 'N/A')}\n"
                f"   • Bias 1H: {a.get('bias_1h', 'N/A')}\n"
                f"   • MACD 1D: {a.get('macd_1d', 'N/A')}"
            )
            send_telegram(msg)
            logger.info(f"✅ [AGGRESSIVE] Exit complet envoyé pour {symbol} - Nouvelle direction: {new_bias_direction}")

        # ===== ENTRÉES AGGRESSIVE =====
        if alert_type == 'supertrend' and tf == '1h':
            direction = "LONG" if val == 'buy' else "SHORT"
            expected = 'bull' if direction == "LONG" else 'bear'
            
            # Filtre EMA 200 (Mean Reversion)
            ema_ok = False
            ema_status = "N/A"
            if a['ema200_4h']:
                if direction == "LONG" and price < a['ema200_4h']:
                    ema_ok = True
                    ema_status = f"✅ Price ${price:.2f} < EMA200 ${a['ema200_4h']:.2f}"
                elif direction == "SHORT" and price > a['ema200_4h']:
                    ema_ok = True
                    ema_status = f"✅ Price ${price:.2f} > EMA200 ${a['ema200_4h']:.2f}"
                else:
                    ema_status = f"❌ Price ${price:.2f} vs EMA200 ${a['ema200_4h']:.2f} - No mean reversion"

            if ema_ok:
                stars = 0
                
                # Standard 3★, 4★, 5★
                if a['st_context_4h'] == val and a['st_context_1h'] == val and a['macd_4h'] == expected:
                    stars = 3
                    if a['bias_1h'] == expected: stars = 4
                    if a['bias_4h'] == expected: stars = 5
                
                # Alternative 4★ avec Bias 1D
                if stars < 4 and a['bias_1d'] == expected and a['st_context_1h'] == val and a['macd_4h'] == expected:
                    stars = 4
                    logger.info(f"[AGGRESSIVE] {symbol} - 4★ alternatif détecté (Bias 1D)")

                if stars >= 3 and should_send(symbol, f"agg_{stars}*"):
                    emoji = "🟢" if direction == "LONG" else "🔴"
                    msg = (
                        f"{emoji} <b>[AGGRESSIVE {stars}⭐]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 <b>Direction:</b> {direction}\n"
                        f"💰 <b>Price:</b> ${price:.4f}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"📊 <b>EMA 200 Filter (Mean Reversion):</b>\n"
                        f"   {ema_status}\n\n"
                        f"✅ ST Context 4H: {a['st_context_4h']}\n"
                        f"✅ ST Context 1H: {a['st_context_1h']}\n"
                        f"✅ MACD 4H: {a['macd_4h']}\n"
                        f"{'✅' if stars >= 4 else '❌'} Bias 1H: {a['bias_1h']}\n"
                        f"{'✅' if stars == 5 else '❌'} Bias 4H: {a['bias_4h']}"
                    )
                    send_telegram(msg)
                    logger.info(f"✅ [AGGRESSIVE] Signal {stars}★ envoyé pour {symbol}")
            else:
                logger.info(f"⏭️ [AGGRESSIVE] {symbol} - Signal ignoré (EMA200 filter not met)")

    return jsonify({'status': 'success', 'symbol': symbol, 'strategy': strat}), 200


@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'running',
        'timestamp': datetime.now().isoformat(),
        'symbols_monitored': len(CONFIG['SYMBOLS'])
    }), 200


@app.route('/state', methods=['GET'])
def state():
    return jsonify({
        'safe_state': SAFE_STATE,
        'aggressive_state': AGGRESSIVE_STATE
    }), 200


if __name__ == '__main__':
    logger.info("🚀 Démarrage du bot...")
    send_start_notification()
    logger.info(f"✅ Bot démarré sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)