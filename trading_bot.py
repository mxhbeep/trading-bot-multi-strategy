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
        'PUMP/USDT': 'okx',
        'DOGE/USDT': 'okx',
        
        # Tier 5 - Wildcard
        'VIRTUAL/USDT': 'okx',
        'HYPE/USDT': 'okx',
    },
    
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,
    'WEBHOOK_PORT': int(os.environ.get("PORT", 5000)),
    'WEBHOOK_HOST': '0.0.0.0',
}

# ============================================================================ #
# ETAT GLOBAL
# ============================================================================ #

LAST_SIGNALS = {}
SAFE_STATE = {}
MOMENTUM_STATE = {}
CONTEXT_STATE = {}  # Nouvel état pour la stratégie CONTEXT

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
# INITIALISATION EXCHANGES
# ============================================================================ #

def init_exchanges():
    global exchanges
    try:
        exchanges['okx'] = ccxt.okx({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        exchanges['binance'] = ccxt.binance({
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
        requests.post(url, json=payload, timeout=10)
        logger.info("✅ Message Telegram envoyé")
    except Exception as e:
        logger.error(f"❌ Erreur Telegram: {e}")

def send_start_notification():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        "🤖 <b>[BOT STARTED]</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Total Assets: {len(CONFIG['SYMBOLS'])}\n\n"
        "📋 <b>STRATEGIES:</b>\n\n"
        "1️⃣ <b>SAFE</b>\n"
        "   • Bias 3D + MACD 4H + Bias 1H + ST 1H\n\n"
        "2️⃣ <b>MOMENTUM</b>\n"
        "   • Bias 1D + EMA 200 1H + ST 1H\n\n"
        "3️⃣ <b>CONTEXT</b> 🆕\n"
        "   • Alerte A: ST Context 4H + Flip ST AI 1H\n"
        "   • Alerte B: EMA 200 1H + ST Context 1H + Flip ST AI 1H\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {now}"
    )
    send_telegram(msg)

# ============================================================================ #
# UTILITAIRES
# ============================================================================ #

def format_tv_symbol(s):
    if ':' in s:
        s = s.split(':')[-1]
    for q in ['USDT', 'USDC', 'BUSD']:
        if s.endswith(q) and '/' not in s:
            return f"{s.replace(q, '')}/{q}"
    return s

def get_exchange_for_symbol(symbol):
    exchange_name = CONFIG['SYMBOLS'].get(symbol)
    if not exchange_name:
        return None
    return exchanges.get(exchange_name)

def should_send(symbol, key):
    now = time.time()
    k = f"{symbol}:{key}"
    if k not in LAST_SIGNALS or (now - LAST_SIGNALS[k] > CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']):
        LAST_SIGNALS[k] = now
        return True
    return False

def init_symbol_states(symbol):
    if symbol not in SAFE_STATE:
        SAFE_STATE[symbol] = {
            'bias_3d': None,
            'macd_4h': None,
            'bias_1h': None,
            'st_1h': None,
            'bias_4h': None,
            'macd_1d': None
        }
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'bias_1d': None,
            'macd_4h': None,
            'ema200_1h': None,
            'st_1h': None,
            'macd_1d': None,
            'st_context_4h': None,
            'st_context_1h': None
        }
    if symbol not in CONTEXT_STATE:
        CONTEXT_STATE[symbol] = {
            # Pour Alerte A : ST Context 4H + Flip ST AI 1H
            'st_context_4h': None,   # 'buy' ou 'sell' — mémorisé jusqu'au signal opposé
            # Pour Alerte B : EMA 200 1H + ST Context 1H + Flip ST AI 1H
            'ema200_1h': None,       # valeur float de l'EMA 200
            'st_context_1h': None,   # 'buy' ou 'sell' — mémorisé jusqu'au signal opposé
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
    tf          = data.get('tf', '').lower()
    alert_type  = data.get('type', '').lower()
    val         = str(data.get('value', '')).lower()
    price       = float(data.get('price', 0))

    logger.info(f"📥 Webhook: {symbol} | strat={strat} | tf={tf} | type={alert_type} | val={val} | price={price}")

    if symbol not in CONFIG['SYMBOLS']:
        logger.info(f"⏭️ {symbol} non dans la watchlist")
        return jsonify({'status': 'ignored', 'reason': 'not_in_watchlist'}), 200

    exchange_name = CONFIG['SYMBOLS'][symbol]
    init_symbol_states(symbol)

    # ========================================================================
    # LOGIQUE SAFE (inchangée)
    # ========================================================================
    if strat in ['safe', 'all']:
        s = SAFE_STATE[symbol]

        if alert_type == 'bias' and tf == '3d':
            s['bias_3d'] = val
        if alert_type == 'macd' and tf == '4h':
            s['macd_4h'] = val
        if alert_type == 'bias' and tf == '1h':
            s['bias_1h'] = val
        if alert_type == 'supertrend' and tf == '1h':
            s['st_1h'] = val
        if alert_type == 'bias_9_26' and tf == '4h':
            s['bias_4h'] = val
        if alert_type == 'macd' and tf == '1d':
            s['macd_1d'] = val

        # Sorties SAFE
        if alert_type == 'macd' and tf == '1d':
            direction_macd = "BULLISH" if val == 'bull' else "BEARISH"
            emoji = "🟢" if val == 'bull' else "🔴"
            msg = (
                f"{emoji} <b>[SAFE - TP PARTIEL]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Trigger: MACD 1D Inversion\n"
                f"📈 New Direction: {direction_macd}\n"
                f"💰 Price: ${price:.4f}\n"
                f"🏦 Exchange: {exchange_name.upper()}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"💡 Action: Take partial profits (30-50%)"
            )
            send_telegram(msg)

        if alert_type == 'macd' and tf == '3d':
            direction_macd = "BULLISH" if val == 'bull' else "BEARISH"
            emoji = "🟢" if val == 'bull' else "🔴"
            msg = (
                f"🚪 <b>[SAFE - EXIT COMPLET]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Trigger: MACD 3D Change\n"
                f"📈 New Direction: {direction_macd}\n"
                f"💰 Price: ${price:.4f}\n"
                f"🏦 Exchange: {exchange_name.upper()}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"❌ Action: EXIT ALL POSITIONS NOW"
            )
            send_telegram(msg)

        # Entrées SAFE
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

            if stars >= 3 and alert_type == 'bias' and tf == '1h' and val == expected and should_send(symbol, f"safe_prep_{stars}*"):
                emoji = "🟡" if direction == "LONG" else "🟠"
                msg = (
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
                send_telegram(msg)

            if stars >= 2 and alert_type == 'supertrend' and tf == '1h' and should_send(symbol, f"safe_{stars}*"):
                emoji = "🟢" if direction == "LONG" else "🔴"
                action = "ENTREE MAINTENANT" if stars >= 4 else "POSITION POSSIBLE"
                msg = (
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
                send_telegram(msg)

    # ========================================================================
    # LOGIQUE MOMENTUM (inchangée)
    # ========================================================================
    if strat in ['momentum', 'momentum_context', 'all']:
        m = MOMENTUM_STATE[symbol]

        if alert_type == 'bias' and tf == '1d':
            m['bias_1d'] = val
        if alert_type == 'macd' and tf == '4h':
            m['macd_4h'] = val
        if alert_type == 'ema200' and tf == '1h':
            m['ema200_1h'] = float(val)
        if alert_type == 'supertrend' and tf == '1h':
            m['st_1h'] = val
        if alert_type == 'macd' and tf == '1d':
            m['macd_1d'] = val
        if alert_type == 'st_context' and tf == '4h':
            m['st_context_4h'] = val
        if alert_type == 'st_context' and tf == '1h':
            m['st_context_1h'] = val

        # Sorties MOMENTUM
        if alert_type == 'macd' and tf == '1d':
            direction_macd = "BULLISH" if val == 'bull' else "BEARISH"
            emoji = "🟢" if val == 'bull' else "🔴"
            msg = (
                f"{emoji} <b>[MOMENTUM - TP PARTIEL]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Trigger: MACD 1D Inversion\n"
                f"📈 New Direction: {direction_macd}\n"
                f"💰 Price: ${price:.4f}\n"
                f"🏦 Exchange: {exchange_name.upper()}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"💡 Action: Take partial profits (40-60%)"
            )
            send_telegram(msg)

        if alert_type == 'bias' and tf == '1d':
            old_bias = m.get('bias_1d')
            if old_bias and old_bias != val:
                new_bias_direction = "BULLISH" if val == 'bull' else "BEARISH"
                msg = (
                    f"🚪 <b>[MOMENTUM - EXIT COMPLET]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Trigger: Bias 1D Inversion\n"
                    f"📈 New Bias: {new_bias_direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"❌ Action: EXIT ALL POSITIONS NOW"
                )
                send_telegram(msg)

        # Entrées MOMENTUM
        direction = None
        if m['bias_1d'] == 'bull':
            direction = "LONG"
        elif m['bias_1d'] == 'bear':
            direction = "SHORT"

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

            if ema_ok and alert_type == 'ema200' and tf == '1h' and should_send(symbol, "momentum_prep"):
                emoji = "🟡" if direction == "LONG" else "🟠"
                msg = (
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
                send_telegram(msg)

            if ema_ok and alert_type == 'supertrend' and tf == '1h':
                st_expected = 'buy' if direction == "LONG" else 'sell'
                if val == st_expected and should_send(symbol, "momentum_entry"):
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
                    msg += f"\n🎯 <b>Position Size: "
                    if stars == 5:
                        msg += "70-80% (SETUP PARFAIT)"
                    elif stars == 4:
                        msg += "60-70% (BONUS ST CONTEXT 1H)"
                    else:
                        msg += "50-60%"
                    msg += "</b>"
                    send_telegram(msg)

    # ========================================================================
    # LOGIQUE CONTEXT (nouvelle stratégie)
    # ========================================================================
    if strat in ['context', 'momentum_context', 'all']:
        c = CONTEXT_STATE[symbol]

        # --- Mise à jour des états CONTEXT ---

        # ST Context 4H → mémorisé jusqu'au signal opposé
        if alert_type == 'st_context' and tf == '4h':
            old_val = c['st_context_4h']
            c['st_context_4h'] = val
            logger.info(f"[CONTEXT] {symbol} - ST Context 4H: {old_val} → {val}")

        # EMA 200 1H → valeur float mise à jour en continu
        if alert_type == 'ema200' and tf == '1h':
            c['ema200_1h'] = float(val)
            logger.info(f"[CONTEXT] {symbol} - EMA200 1H: {val}")

        # ST Context 1H → mémorisé jusqu'au signal opposé
        if alert_type == 'st_context' and tf == '1h':
            old_val = c['st_context_1h']
            c['st_context_1h'] = val
            logger.info(f"[CONTEXT] {symbol} - ST Context 1H: {old_val} → {val}")

        # --- Déclenchement des alertes au flip SuperTrend AI 1H ---
        if alert_type == 'supertrend' and tf == '1h':

            direction = "LONG" if val == 'buy' else "SHORT"
            emoji = "🟢" if val == 'buy' else "🔴"

            # ----------------------------------------------------------------
            # ALERTE A : ST Context 4H + Flip ST AI 1H (même direction)
            # ----------------------------------------------------------------
            if c['st_context_4h'] == val and should_send(symbol, f"context_A_{val}"):
                msg = (
                    f"{emoji} <b>[CONTEXT A - ALERTE {direction}]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ ST Context 4H: {c['st_context_4h'].upper()} (zone active)\n"
                    f"✅ Flip SuperTrend AI 1H: {val.upper()} (signal)\n\n"
                    f"💡 Zone 4H alignée avec signal 1H\n"
                    f"🛑 SL: {'Sous dernier swing low' if direction == 'LONG' else 'Au-dessus dernier swing high'}"
                )
                send_telegram(msg)
                logger.info(f"[CONTEXT A] Alerte envoyée: {symbol} {direction}")

            # ----------------------------------------------------------------
            # ALERTE B : EMA 200 1H (tendance) + ST Context 1H + Flip ST AI 1H
            # ----------------------------------------------------------------
            ema_trend_ok = False
            ema_status = "N/A"

            if c['ema200_1h']:
                if val == 'buy' and price > c['ema200_1h']:
                    ema_trend_ok = True
                    ema_status = f"✅ ${price:.4f} > EMA200 ${c['ema200_1h']:.4f} (tendance HAUSSIERE)"
                elif val == 'sell' and price < c['ema200_1h']:
                    ema_trend_ok = True
                    ema_status = f"✅ ${price:.4f} < EMA200 ${c['ema200_1h']:.4f} (tendance BAISSIERE)"
                else:
                    ema_status = f"❌ Contre-tendance EMA200 (prix: ${price:.4f} | EMA200: ${c['ema200_1h']:.4f})"

            if ema_trend_ok and c['st_context_1h'] == val and should_send(symbol, f"context_B_{val}"):
                msg = (
                    f"{emoji} <b>[CONTEXT B - ALERTE {direction}]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Tendance EMA 200 1H: {ema_status}\n"
                    f"✅ ST Context 1H: {c['st_context_1h'].upper()} (zone active)\n"
                    f"✅ Flip SuperTrend AI 1H: {val.upper()} (signal)\n\n"
                    f"💡 Tendance + Zone 1H + Signal alignés\n"
                    f"🛑 SL: {'Sous dernier swing low' if direction == 'LONG' else 'Au-dessus dernier swing high'}"
                )
                send_telegram(msg)
                logger.info(f"[CONTEXT B] Alerte envoyée: {symbol} {direction}")

            # Bonus : si Alerte B ET ST Context 4H aussi aligné → signal renforcé
            if ema_trend_ok and c['st_context_1h'] == val and c['st_context_4h'] == val and should_send(symbol, f"context_B_plus_{val}"):
                msg = (
                    f"{emoji} <b>[CONTEXT B+ - SETUP COMPLET {direction}]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔥 <b>CONFLUENCE MAXIMALE — TOUS LES FILTRES ALIGNES</b>\n\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Tendance EMA 200 1H: {ema_status}\n"
                    f"✅ ST Context 4H: {c['st_context_4h'].upper()} (zone active)\n"
                    f"✅ ST Context 1H: {c['st_context_1h'].upper()} (zone active)\n"
                    f"✅ Flip SuperTrend AI 1H: {val.upper()} (signal)\n\n"
                    f"🎯 <b>Position Size: 70-80% (SETUP PARFAIT)</b>\n"
                    f"🛑 SL: {'Sous dernier swing low' if direction == 'LONG' else 'Au-dessus dernier swing high'}"
                )
                send_telegram(msg)
                logger.info(f"[CONTEXT B+] Alerte MAXIMALE envoyée: {symbol} {direction}")

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
        'exchanges': {k: '✅' for k in exchanges}
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
    """Voir l'état actuel de tous les signaux CONTEXT"""
    return jsonify(CONTEXT_STATE), 200


# ============================================================================ #
# POINT D'ENTREE
# ============================================================================ #

if __name__ == '__main__':
    logger.info("🚀 Démarrage du bot...")
    init_exchanges()
    send_start_notification()
    logger.info(f"✅ Bot démarré sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)
