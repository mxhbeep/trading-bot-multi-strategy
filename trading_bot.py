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
    
    # Watchlist avec mapping des exchanges
    'SYMBOLS': {
        # 🛡️ Majors & Infrastructure - OKX
        'BTC/USDT': 'okx',
        'ETH/USDT': 'okx',
        'SOL/USDT': 'okx',
        'XRP/USDT': 'okx',
        'LINK/USDT': 'okx',
        'TIA/USDT': 'okx',
        'STX/USDT': 'okx',
        
        # 🛡️ Majors & Infrastructure - Binance (non disponibles sur OKX)
        'VET/USDT': 'binance',
        'PYTH/USDT': 'binance',
        'QNT/USDT': 'binance',
        'FRM/USDT': 'binance',
        
        # 🧠 IA, DePIN & Tech - OKX
        'TAO/USDT': 'okx',
        'FET/USDT': 'okx',
        'RENDER/USDT': 'okx',
        'ZK/USDT': 'okx',
        
        # 💸 Finance & RWA - OKX
        'ONDO/USDT': 'okx',
        'CVX/USDT': 'okx',
        'CRV/USDT': 'okx',
        'PENDLE/USDT': 'okx',
        
        # 🕶️ Privacy
        'ZEC/USDT': 'okx',
        'XMR/USDT': 'binance',  # Monero souvent indisponible, on teste
        
        # 🎭 Culture & Mèmes - OKX
        'PEPE/USDT': 'okx',
        'BONK/USDT': 'okx',
        'DOGE/USDT': 'okx',
        'WIF/USDT': 'okx',
        'PENGU/USDT': 'okx',
        'PUMP/USDT': 'okx',
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

# Exchanges initialisés
exchanges = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/')
def home():
    total_symbols = len(CONFIG['SYMBOLS'])
    okx_count = sum(1 for ex in CONFIG['SYMBOLS'].values() if ex == 'okx')
    binance_count = sum(1 for ex in CONFIG['SYMBOLS'].values() if ex == 'binance')
    
    return f"""
    <h1>🤖 Trading Bot Multi-Exchange</h1>
    <p>Status: ✅ Running</p>
    <p>Total assets: {total_symbols}</p>
    <p>OKX: {okx_count} | Binance: {binance_count}</p>
    <p>Strategies: SAFE + AGGRESSIVE</p>
    """

# ============================================================================ #
# INITIALISATION EXCHANGES
# ============================================================================ #

def init_exchanges():
    """Initialise les connexions aux exchanges."""
    global exchanges
    
    try:
        # OKX
        exchanges['okx'] = ccxt.okx({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        logger.info("✅ OKX initialisé")
        
        # Binance
        exchanges['binance'] = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        logger.info("✅ Binance initialisé")
        
        # Vérifier la connectivité
        for name, exchange in exchanges.items():
            try:
                exchange.load_markets()
                logger.info(f"✅ {name.upper()} - Markets chargés avec succès")
            except Exception as e:
                logger.error(f"❌ {name.upper()} - Erreur chargement markets: {e}")
        
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
        logger.info(f"✅ Message Telegram envoyé")
    except Exception as e:
        logger.error(f"❌ Erreur Telegram: {e}")

def send_start_notification():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    okx_symbols = [s for s, ex in CONFIG['SYMBOLS'].items() if ex == 'okx']
    binance_symbols = [s for s, ex in CONFIG['SYMBOLS'].items() if ex == 'binance']
    
    msg = (
        "🤖 <b>[BOT STARTED - MULTI-EXCHANGE]</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "📊 <b>Configuration:</b>\n"
        f"   • Total Assets: {len(CONFIG['SYMBOLS'])}\n"
        f"   • OKX: {len(okx_symbols)} assets\n"
        f"   • Binance: {len(binance_symbols)} assets\n\n"
        "📋 <b>STRATEGIES:</b>\n\n"
        "1️⃣ <b>SAFE</b>\n"
        "   • Entry: 2★ to 5★\n"
        "   • TP Partiel: MACD 1D\n"
        "   • Exit: MACD 3D\n\n"
        "2️⃣ <b>MOMENTUM</b> ⚡\n"
        "   • Filter: EMA 200 1H\n"
        "   • Entry: 3★ to 5★\n"
        "   • Bonus: ST Context aligné\n"
        "   • TP Partiel: MACD 1D\n"
        "   • Exit: Bias 1D\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Binance Assets:</b>\n{', '.join([s.split('/')[0] for s in binance_symbols])}\n\n"
        "✅ <b>Ready for TradingView</b>\n"
        f"⏰ {now}"
    )
    send_telegram(msg)

# ============================================================================ #
# UTILITAIRES
# ============================================================================ #

def format_tv_symbol(s):
    """Convertit le symbole TradingView en format unifié."""
    if ':' in s: 
        s = s.split(':')[-1]
    
    # Gérer les formats sans /
    for q in ['USDT', 'USDC', 'BUSD']:
        if s.endswith(q) and '/' not in s:
            return f"{s.replace(q, '')}/{q}"
    
    return s

def get_exchange_for_symbol(symbol):
    """Retourne l'exchange configuré pour un symbole."""
    exchange_name = CONFIG['SYMBOLS'].get(symbol)
    
    if not exchange_name:
        logger.warning(f"⚠️ Symbole {symbol} non configuré")
        return None
    
    exchange = exchanges.get(exchange_name)
    
    if not exchange:
        logger.error(f"❌ Exchange {exchange_name} non initialisé")
        return None
    
    return exchange

def should_send(symbol, key):
    now = time.time()
    k = f"{symbol}:{key}"
    if k not in LAST_SIGNALS or (now - LAST_SIGNALS[k] > CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']):
        LAST_SIGNALS[k] = now
        return True
    return False

# ============================================================================ #
# WEBHOOK HANDLER
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

    logger.info(f"📥 Webhook: {symbol} | {strat} | {tf} | {alert_type} | {val}")

    # Vérifier si le symbole est dans la watchlist
    if symbol not in CONFIG['SYMBOLS']:
        logger.info(f"⏭️ Symbole {symbol} non surveillé")
        return jsonify({'status': 'ignored', 'reason': 'not_in_watchlist'}), 200
    
    # Obtenir l'exchange pour ce symbole
    exchange_name = CONFIG['SYMBOLS'][symbol]
    logger.info(f"📊 {symbol} → Exchange: {exchange_name.upper()}")

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
    if symbol not in MOMENTUM_STATE:
        MOMENTUM_STATE[symbol] = {
            'bias_1d': None,
            'macd_4h': None,
            'ema200_1h': None,
            'st_1h': None,
            'macd_1d': None,
            'st_context_4h': None,
            'st_context_1h': None,
            'bias_4h': None
        }

    # ========================================================================
    # LOGIQUE SAFE
    # ========================================================================
    if strat in ['safe', 'both', 'all']:
        s = SAFE_STATE[symbol]
        
        # Mise à jour états
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
            logger.info(f"[SAFE] {symbol} - Bias 4H: {val}")
        if alert_type == 'macd' and tf == '1d': 
            s['macd_1d'] = val
            logger.info(f"[SAFE] {symbol} - MACD 1D: {val}")

        # SORTIES SAFE
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
        
        if alert_type == 'macd_exit' and tf == '3d':
            direction_macd = "BULLISH" if val == 'bull' else "BEARISH"
            emoji = "🟢" if val == 'bull' else "🔴"
            
            msg = (
                f"🚪 <b>[SAFE - EXIT COMPLET]</b> {symbol}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Trigger: MACD 3D Opposite Cross\n"
                f"📈 New Direction: {direction_macd}\n"
                f"💰 Price: ${price:.4f}\n"
                f"🏦 Exchange: {exchange_name.upper()}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                f"❌ Action: EXIT ALL POSITIONS NOW"
            )
            send_telegram(msg)

        # ENTRÉES SAFE
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
            
            # ALERTE DE PRÉPARATION 3★ (sans SuperTrend 1H)
            # Trigger : quand Bias 1H s'aligne (3★ atteint)
            if stars >= 3 and alert_type == 'bias' and tf == '1h' and val == expected and should_send(symbol, f"safe_prep_{stars}*"):
                emoji = "🟡" if direction == "LONG" else "🟠"
                msg = (
                    f"{emoji} <b>[SAFE {stars}⭐ - PRÉPARATION]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <b>ATTENTION : Setup en formation</b>\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 3D: {s['bias_3d']}\n"
                    f"✅ MACD 4H: {s['macd_4h']}\n"
                    f"✅ Bias 1H: {s['bias_1h']} (vient de s'aligner)\n"
                    f"⏳ SuperTrend 1H: En attente...\n"
                    f"{'✅' if stars >= 5 else '❌'} Bias 4H: {s['bias_4h']}\n\n"
                    f"💡 <b>Préparez-vous à entrer si SuperTrend 1H confirme</b>"
                )
                send_telegram(msg)
            
            # ALERTE D'ENTRÉE (avec SuperTrend 1H)
            if stars >= 2 and alert_type == 'supertrend' and tf == '1h' and should_send(symbol, f"safe_{stars}*"):
                emoji = "🟢" if direction == "LONG" else "🔴"
                action = "ENTRÉE MAINTENANT" if stars >= 4 else "POSITION POSSIBLE"
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
                    f"✅ SuperTrend 1H: {s['st_1h']} (CONFIRMÉ)\n"
                    f"{'✅' if stars == 5 else '❌'} Bias 4H: {s['bias_4h']}"
                )
                send_telegram(msg)

    # ========================================================================
    # LOGIQUE MOMENTUM
    # ========================================================================
    if strat in ['momentum', 'both', 'all']:
        m = MOMENTUM_STATE[symbol]
        
        # Mise à jour états
        if alert_type == 'bias' and tf == '1d':
            m['bias_1d'] = val
            logger.info(f"[MOMENTUM] {symbol} - Bias 1D: {val}")
        if alert_type == 'macd' and tf == '4h':
            m['macd_4h'] = val
            logger.info(f"[MOMENTUM] {symbol} - MACD 4H: {val}")
        if alert_type == 'ema200' and tf == '1h':
            m['ema200_1h'] = float(val)
            logger.info(f"[MOMENTUM] {symbol} - EMA200 1H: {val}")
        if alert_type == 'supertrend' and tf == '1h':
            m['st_1h'] = val
            logger.info(f"[MOMENTUM] {symbol} - SuperTrend 1H: {val}")
        if alert_type == 'macd' and tf == '1d':
            m['macd_1d'] = val
            logger.info(f"[MOMENTUM] {symbol} - MACD 1D: {val}")
        if alert_type == 'st_context' and tf == '4h':
            m['st_context_4h'] = val
            logger.info(f"[MOMENTUM] {symbol} - ST Context 4H: {val}")
        if alert_type == 'st_context' and tf == '1h':
            m['st_context_1h'] = val
            logger.info(f"[MOMENTUM] {symbol} - ST Context 1H: {val}")
        if alert_type == 'bias' and tf == '4h':
            m['bias_4h'] = val
            logger.info(f"[MOMENTUM] {symbol} - Bias 4H: {val}")
        
        # SORTIES MOMENTUM
        # TP Partiel - MACD 1D croise opposé
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
        
        # Exit Complet - Bias 1D inverse
        if alert_type == 'bias' and tf == '1d':
            old_bias = m.get('bias_1d')
            if old_bias and old_bias != val:  # Changement de direction
                new_bias_direction = "BULLISH" if val == 'bull' else "BEARISH"
                emoji = "🟢" if val == 'bull' else "🔴"
                
                msg = (
                    f"🚪 <b>[MOMENTUM - EXIT COMPLET]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Trigger: Bias 1D Inversion\n"
                    f"📈 New Bias: {new_bias_direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"❌ Action: EXIT ALL POSITIONS NOW\n\n"
                    f"📋 Context:\n"
                    f"   • Old Bias 1D: {old_bias} → New: {val}\n"
                    f"   • MACD 4H: {m.get('macd_4h', 'N/A')}\n"
                    f"   • MACD 1D: {m.get('macd_1d', 'N/A')}"
                )
                send_telegram(msg)
        
        # ENTRÉES MOMENTUM
        direction = None
        if m['bias_1d'] == 'bull' and m['macd_4h'] == 'bull':
            direction = "LONG"
        elif m['bias_1d'] == 'bear' and m['macd_4h'] == 'bear':
            direction = "SHORT"
        
        if direction:
            # Filtre EMA 200 1H
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
                    ema_status = f"❌ Filtre EMA non validé (price: ${price:.2f}, EMA200: ${m['ema200_1h']:.2f})"
            
            # ALERTE DE PRÉPARATION (quand filtre EMA validé mais pas encore de SuperTrend)
            if ema_ok and alert_type == 'ema200' and tf == '1h' and should_send(symbol, f"momentum_prep"):
                emoji = "🟡" if direction == "LONG" else "🟠"
                msg = (
                    f"{emoji} <b>[MOMENTUM - PRÉPARATION]</b> {symbol}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ <b>ATTENTION : Setup en formation</b>\n"
                    f"📈 Direction: {direction}\n"
                    f"💰 Price: ${price:.4f}\n"
                    f"🏦 Exchange: {exchange_name.upper()}\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"✅ Bias 1D: {m['bias_1d']}\n"
                    f"✅ MACD 4H: {m['macd_4h']}\n"
                    f"✅ Prix vs EMA200 1H: {ema_status}\n"
                    f"⏳ SuperTrend 1H: En attente...\n\n"
                    f"💡 <b>Préparez-vous à entrer si SuperTrend 1H confirme</b>"
                )
                send_telegram(msg)
            
            # ALERTE D'ENTRÉE (quand SuperTrend 1H confirme)
            if ema_ok and alert_type == 'supertrend' and tf == '1h':
                st_expected = 'buy' if direction == "LONG" else 'sell'
                if val == st_expected and should_send(symbol, f"momentum_entry"):
                    # Calcul des étoiles avec bonus ST Context
                    stars = 3  # Base : Bias 1D + MACD 4H + EMA filter + ST 1H
                    
                    # Bonus : ST Context aligné
                    if m['st_context_4h'] == st_expected and m['st_context_1h'] == st_expected:
                        stars = 4
                        
                    # Bonus : Bias 4H aligné
                    expected_bias = 'bull' if direction == "LONG" else 'bear'
                    if m['bias_4h'] == expected_bias:
                        stars = 5
                    
                    emoji = "🟢" if direction == "LONG" else "🔴"
                    
                    # Construire le message avec les étoiles
                    st_context_status = ""
                    if m['st_context_4h'] and m['st_context_1h']:
                        if m['st_context_4h'] == st_expected and m['st_context_1h'] == st_expected:
                            st_context_status = f"✅ ST Context: 4H={m['st_context_4h']}, 1H={m['st_context_1h']} (BONUS)"
                        else:
                            st_context_status = f"⚪ ST Context: 4H={m['st_context_4h']}, 1H={m['st_context_1h']}"
                    
                    msg = (
                        f"{emoji} <b>[MOMENTUM {stars}⭐ - ENTRÉE MAINTENANT]</b> {symbol}\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Direction: {direction}\n"
                        f"💰 Price: ${price:.4f}\n"
                        f"🏦 Exchange: {exchange_name.upper()}\n"
                        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                        f"✅ Bias 1D: {m['bias_1d']}\n"
                        f"✅ MACD 4H: {m['macd_4h']}\n"
                        f"✅ Prix vs EMA200 1H: {ema_status}\n"
                        f"✅ SuperTrend 1H: {val} (CONFIRMÉ)\n"
                    )
                    
                    if st_context_status:
                        msg += f"{st_context_status}\n"
                    
                    if m['bias_4h']:
                        bias_icon = "✅" if m['bias_4h'] == expected_bias else "❌"
                        msg += f"{bias_icon} Bias 4H: {m['bias_4h']}\n"
                    
                    msg += f"\n🎯 <b>Position Size: "
                    if stars == 5:
                        msg += "70-80% de l'allocation (SETUP PARFAIT)"
                    elif stars == 4:
                        msg += "60-70% de l'allocation (BONUS ST CONTEXT)"
                    else:
                        msg += "50-60% de l'allocation"
                    msg += "</b>\n"
                    msg += f"🛑 Stop-Loss: {'Sous dernier swing low' if direction == 'LONG' else 'Au-dessus dernier swing high'}"
                    
                    send_telegram(msg)

    return jsonify({'status': 'success', 'symbol': symbol, 'exchange': exchange_name}), 200


@app.route('/health', methods=['GET'])
def health():
    okx_ok = 'okx' in exchanges
    binance_ok = 'binance' in exchanges
    
    return jsonify({
        'status': 'running',
        'timestamp': datetime.now().isoformat(),
        'symbols_total': len(CONFIG['SYMBOLS']),
        'okx_symbols': sum(1 for ex in CONFIG['SYMBOLS'].values() if ex == 'okx'),
        'binance_symbols': sum(1 for ex in CONFIG['SYMBOLS'].values() if ex == 'binance'),
        'exchanges': {
            'okx': '✅' if okx_ok else '❌',
            'binance': '✅' if binance_ok else '❌'
        }
    }), 200


@app.route('/state', methods=['GET'])
def state():
    return jsonify({
        'safe_state': SAFE_STATE,
        'momentum_state': MOMENTUM_STATE,
        'watchlist': CONFIG['SYMBOLS']
    }), 200


if __name__ == '__main__':
    logger.info("🚀 Démarrage du bot multi-exchange...")
    
    # Initialiser les exchanges
    init_exchanges()
    
    # Envoyer notification de démarrage
    send_start_notification()
    
    logger.info(f"✅ Bot démarré sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)
