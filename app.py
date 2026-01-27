#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified Trading Monitor Bot - Multi-Strategies (26 janvier 2026)
"""

import ccxt
import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime, timezone
import logging
from flask import Flask, request, jsonify
import threading
from typing import Optional, Dict, Tuple
import signal
import sys

# ============================================================================ #
# CONFIGURATION
# ============================================================================ #

CONFIG = {
    'EXCHANGE': 'okx',
    'API_KEY': '',
    'SECRET': '',
    
    'TELEGRAM_BOT_TOKEN': '8110041550:AAHJKAWxIG1ZBjZ8fRfFMKq-4iTeo5v4-Hw',
    'TELEGRAM_CHAT_ID': '6473214015',
    
    'SYMBOLS': [
        'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'CRV/USDT',
        'PEPE/USDT', 'DOGE/USDT', 'WIF/USDT', 'BONK/USDT', 'CVX/USDT'
    ],
    
    'TF_3D': '3d',
    'EMA_3D': 13,
    'SMA_3D': 30,
    
    'TF_4H': '4h',
    'MACD_4H_FAST': 13,
    'MACD_4H_SLOW': 34,
    'MACD_4H_SIGNAL': 8,
    'EMA_4H_9': 9,
    'SMA_4H_26': 26,
    
    'TF_1H': '1h',
    'EMA_1H': 13,
    'SMA_1H': 34,
    
    'CHECK_INTERVAL': 300,
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,
    'DATA_LIMIT': 300,
    'RETRY_DELAY': 12,
    'MAX_RETRIES': 4,
    
    'WEBHOOK_PORT': 5000,
    'WEBHOOK_HOST': '0.0.0.0',
}

# ============================================================================ #
# ETAT GLOBAL
# ============================================================================ #

LAST_SIGNALS: Dict[str, Dict] = {}
exchange: Optional[ccxt.okx] = None
AGGRESSIVE_STATE: Dict[str, Dict] = {}
SAFE_STATE: Dict[str, Dict] = {}
shutdown_flag = threading.Event()

# ============================================================================ #
# LOGGING
# ============================================================================ #

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================ #
# FLASK APP
# ============================================================================ #

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot trading OK - Multi-strategies monitoring actif"

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# ============================================================================ #
# UTILITAIRES
# ============================================================================ #

def format_tradingview_symbol(tv_symbol: str) -> str:
    if ':' in tv_symbol:
        tv_symbol = tv_symbol.split(':')[-1]
    
    if '/' in tv_symbol:
        return tv_symbol
    
    for quote in ['USDT', 'USDC', 'BUSD']:
        if tv_symbol.endswith(quote):
            base = tv_symbol.replace(quote, '')
            return f"{base}/{quote}"
    
    return tv_symbol

def get_signal_key(symbol: str, strategy: str, signal_type: str = "") -> str:
    if signal_type:
        return f"{symbol}:{strategy}:{signal_type}"
    return f"{symbol}:{strategy}"

def should_send_alert(symbol: str, signal_identifier: str, strategy: str) -> bool:
    now = time.time()
    key = get_signal_key(symbol, strategy, signal_identifier)
    
    if key not in LAST_SIGNALS:
        LAST_SIGNALS[key] = {'timestamp': 0}
        return True
    
    return now - LAST_SIGNALS[key]['timestamp'] >= CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']

def update_last_signal(symbol: str, signal_identifier: str, strategy: str):
    key = get_signal_key(symbol, strategy, signal_identifier)
    LAST_SIGNALS[key] = {'timestamp': time.time()}

# ============================================================================ #
# INITIALISATION EXCHANGE
# ============================================================================ #

def init_exchange():
    global exchange
    if exchange is None:
        exchange = ccxt.okx({
            'apiKey': CONFIG['API_KEY'],
            'secret': CONFIG['SECRET'],
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        logger.info("Exchange OKX initialise")
    return exchange

def get_exchange():
    return exchange or init_exchange()

# ============================================================================ #
# TELEGRAM - ALERTES AMÉLIORÉES
# ============================================================================ #

def send_telegram(url, payload):
    r = requests.post(url, json=payload, timeout=15)
    r.raise_for_status()

def send_telegram_safe(symbol: str, signal_type: str, stars: int, price: float, state: Dict, source="Webhook"):
    """
    Alerte SAFE avec affichage complet des conditions
    """
    emoji = "🟢" if signal_type == 'LONG' else "🔴"
    stars_emoji = "⭐" * stars
    
    msg = f"{emoji} [{signal_type} {stars_emoji}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATÉGIE: SAFE\n"
    msg += f"💰 Prix: ${price:.4f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    
    msg += f"📋 CONDITIONS ({stars}★):\n\n"
    
    # Récupérer l'état complet
    bias_3d = state.get('bias_3d')
    macd_4h = state.get('macd_4h')
    bias_4h = state.get('bias_4h_9_26')
    bias_1h = state.get('bias_1h')
    st_1h = state.get('st_1h')
    st_context_1h = state.get('st_context_1h')
    
    # Déterminer la direction attendue
    expected = 'bull' if signal_type == 'LONG' else 'bear'
    expected_st = 'buy' if signal_type == 'LONG' else 'sell'
    
    # Afficher les conditions selon les étoiles
    # Toujours afficher Biais 3D et MACD 4H
    check_3d = "✅" if bias_3d == expected else "❌"
    check_macd = "✅" if macd_4h == expected else "❌"
    
    msg += f"{check_3d} Biais 3D: {bias_3d or 'N/A'}\n"
    msg += f"{check_macd} MACD 4H: {macd_4h or 'N/A'}\n"
    
    # Si 3★ ou plus, afficher Biais 1H
    if stars >= 3:
        check_1h = "✅" if bias_1h == expected else "❌"
        msg += f"{check_1h} Biais 1H: {bias_1h or 'N/A'}\n"
    
    # Si 4★ ou plus, afficher SuperTrend AI 1H
    if stars >= 4:
        check_st = "✅" if st_1h == expected_st else "❌"
        msg += f"{check_st} SuperTrend AI 1H: {st_1h or 'N/A'}\n"
    
    # Si 5★, afficher Biais 4H (9/26)
    if stars == 5:
        check_4h = "✅" if bias_4h == expected else "❌"
        msg += f"{check_4h} Biais 4H (9/26): {bias_4h or 'N/A'}\n"
    
    # Afficher ST Context 1H si présent dans l'alerte
    if st_context_1h and stars <= 4:
        check_context = "✅" if st_context_1h == expected_st else "❌"
        msg += f"{check_context} ST Context 1H: {st_context_1h or 'N/A'}\n"
    
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    
    if stars == 4 or stars == 5:
        msg += "✅ SIGNAL D'ENTRÉE VALIDÉ\n"
        msg += "⚠️ Vérifie SuperTrend AI 20min avant d'entrer\n"
    elif stars == 3:
        msg += "🔔 Attente SuperTrend AI 1H pour 4★\n"
    elif stars == 2:
        msg += "🔔 Préparation - Attente alignement 1H\n"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    logger.info(f"Alerte Safe {signal_type} {stars}★ envoyée pour {symbol}")

def send_telegram_safe_exit(symbol: str, exit_type: str, price: float):
    msg = f"🚪 [SORTIE {exit_type}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATÉGIE: SAFE\n"
    msg += f"💰 Prix: ${price:.4f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"❌ RAISON: Croisement MACD opposé en 3D\n\n"
    msg += "⚠️ SORTEZ LA POSITION SI EN TRADE"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    logger.info(f"Alerte sortie Safe envoyée pour {symbol}")

def send_telegram_aggressive_preparation(symbol: str, zone_type: str, price: float, state: Dict):
    """
    Alerte AGGRESSIVE - Préparation avec affichage complet des conditions
    """
    emoji = "🟢" if zone_type == 'buy' else "🔴"
    direction = "LONG" if zone_type == 'buy' else "SHORT"
    
    msg = f"{emoji} [PREPARATION {direction}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATÉGIE: AGGRESSIVE\n"
    msg += f"💰 Prix: ${price:.2f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    
    msg += f"📋 CONDITIONS:\n\n"
    
    # Récupérer l'état complet
    zone_4h = state.get('zone_4h')
    long_term_4h = state.get('long_term_4h')
    zone_1h = state.get('zone_1h')
    st_1h = state.get('st_1h')
    
    # Zone 4H
    check_zone_4h = "✅" if zone_4h == zone_type else "❌"
    msg += f"{check_zone_4h} Zone ST Context 4H: {zone_4h or 'N/A'}\n"
    
    # Long Term 4H
    if long_term_4h is not None:
        in_range = -2 <= long_term_4h <= 2
        check_lt = "✅" if in_range else "❌"
        msg += f"{check_lt} Long Term 4H: {long_term_4h:.2f} {'(OK)' if in_range else '(Hors range)'}\n"
    else:
        msg += f"❌ Long Term 4H: N/A\n"
    
    # Zone 1H
    check_zone_1h = "✅" if zone_1h == zone_type else "❌"
    msg += f"{check_zone_1h} Zone ST Context 1H: {zone_1h or 'En attente'}\n"
    
    # SuperTrend AI 1H
    expected_st = zone_type  # 'buy' ou 'sell'
    check_st = "✅" if st_1h == expected_st else "❌"
    msg += f"{check_st} SuperTrend AI 1H: {st_1h or 'En attente'}\n"
    
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    
    # Compter les conditions validées
    validated = sum([
        zone_4h == zone_type,
        long_term_4h is not None and -2 <= long_term_4h <= 2,
        zone_1h == zone_type,
        st_1h == expected_st
    ])
    
    if validated == 4:
        msg += "✅ TOUTES CONDITIONS VALIDÉES - SIGNAL IMMINENT\n"
    elif validated >= 2:
        msg += f"🔔 {validated}/4 conditions validées\n"
        msg += "⏳ Attente des conditions restantes...\n"
    else:
        msg += "🔔 Préparation en cours...\n"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    logger.info(f"Alerte préparation Aggressive envoyée pour {symbol}")

def send_telegram_aggressive_entry(symbol: str, signal_type: str, price: float, state: Dict):
    """
    Alerte AGGRESSIVE - Entrée avec affichage complet des conditions
    """
    emoji = "🟢" if signal_type == 'LONG' else "🔴"
    
    msg = f"{emoji} [SIGNAL {signal_type}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATÉGIE: AGGRESSIVE\n"
    msg += f"💰 Prix: ${price:.2f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    
    msg += f"📋 CONDITIONS (TOUTES VALIDÉES):\n\n"
    
    # Récupérer l'état complet
    zone_4h = state.get('zone_4h')
    long_term_4h = state.get('long_term_4h')
    zone_1h = state.get('zone_1h')
    st_1h = state.get('st_1h')
    
    msg += f"✅ Zone ST Context 4H: {zone_4h}\n"
    msg += f"✅ Long Term 4H: {long_term_4h:.2f}\n"
    msg += f"✅ Zone ST Context 1H: {zone_1h}\n"
    msg += f"✅ SuperTrend AI 1H: {st_1h}\n"
    
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += "✅ SIGNAL D'ENTRÉE VALIDÉ\n"
    msg += "⚠️ Vérifie SuperTrend AI 20min avant d'entrer\n"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    logger.info(f"Alerte entrée Aggressive envoyée pour {symbol}")

def send_telegram_aggressive_exit(symbol: str, exit_type: str, price: float):
    msg = f"🚪 [SORTIE {exit_type}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATÉGIE: AGGRESSIVE\n"
    msg += f"💰 Prix: ${price:.2f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"❌ RAISON: Biais 4H croisé direction opposée\n\n"
    msg += "⚠️ SORTEZ LA POSITION SI EN TRADE"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    logger.info(f"Alerte sortie Aggressive envoyée pour {symbol}")

# ============================================================================ #
# WEBHOOK
# ============================================================================ #

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({'status': 'error', 'message': 'JSON manquant'}), 400
        
        logger.info(f"Webhook reçu: {data}")
        
        required_fields = ['symbol', 'strategy', 'tf', 'type', 'price']
        for field in required_fields:
            if field not in data:
                return jsonify({'status': 'error', 'message': f'Champ {field} manquant'}), 400
        
        symbol_raw = data['symbol']
        strategy = data['strategy'].lower()
        tf = data['tf'].lower()
        alert_type = data['type'].lower()
        price = float(data['price'])
        
        symbol = format_tradingview_symbol(symbol_raw)
        
        if symbol not in CONFIG['SYMBOLS']:
            return jsonify({'status': 'ignored', 'message': 'Symbole non surveillé'}), 200
        
        strategies_to_process = ['safe', 'aggressive'] if strategy == 'both' else [strategy]
        
        for current_strategy in strategies_to_process:
            
            # ================================================================
            # STRATEGIE SAFE
            # ================================================================
            if current_strategy == 'safe':
                if symbol not in SAFE_STATE:
                    SAFE_STATE[symbol] = {
                        'bias_3d': None,
                        'macd_4h': None,
                        'bias_4h_9_26': None,
                        'bias_1h': None,
                        'st_1h': None,
                        'st_context_1h': None
                    }
                
                state = SAFE_STATE[symbol]
                
                if alert_type == 'bias' and tf == '3d':
                    state['bias_3d'] = data.get('value', '').lower()
                elif alert_type == 'macd' and tf == '4h':
                    state['macd_4h'] = data.get('value', '').lower()
                elif alert_type == 'bias_9_26' and tf == '4h':
                    state['bias_4h_9_26'] = data.get('value', '').lower()
                elif alert_type == 'bias' and tf == '1h':
                    state['bias_1h'] = data.get('value', '').lower()
                elif alert_type == 'supertrend' and tf == '1h':
                    state['st_1h'] = data.get('value', '').lower()
                elif alert_type == 'st_context' and tf == '1h':
                    state['st_context_1h'] = data.get('value', '').lower()
                elif alert_type == 'macd_exit' and tf == '3d':
                    value = data.get('value', '').lower()
                    exit_type = 'LONG' if value == 'bear' else 'SHORT'
                    send_telegram_safe_exit(symbol, exit_type, price)
                    continue
                
                bias_3d = state.get('bias_3d')
                macd_4h = state.get('macd_4h')
                bias_4h = state.get('bias_4h_9_26')
                bias_1h = state.get('bias_1h')
                st_1h = state.get('st_1h')
                st_context_1h = state.get('st_context_1h')
                
                direction = None
                if bias_3d == 'bull' and macd_4h == 'bull':
                    direction = 'LONG'
                elif bias_3d == 'bear' and macd_4h == 'bear':
                    direction = 'SHORT'
                
                if direction:
                    if should_send_alert(symbol, f"2stars_{direction}", 'safe'):
                        send_telegram_safe(symbol, direction, 2, price, state, "Webhook TradingView")
                        update_last_signal(symbol, f"2stars_{direction}", 'safe')
                    
                    if bias_1h == bias_3d:
                        if should_send_alert(symbol, f"3stars_{direction}", 'safe'):
                            send_telegram_safe(symbol, direction, 3, price, state, "Webhook TradingView")
                            update_last_signal(symbol, f"3stars_{direction}", 'safe')
                        
                        st_match = (st_1h == 'buy' and direction == 'LONG') or (st_1h == 'sell' and direction == 'SHORT')
                        if st_match:
                            if should_send_alert(symbol, f"4stars_{direction}", 'safe'):
                                send_telegram_safe(symbol, direction, 4, price, state, "Webhook TradingView")
                                update_last_signal(symbol, f"4stars_{direction}", 'safe')
                            
                            if bias_4h == bias_3d:
                                if should_send_alert(symbol, f"5stars_{direction}", 'safe'):
                                    send_telegram_safe(symbol, direction, 5, price, state, "Webhook TradingView")
                                    update_last_signal(symbol, f"5stars_{direction}", 'safe')
                    
                    if st_context_1h:
                        context_match = (st_context_1h == 'buy' and direction == 'LONG') or (st_context_1h == 'sell' and direction == 'SHORT')
                        if context_match:
                            if should_send_alert(symbol, f"2stars_context_{direction}", 'safe'):
                                send_telegram_safe(symbol, direction, 2, price, state, "Webhook TradingView")
                                update_last_signal(symbol, f"2stars_context_{direction}", 'safe')
                            
                            st_match = (st_1h == 'buy' and direction == 'LONG') or (st_1h == 'sell' and direction == 'SHORT')
                            if st_match:
                                if should_send_alert(symbol, f"3stars_context_{direction}", 'safe'):
                                    send_telegram_safe(symbol, direction, 3, price, state, "Webhook TradingView")
                                    update_last_signal(symbol, f"3stars_context_{direction}", 'safe')
                            
                            if bias_1h == bias_3d:
                                if should_send_alert(symbol, f"4stars_full_{direction}", 'safe'):
                                    send_telegram_safe(symbol, direction, 4, price, state, "Webhook TradingView")
                                    update_last_signal(symbol, f"4stars_full_{direction}", 'safe')
            
            # ================================================================
            # STRATEGIE AGGRESSIVE
            # ================================================================
            elif current_strategy == 'aggressive':
                if symbol not in AGGRESSIVE_STATE:
                    AGGRESSIVE_STATE[symbol] = {
                        'zone_4h': None,
                        'short_term_4h': None,
                        'long_term_4h': None,
                        'zone_1h': None,
                        'short_term_1h': None,
                        'st_1h': None,
                        'timestamp_4h': 0,
                        'timestamp_1h': 0
                    }
                
                state = AGGRESSIVE_STATE[symbol]
                
                if alert_type == 'st_context' and tf == '4h':
                    value = data.get('value', '').lower()
                    short_term = float(data.get('short_term', 0))
                    long_term = float(data.get('long_term', 0))
                    
                    # Récupérer l'état précédent
                    prev_short_term = state.get('short_term_4h')
                    prev_zone = state.get('zone_4h')
                    
                    # Mise à jour de l'état
                    state['short_term_4h'] = short_term
                    state['long_term_4h'] = long_term
                    state['timestamp_4h'] = time.time()
                    
                    # DÉTECTION DES SORTIES (avant de traiter les zones)
                    # Sortie LONG : on était en zone BUY et short_term repasse > -2
                    if prev_zone == 'buy' and prev_short_term is not None and prev_short_term < -2 and short_term > -2:
                        logger.info(f"[AGGRESSIVE] 🚪 SORTIE LONG détectée pour {symbol} (short_term {prev_short_term:.2f} -> {short_term:.2f})")
                        send_telegram_aggressive_exit(symbol, 'LONG', price)
                        state['zone_4h'] = None
                        state['zone_1h'] = None
                        continue
                    
                    # Sortie SHORT : on était en zone SELL et short_term repasse < 2
                    if prev_zone == 'sell' and prev_short_term is not None and prev_short_term > 2 and short_term < 2:
                        logger.info(f"[AGGRESSIVE] 🚪 SORTIE SHORT détectée pour {symbol} (short_term {prev_short_term:.2f} -> {short_term:.2f})")
                        send_telegram_aggressive_exit(symbol, 'SHORT', price)
                        state['zone_4h'] = None
                        state['zone_1h'] = None
                        continue
                    
                    # DÉTECTION DES ZONES (entrées/activations)
                    zone = None
                    if value == 'buy' and short_term < -2 and long_term >= -2:
                        zone = 'buy'
                    elif value == 'sell' and short_term > 2 and long_term <= 2:
                        zone = 'sell'
                    
                    # VÉRIFICATION INVALIDATION AUTOMATIQUE
                    if state['zone_4h'] == 'buy' and long_term < -2:
                        logger.info(f"[AGGRESSIVE] ❌ Zone BUY 4H invalidée pour {symbol} (long_term={long_term:.2f} < -2)")
                        state['zone_4h'] = None
                    elif state['zone_4h'] == 'sell' and long_term > 2:
                        logger.info(f"[AGGRESSIVE] ❌ Zone SELL 4H invalidée pour {symbol} (long_term={long_term:.2f} > 2)")
                        state['zone_4h'] = None
                    else:
                        state['zone_4h'] = zone
                    
                    logger.info(f"[AGGRESSIVE] ST Context 4H mis à jour pour {symbol}: zone={zone}, short_term={short_term:.2f}, long_term={long_term:.2f}")
                    
                    # Envoyer alerte de préparation si zone valide et nouvelle
                    if zone and state['zone_4h'] == zone and prev_zone != zone:
                        if should_send_alert(symbol, f"prep_4h_{zone}", 'aggressive'):
                            send_telegram_aggressive_preparation(symbol, zone, price, state)
                            update_last_signal(symbol, f"prep_4h_{zone}", 'aggressive')
                    
                    continue
                
                elif alert_type == 'st_context_invalid' and tf == '4h':
                    value = data.get('value', '').lower()
                    long_term = float(data.get('long_term', 0))
                    
                    if value == 'buy' and long_term < -2:
                        logger.info(f"[AGGRESSIVE] ❌ Zone BUY 4H invalidée pour {symbol} (long_term={long_term:.2f} < -2)")
                        state['zone_4h'] = None
                        state['long_term_4h'] = long_term
                    elif value == 'sell' and long_term > 2:
                        logger.info(f"[AGGRESSIVE] ❌ Zone SELL 4H invalidée pour {symbol} (long_term={long_term:.2f} > 2)")
                        state['zone_4h'] = None
                        state['long_term_4h'] = long_term
                    
                    continue
                
                elif alert_type == 'st_context' and tf == '1h':
                    value = data.get('value', '').lower()
                    short_term = float(data.get('short_term', 0))
                    
                    state['short_term_1h'] = short_term
                    state['timestamp_1h'] = time.time()
                    
                    zone = None
                    if value == 'buy' and short_term < -2:
                        zone = 'buy'
                    elif value == 'sell' and short_term > 2:
                        zone = 'sell'
                    
                    state['zone_1h'] = zone
                    
                    logger.info(f"[AGGRESSIVE] ST Context 1H mis à jour pour {symbol}: zone={zone}, short_term={short_term:.2f}")
                    
                    continue
                
                elif alert_type == 'supertrend' and tf == '1h':
                    value = data.get('value', '').lower()
                    state['st_1h'] = value
                    
                    logger.info(f"[AGGRESSIVE] SuperTrend AI 1H mis à jour pour {symbol}: {value}")
                    
                    # Vérifier alignement pour signal d'entrée
                    zone_4h = state.get('zone_4h')
                    zone_1h = state.get('zone_1h')
                    
                    if not zone_4h or not zone_1h:
                        logger.debug(f"[AGGRESSIVE] Zones manquantes pour {symbol}: 4H={zone_4h}, 1H={zone_1h}")
                        continue
                    
                    # Les deux zones doivent être alignées
                    if zone_4h != zone_1h:
                        logger.info(f"[AGGRESSIVE] Zones non alignées pour {symbol}: 4H={zone_4h}, 1H={zone_1h}")
                        continue
                    
                    # Vérifier si SuperTrend AI correspond
                    signal_type = None
                    if value == 'buy' and zone_4h == 'buy' and zone_1h == 'buy':
                        signal_type = 'LONG'
                    elif value == 'sell' and zone_4h == 'sell' and zone_1h == 'sell':
                        signal_type = 'SHORT'
                    
                    if signal_type:
                        if should_send_alert(symbol, f"entry_{signal_type}", 'aggressive'):
                            send_telegram_aggressive_entry(symbol, signal_type, price, state)
                            update_last_signal(symbol, f"entry_{signal_type}", 'aggressive')
                    
                    continue
        
        return jsonify({'status': 'success', 'strategies_processed': strategies_to_process}), 200
    
    except Exception as e:
        logger.error(f"Erreur webhook: {type(e).__name__} - {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'Erreur serveur'}), 500

# ============================================================================ #
# ETAT / HEALTH
# ============================================================================ #

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'running',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'symbols_monitored': len(CONFIG['SYMBOLS']),
        'safe_active_signals': len([k for k in LAST_SIGNALS if ':safe:' in k]),
        'aggressive_active_signals': len([k for k in LAST_SIGNALS if ':aggressive:' in k]),
        'safe_states': len(SAFE_STATE),
        'aggressive_states': len(AGGRESSIVE_STATE)
    }), 200

@app.route('/state', methods=['GET'])
def get_state():
    return jsonify({
        'safe_state': SAFE_STATE,
        'aggressive_state': {
            symbol: {
                'zone_4h': data.get('zone_4h'),
                'long_term_4h': data.get('long_term_4h'),
                'zone_1h': data.get('zone_1h'),
                'st_1h': data.get('st_1h'),
                'age_4h_seconds': time.time() - data.get('timestamp_4h', 0),
                'age_1h_seconds': time.time() - data.get('timestamp_1h', 0)
            }
            for symbol, data in AGGRESSIVE_STATE.items()
        }
    }), 200

@app.route('/state/<symbol>', methods=['GET'])
def get_symbol_state(symbol):
    symbol_formatted = symbol.replace('-', '/')
    
    if symbol_formatted not in CONFIG['SYMBOLS']:
        return jsonify({'error': 'Symbole non surveillé'}), 404
    
    return jsonify({
        'symbol': symbol_formatted,
        'safe': SAFE_STATE.get(symbol_formatted, 'No data'),
        'aggressive': AGGRESSIVE_STATE.get(symbol_formatted, 'No data')
    }), 200

# ============================================================================ #
# ARRET PROPRE
# ============================================================================ #

def signal_handler(signum, frame):
    logger.info("Signal d'arrêt reçu")
    shutdown_flag.set()
    try:
        send_telegram(
            f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
            {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': "[BOT ARRETE]\n\nArrêt manuel par utilisateur"}
        )
    except:
        pass
    sys.exit(0)

# ============================================================================ #
# MAIN
# ============================================================================ #

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("="*60)
    logger.info("Unified Trading Monitor Bot - Démarrage")
    logger.info("="*60)
    
    if not CONFIG['TELEGRAM_BOT_TOKEN']:
        logger.error("TELEGRAM_BOT_TOKEN non configuré")
        sys.exit(1)
    
    if not CONFIG['TELEGRAM_CHAT_ID']:
        logger.error("TELEGRAM_CHAT_ID non configuré")
        sys.exit(1)
    
    try:
        start_msg = (
            "🤖 [BOT DEMARRE]\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "📊 Mode: Webhook uniquement\n"
            f"🎯 Surveillance: {len(CONFIG['SYMBOLS'])} actifs\n\n"
            "📋 STRATÉGIES ACTIVES:\n\n"
            "1️⃣ SAFE (Conservatrice)\n"
            "   • Système 2★ à 5★\n"
            "   • Sortie: MACD 3D opposé\n\n"
            "2️⃣ AGGRESSIVE (Rapide)\n"
            "   • ST Context 4H + 1H\n"
            "   • SuperTrend AI 1H\n"
            "   • Sortie: Biais 4H opposé\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "✅ Prêt à recevoir les webhooks TradingView"
        )
        
        send_telegram(
            f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
            {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': start_msg}
        )
        logger.info("Message de démarrage envoyé")
    except Exception as e:
        logger.warning(f"Impossible d'envoyer message de démarrage: {e}")
    
    try:
        logger.info(f"Démarrage serveur webhook sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
        app.run(
            host=CONFIG['WEBHOOK_HOST'],
            port=CONFIG['WEBHOOK_PORT'],
            debug=False,
            use_reloader=False
        )
    except Exception as e:
        logger.error(f"Erreur serveur Flask: {e}")
        shutdown_flag.set()
        sys.exit(1)