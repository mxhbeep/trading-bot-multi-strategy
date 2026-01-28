#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified Trading Monitor Bot - Multi-Strategies (Enhanced Version)
Features: JSON Logging, CSV Export, Web Dashboard, Weekly Reports
"""

import ccxt
import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime, timezone, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
from flask import Flask, request, jsonify, render_template_string
import threading
from typing import Optional, Dict, Tuple
import signal
import sys
import json
import csv
import uuid
import os

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
# LOGGING SETUP
# ============================================================================ #

os.makedirs('logs', exist_ok=True)

# System logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# JSON alerts logger
alerts_logger = logging.getLogger('alerts')
alerts_logger.setLevel(logging.INFO)

alerts_handler = TimedRotatingFileHandler(
    'logs/alerts.jsonl',
    when='midnight',
    interval=1,
    backupCount=30,
    encoding='utf-8'
)
alerts_handler.setFormatter(logging.Formatter('%(message)s'))
alerts_logger.addHandler(alerts_handler)
alerts_logger.propagate = False

# ============================================================================ #
# CSV EXPORT
# ============================================================================ #

CSV_FILE = 'logs/alerts.csv'
CSV_HEADERS = ['timestamp', 'request_id', 'alert_type', 'strategy', 'symbol', 
               'direction', 'price', 'stars', 'conditions']

if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)

def export_to_csv(entry: Dict):
    """Export alert to CSV"""
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                entry.get('timestamp'),
                entry.get('request_id'),
                entry.get('alert_type'),
                entry.get('strategy'),
                entry.get('symbol'),
                entry.get('direction'),
                entry.get('price'),
                entry.get('stars'),
                json.dumps(entry.get('conditions'), ensure_ascii=False)
            ])
    except Exception as e:
        logger.error(f"CSV export error: {e}")

# ============================================================================ #
# FLASK APP
# ============================================================================ #

app = Flask(__name__)

@app.route('/')
def home():
    return "Trading Bot OK - Multi-strategies monitoring active"

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
        logger.info("Exchange OKX initialized")
    return exchange

def get_exchange():
    return exchange or init_exchange()

# ============================================================================ #
# TELEGRAM & LOGGING
# ============================================================================ #

def send_telegram(url, payload):
    r = requests.post(url, json=payload, timeout=15)
    r.raise_for_status()

def log_alert(alert_type: str, strategy: str, symbol: str, direction: str, 
              price: float, conditions: Dict, stars: int = None, request_id: str = None):
    """Log alert to JSON and CSV"""
    entry = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'request_id': request_id,
        'alert_type': alert_type,
        'strategy': strategy.upper(),
        'symbol': symbol,
        'direction': direction,
        'price': round(price, 6),
        'stars': stars,
        'conditions': conditions
    }
    
    # JSON log
    alerts_logger.info(json.dumps(entry, ensure_ascii=False))
    
    # CSV export
    export_to_csv(entry)

def send_telegram_safe(symbol: str, signal_type: str, stars: int, price: float, 
                       state: Dict, source="Webhook", request_id: str = None):
    """Safe strategy alert with full conditions display"""
    emoji = "🟢" if signal_type == 'LONG' else "🔴"
    stars_emoji = "⭐" * stars
    
    msg = f"{emoji} [{signal_type} {stars_emoji}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATEGY: SAFE\n"
    msg += f"💰 Price: ${price:.4f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"📋 CONDITIONS ({stars}★):\n\n"
    
    bias_3d = state.get('bias_3d')
    macd_4h = state.get('macd_4h')
    bias_4h = state.get('bias_4h_9_26')
    bias_1h = state.get('bias_1h')
    st_1h = state.get('st_1h')
    st_context_1h = state.get('st_context_1h')
    
    expected = 'bull' if signal_type == 'LONG' else 'bear'
    expected_st = 'buy' if signal_type == 'LONG' else 'sell'
    
    check_3d = "✅" if bias_3d == expected else "❌"
    check_macd = "✅" if macd_4h == expected else "❌"
    
    msg += f"{check_3d} Bias 3D: {bias_3d or 'N/A'}\n"
    msg += f"{check_macd} MACD 4H: {macd_4h or 'N/A'}\n"
    
    if stars >= 3:
        check_1h = "✅" if bias_1h == expected else "❌"
        msg += f"{check_1h} Bias 1H: {bias_1h or 'N/A'}\n"
    
    if stars >= 4:
        check_st = "✅" if st_1h == expected_st else "❌"
        msg += f"{check_st} SuperTrend AI 1H: {st_1h or 'N/A'}\n"
    
    if stars == 5:
        check_4h = "✅" if bias_4h == expected else "❌"
        msg += f"{check_4h} Bias 4H (9/26): {bias_4h or 'N/A'}\n"
    
    if st_context_1h and stars <= 4:
        check_context = "✅" if st_context_1h == expected_st else "❌"
        msg += f"{check_context} ST Context 1H: {st_context_1h or 'N/A'}\n"
    
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    
    if stars == 4 or stars == 5:
        msg += "✅ ENTRY SIGNAL VALIDATED\n"
        msg += "⚠️ Check SuperTrend AI 20min before entry\n"
    elif stars == 3:
        msg += "🔔 Waiting SuperTrend AI 1H for 4★\n"
    elif stars == 2:
        msg += "🔔 Preparation - Waiting 1H alignment\n"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    
    conditions = {
        'bias_3d': bias_3d,
        'macd_4h': macd_4h,
        'bias_1h': bias_1h,
        'st_1h': st_1h,
        'bias_4h': bias_4h,
        'st_context_1h': st_context_1h
    }
    log_alert('SIGNAL', 'SAFE', symbol, signal_type, price, conditions, stars, request_id)
    
    logger.info(f"Safe alert {signal_type} {stars}★ sent for {symbol}")

def send_telegram_safe_exit(symbol: str, exit_type: str, price: float, request_id: str = None):
    msg = f"🚪 [EXIT {exit_type}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATEGY: SAFE\n"
    msg += f"💰 Price: ${price:.4f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"❌ REASON: Opposite MACD cross on 3D\n\n"
    msg += "⚠️ EXIT POSITION IF IN TRADE"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    
    log_alert('EXIT', 'SAFE', symbol, exit_type, price, {'reason': 'MACD 3D opposite'}, None, request_id)
    logger.info(f"Safe exit alert sent for {symbol}")

def send_telegram_aggressive_preparation(symbol: str, zone_type: str, price: float, 
                                        state: Dict, request_id: str = None):
    emoji = "🟢" if zone_type == 'buy' else "🔴"
    direction = "LONG" if zone_type == 'buy' else "SHORT"
    
    msg = f"{emoji} [PREPARATION {direction}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATEGY: AGGRESSIVE\n"
    msg += f"💰 Price: ${price:.2f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"📋 CONDITIONS:\n\n"
    
    zone_4h = state.get('zone_4h')
    long_term_4h = state.get('long_term_4h')
    zone_1h = state.get('zone_1h')
    st_1h = state.get('st_1h')
    
    check_zone_4h = "✅" if zone_4h == zone_type else "❌"
    msg += f"{check_zone_4h} ST Context Zone 4H: {zone_4h or 'N/A'}\n"
    
    if long_term_4h is not None:
        in_range = -2 <= long_term_4h <= 2
        check_lt = "✅" if in_range else "❌"
        msg += f"{check_lt} Long Term 4H: {long_term_4h:.2f} {'(OK)' if in_range else '(Out of range)'}\n"
    else:
        msg += f"❌ Long Term 4H: N/A\n"
    
    check_zone_1h = "✅" if zone_1h == zone_type else "❌"
    msg += f"{check_zone_1h} ST Context Zone 1H: {zone_1h or 'Waiting'}\n"
    
    expected_st = zone_type
    check_st = "✅" if st_1h == expected_st else "❌"
    msg += f"{check_st} SuperTrend AI 1H: {st_1h or 'Waiting'}\n"
    
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    
    validated = sum([
        zone_4h == zone_type,
        long_term_4h is not None and -2 <= long_term_4h <= 2,
        zone_1h == zone_type,
        st_1h == expected_st
    ])
    
    if validated == 4:
        msg += "✅ ALL CONDITIONS VALIDATED - SIGNAL IMMINENT\n"
    elif validated >= 2:
        msg += f"🔔 {validated}/4 conditions validated\n"
        msg += "⏳ Waiting for remaining conditions...\n"
    else:
        msg += "🔔 Preparation in progress...\n"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    
    conditions = {
        'zone_4h': zone_4h,
        'long_term_4h': f"{long_term_4h:.2f}" if long_term_4h is not None else None,
        'zone_1h': zone_1h,
        'st_1h': st_1h,
        'validated': f"{validated}/4"
    }
    log_alert('PREPARATION', 'AGGRESSIVE', symbol, direction, price, conditions, None, request_id)
    logger.info(f"Aggressive preparation alert sent for {symbol}")

def send_telegram_aggressive_entry(symbol: str, signal_type: str, price: float, 
                                   state: Dict, request_id: str = None):
    emoji = "🟢" if signal_type == 'LONG' else "🔴"
    
    msg = f"{emoji} [SIGNAL {signal_type}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATEGY: AGGRESSIVE\n"
    msg += f"💰 Price: ${price:.2f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"📋 CONDITIONS (ALL VALIDATED):\n\n"
    
    zone_4h = state.get('zone_4h')
    long_term_4h = state.get('long_term_4h')
    zone_1h = state.get('zone_1h')
    st_1h = state.get('st_1h')
    
    msg += f"✅ ST Context Zone 4H: {zone_4h}\n"
    msg += f"✅ Long Term 4H: {long_term_4h:.2f}\n"
    msg += f"✅ ST Context Zone 1H: {zone_1h}\n"
    msg += f"✅ SuperTrend AI 1H: {st_1h}\n"
    
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += "✅ ENTRY SIGNAL VALIDATED\n"
    msg += "⚠️ Check SuperTrend AI 20min before entry\n"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    
    conditions = {
        'zone_4h': zone_4h,
        'long_term_4h': f"{long_term_4h:.2f}" if long_term_4h is not None else None,
        'zone_1h': zone_1h,
        'st_1h': st_1h
    }
    log_alert('ENTRY', 'AGGRESSIVE', symbol, signal_type, price, conditions, None, request_id)
    logger.info(f"Aggressive entry alert sent for {symbol}")

def send_telegram_aggressive_exit(symbol: str, exit_type: str, price: float, request_id: str = None):
    msg = f"🚪 [EXIT {exit_type}] {symbol}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 STRATEGY: AGGRESSIVE\n"
    msg += f"💰 Price: ${price:.2f}\n"
    msg += f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"❌ REASON: Bias 4H crossed opposite direction\n\n"
    msg += "⚠️ EXIT POSITION IF IN TRADE"
    
    send_telegram(
        f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
        {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'disable_web_page_preview': True}
    )
    
    log_alert('EXIT', 'AGGRESSIVE', symbol, exit_type, price, {'reason': 'Bias 4H opposite'}, None, request_id)
    logger.info(f"Aggressive exit alert sent for {symbol}")

# ============================================================================ #
# WEBHOOK
# ============================================================================ #

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    request_id = str(uuid.uuid4())
    
    try:
        data = request.get_json(silent=True)
        if not data:
            return jsonify({'status': 'error', 'message': 'Missing JSON'}), 400
        
        logger.info(f"[{request_id}] Webhook received: {data}")
        
        required_fields = ['symbol', 'strategy', 'tf', 'type', 'price']
        for field in required_fields:
            if field not in data:
                return jsonify({'status': 'error', 'message': f'Missing field {field}'}), 400
        
        symbol_raw = data['symbol']
        strategy = data['strategy'].lower()
        tf = data['tf'].lower()
        alert_type = data['type'].lower()
        price = float(data['price'])
        
        symbol = format_tradingview_symbol(symbol_raw)
        
        if symbol not in CONFIG['SYMBOLS']:
            return jsonify({'status': 'ignored', 'message': 'Symbol not monitored'}), 200
        
        strategies_to_process = ['safe', 'aggressive'] if strategy == 'both' else [strategy]
        
        for current_strategy in strategies_to_process:
            
            # SAFE STRATEGY
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
                    send_telegram_safe_exit(symbol, exit_type, price, request_id)
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
                        send_telegram_safe(symbol, direction, 2, price, state, "Webhook", request_id)
                        update_last_signal(symbol, f"2stars_{direction}", 'safe')
                    
                    if bias_1h == bias_3d:
                        if should_send_alert(symbol, f"3stars_{direction}", 'safe'):
                            send_telegram_safe(symbol, direction, 3, price, state, "Webhook", request_id)
                            update_last_signal(symbol, f"3stars_{direction}", 'safe')
                        
                        st_match = (st_1h == 'buy' and direction == 'LONG') or (st_1h == 'sell' and direction == 'SHORT')
                        if st_match:
                            if should_send_alert(symbol, f"4stars_{direction}", 'safe'):
                                send_telegram_safe(symbol, direction, 4, price, state, "Webhook", request_id)
                                update_last_signal(symbol, f"4stars_{direction}", 'safe')
                            
                            if bias_4h == bias_3d:
                                if should_send_alert(symbol, f"5stars_{direction}", 'safe'):
                                    send_telegram_safe(symbol, direction, 5, price, state, "Webhook", request_id)
                                    update_last_signal(symbol, f"5stars_{direction}", 'safe')
                    
                    if st_context_1h:
                        context_match = (st_context_1h == 'buy' and direction == 'LONG') or (st_context_1h == 'sell' and direction == 'SHORT')
                        if context_match:
                            if should_send_alert(symbol, f"2stars_context_{direction}", 'safe'):
                                send_telegram_safe(symbol, direction, 2, price, state, "Webhook", request_id)
                                update_last_signal(symbol, f"2stars_context_{direction}", 'safe')
                            
                            st_match = (st_1h == 'buy' and direction == 'LONG') or (st_1h == 'sell' and direction == 'SHORT')
                            if st_match:
                                if should_send_alert(symbol, f"3stars_context_{direction}", 'safe'):
                                    send_telegram_safe(symbol, direction, 3, price, state, "Webhook", request_id)
                                    update_last_signal(symbol, f"3stars_context_{direction}", 'safe')
                            
                            if bias_1h == bias_3d:
                                if should_send_alert(symbol, f"4stars_full_{direction}", 'safe'):
                                    send_telegram_safe(symbol, direction, 4, price, state, "Webhook", request_id)
                                    update_last_signal(symbol, f"4stars_full_{direction}", 'safe')
            
            # AGGRESSIVE STRATEGY
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
                    
                    prev_short_term = state.get('short_term_4h')
                    prev_zone = state.get('zone_4h')
                    
                    state['short_term_4h'] = short_term
                    state['long_term_4h'] = long_term
                    state['timestamp_4h'] = time.time()
                    
                    # EXIT DETECTION
                    if prev_zone == 'buy' and prev_short_term is not None and prev_short_term < -2 and short_term > -2:
                        logger.info(f"[AGGRESSIVE] LONG EXIT detected for {symbol}")
                        send_telegram_aggressive_exit(symbol, 'LONG', price, request_id)
                        state['zone_4h'] = None
                        state['zone_1h'] = None
                        continue
                    
                    if prev_zone == 'sell' and prev_short_term is not None and prev_short_term > 2 and short_term < 2:
                        logger.info(f"[AGGRESSIVE] SHORT EXIT detected for {symbol}")
                        send_telegram_aggressive_exit(symbol, 'SHORT', price, request_id)
                        state['zone_4h'] = None
                        state['zone_1h'] = None
                        continue
                    
                    # ZONE DETECTION
                    zone = None
                    if value == 'buy' and short_term < -2 and long_term >= -2:
                        zone = 'buy'
                    elif value == 'sell' and short_term > 2 and long_term <= 2:
                        zone = 'sell'
                    
                    # AUTO INVALIDATION
                    if state['zone_4h'] == 'buy' and long_term < -2:
                        logger.info(f"[AGGRESSIVE] BUY zone 4H invalidated for {symbol}")
                        state['zone_4h'] = None
                    elif state['zone_4h'] == 'sell' and long_term > 2:
                        logger.info(f"[AGGRESSIVE] SELL zone 4H invalidated for {symbol}")
                        state['zone_4h'] = None
                    else:
                        state['zone_4h'] = zone
                    
                    logger.info(f"[AGGRESSIVE] ST Context 4H updated for {symbol}: zone={zone}")
                    
                    if zone and state['zone_4h'] == zone and prev_zone != zone:
                        if should_send_alert(symbol, f"prep_4h_{zone}", 'aggressive'):
                            send_telegram_aggressive_preparation(symbol, zone, price, state, request_id)
                            update_last_signal(symbol, f"prep_4h_{zone}", 'aggressive')
                    
                    continue
                
                elif alert_type == 'st_context_invalid' and tf == '4h':
                    value = data.get('value', '').lower()
                    long_term = float(data.get('long_term', 0))
                    
                    if value == 'buy' and long_term < -2:
                        logger.info(f"[AGGRESSIVE] BUY zone 4H invalidated for {symbol}")
                        state['zone_4h'] = None
                        state['long_term_4h'] = long_term
                    elif value == 'sell' and long_term > 2:
                        logger.info(f"[AGGRESSIVE] SELL zone 4H invalidated for {symbol}")
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
                    logger.info(f"[AGGRESSIVE] ST Context 1H updated for {symbol}: zone={zone}")
                    continue
                
                elif alert_type == 'supertrend' and tf == '1h':
                    value = data.get('value', '').lower()
                    state['st_1h'] = value
                    
                    logger.info(f"[AGGRESSIVE] SuperTrend AI 1H updated for {symbol}: {value}")
                    
                    zone_4h = state.get('zone_4h')
                    zone_1h = state.get('zone_1h')
                    
                    if not zone_4h or not zone_1h:
                        logger.debug(f"[AGGRESSIVE] Missing zones for {symbol}")
                        continue
                    
                    if zone_4h != zone_1h:
                        logger.info(f"[AGGRESSIVE] Zones not aligned for {symbol}")
                        continue
                    
                    signal_type = None
                    if value == 'buy' and zone_4h == 'buy' and zone_1h == 'buy':
                        signal_type = 'LONG'
                    elif value == 'sell' and zone_4h == 'sell' and zone_1h == 'sell':
                        signal_type = 'SHORT'
                    
                    if signal_type:
                        if should_send_alert(symbol, f"entry_{signal_type}", 'aggressive'):
                            send_telegram_aggressive_entry(symbol, signal_type, price, state, request_id)
                            update_last_signal(symbol, f"entry_{signal_type}", 'aggressive')
                    
                    continue
        
        return jsonify({'status': 'success', 'request_id': request_id, 'strategies_processed': strategies_to_process}), 200
    
    except Exception as e:
        logger.error(f"[{request_id}] Webhook error: {type(e).__name__} - {e}", exc_info=True)
        return jsonify({'status': 'error', 'request_id': request_id, 'message': 'Server error'}), 500

# ============================================================================ #
# STATE / HEALTH / LOGS ENDPOINTS
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

@app.route('/logs/alerts', methods=['GET'])
def get_alerts_logs():
    """Endpoint to retrieve alert logs (JSON)"""
    try:
        lines = request.args.get('lines', 100, type=int)
        
        if not os.path.exists('logs/alerts.jsonl'):
            return jsonify({'logs': [], 'message': 'No logs available'}), 200
        
        with open('logs/alerts.jsonl', 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
            last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
        
        logs = [json.loads(line) for line in last_lines]
        
        return jsonify({
            'logs': logs,
            'total': len(all_lines),
            'returned': len(logs)
        }), 200
    except Exception as e:
        logger.error(f"Alerts log read error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/state/<symbol>', methods=['GET'])
def get_symbol_state(symbol):
    symbol_formatted = symbol.replace('-', '/')
    
    if symbol_formatted not in CONFIG['SYMBOLS']:
        return jsonify({'error': 'Symbol not monitored'}), 404
    
    return jsonify({
        'symbol': symbol_formatted,
        'safe': SAFE_STATE.get(symbol_formatted, 'No data'),
        'aggressive': AGGRESSIVE_STATE.get(symbol_formatted, 'No data')
    }), 200

# ============================================================================ #
# DASHBOARD WEB
# ============================================================================ #

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Trading Bot Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial; background: #0f172a; color: white; padding: 20px; }
        h1 { color: #38bdf8; }
        .card { background: #1e293b; padding: 20px; border-radius: 10px; margin-bottom: 20px; }
        canvas { max-width: 100%; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .stat-box { background: #334155; padding: 15px; border-radius: 8px; text-align: center; }
        .stat-value { font-size: 32px; font-weight: bold; color: #38bdf8; }
        .stat-label { color: #94a3b8; margin-top: 5px; }
    </style>
</head>
<body>
    <h1>📊 Trading Bot Dashboard</h1>

    <div class="card">
        <h2>Statistics</h2>
        <div class="stats" id="stats"></div>
    </div>

    <div class="card">
        <h2>Signals per Day</h2>
        <canvas id="signalsChart"></canvas>
    </div>

    <div class="card">
        <h2>Direction Distribution</h2>
        <canvas id="directionChart"></canvas>
    </div>

    <div class="card">
        <h2>Strategy Distribution</h2>
        <canvas id="strategyChart"></canvas>
    </div>

<script>
fetch('/dashboard/data')
  .then(res => res.json())
  .then(data => {
    // Stats
    document.getElementById('stats').innerHTML = `
        <div class="stat-box">
            <div class="stat-value">${data.total_alerts}</div>
            <div class="stat-label">Total Alerts</div>
        </div>
        <div class="stat-box">
            <div class="stat-value">${data.long_count}</div>
            <div class="stat-label">LONG Signals</div>
        </div>
        <div class="stat-box">
            <div class="stat-value">${data.short_count}</div>
            <div class="stat-label">SHORT Signals</div>
        </div>
        <div class="stat-box">
            <div class="stat-value">${data.avg_per_day}</div>
            <div class="stat-label">Avg/Day</div>
        </div>
    `;

    // Signals per day
    const ctx1 = document.getElementById('signalsChart').getContext('2d');
    new Chart(ctx1, {
      type: 'line',
      data: {
        labels: data.dates,
        datasets: [{
          label: 'Signals',
          data: data.signals_per_day,
          borderColor: '#38bdf8',
          backgroundColor: 'rgba(56, 189, 248, 0.1)',
          fill: true,
          tension: 0.4
        }]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { labels: { color: 'white' } }
        },
        scales: {
          y: { ticks: { color: 'white' }, grid: { color: '#334155' } },
          x: { ticks: { color: 'white' }, grid: { color: '#334155' } }
        }
      }
    });

    // Direction distribution
    const ctx2 = document.getElementById('directionChart').getContext('2d');
    new Chart(ctx2, {
      type: 'pie',
      data: {
        labels: ['LONG', 'SHORT'],
        datasets: [{
          data: [data.long_count, data.short_count],
          backgroundColor: ['#22c55e', '#ef4444']
        }]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { labels: { color: 'white' } }
        }
      }
    });

    // Strategy distribution
    const ctx3 = document.getElementById('strategyChart').getContext('2d');
    new Chart(ctx3, {
      type: 'doughnut',
      data: {
        labels: data.strategy_labels,
        datasets: [{
          data: data.strategy_counts,
          backgroundColor: ['#8b5cf6', '#f59e0b']
        }]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { labels: { color: 'white' } }
        }
      }
    });
  });
</script>
</body>
</html>
"""

@app.route('/dashboard', methods=['GET'])
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route('/dashboard/data', methods=['GET'])
def dashboard_data():
    try:
        if not os.path.exists('logs/alerts.jsonl'):
            return jsonify({
                'dates': [], 'signals_per_day': [], 'long_count': 0, 'short_count': 0,
                'total_alerts': 0, 'avg_per_day': 0, 'strategy_labels': [], 'strategy_counts': []
            })

        with open('logs/alerts.jsonl', 'r', encoding='utf-8') as f:
            lines = f.readlines()

        data = [json.loads(line) for line in lines]

        daily_counts = {}
        long_count = 0
        short_count = 0
        strategy_counts = {}

        for entry in data:
            date = entry['timestamp'][:10]
            daily_counts[date] = daily_counts.get(date, 0) + 1

            if entry.get('direction') == 'LONG':
                long_count += 1
            elif entry.get('direction') == 'SHORT':
                short_count += 1
            
            strategy = entry.get('strategy', 'UNKNOWN')
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        dates = sorted(daily_counts.keys())
        signals_per_day = [daily_counts[d] for d in dates]
        avg_per_day = round(sum(signals_per_day) / len(signals_per_day), 1) if signals_per_day else 0

        return jsonify({
            'dates': dates,
            'signals_per_day': signals_per_day,
            'long_count': long_count,
            'short_count': short_count,
            'total_alerts': len(data),
            'avg_per_day': avg_per_day,
            'strategy_labels': list(strategy_counts.keys()),
            'strategy_counts': list(strategy_counts.values())
        })

    except Exception as e:
        logger.error(f"Dashboard data error: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================ #
# WEEKLY SUMMARY
# ============================================================================ #

def generate_weekly_summary():
    """Generate and send weekly summary to Telegram"""
    try:
        if not os.path.exists('logs/alerts.jsonl'):
            logger.info("No alerts for weekly summary")
            return

        with open('logs/alerts.jsonl', 'r', encoding='utf-8') as f:
            lines = f.readlines()

        one_week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        data = [json.loads(line) for line in lines]

        recent = [
            entry for entry in data
            if datetime.fromisoformat(entry['timestamp']) >= one_week_ago
        ]

        if not recent:
            logger.info("No alerts in the last 7 days")
            return

        total = len(recent)
        longs = sum(1 for e in recent if e.get('direction') == 'LONG')
        shorts = sum(1 for e in recent if e.get('direction') == 'SHORT')

        strategy_counts = {}
        for e in recent:
            strategy = e.get('strategy', 'UNKNOWN')
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        summary = (
            f"📅 *WEEKLY TRADING SUMMARY*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 Total alerts: {total}\n"
            f"📈 LONG: {longs}\n"
            f"📉 SHORT: {shorts}\n\n"
            f"🧠 By strategy:\n"
        )
        
        for strategy, count in strategy_counts.items():
            summary += f"   • {strategy}: {count}\n"

        summary += f"\n━━━━━━━━━━━━━━━━━━━━\n"
        summary += f"Period: {one_week_ago.strftime('%Y-%m-%d')} to {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"

        send_telegram(
            f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
            {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': summary, 'parse_mode': 'Markdown'}
        )
        
        logger.info("Weekly summary sent")

    except Exception as e:
        logger.error(f"Weekly summary error: {e}")

def weekly_summary_scheduler():
    """Run weekly summary every Monday at 09:00 UTC"""
    while not shutdown_flag.is_set():
        now = datetime.now(timezone.utc)
        # Send every Monday at 09:00 UTC
        if now.weekday() == 0 and now.hour == 9 and now.minute < 5:
            generate_weekly_summary()
            time.sleep(3600)  # Sleep 1 hour to avoid duplicates
        time.sleep(60)

# ============================================================================ #
# CLEAN SHUTDOWN
# ============================================================================ #

def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_flag.set()
    try:
        send_telegram(
            f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
            {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': "[BOT STOPPED]\n\nManual shutdown by user"}
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
    logger.info("Unified Trading Monitor Bot - Starting (Enhanced)")
    logger.info("="*60)
    
    if not CONFIG['TELEGRAM_BOT_TOKEN']:
        logger.error("TELEGRAM_BOT_TOKEN not configured")
        sys.exit(1)
    
    if not CONFIG['TELEGRAM_CHAT_ID']:
        logger.error("TELEGRAM_CHAT_ID not configured")
        sys.exit(1)
    
    try:
        start_msg = (
            "🤖 [BOT STARTED]\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "📊 Mode: Webhook only\n"
            f"🎯 Monitoring: {len(CONFIG['SYMBOLS'])} assets\n\n"
            "📋 ACTIVE STRATEGIES:\n\n"
            "1️⃣ SAFE (Conservative)\n"
            "   • System 2★ to 5★\n"
            "   • Exit: MACD 3D opposite\n\n"
            "2️⃣ AGGRESSIVE (Fast)\n"
            "   • ST Context 4H + 1H\n"
            "   • SuperTrend AI 1H\n"
            "   • Exit: Bias 4H opposite\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "✅ Ready to receive TradingView webhooks\n\n"
            "📊 Dashboard: http://localhost:5000/dashboard\n"
            "📝 Logs: JSON + CSV export enabled\n"
            "📅 Weekly reports: Every Monday 09:00 UTC"
        )
        
        send_telegram(
            f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage",
            {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': start_msg}
        )
        logger.info("Startup message sent")
    except Exception as e:
        logger.warning(f"Could not send startup message: {e}")
    
    # Start weekly summary scheduler thread
    threading.Thread(target=weekly_summary_scheduler, daemon=True).start()
    logger.info("Weekly summary scheduler started")
    
    try:
        logger.info(f"Starting webhook server on {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
        logger.info(f"Dashboard available at: http://localhost:{CONFIG['WEBHOOK_PORT']}/dashboard")
        app.run(
            host=CONFIG['WEBHOOK_HOST'],
            port=CONFIG['WEBHOOK_PORT'],
            debug=False,
            use_reloader=False
        )
    except Exception as e:
        logger.error(f"Flask server error: {e}")
        shutdown_flag.set()
        sys.exit(1)