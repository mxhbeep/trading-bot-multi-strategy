#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified Trading Monitor Bot - Multi-Strategies (26 janvier 2026)
- Exchange : OKX spot
- Watchlist : fixe dans le code (10 paires)

STRATEGIE 1 (safe):
- 3D : Biais EMA(13) vs SMA(30)
- 4H : MACD (13, 34, 8)
- 1H : Biais EMA(13) vs SMA(34)
- Système d'étoiles : 2★ à 5★
- Sortie : Croisement MACD opposé en 3D

STRATEGIE 2 (aggressive):
- 4H : ST Context zones (buy/sell) validées par Long Term
- 1H : ST Context zones (buy/sell)
- 1H : SuperTrend AI (buy/sell)
- Sortie : Biais croise direction opposée en 4H

Webhook TradingView avec champ "strategy" pour choisir
Alertes Telegram avec anti-spam intelligent
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
from typing import Optional, Dict, List, Tuple
import signal
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    'EXCHANGE': 'okx',
    'API_KEY': '',
    'SECRET': '',
    
    'TELEGRAM_BOT_TOKEN': '8110041550:AAHJKAWxIG1ZBjZ8fRfFMKq-4iTeo5v4-Hw',
    'TELEGRAM_CHAT_ID': '6473214015',
    
    # Paires fixes dans le code
    'SYMBOLS': [
        'BTC/USDT',
        'ETH/USDT',
        'SOL/USDT',
        'XRP/USDT',
        'CRV/USDT',
        'PEPE/USDT',
        'DOGE/USDT',
        'WIF/USDT',
        'BONK/USDT',
        'CVX/USDT'
    ],
    
    # Timeframes et indicateurs - Strategie 1 (safe)
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
    
    # Parametres bot
    'CHECK_INTERVAL': 300,                      # 5 minutes
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,        # 30 min mini entre 2 alertes identiques
    'DATA_LIMIT': 300,
    'RETRY_DELAY': 12,
    'MAX_RETRIES': 4,
    
    # Webhook Flask
    'WEBHOOK_PORT': 5000,
    'WEBHOOK_HOST': '0.0.0.0',
}

# ============================================================================
# ETAT GLOBAL
# ============================================================================

# Etat anti-spam par symbole et strategie
LAST_SIGNALS: Dict[str, Dict] = {}

# Exchange global
exchange: Optional[ccxt.okx] = None

# Etat pour strategie aggressive (ST Context)
AGGRESSIVE_STATE: Dict[str, Dict] = {}  # {'ETH/USDT': {'zone_4h': 'buy', 'long_term_4h': 1.5, 'zone_1h': 'buy', ...}}

# Etat pour strategie safe
SAFE_STATE: Dict[str, Dict] = {}  # {'BTC/USDT': {'bias_3d': 'bull', 'macd_4h': 'bull', 'bias_1h': 'bull', ...}}

# Flag pour arret propre
shutdown_flag = threading.Event()

# ============================================================================
# UTILITAIRES
# ============================================================================

def format_tradingview_symbol(tv_symbol: str) -> str:
    """
    Convertit un symbole TradingView en format attendu par le bot
    """
    if ':' in tv_symbol:
        parts = tv_symbol.split(':')
        tv_symbol = parts[-1] if len(parts) > 1 else tv_symbol
    
    if '/' in tv_symbol:
        return tv_symbol
    
    if 'USDT' in tv_symbol:
        base = tv_symbol.replace('USDT', '')
        return f"{base}/USDT"
    elif 'USDC' in tv_symbol:
        base = tv_symbol.replace('USDC', '')
        return f"{base}/USDC"
    elif 'BUSD' in tv_symbol:
        base = tv_symbol.replace('BUSD', '')
        return f"{base}/BUSD"
    
    return tv_symbol

# ============================================================================
# INITIALISATION EXCHANGE
# ============================================================================

def init_exchange():
    """Initialise et retourne l'instance exchange"""
    global exchange
    if exchange is None:
        try:
            exchange = ccxt.okx({
                'apiKey': CONFIG['API_KEY'],
                'secret': CONFIG['SECRET'],
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'}
            })
            logger.info("Exchange OKX initialise")
        except Exception as e:
            logger.error(f"Erreur initialisation exchange: {e}")
            raise
    return exchange

def get_exchange():
    """Retourne l'exchange, l'initialise si necessaire"""
    global exchange
    if exchange is None:
        return init_exchange()
    return exchange

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# FLASK APP
# ============================================================================

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot trading OK - Multi-strategies monitoring actif"

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# ============================================================================
# INDICATEURS TECHNIQUES
# ============================================================================

def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calcule l'Exponential Moving Average"""
    return series.ewm(span=period, adjust=False).mean()

def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """Calcule la Simple Moving Average"""
    return series.rolling(window=period).mean()

def calculate_macd(series: pd.Series, fast: int, slow: int, signal: int) -> Tuple[pd.Series, pd.Series]:
    """Calcule le MACD et retourne (macd_line, signal_line)"""
    ema_fast = calculate_ema(series, fast)
    ema_slow = calculate_ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = calculate_ema(macd_line, signal)
    return macd_line, signal_line

# ============================================================================
# RECUPERATION DONNEES OHLCV
# ============================================================================

def fetch_ohlcv(symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
    """
    Recupere les donnees OHLCV avec retry automatique
    """
    try:
        ex = get_exchange()
    except Exception as e:
        logger.error(f"Impossible d'obtenir l'exchange: {e}")
        return None
    
    for attempt in range(CONFIG['MAX_RETRIES']):
        try:
            ohlcv = ex.fetch_ohlcv(symbol, timeframe, limit=CONFIG['DATA_LIMIT'])
            
            if not ohlcv or len(ohlcv) < 50:
                raise ValueError(f"Donnees insuffisantes: {len(ohlcv) if ohlcv else 0} bougies")
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            return df
            
        except ccxt.RateLimitExceeded:
            wait_time = CONFIG['RETRY_DELAY'] * (attempt + 1)
            logger.warning(f"Rate limit atteint pour {symbol} {timeframe}, attente {wait_time}s...")
            time.sleep(wait_time)
            
        except Exception as e:
            if attempt < CONFIG['MAX_RETRIES'] - 1:
                logger.warning(f"{symbol} {timeframe} erreur (essai {attempt+1}/{CONFIG['MAX_RETRIES']}): {e}")
                time.sleep(CONFIG['RETRY_DELAY'])
            else:
                logger.error(f"Echec definitif pour {symbol} {timeframe}: {e}")
                return None
    
    return None

# ============================================================================
# ENVOI TELEGRAM
# ============================================================================

def send_telegram_safe(symbol: str, signal_type: str, stars: int, price: float, details: Dict, source: str = "Webhook"):
    """
    Envoie une alerte formatee sur Telegram (strategie safe)
    """
    try:
        emoji = "🟢" if signal_type == 'LONG' else "🔴"
        star_emoji = "⭐" * stars
        
        msg = f"{emoji} [{signal_type} {star_emoji}] {symbol}\n"
        msg += f"Strategie: Safe ({stars} étoiles)\n\n"
        msg += f"Prix : ${price:.4f}\n"
        msg += f"Heure : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
        msg += f"Source : {source}\n\n"
        
        if stars == 2:
            msg += "Préparation 1 (2★):\n"
            msg += f"  Biais 3D: {details.get('bias_3d', 'N/A')}\n"
            msg += f"  MACD 4H: {details.get('macd_4h', 'N/A')}\n"
        elif stars == 3:
            msg += "Préparation 2 (3★):\n"
            msg += f"  Biais 3D: {details.get('bias_3d', 'N/A')}\n"
            msg += f"  MACD 4H: {details.get('macd_4h', 'N/A')}\n"
            msg += f"  Biais 1H: {details.get('bias_1h', 'N/A')}\n"
        elif stars == 4:
            msg += "Signal d'entrée (4★):\n"
            msg += f"  Biais 3D: {details.get('bias_3d', 'N/A')}\n"
            msg += f"  MACD 4H: {details.get('macd_4h', 'N/A')}\n"
            msg += f"  Biais 1H: {details.get('bias_1h', 'N/A')}\n"
            msg += f"  SuperTrend AI 1H: {details.get('st_1h', 'N/A')}\n"
        elif stars == 5:
            msg += "Setup premium (5★):\n"
            msg += f"  Biais 3D: {details.get('bias_3d', 'N/A')}\n"
            msg += f"  MACD 4H: {details.get('macd_4h', 'N/A')}\n"
            msg += f"  Biais 4H (9/26): {details.get('bias_4h', 'N/A')}\n"
            msg += f"  Biais 1H: {details.get('bias_1h', 'N/A')}\n"
            msg += f"  SuperTrend AI 1H: {details.get('st_1h', 'N/A')}\n"
        
        msg += "\n⚠️ Verifiez SuperTrend AI 20min avant d'entrer"
        msg += "\n📊 Ce bot ne trade pas automatiquement"
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'disable_web_page_preview': True
        }
        
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Alerte {signal_type} {stars}★ envoyee pour {symbol} [Safe] (source: {source})")
        return True
        
    except Exception as e:
        logger.error(f"Echec envoi Telegram pour {symbol}: {e}")
        return False

def send_telegram_safe_exit(symbol: str, exit_type: str, price: float):
    """
    Envoie une alerte de sortie pour la strategie safe
    """
    try:
        msg = f"🚪 [SORTIE {exit_type}] {symbol}\n"
        msg += f"Strategie: Safe\n\n"
        msg += f"Prix : ${price:.4f}\n"
        msg += f"Heure : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        msg += f"Raison: Croisement MACD opposé en 3D\n"
        msg += "Sortez la position si vous êtes en trade"
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'disable_web_page_preview': True
        }
        
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Alerte sortie {exit_type} envoyee pour {symbol} [Safe]")
        return True
        
    except Exception as e:
        logger.error(f"Echec envoi Telegram sortie pour {symbol}: {e}")
        return False

def send_telegram_aggressive_preparation(symbol: str, zone_type: str, price: float, details: Dict):
    """
    Envoie une alerte de preparation pour la strategie aggressive
    """
    try:
        emoji = "🟢" if zone_type == 'buy' else "🔴"
        direction = "LONG" if zone_type == 'buy' else "SHORT"
        
        msg = f"{emoji} [PREPARATION {direction}] {symbol}\n"
        msg += f"Strategie: Aggressive\n\n"
        msg += f"Prix : ${price:.2f}\n"
        msg += f"Heure : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        
        msg += f"Zone ST Context 4H : {zone_type.upper()} ACTIVE\n"
        msg += f"Long Term 4H : {details.get('long_term_4h', 'N/A')}\n"
        
        if details.get('zone_1h'):
            msg += f"Zone ST Context 1H : {details['zone_1h'].upper()} ACTIVE\n"
        
        msg += "\nEn attente du signal SuperTrend AI 1H...\n"
        msg += f"Surveille le SuperTrend AI pour confirmation {direction}"
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'disable_web_page_preview': True
        }
        
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Alerte preparation {direction} envoyee pour {symbol} [Aggressive]")
        return True
        
    except Exception as e:
        logger.error(f"Echec envoi Telegram preparation pour {symbol}: {e}")
        return False

def send_telegram_aggressive_entry(symbol: str, signal_type: str, price: float, details: Dict):
    """
    Envoie une alerte d'entrée pour la strategie aggressive
    """
    try:
        emoji = "🟢" if signal_type == 'LONG' else "🔴"
        
        msg = f"{emoji} [SIGNAL {signal_type}] {symbol}\n"
        msg += f"Strategie: Aggressive\n\n"
        msg += f"Prix : ${price:.2f}\n"
        msg += f"Heure : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        
        msg += f"Zone ST Context 4H : {details.get('zone_4h', 'N/A').upper()}\n"
        msg += f"Long Term 4H : {details.get('long_term_4h', 'N/A')}\n"
        msg += f"Zone ST Context 1H : {details.get('zone_1h', 'N/A').upper()}\n"
        msg += f"SuperTrend AI 1H : {details.get('st_1h', 'N/A').upper()}\n\n"
        
        msg += "✅ Tous les critères alignés pour entrée\n"
        msg += "⚠️ Verifiez SuperTrend AI 20min avant d'entrer\n"
        msg += "📊 Ce bot ne trade pas automatiquement"
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'disable_web_page_preview': True
        }
        
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Alerte entrée {signal_type} envoyee pour {symbol} [Aggressive]")
        return True
        
    except Exception as e:
        logger.error(f"Echec envoi Telegram entrée pour {symbol}: {e}")
        return False

def send_telegram_aggressive_exit(symbol: str, exit_type: str, price: float):
    """
    Envoie une alerte de sortie pour la strategie aggressive
    """
    try:
        msg = f"🚪 [SORTIE {exit_type}] {symbol}\n"
        msg += f"Strategie: Aggressive\n\n"
        msg += f"Prix : ${price:.2f}\n"
        msg += f"Heure : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        msg += f"Raison: Biais croisé direction opposée en 4H\n"
        msg += "Sortez la position si vous êtes en trade"
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'disable_web_page_preview': True
        }
        
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Alerte sortie {exit_type} envoyee pour {symbol} [Aggressive]")
        return True
        
    except Exception as e:
        logger.error(f"Echec envoi Telegram sortie pour {symbol}: {e}")
        return False

# ============================================================================
# GESTION ANTI-SPAM
# ============================================================================

def get_signal_key(symbol: str, strategy: str, signal_type: str = "") -> str:
    """Genere une cle unique pour le tracking anti-spam"""
    if signal_type:
        return f"{symbol}:{strategy}:{signal_type}"
    return f"{symbol}:{strategy}"

def should_send_alert(symbol: str, signal_identifier: str, strategy: str) -> bool:
    """
    Determine si une alerte doit etre envoyee en fonction de l'anti-spam
    """
    now = time.time()
    signal_key = get_signal_key(symbol, strategy, signal_identifier)
    
    if signal_key not in LAST_SIGNALS:
        LAST_SIGNALS[signal_key] = {'timestamp': 0}
        return True
    
    time_elapsed = now - LAST_SIGNALS[signal_key]['timestamp']
    if time_elapsed >= CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']:
        return True
    
    return False

def update_last_signal(symbol: str, signal_identifier: str, strategy: str):
    """
    Met a jour l'etat du dernier signal pour un symbole et une strategie
    """
    signal_key = get_signal_key(symbol, strategy, signal_identifier)
    LAST_SIGNALS[signal_key] = {'timestamp': time.time()}

# ============================================================================
# WEBHOOK TRADINGVIEW
# ============================================================================

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    """
    Endpoint pour recevoir les webhooks TradingView
    
    STRATEGIE SAFE:
    - Biais 3D: {"symbol": "BTC/USDT", "strategy": "safe", "tf": "3d", "type": "bias", "value": "bull/bear", "price": 43250}
    - MACD 4H: {"symbol": "BTC/USDT", "strategy": "safe", "tf": "4h", "type": "macd", "value": "bull/bear", "price": 43250}
    - Biais 4H (9/26): {"symbol": "BTC/USDT", "strategy": "safe", "tf": "4h", "type": "bias_9_26", "value": "bull/bear", "price": 43250}
    - Biais 1H: {"symbol": "BTC/USDT", "strategy": "safe", "tf": "1h", "type": "bias", "value": "bull/bear", "price": 43250}
    
    STRATEGIE AGGRESSIVE:
    - ST Context 4H: {"symbol": "BTC/USDT", "strategy": "aggressive", "tf": "4h", "type": "st_context", "value": "buy/sell", "short_term": -2.5, "long_term": 1.5, "price": 43250}
    - ST Context 4H Invalid: {"symbol": "BTC/USDT", "strategy": "aggressive", "tf": "4h", "type": "st_context_invalid", "value": "buy/sell", "long_term": -2.5, "price": 43250}
    
    Note: Les sorties sont détectées automatiquement par les alertes ST Context 4H (quand short_term repasse les seuils -2 ou 2)
    
    ALERTES COMMUNES (both):
    - SuperTrend AI 1H: {"symbol": "BTC/USDT", "strategy": "both", "tf": "1h", "type": "supertrend", "value": "buy/sell", "price": 43250}
    - ST Context 1H: {"symbol": "BTC/USDT", "strategy": "both", "tf": "1h", "type": "st_context", "value": "buy/sell", "short_term": -2.5, "price": 43250}
    """
    try:
        data = request.get_json(silent=True)
        
        if not data:
            logger.warning("Webhook recu sans donnees JSON")
            return jsonify({'status': 'error', 'message': 'Donnees JSON manquantes'}), 400
        
        logger.info(f"Webhook recu: {data}")
        
        # Validation champs requis
        required_fields = ['symbol', 'strategy', 'tf', 'type', 'price']
        for field in required_fields:
            if field not in data:
                return jsonify({'status': 'error', 'message': f'Champ {field} manquant'}), 400
        
        symbol_raw = data['symbol']
        strategy = data['strategy'].lower()
        tf = data['tf'].lower()
        alert_type = data['type'].lower()
        price = float(data['price'])
        
        # Conversion du symbole TradingView
        symbol = format_tradingview_symbol(symbol_raw)
        
        # Verification symbole dans watchlist
        if symbol not in CONFIG['SYMBOLS']:
            logger.warning(f"Symbole {symbol} non surveille")
            return jsonify({'status': 'ignored', 'message': 'Symbole non surveille'}), 200
        
        # Gestion du strategy "both" - dupliquer pour safe et aggressive
        strategies_to_process = []
        if strategy == 'both':
            strategies_to_process = ['safe', 'aggressive']
        else:
            strategies_to_process = [strategy]
        
        # Traiter chaque strategie concernée
        for current_strategy in strategies_to_process:
            
            # =================================================================
            # STRATEGIE SAFE
            # =================================================================
            if current_strategy == 'safe':
            # Initialiser l'état si nécessaire
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
            
            # Traitement des différents types d'alertes
            if alert_type == 'bias' and tf == '3d':
                value = data.get('value', '').lower()
                state['bias_3d'] = value
                logger.info(f"Biais 3D mis à jour pour {symbol}: {value}")
                
            elif alert_type == 'macd' and tf == '4h':
                value = data.get('value', '').lower()
                state['macd_4h'] = value
                logger.info(f"MACD 4H mis à jour pour {symbol}: {value}")
                
            elif alert_type == 'bias_9_26' and tf == '4h':
                value = data.get('value', '').lower()
                state['bias_4h_9_26'] = value
                logger.info(f"Biais 4H (9/26) mis à jour pour {symbol}: {value}")
                
            elif alert_type == 'bias' and tf == '1h':
                value = data.get('value', '').lower()
                state['bias_1h'] = value
                logger.info(f"Biais 1H mis à jour pour {symbol}: {value}")
                
            elif alert_type == 'supertrend' and tf == '1h':
                value = data.get('value', '').lower()
                state['st_1h'] = value
                logger.info(f"SuperTrend AI 1H mis à jour pour {symbol}: {value}")
                
            elif alert_type == 'st_context' and tf == '1h':
                value = data.get('value', '').lower()
                state['st_context_1h'] = value
                logger.info(f"ST Context 1H mis à jour pour {symbol}: {value}")
                
            elif alert_type == 'macd_exit' and tf == '3d':
                value = data.get('value', '').lower()
                exit_type = 'LONG' if value == 'bear' else 'SHORT'
                send_telegram_safe_exit(symbol, exit_type, price)
                return jsonify({'status': 'success', 'message': 'Alerte sortie envoyée'}), 200
            
            # Vérification des setups et envoi d'alertes
            bias_3d = state.get('bias_3d')
            macd_4h = state.get('macd_4h')
            bias_4h = state.get('bias_4h_9_26')
            bias_1h = state.get('bias_1h')
            st_1h = state.get('st_1h')
            st_context_1h = state.get('st_context_1h')
            
            # Déterminer la direction
            direction = None
            if bias_3d == 'bull' and macd_4h == 'bull':
                direction = 'LONG'
            elif bias_3d == 'bear' and macd_4h == 'bear':
                direction = 'SHORT'
            
            if direction:
                # Setup 2 étoiles : Biais 3D + MACD 4H
                if should_send_alert(symbol, f"2stars_{direction}", 'safe'):
                    details = {'bias_3d': bias_3d, 'macd_4h': macd_4h}
                    send_telegram_safe(symbol, direction, 2, price, details, "Webhook TradingView")
                    update_last_signal(symbol, f"2stars_{direction}", 'safe')
                
                # Setup 3 étoiles : + Biais 1H
                if bias_1h == bias_3d:
                    if should_send_alert(symbol, f"3stars_{direction}", 'safe'):
                        details = {'bias_3d': bias_3d, 'macd_4h': macd_4h, 'bias_1h': bias_1h}
                        send_telegram_safe(symbol, direction, 3, price, details, "Webhook TradingView")
                        update_last_signal(symbol, f"3stars_{direction}", 'safe')
                    
                    # Setup 4 étoiles : + SuperTrend AI 1H
                    st_match = (st_1h == 'buy' and direction == 'LONG') or (st_1h == 'sell' and direction == 'SHORT')
                    if st_match:
                        if should_send_alert(symbol, f"4stars_{direction}", 'safe'):
                            details = {'bias_3d': bias_3d, 'macd_4h': macd_4h, 'bias_1h': bias_1h, 'st_1h': st_1h}
                            send_telegram_safe(symbol, direction, 4, price, details, "Webhook TradingView")
                            update_last_signal(symbol, f"4stars_{direction}", 'safe')
                        
                        # Setup 5 étoiles : + Biais 4H (9/26)
                        if bias_4h == bias_3d:
                            if should_send_alert(symbol, f"5stars_{direction}", 'safe'):
                                details = {
                                    'bias_3d': bias_3d, 
                                    'macd_4h': macd_4h, 
                                    'bias_4h': bias_4h,
                                    'bias_1h': bias_1h, 
                                    'st_1h': st_1h
                                }
                                send_telegram_safe(symbol, direction, 5, price, details, "Webhook TradingView")
                                update_last_signal(symbol, f"5stars_{direction}", 'safe')
                
                # Alertes supplémentaires avec ST Context 1H
                if st_context_1h:
                    # Biais 3D + ST Context 1H
                    context_match = (st_context_1h == 'buy' and direction == 'LONG') or (st_context_1h == 'sell' and direction == 'SHORT')
                    if context_match:
                        if should_send_alert(symbol, f"2stars_context_{direction}", 'safe'):
                            details = {'bias_3d': bias_3d, 'st_context_1h': st_context_1h}
                            send_telegram_safe(symbol, direction, 2, price, details, "Webhook TradingView")
                            update_last_signal(symbol, f"2stars_context_{direction}", 'safe')
                        
                        # Biais 3D + ST Context 1H + SuperTrend AI 1H
                        if st_match:
                            if should_send_alert(symbol, f"3stars_context_{direction}", 'safe'):
                                details = {'bias_3d': bias_3d, 'st_context_1h': st_context_1h, 'st_1h': st_1h}
                                send_telegram_safe(symbol, direction, 3, price, details, "Webhook TradingView")
                                update_last_signal(symbol, f"3stars_context_{direction}", 'safe')
                        
                        # Biais 3D + MACD 4H + Biais 1H + ST Context 1H
                        if bias_1h == bias_3d:
                            if should_send_alert(symbol, f"4stars_full_{direction}", 'safe'):
                                details = {
                                    'bias_3d': bias_3d,
                                    'macd_4h': macd_4h,
                                    'bias_1h': bias_1h,
                                    'st_context_1h': st_context_1h
                                }
                                send_telegram_safe(symbol, direction, 4, price, details, "Webhook TradingView")
                                update_last_signal(symbol, f"4stars_full_{direction}", 'safe')
            
            # =================================================================
            # STRATEGIE AGGRESSIVE
            # =================================================================
            elif current_strategy == 'aggressive':
            # Initialiser l'état si nécessaire
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
            
            # Traitement ST Context 4H
            if alert_type == 'st_context' and tf == '4h':
                short_term = float(data.get('short_term', 0))
                long_term = float(data.get('long_term', 0))
                
                state['short_term_4h'] = short_term
                state['long_term_4h'] = long_term
                state['timestamp_4h'] = time.time()
                
                # Déterminer la zone
                zone = None
                if short_term < -2 and long_term >= -2:
                    zone = 'buy'
                elif short_term > 2 and long_term <= 2:
                    zone = 'sell'
                
                # Vérifier invalidation
                if state['zone_4h'] == 'buy' and long_term < -2:
                    logger.info(f"Zone BUY 4H invalidée pour {symbol} (long_term={long_term} < -2)")
                    state['zone_4h'] = None
                elif state['zone_4h'] == 'sell' and long_term > 2:
                    logger.info(f"Zone SELL 4H invalidée pour {symbol} (long_term={long_term} > 2)")
                    state['zone_4h'] = None
                else:
                    state['zone_4h'] = zone
                
                logger.info(f"ST Context 4H mis à jour pour {symbol}: zone={zone}, short_term={short_term}, long_term={long_term}")
                
                # Envoyer alerte de préparation si zone valide
                if zone and state['zone_4h'] == zone:
                    if should_send_alert(symbol, f"prep_4h_{zone}", 'aggressive'):
                        details = {'long_term_4h': long_term, 'zone_1h': state.get('zone_1h')}
                        send_telegram_aggressive_preparation(symbol, zone, price, details)
                        update_last_signal(symbol, f"prep_4h_{zone}", 'aggressive')
                
                return jsonify({'status': 'success', 'message': 'ST Context 4H mis à jour', 'zone': zone}), 200
            
            # Traitement ST Context 1H
            elif alert_type == 'st_context' and tf == '1h':
                short_term = float(data.get('short_term', 0))
                
                state['short_term_1h'] = short_term
                state['timestamp_1h'] = time.time()
                
                # Déterminer la zone (pas de condition long_term en 1H)
                zone = None
                if short_term < -2:
                    zone = 'buy'
                elif short_term > 2:
                    zone = 'sell'
                
                state['zone_1h'] = zone
                
                logger.info(f"ST Context 1H mis à jour pour {symbol}: zone={zone}, short_term={short_term}")
                
                return jsonify({'status': 'success', 'message': 'ST Context 1H mis à jour', 'zone': zone}), 200
            
            # Traitement SuperTrend AI 1H
            elif alert_type == 'supertrend' and tf == '1h':
                value = data.get('value', '').lower()
                state['st_1h'] = value
                
                logger.info(f"SuperTrend AI 1H mis à jour pour {symbol}: {value}")
                
                # Vérifier alignement pour signal d'entrée
                zone_4h = state.get('zone_4h')
                zone_1h = state.get('zone_1h')
                
                if not zone_4h or not zone_1h:
                    return jsonify({'status': 'not_aligned', 'message': 'Zones 4H ou 1H manquantes'}), 200
                
                # Les deux zones doivent être alignées
                if zone_4h != zone_1h:
                    logger.info(f"Zones non alignées pour {symbol}: 4H={zone_4h}, 1H={zone_1h}")
                    return jsonify({'status': 'not_aligned', 'message': 'Zones 4H et 1H non alignées'}), 200
                
                # Vérifier si SuperTrend AI correspond
                signal_type = None
                if value == 'buy' and zone_4h == 'buy' and zone_1h == 'buy':
                    signal_type = 'LONG'
                elif value == 'sell' and zone_4h == 'sell' and zone_1h == 'sell':
                    signal_type = 'SHORT'
                
                if signal_type:
                    if should_send_alert(symbol, f"entry_{signal_type}", 'aggressive'):
                        details = {
                            'zone_4h': zone_4h,
                            'long_term_4h': state.get('long_term_4h'),
                            'zone_1h': zone_1h,
                            'st_1h': value
                        }
                        send_telegram_aggressive_entry(symbol, signal_type, price, details)
                        update_last_signal(symbol, f"entry_{signal_type}", 'aggressive')
                        
                        return jsonify({'status': 'success', 'message': 'Signal entrée envoyé', 'signal': signal_type}), 200
                
                return jsonify({'status': 'not_aligned', 'message': 'SuperTrend AI non aligné avec zones'}), 200
            
            # Traitement sortie 4H
            elif alert_type == 'bias_exit' and tf == '4h':
                value = data.get('value', '').lower()
                exit_type = 'LONG' if value == 'bull' else 'SHORT'
                send_telegram_aggressive_exit(symbol, exit_type, price)
                return jsonify({'status': 'success', 'message': 'Alerte sortie envoyée'}), 200
            
            return jsonify({'status': 'success', 'message': 'État aggressive mis à jour', 'symbol': symbol}), 200
        
        else:
            return jsonify({'status': 'error', 'message': f'Strategie invalide: {strategy}'}), 400
        
    except ValueError as e:
        logger.error(f"Erreur de conversion dans webhook: {e}")
        return jsonify({'status': 'error', 'message': f'Erreur de conversion: {e}'}), 400
    
    except KeyError as e:
        logger.error(f"Cle manquante dans webhook: {e}")
        return jsonify({'status': 'error', 'message': f'Cle manquante: {e}'}), 400
    
    except Exception as e:
        logger.error(f"Erreur webhook: {type(e).__name__} - {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'Erreur serveur'}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de sante pour verifier que le bot fonctionne"""
    return jsonify({
        'status': 'running',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'symbols_monitored': len(CONFIG['SYMBOLS']),
        'safe_active_signals': len([k for k in LAST_SIGNALS.keys() if 'safe' in k]),
        'aggressive_active_signals': len([k for k in LAST_SIGNALS.keys() if 'aggressive' in k]),
        'safe_states': len(SAFE_STATE),
        'aggressive_states': len(AGGRESSIVE_STATE)
    }), 200

@app.route('/state', methods=['GET'])
def get_state():
    """Endpoint pour voir l'etat actuel de tous les symboles"""
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
    """Endpoint pour voir l'etat d'un symbole specifique"""
    symbol_formatted = symbol.replace('-', '/')
    
    if symbol_formatted not in CONFIG['SYMBOLS']:
        return jsonify({'error': 'Symbole non surveille'}), 404
    
    response = {
        'symbol': symbol_formatted,
        'safe': SAFE_STATE.get(symbol_formatted, 'No data'),
        'aggressive': AGGRESSIVE_STATE.get(symbol_formatted, 'No data')
    }
    
    return jsonify(response), 200

# ============================================================================
# GESTION ARRET PROPRE
# ============================================================================

def signal_handler(signum, frame):
    """Gestionnaire pour arret propre du bot"""
    logger.info("\nSignal d'arret recu (Ctrl+C)")
    shutdown_flag.set()
    
    # Message d'arret Telegram
    try:
        stop_msg = "[BOT ARRETE]\n\nArret manuel par utilisateur"
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        requests.post(url, json={
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': stop_msg
        }, timeout=10)
    except:
        pass
    
    sys.exit(0)

# ============================================================================
# POINT D'ENTREE
# ============================================================================

if __name__ == "__main__":
    # Configuration des signaux pour arret propre
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("="*60)
    logger.info("Unified Trading Monitor Bot - Demarrage")
    logger.info("="*60)
    logger.info(f"Mode: Webhook uniquement")
    logger.info(f"Symboles surveilles: {len(CONFIG['SYMBOLS'])}")
    logger.info("Strategies:")
    logger.info("  1. Safe (conservatrice) - Systeme d'etoiles 2★ à 5★")
    logger.info("  2. Aggressive (rapide) - ST Context + SuperTrend AI")
    logger.info(f"Webhook port: {CONFIG['WEBHOOK_PORT']}")
    logger.info("="*60)
    
    # Validation configuration
    if not CONFIG['TELEGRAM_BOT_TOKEN'] or CONFIG['TELEGRAM_BOT_TOKEN'] == 'YOUR_BOT_TOKEN_HERE':
        logger.error("TELEGRAM_BOT_TOKEN non configure")
        sys.exit(1)
    
    if not CONFIG['TELEGRAM_CHAT_ID'] or CONFIG['TELEGRAM_CHAT_ID'] == 'YOUR_CHAT_ID_HERE':
        logger.error("TELEGRAM_CHAT_ID non configure")
        sys.exit(1)
    
    # Envoi message de demarrage
    try:
        start_msg = f"🤖 [BOT DEMARRE]\n\n"
        start_msg += f"Mode: Webhook uniquement\n"
        start_msg += f"Surveillance de {len(CONFIG['SYMBOLS'])} actifs\n\n"
        start_msg += "Strategies actives:\n"
        start_msg += "  1️⃣ Safe (conservatrice)\n"
        start_msg += "     • Systeme 2★ à 5★\n"
        start_msg += "     • Sortie: MACD 3D opposé\n\n"
        start_msg += "  2️⃣ Aggressive (rapide)\n"
        start_msg += "     • ST Context 4H + 1H\n"
        start_msg += "     • SuperTrend AI 1H\n"
        start_msg += "     • Sortie: Biais 4H opposé\n\n"
        start_msg += "Prêt à recevoir les webhooks TradingView ✅"
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        requests.post(url, json={
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': start_msg
        }, timeout=15)
        logger.info("Message de demarrage envoye")
    except Exception as e:
        logger.warning(f"Impossible d'envoyer message de demarrage: {e}")
    
    # Demarrage du serveur Flask (bloquant)
    try:
        logger.info(f"Demarrage serveur webhook sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
        logger.info("Pret a recevoir les webhooks TradingView...")
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