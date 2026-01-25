#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified Trading Monitor Bot - Multi-Strategies (24 janvier 2026)
- Exchange : OKX spot
- Watchlist : fixe dans le code (10 paires)

STRATEGIE 1 (macd_bias):
- 4H : MACD (12, 34, 9)
- 1H : Biais EMA(13) vs SMA(34)

STRATEGIE 2 (st_context):
- 4H : ST Context zones (buy/sell) + Long Term Context (-2 a 2)
- 1H : SuperTrend AI (buy/sell)

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
    
    # Timeframes et indicateurs - Strategie 1 (macd_bias)
    'TF_4H': '4h',
    'MACD_4H_FAST': 12,
    'MACD_4H_SLOW': 34,
    'MACD_4H_SIGNAL': 9,
    
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
LAST_SIGNALS: Dict[str, Dict] = {}  # {'BTC/USDT:macd_bias': {...}, 'BTC/USDT:st_context': {...}}

# Exchange global
exchange: Optional[ccxt.okx] = None

# Etat ST Context par symbole (pour strategie 2)
ST_CONTEXT_STATE: Dict[str, Dict] = {}  # {'ETH/USDT': {'zone_4h': 'buy', 'long_term': 1.5, 'timestamp': ...}}

# Flag pour arret propre
shutdown_flag = threading.Event()

# ============================================================================
# UTILITAIRES
# ============================================================================

def format_tradingview_symbol(tv_symbol: str) -> str:
    """
    Convertit un symbole TradingView en format attendu par le bot
    Exemples:
        OKX:ETHUSDT -> ETH/USDT
        ETHUSDT -> ETH/USDT
        ETH/USDT -> ETH/USDT (déjà au bon format)
    """
    # Enlever le prefix exchange si présent
    if ':' in tv_symbol:
        tv_symbol = tv_symbol.split(':')[-1]
    
    # Si déjà au bon format, retourner tel quel
    if '/' in tv_symbol:
        return tv_symbol
    
    # Convertir ETHUSDT en ETH/USDT
    if 'USDT' in tv_symbol:
        base = tv_symbol.replace('USDT', '')
        return f"{base}/USDT"
    elif 'USDC' in tv_symbol:
        base = tv_symbol.replace('USDC', '')
        return f"{base}/USDC"
    elif 'BUSD' in tv_symbol:
        base = tv_symbol.replace('BUSD', '')
        return f"{base}/BUSD"
    
    # Si format inconnu, retourner tel quel
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

# Desactiver les logs Flask par defaut (sauf erreurs)
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# ============================================================================
# INDICATEURS TECHNIQUES (STRATEGIE 1)
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
# ANALYSE TIMEFRAME (STRATEGIE 1)
# ============================================================================

def analyze_4h(df: pd.DataFrame) -> Optional[Dict]:
    """
    Analyse le timeframe 4H avec MACD
    Retourne un dictionnaire avec les resultats ou None
    """
    if df is None or len(df) < 50:
        return None

    try:
        close = df['close']
        macd_line, signal_line = calculate_macd(
            close, 
            CONFIG['MACD_4H_FAST'], 
            CONFIG['MACD_4H_SLOW'], 
            CONFIG['MACD_4H_SIGNAL']
        )
        
        # Trouver la derniere valeur non-NaN
        idx = -1
        while idx >= -len(close) and (pd.isna(macd_line.iloc[idx]) or pd.isna(signal_line.iloc[idx])):
            idx -= 1
        
        if abs(idx) >= len(close):
            return None
        
        macd_val = macd_line.iloc[idx]
        sig_val = signal_line.iloc[idx]
        
        return {
            'macd_bull': macd_val > sig_val,
            'macd_bear': macd_val < sig_val,
            'macd_line': round(float(macd_val), 6),
            'signal_line': round(float(sig_val), 6),
            'price': round(float(close.iloc[-1]), 6)
        }
    except Exception as e:
        logger.error(f"Erreur analyse 4H: {e}")
        return None

def analyze_1h(df: pd.DataFrame) -> Optional[Dict]:
    """
    Analyse le timeframe 1H avec biais EMA/SMA
    Retourne un dictionnaire avec les resultats ou None
    """
    if df is None or len(df) < 50:
        return None

    try:
        close = df['close']
        ema_val = calculate_ema(close, CONFIG['EMA_1H'])
        sma_val = calculate_sma(close, CONFIG['SMA_1H'])
        
        # Trouver la derniere valeur non-NaN
        idx = -1
        while idx >= -len(close) and (pd.isna(ema_val.iloc[idx]) or pd.isna(sma_val.iloc[idx])):
            idx -= 1
        
        if abs(idx) >= len(close):
            return None
        
        ema = ema_val.iloc[idx]
        sma = sma_val.iloc[idx]
        
        return {
            'bias_bull': ema > sma,
            'bias_bear': ema < sma,
            'ema': round(float(ema), 6),
            'sma': round(float(sma), 6),
            'price': round(float(close.iloc[-1]), 6)
        }
    except Exception as e:
        logger.error(f"Erreur analyse 1H: {e}")
        return None

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
# DETECTION SIGNAL STRATEGIE 1 (MACD_BIAS)
# ============================================================================

def detect_signal_macd_bias(analysis_4h: Optional[Dict], analysis_1h: Optional[Dict]) -> Optional[str]:
    """
    Detecte si un signal LONG ou SHORT est present (strategie MACD + Biais)
    Retourne 'LONG', 'SHORT', ou None
    """
    if not analysis_4h or not analysis_1h:
        return None
    
    # Signal LONG : MACD bull 4H + Biais bull 1H
    if analysis_4h['macd_bull'] and analysis_1h['bias_bull']:
        return 'LONG'
    
    # Signal SHORT : MACD bear 4H + Biais bear 1H
    if analysis_4h['macd_bear'] and analysis_1h['bias_bear']:
        return 'SHORT'
    
    return None

# ============================================================================
# DETECTION SIGNAL STRATEGIE 2 (ST_CONTEXT)
# ============================================================================

def check_st_context_alignment(symbol: str, supertrend_1h: str) -> Optional[str]:
    """
    Verifie l'alignement entre ST Context 4H et SuperTrend AI 1H
    Retourne 'LONG', 'SHORT', ou None
    """
    if symbol not in ST_CONTEXT_STATE:
        logger.debug(f"Pas de zone ST Context 4H pour {symbol}")
        return None

    state = ST_CONTEXT_STATE[symbol]
    zone_4h = state.get('zone_4h')
    long_term = state.get('long_term')

    # Verification long term context
    if long_term is None:
        logger.debug(f"Long term context manquant pour {symbol}")
        return None
    
    if not (-2 <= long_term <= 2):
        logger.info(f"Long term context hors range pour {symbol}: {long_term} (doit etre entre -2 et 2)")
        return None

    # Verification alignement
    supertrend_lower = supertrend_1h.lower()
    zone_lower = zone_4h.lower() if zone_4h else None
    
    if supertrend_lower == 'buy' and zone_lower == 'buy':
        return 'LONG'
    elif supertrend_lower == 'sell' and zone_lower == 'sell':
        return 'SHORT'

    logger.debug(f"Non aligne pour {symbol}: SuperTrend {supertrend_1h} vs zone {zone_4h}")
    return None

# ============================================================================
# ENVOI TELEGRAM
# ============================================================================

def send_telegram_alert_macd_bias(symbol: str, signal_type: str, price: float, a4: Dict, a1: Dict, source: str = "Scanner"):
    """
    Envoie une alerte formatee sur Telegram (strategie MACD + Biais)
    """
    try:
        msg = f"[SIGNAL {signal_type}] {symbol}\n"
        msg += f"Strategie: MACD + Biais\n\n"
        msg += f"Prix : ${price:.4f}\n"
        msg += f"Heure : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
        msg += f"Source : {source}\n\n"
        
        msg += "Timeframe 4H (MACD):\n"
        msg += f"  Signal : {'Bull' if a4['macd_bull'] else 'Bear'}\n"
        msg += f"  MACD Line : {a4['macd_line']:.6f}\n"
        msg += f"  Signal Line : {a4['signal_line']:.6f}\n\n"
        
        msg += "Timeframe 1H (Biais):\n"
        msg += f"  Signal : {'Bull' if a1['bias_bull'] else 'Bear'}\n"
        msg += f"  EMA({CONFIG['EMA_1H']}) : {a1['ema']:.6f}\n"
        msg += f"  SMA({CONFIG['SMA_1H']}) : {a1['sma']:.6f}\n\n"
        
        msg += "ATTENTION: Verifiez SuperTrend AI 20min avant d'entrer\n"
        msg += "INFO: Ce bot ne trade pas automatiquement."
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'disable_web_page_preview': True
        }
        
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Alerte {signal_type} envoyee pour {symbol} [MACD+Biais] (source: {source})")
        return True
        
    except Exception as e:
        logger.error(f"Echec envoi Telegram pour {symbol}: {e}")
        return False

def send_telegram_alert_st_context(symbol: str, signal_type: str, price: float, long_term: float, zone_4h: str, supertrend_1h: str):
    """
    Envoie une alerte formatee sur Telegram (strategie ST Context)
    """
    try:
        msg = f"[SIGNAL {signal_type}] {symbol}\n"
        msg += f"Strategie: ST Context + SuperTrend AI\n\n"
        msg += f"Prix : ${price:.2f}\n"
        msg += f"Heure : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"

        msg += f"ST Context 4H : Zone {zone_4h.upper()}\n"
        msg += f"Long Term Context : {long_term:.2f}\n"
        msg += f"SuperTrend AI 1H : {supertrend_1h.upper()}\n\n"

        msg += "ATTENTION: Verifie SuperTrend AI 20min avant d'entrer\n"
        msg += "INFO: Ce bot ne trade pas automatiquement."

        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'disable_web_page_preview': True
        }

        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Alerte {signal_type} envoyee pour {symbol} [ST Context]")
        return True
        
    except Exception as e:
        logger.error(f"Echec envoi Telegram pour {symbol}: {e}")
        return False

# ============================================================================
# GESTION ANTI-SPAM
# ============================================================================

def get_signal_key(symbol: str, strategy: str) -> str:
    """Genere une cle unique pour le tracking anti-spam"""
    return f"{symbol}:{strategy}"

def should_send_alert(symbol: str, signal_type: str, strategy: str) -> bool:
    """
    Determine si une alerte doit etre envoyee en fonction de l'anti-spam
    """
    now = time.time()
    signal_key = get_signal_key(symbol, strategy)
    
    if signal_key not in LAST_SIGNALS:
        LAST_SIGNALS[signal_key] = {'type': None, 'timestamp': 0, 'price': None}
    
    prev = LAST_SIGNALS[signal_key]
    
    # Nouveau type de signal : toujours envoyer
    if signal_type != prev['type']:
        return True
    
    # Meme signal : verifier le cooldown
    time_elapsed = now - prev['timestamp']
    if time_elapsed >= CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']:
        return True
    
    return False

def update_last_signal(symbol: str, signal_type: str, price: float, strategy: str):
    """
    Met a jour l'etat du dernier signal pour un symbole et une strategie
    """
    signal_key = get_signal_key(symbol, strategy)
    LAST_SIGNALS[signal_key] = {
        'type': signal_type,
        'timestamp': time.time(),
        'price': price
    }

def clear_signal(symbol: str, strategy: str):
    """
    Reinitialise le signal pour un symbole (quand plus de signal actif)
    """
    signal_key = get_signal_key(symbol, strategy)
    if signal_key in LAST_SIGNALS and LAST_SIGNALS[signal_key]['type'] is not None:
        logger.info(f"Signal precedent termine pour {symbol} [{strategy}]")
        LAST_SIGNALS[signal_key]['type'] = None

# ============================================================================
# WEBHOOK TRADINGVIEW
# ============================================================================

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    """
    Endpoint pour recevoir les webhooks TradingView
    
    STRATEGIE 1 (macd_bias):
    {"symbol": "BTC/USDT", "signal": "buy", "price": 43250, "strategy": "macd_bias"}
    
    STRATEGIE 2 (st_context) - ST Context 4H:
    {"symbol": "BTC/USDT", "tf": "4h", "zone": "buy", "long_term": 1.5, "price": 43250, "strategy": "st_context"}
    
    STRATEGIE 2 (st_context) - SuperTrend AI 1H:
    {"symbol": "BTC/USDT", "tf": "1h", "supertrend": "buy", "price": 43250, "strategy": "st_context"}
    """
    try:
        data = request.get_json(silent=True)
        
        if not data:
            logger.warning("Webhook recu sans donnees JSON")
            return jsonify({'status': 'error', 'message': 'Donnees JSON manquantes'}), 400
        
        logger.info(f"Webhook recu: {data}")
        
        # Validation champs requis
        if 'symbol' not in data:
            return jsonify({'status': 'error', 'message': 'Champ symbol manquant'}), 400
        
        if 'strategy' not in data:
            return jsonify({'status': 'error', 'message': 'Champ strategy manquant'}), 400
        
        symbol_raw = data['symbol']
        strategy = data['strategy'].lower()
        price = data.get('price', 0)
        
        # Conversion du symbole TradingView (ETHUSDT ou OKX:ETHUSDT) -> ETH/USDT
        symbol = format_tradingview_symbol(symbol_raw)
        
        # Verification symbole dans watchlist
        if symbol not in CONFIG['SYMBOLS']:
            logger.warning(f"Symbole {symbol} non surveille")
            return jsonify({'status': 'ignored', 'message': 'Symbole non surveille'}), 200
        
        # =====================================================================
        # STRATEGIE 1 : MACD + BIAIS
        # =====================================================================
        if strategy == 'macd_bias':
            if 'signal' not in data:
                return jsonify({'status': 'error', 'message': 'Champ signal manquant pour macd_bias'}), 400
            
            st_signal = data['signal'].lower()
            
            # Initialiser l'exchange si necessaire
            try:
                get_exchange()
            except Exception as e:
                logger.error(f"Impossible d'initialiser l'exchange: {e}")
                return jsonify({'status': 'error', 'message': 'Erreur initialisation exchange'}), 500
            
            # Conversion du signal TradingView
            if st_signal not in ['buy', 'sell']:
                logger.warning(f"Signal invalide: {st_signal}")
                return jsonify({'status': 'error', 'message': 'Signal doit etre buy ou sell'}), 400
            
            signal_type = 'LONG' if st_signal == 'buy' else 'SHORT'
            
            # Recuperation et analyse des donnees
            df_4h = fetch_ohlcv(symbol, CONFIG['TF_4H'])
            df_1h = fetch_ohlcv(symbol, CONFIG['TF_1H'])
            
            a4 = analyze_4h(df_4h)
            a1 = analyze_1h(df_1h)
            
            if not a4 or not a1:
                logger.warning(f"Donnees incompletes pour {symbol}")
                return jsonify({'status': 'error', 'message': 'Donnees incompletes'}), 200
            
            # Verification alignement
            detected_signal = detect_signal_macd_bias(a4, a1)
            
            if detected_signal == signal_type:
                if should_send_alert(symbol, signal_type, 'macd_bias'):
                    actual_price = a1['price']
                    send_telegram_alert_macd_bias(symbol, signal_type, actual_price, a4, a1, source="Webhook TradingView")
                    update_last_signal(symbol, signal_type, actual_price, 'macd_bias')
                    
                    return jsonify({
                        'status': 'success',
                        'message': 'Alerte envoyee',
                        'symbol': symbol,
                        'signal': signal_type,
                        'strategy': 'macd_bias'
                    }), 200
                else:
                    return jsonify({
                        'status': 'cooldown',
                        'message': 'Alerte ignoree (cooldown)',
                        'strategy': 'macd_bias'
                    }), 200
            else:
                return jsonify({
                    'status': 'not_aligned',
                    'message': 'Signal non aligne avec strategie',
                    'tv_signal': signal_type,
                    'detected_signal': detected_signal,
                    'strategy': 'macd_bias'
                }), 200
        
        # =====================================================================
        # STRATEGIE 2 : ST CONTEXT + SUPERTREND AI
        # =====================================================================
        elif strategy == 'st_context':
            if 'tf' not in data:
                return jsonify({'status': 'error', 'message': 'Champ tf manquant pour st_context'}), 400
            
            tf = data['tf'].lower()
            
            # Traitement ST Context 4H
            if tf == '4h':
                long_term = data.get('long_term')
                zone = data.get('zone')
                
                if long_term is None:
                    return jsonify({'status': 'error', 'message': 'Champ long_term requis pour 4h'}), 400
                
                # Si pas de zone specifique, c'est un event d'invalidation (neutral)
                if not zone:
                    zone = 'neutral'
                
                # Mise a jour de l'etat
                ST_CONTEXT_STATE[symbol] = {
                    'zone_4h': zone.lower(),
                    'long_term': float(long_term),
                    'timestamp': time.time()
                }
                
                logger.info(f"ST Context 4H mis a jour pour {symbol}: zone={zone}, long_term={long_term}")
                
                return jsonify({
                    'status': 'success',
                    'message': 'ST Context 4H mis a jour',
                    'symbol': symbol,
                    'zone': zone,
                    'long_term': long_term,
                    'strategy': 'st_context'
                }), 200
            
            # Traitement SuperTrend AI 1H
            elif tf == '1h':
                supertrend = data.get('supertrend')
                
                if not supertrend:
                    return jsonify({'status': 'error', 'message': 'Champ supertrend requis pour 1h'}), 400
                
                # Verification alignement
                signal_type = check_st_context_alignment(symbol, supertrend)
                
                if signal_type:
                    if should_send_alert(symbol, signal_type, 'st_context'):
                        state = ST_CONTEXT_STATE[symbol]
                        send_telegram_alert_st_context(
                            symbol, 
                            signal_type, 
                            price, 
                            state['long_term'], 
                            state['zone_4h'], 
                            supertrend
                        )
                        update_last_signal(symbol, signal_type, price, 'st_context')
                        
                        return jsonify({
                            'status': 'success',
                            'message': 'Alerte envoyee',
                            'symbol': symbol,
                            'signal': signal_type,
                            'strategy': 'st_context'
                        }), 200
                    else:
                        return jsonify({
                            'status': 'cooldown',
                            'message': 'Alerte ignoree (cooldown)',
                            'strategy': 'st_context'
                        }), 200
                else:
                    return jsonify({
                        'status': 'not_aligned',
                        'message': 'Signal non aligne avec ST Context 4H',
                        'symbol': symbol,
                        'supertrend_1h': supertrend,
                        'zone_4h': ST_CONTEXT_STATE.get(symbol, {}).get('zone_4h', 'unknown'),
                        'strategy': 'st_context'
                    }), 200
            
            else:
                return jsonify({'status': 'error', 'message': f'Timeframe invalide: {tf} (doit etre 4h ou 1h)'}), 400
        
        else:
            return jsonify({'status': 'error', 'message': f'Strategie invalide: {strategy} (doit etre macd_bias ou st_context)'}), 400
        
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
        'active_signals': {
            'macd_bias': len([k for k, v in LAST_SIGNALS.items() if 'macd_bias' in k and v['type'] is not None]),
            'st_context': len([k for k, v in LAST_SIGNALS.items() if 'st_context' in k and v['type'] is not None])
        },
        'st_context_states': len(ST_CONTEXT_STATE)
    }), 200

@app.route('/state', methods=['GET'])
def get_state():
    """Endpoint pour voir l'etat actuel de tous les symboles"""
    return jsonify({
        'last_signals': LAST_SIGNALS,
        'st_context_state': {
            symbol: {
                'zone_4h': data.get('zone_4h'),
                'long_term': data.get('long_term'),
                'age_seconds': time.time() - data.get('timestamp', 0)
            }
            for symbol, data in ST_CONTEXT_STATE.items()
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
        'strategies': {}
    }
    
    # Info strategie MACD + Biais
    macd_key = get_signal_key(symbol_formatted, 'macd_bias')
    if macd_key in LAST_SIGNALS:
        response['strategies']['macd_bias'] = LAST_SIGNALS[macd_key]
    
    # Info strategie ST Context
    st_key = get_signal_key(symbol_formatted, 'st_context')
    if st_key in LAST_SIGNALS:
        response['strategies']['st_context'] = {
            'last_signal': LAST_SIGNALS[st_key],
            'st_context_4h': ST_CONTEXT_STATE.get(symbol_formatted, 'No data')
        }
    
    return jsonify(response), 200

# ============================================================================
# BOUCLE PRINCIPALE DE MONITORING (DESACTIVEE - WEBHOOK ONLY)
# ============================================================================

def main_scanning_loop():
    """
    Boucle principale desactivee - Le bot fonctionne uniquement par webhooks
    """
    logger.info("Mode webhook uniquement - Pas de scanning automatique")
    
    # Message de demarrage Telegram
    try:
        start_msg = f"[BOT DEMARRE]\n\n"
        start_msg += f"Mode: Webhook uniquement\n"
        start_msg += f"Surveillance de {len(CONFIG['SYMBOLS'])} actifs:\n"
        start_msg += "\n".join([f"  - {s}" for s in CONFIG['SYMBOLS']])
        start_msg += f"\n\nStrategies actives:\n"
        start_msg += "  1. MACD + Biais (webhook)\n"
        start_msg += "  2. ST Context + SuperTrend AI (webhook)\n"
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        requests.post(url, json={
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': start_msg
        }, timeout=15)
        logger.info("Message de demarrage envoye")
    except Exception as e:
        logger.warning(f"Impossible d'envoyer message de demarrage: {e}")

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
    logger.info("  1. MACD + Biais (webhook)")
    logger.info("  2. ST Context + SuperTrend AI (webhook)")
    logger.info(f"Webhook port: {CONFIG['WEBHOOK_PORT']}")
    logger.info("="*60)
    
    # Validation configuration
    if CONFIG['TELEGRAM_BOT_TOKEN'] == 'YOUR_BOT_TOKEN_HERE':
        logger.error("TELEGRAM_BOT_TOKEN non configure")
        sys.exit(1)
    
    if CONFIG['TELEGRAM_CHAT_ID'] == 'YOUR_CHAT_ID_HERE':
        logger.error("TELEGRAM_CHAT_ID non configure")
        sys.exit(1)
    
    # Envoi message de demarrage
    main_scanning_loop()
    
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