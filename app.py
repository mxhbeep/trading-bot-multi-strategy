#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trading Monitor Bot - Version Optimisee (24 janvier 2026)
- Exchange : OKX spot
- Watchlist : fixe dans le code (10 paires)
- 4H : MACD (12, 34, 9)
- 1H : Biais EMA(13) vs SMA(34)
- Webhook TradingView pour ST Context / SuperTrend AI
- Alertes Telegram avec anti-spam intelligent
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
    
    # Timeframes et indicateurs
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
    
    # Securite
    'WEBHOOK_SECRET': 'your_secret_key_here',   # Optionnel : cle secrete pour webhook
}

# ============================================================================
# ETAT GLOBAL
# ============================================================================

# Etat anti-spam par symbole
LAST_SIGNALS: Dict[str, Dict] = {}

# Exchange global
exchange: Optional[ccxt.okx] = None

# Flag pour arret propre
shutdown_flag = threading.Event()

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
    return "Bot trading OK - Monitoring actif"

# Desactiver les logs Flask par defaut (sauf erreurs)
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
# ANALYSE TIMEFRAME
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
# DETECTION SIGNAL
# ============================================================================

def detect_signal(analysis_4h: Optional[Dict], analysis_1h: Optional[Dict]) -> Optional[str]:
    """
    Detecte si un signal LONG ou SHORT est present
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
# ENVOI TELEGRAM
# ============================================================================

def send_telegram_alert(symbol: str, signal_type: str, price: float, a4: Dict, a1: Dict, source: str = "Scanner"):
    """
    Envoie une alerte formatee sur Telegram
    """
    try:
        # Construction du message
        msg = f"[SIGNAL {signal_type}] {symbol}\n\n"
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
        
        # Envoi
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': msg,
            'disable_web_page_preview': True
        }
        
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        
        logger.info(f"Alerte {signal_type} envoyee pour {symbol} (source: {source})")
        return True
        
    except Exception as e:
        logger.error(f"Echec envoi Telegram pour {symbol}: {e}")
        return False

# ============================================================================
# GESTION ANTI-SPAM
# ============================================================================

def should_send_alert(symbol: str, signal_type: str) -> bool:
    """
    Determine si une alerte doit etre envoyee en fonction de l'anti-spam
    """
    now = time.time()
    
    if symbol not in LAST_SIGNALS:
        LAST_SIGNALS[symbol] = {'type': None, 'timestamp': 0, 'price': None}
    
    prev = LAST_SIGNALS[symbol]
    
    # Nouveau type de signal : toujours envoyer
    if signal_type != prev['type']:
        return True
    
    # Meme signal : verifier le cooldown
    time_elapsed = now - prev['timestamp']
    if time_elapsed >= CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']:
        return True
    
    return False

def update_last_signal(symbol: str, signal_type: str, price: float):
    """
    Met a jour l'etat du dernier signal pour un symbole
    """
    LAST_SIGNALS[symbol] = {
        'type': signal_type,
        'timestamp': time.time(),
        'price': price
    }

def clear_signal(symbol: str):
    """
    Reinitialise le signal pour un symbole (quand plus de signal actif)
    """
    if symbol in LAST_SIGNALS and LAST_SIGNALS[symbol]['type'] is not None:
        logger.info(f"Signal precedent termine pour {symbol}")
        LAST_SIGNALS[symbol]['type'] = None

# ============================================================================
# WEBHOOK TRADINGVIEW
# ============================================================================

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    """
    Endpoint pour recevoir les webhooks TradingView
    Format attendu: {"symbol": "BTC/USDT", "signal": "buy", "price": 43250.50}
    """
    try:
        # Recuperation des donnees
        data = request.get_json(silent=True)
        
        if not data:
            logger.warning("Webhook recu sans donnees JSON")
            return jsonify({'status': 'error', 'message': 'Donnees JSON manquantes'}), 400
        
        logger.info(f"Webhook recu: {data}")
        
        # Validation des champs requis
        required_fields = ['symbol', 'signal']
        missing_fields = [f for f in required_fields if f not in data]
        
        if missing_fields:
            logger.warning(f"Champs manquants dans webhook: {missing_fields}")
            return jsonify({'status': 'error', 'message': f'Champs manquants: {missing_fields}'}), 400
        
        symbol = data['symbol']
        st_signal = data['signal'].lower()
        price = data.get('price', 0)
        
        # Initialiser l'exchange si necessaire
        try:
            get_exchange()
        except Exception as e:
            logger.error(f"Impossible d'initialiser l'exchange: {e}")
            return jsonify({'status': 'error', 'message': 'Erreur initialisation exchange'}), 500
        
        # Verification du symbole dans la watchlist
        if symbol not in CONFIG['SYMBOLS']:
            logger.warning(f"Symbole {symbol} non surveille (watchlist: {CONFIG['SYMBOLS']})")
            return jsonify({'status': 'ignored', 'message': 'Symbole non surveille'}), 200
        
        # Conversion du signal TradingView
        if st_signal not in ['buy', 'sell']:
            logger.warning(f"Signal invalide: {st_signal}")
            return jsonify({'status': 'error', 'message': 'Signal doit etre buy ou sell'}), 400
        
        signal_type = 'LONG' if st_signal == 'buy' else 'SHORT'
        
        # Recuperation et analyse des donnees
        logger.debug(f"Recuperation donnees 4H pour {symbol}...")
        df_4h = fetch_ohlcv(symbol, CONFIG['TF_4H'])
        
        logger.debug(f"Recuperation donnees 1H pour {symbol}...")
        df_1h = fetch_ohlcv(symbol, CONFIG['TF_1H'])
        
        # Analyse
        a4 = analyze_4h(df_4h)
        a1 = analyze_1h(df_1h)
        
        if not a4 or not a1:
            logger.warning(f"Donnees incompletes pour {symbol}")
            return jsonify({'status': 'error', 'message': 'Donnees incompletes'}), 200
        
        # Verification alignement avec notre strategie
        detected_signal = detect_signal(a4, a1)
        
        if detected_signal == signal_type:
            # Signal aligne : envoyer l'alerte si anti-spam OK
            if should_send_alert(symbol, signal_type):
                actual_price = a1['price']
                send_telegram_alert(symbol, signal_type, actual_price, a4, a1, source="Webhook TradingView")
                update_last_signal(symbol, signal_type, actual_price)
                
                return jsonify({
                    'status': 'success',
                    'message': 'Alerte envoyee',
                    'symbol': symbol,
                    'signal': signal_type
                }), 200
            else:
                logger.info(f"Signal {signal_type} pour {symbol} - cooldown actif (pas d'alerte)")
                return jsonify({
                    'status': 'cooldown',
                    'message': 'Alerte ignoree (cooldown)'
                }), 200
        else:
            logger.info(f"Signal {st_signal} TradingView pour {symbol} non aligne avec strategie (detecte: {detected_signal})")
            return jsonify({
                'status': 'not_aligned',
                'message': 'Signal non aligne avec strategie',
                'tv_signal': signal_type,
                'detected_signal': detected_signal
            }), 200
        
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
        'active_signals': len([s for s in LAST_SIGNALS.values() if s['type'] is not None])
    }), 200

# ============================================================================
# BOUCLE PRINCIPALE DE MONITORING
# ============================================================================

def main_scanning_loop():
    """
    Boucle principale qui scanne periodiquement tous les symboles
    """
    symbols = CONFIG['SYMBOLS']
    logger.info(f"Watchlist chargee: {len(symbols)} actifs")
    logger.info(f"Symboles: {', '.join(symbols)}")
    
    # Initialiser l'exchange
    try:
        get_exchange()
    except Exception as e:
        logger.error(f"Impossible d'initialiser l'exchange dans le scanner: {e}")
        return
    
    # Message de demarrage Telegram
    try:
        start_msg = f"[BOT DEMARRE]\n\n"
        start_msg += f"Surveillance de {len(symbols)} actifs:\n"
        start_msg += "\n".join([f"  - {s}" for s in symbols])
        start_msg += f"\n\nIntervalle: {CONFIG['CHECK_INTERVAL']/60:.0f} min"
        start_msg += f"\nAnti-spam: {CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']/60:.0f} min"
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        requests.post(url, json={
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': start_msg
        }, timeout=15)
        logger.info("Message de demarrage envoye")
    except Exception as e:
        logger.warning(f"Impossible d'envoyer message de demarrage: {e}")
    
    # Boucle principale
    iteration = 0
    while not shutdown_flag.is_set():
        try:
            iteration += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"Scan #{iteration} - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info(f"{'='*60}")
            
            for symbol in symbols:
                if shutdown_flag.is_set():
                    break
                
                try:
                    # Recuperation des donnees
                    df_4h = fetch_ohlcv(symbol, CONFIG['TF_4H'])
                    df_1h = fetch_ohlcv(symbol, CONFIG['TF_1H'])
                    
                    # Analyse
                    a4 = analyze_4h(df_4h)
                    a1 = analyze_1h(df_1h)
                    
                    if not a4 or not a1:
                        logger.debug(f"Donnees incompletes pour {symbol}")
                        continue
                    
                    # Detection du signal
                    signal = detect_signal(a4, a1)
                    price = a1['price']
                    
                    if signal:
                        # Signal detecte
                        if should_send_alert(symbol, signal):
                            logger.info(f"Signal {signal} detecte sur {symbol} @ ${price:.4f}")
                            send_telegram_alert(symbol, signal, price, a4, a1, source="Scanner periodique")
                            update_last_signal(symbol, signal, price)
                        else:
                            logger.debug(f"Signal {signal} pour {symbol} - cooldown actif")
                    else:
                        # Plus de signal actif
                        clear_signal(symbol)
                    
                    # Petit delai entre symboles pour eviter rate limit
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Erreur lors du traitement de {symbol}: {e}")
                    continue
            
            # Resume du scan
            active_signals = sum(1 for s in LAST_SIGNALS.values() if s['type'] is not None)
            logger.info(f"Scan termine - Signaux actifs: {active_signals}/{len(symbols)}")
            
            # Attente avant prochain scan
            logger.info(f"Prochain scan dans {CONFIG['CHECK_INTERVAL']}s...")
            shutdown_flag.wait(CONFIG['CHECK_INTERVAL'])
            
        except Exception as e:
            logger.error(f"Erreur dans la boucle principale: {e}", exc_info=True)
            if not shutdown_flag.is_set():
                logger.info("Attente de 60s avant retry...")
                shutdown_flag.wait(60)

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
    logger.info("Trading Monitor Bot - Demarrage")
    logger.info("="*60)
    logger.info(f"Exchange: {CONFIG['EXCHANGE'].upper()}")
    logger.info(f"Symboles surveilles: {len(CONFIG['SYMBOLS'])}")
    logger.info(f"Webhook port: {CONFIG['WEBHOOK_PORT']}")
    logger.info("="*60)
    
    # Validation configuration
    if CONFIG['TELEGRAM_BOT_TOKEN'] == 'YOUR_BOT_TOKEN_HERE':
        logger.error("TELEGRAM_BOT_TOKEN non configure")
        sys.exit(1)
    
    if CONFIG['TELEGRAM_CHAT_ID'] == 'YOUR_CHAT_ID_HERE':
        logger.error("TELEGRAM_CHAT_ID non configure")
        sys.exit(1)
    
    # Demarrage du thread de scanning
    scanner_thread = threading.Thread(target=main_scanning_loop, daemon=True, name="Scanner")
    scanner_thread.start()
    logger.info("Thread de scanning demarre")
    
    # Petit delai pour laisser le temps au scanner de s'initialiser
    time.sleep(2)
    
    # Demarrage du serveur Flask (bloquant)
    try:
        logger.info(f"Demarrage serveur webhook sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
        app.run(
            host=CONFIG['WEBHOOK_HOST'],
            port=CONFIG['WEBHOOK_PORT'],
            debug=False,
            use_reloader=False  # Important pour eviter double demarrage
        )
    except Exception as e:
        logger.error(f"Erreur serveur Flask: {e}")
        shutdown_flag.set()
        sys.exit(1)