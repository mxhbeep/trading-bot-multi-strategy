#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Asset Trading Monitor Bot - Version finale propre (janvier 2026)
4H : MACD (12, 34, 9)
1H : Biais EMA(13) vs SMA(34)
Alerte quand alignement + signal ST Context via webhook TradingView
Vérification manuelle SuperTrend AI 20min

Dépendances locales : pip install ccxt pandas numpy requests flask gunicorn

Lancement local : python trading_bot.py
Webhook TradingView : https://mxh-trading-bot-8993c334a44a.herokuapp.com/webhook
"""

import ccxt
import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime, timezone
import logging
import os
import json
from flask import Flask, request
import threading

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    'EXCHANGE': 'okx',
    'API_KEY': '',
    'SECRET': '',
    
    'TELEGRAM_BOT_TOKEN': '8110041550:AAHJKAWxIG1ZBjZ8fRfFMKq-4iTeo5v4-Hw',
    'TELEGRAM_CHAT_ID': '6473214015',
    
    'WATCHLIST_FILE': 'watchlist.txt',
    
    # Timeframes et indicateurs
    'TF_4H': '4h',
    'MACD_4H_FAST': 12,
    'MACD_4H_SLOW': 34,
    'MACD_4H_SIGNAL': 9,
    
    'TF_1H': '1h',
    'EMA_1H': 13,
    'SMA_1H': 34,
    
    # Paramètres bot
    'CHECK_INTERVAL': 300,          # 5 minutes
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,  # 30 min mini entre 2 alertes identiques par symbole
    'DATA_LIMIT': 300,
    'RETRY_DELAY': 12,
    'MAX_RETRIES': 4,
    
    # Webhook Flask
    'WEBHOOK_PORT': 5000,
    'WEBHOOK_HOST': '0.0.0.0',
}

# État anti-spam par symbole
LAST_SIGNALS = {}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)

# Variables globales
symbols = []
exchange = None

# ============================================================================
# INDICATEURS
# ============================================================================

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period).mean()

def macd(series: pd.Series, fast: int, slow: int, sig: int):
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, sig)
    return macd_line, signal_line

# ============================================================================
# ANALYSE TIMEFRAME
# ============================================================================

def analyze_tf(df: pd.DataFrame, tf: str) -> dict | None:
    if df is None or len(df) < 100:
        logger.debug(f"Données insuffisantes pour {tf}")
        return None

    close = df['close']

    if tf == '4h':
        macd_line, sig_line = macd(close, CONFIG['MACD_4H_FAST'], CONFIG['MACD_4H_SLOW'], CONFIG['MACD_4H_SIGNAL'])
        idx = -1
        while idx > -len(close) and pd.isna(macd_line.iloc[idx]):
            idx -= 1
        if idx == -len(close):
            return None
        macd_val = macd_line.iloc[idx]
        sig_val = sig_line.iloc[idx]
        return {
            'macd_bull': macd_val > sig_val,
            'macd_bear': macd_val < sig_val,
            'macd_line': round(macd_val, 4),
            'signal_line': round(sig_val, 4),
            'price': round(close.iloc[-1], 2)
        }

    else:  # 1h
        em = ema(close, CONFIG['EMA_1H'])
        sm = sma(close, CONFIG['SMA_1H'])
        idx = -1
        while idx > -len(close) and pd.isna(sm.iloc[idx]):
            idx -= 1
        if idx == -len(close):
            return None
        em_val = em.iloc[idx]
        sm_val = sm.iloc[idx]
        return {
            'bias_bull': em_val > sm_val,
            'bias_bear': em_val < sm_val,
            'ema': round(em_val, 2),
            'sma': round(sm_val, 2),
            'price': round(close.iloc[-1], 2)
        }

# ============================================================================
# RÉCUPÉRATION OHLCV
# ============================================================================

def fetch_ohlcv(symbol: str, timeframe: str) -> pd.DataFrame | None:
    for attempt in range(CONFIG['MAX_RETRIES']):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=CONFIG['DATA_LIMIT'])
            if not ohlcv:
                raise ValueError("Réponse vide")
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.warning(f"{symbol} {timeframe} erreur (essai {attempt+1}): {e}")
            time.sleep(CONFIG['RETRY_DELAY'])
    return None

# ============================================================================
# ENVOI TELEGRAM
# ============================================================================

def send_alert(symbol: str, signal_type: str, price: float, a4: dict, a1: dict, st_context_signal: str = None):
    msg = f"{signal_type} - {symbol}\n\n"
    msg += f"Prix : ${price:.2f}\n"
    msg += f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"

    msg += "4H  •  MACD : " + ("Bull" if a4['macd_bull'] else "Bear") + "\n"
    msg += f"     •  MACD line : {a4['macd_line']}\n"
    msg += f"     •  Signal line : {a4['signal_line']}\n\n"

    msg += "1H  •  Biais : " + ("Bull" if a1['bias_bull'] else "Bear") + "\n"
    msg += f"     •  EMA({CONFIG['EMA_1H']}) : {a1['ema']}\n"
    msg += f"     •  SMA({CONFIG['SMA_1H']}) : {a1['sma']}\n\n"

    if st_context_signal:
        msg += f"ST Context : {st_context_signal}\n\n"

    msg += "Vérifie SuperTrend AI 20min avant d'entrer\n"
    msg += "Ce bot ne trade pas automatiquement."

    url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
    payload = {'chat_id': CONFIG['TELEGRAM_CHAT_ID'], 'text': msg, 'parse_mode': 'HTML'}

    try:
        r = requests.post(url, json=payload, timeout=12)
        r.raise_for_status()
        logger.info(f"Alerte {signal_type} envoyée pour {symbol}")
    except Exception as e:
        logger.error(f"Échec envoi Telegram pour {symbol}: {e}")

# ============================================================================
# DÉTECTION SIGNAL (MACD 4H + Biais 1H)
# ============================================================================

def get_signal(a4: dict, a1: dict) -> str | None:
    if not a4 or not a1:
        return None
    if a4['macd_bull'] and a1['bias_bull']:
        return 'LONG'
    if a4['macd_bear'] and a1['bias_bear']:
        return 'SHORT'
    return None

# ============================================================================
# LECTURE WATCHLIST
# ============================================================================

def load_watchlist() -> list[str]:
    path = CONFIG['WATCHLIST_FILE']
    if not os.path.exists(path):
        logger.error(f"Fichier watchlist introuvable : {path}")
        return []
    
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()
    
    symbols_list = [line.strip() for line in lines if line.strip() and not line.strip().startswith('#')]
    logger.info(f"Watchlist chargée : {len(symbols_list)} actifs")
    return symbols_list

# ============================================================================
# WEBHOOK TRADINGVIEW (ST Context)
# ============================================================================

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    if not data:
        logger.warning("Webhook reçu sans JSON")
        return "Données invalides", 400

    logger.info(f"Webhook reçu : {data}")

    try:
        symbol = data['symbol']
        st_signal = data['signal']  # ex: 'buy' ou 'sell'
        price = data.get('price', 0)  # optionnel

        if symbol not in symbols:
            logger.warning(f"Symbole {symbol} non dans watchlist")
            return "Symbole non surveillé", 200

        df4 = fetch_ohlcv(symbol, CONFIG['TF_4H'])
        df1 = fetch_ohlcv(symbol, CONFIG['TF_1H'])

        a4 = analyze_tf(df4, '4h')
        a1 = analyze_tf(df1, '1h')

        if not a4 or not a1:
            logger.warning(f"Données incomplètes pour {symbol}")
            return "Données incomplètes", 200

        signal_type = 'LONG' if st_signal.lower() == 'buy' else 'SHORT' if st_signal.lower() == 'sell' else None

        if signal_type and get_signal(a4, a1) == signal_type:
            send_alert(symbol, signal_type, a4['price'], a4, a1, f"Signal ST Context : {st_signal}")
        else:
            logger.info(f"Signal ST Context {st_signal} pour {symbol} non aligné")

        return "Webhook traité", 200

    except KeyError as e:
        logger.error(f"Clé manquante dans webhook : {e}")
        return f"Clé manquante : {e}", 400

    except Exception as e:
        logger.error(f"Erreur inattendue dans webhook : {e}")
        return "Erreur interne", 500

# ============================================================================
# BOUCLE PRINCIPALE (check périodique)
# ============================================================================

def main_loop():
    global symbols, exchange

    symbols = load_watchlist()
    if not symbols:
        logger.error("Aucun symbole dans la watchlist → arrêt")
        return

    # Message de démarrage Telegram
    try:
        start_msg = f"Bot démarré - Surveillance de {len(symbols)} actifs :\n"
        start_msg += "\n".join([f"• {s}" for s in symbols[:12]])
        if len(symbols) > 12:
            start_msg += f"\n... et {len(symbols)-12} autres"
        start_msg += f"\n\nIntervalle : {CONFIG['CHECK_INTERVAL']/60} min"

        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        r = requests.post(url, json={
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': start_msg,
            'parse_mode': 'HTML'
        }, timeout=12)
        if r.status_code == 200:
            logger.info("Message de démarrage envoyé")
        else:
            logger.warning(f"Échec message démarrage (code {r.status_code})")
    except Exception as e:
        logger.warning(f"Impossible d'envoyer message de démarrage : {e}")

    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })

    while True:
        try:
            for symbol in symbols:
                logger.debug(f"Analyse {symbol}...")
                df4 = fetch_ohlcv(symbol, CONFIG['TF_4H'])
                df1 = fetch_ohlcv(symbol, CONFIG['TF_1H'])

                a4 = analyze_tf(df4, '4h')
                a1 = analyze_tf(df1, '1h')

                if not a4 or not a1:
                    logger.debug(f"Données incomplètes pour {symbol}")
                    continue

                price = a1['price']
                signal = get_signal(a4, a1)

                if not signal:
                    if symbol in LAST_SIGNALS and LAST_SIGNALS[symbol]['type'] is not None:
                        logger.info(f"{symbol} : signal précédent terminé")
                        LAST_SIGNALS[symbol]['type'] = None
                    continue

                now = time.time()
                if symbol not in LAST_SIGNALS:
                    LAST_SIGNALS[symbol] = {'type': None, 'timestamp': 0, 'price': None}

                prev = LAST_SIGNALS[symbol]
                cooldown_ok = now - prev['timestamp'] >= CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']

                if signal != prev['type'] or cooldown_ok:
                    logger.info(f"Signal {signal} détecté sur {symbol} @ ${price:.2f}")
                    send_alert(symbol, signal, price, a4, a1)
                    LAST_SIGNALS[symbol] = {'type': signal, 'timestamp': now, 'price': price}
                else:
                    logger.debug(f"{symbol} : {signal} déjà récent – pas d'alerte")

            time.sleep(CONFIG['CHECK_INTERVAL'])

        except KeyboardInterrupt:
            logger.info("Arrêt demandé")
            break
        except Exception as e:
            logger.error(f"Erreur boucle principale : {e}")
            time.sleep(60)

# ============================================================================
# DÉMARRAGE
# ============================================================================

if __name__ == "__main__":
    logger.info("Démarrage bot local")
    print("Module trading_bot chargé avec succès")  # Test import

    # Lance la boucle de check en thread séparé
    threading.Thread(target=main_loop, daemon=True).start()

    # Lance le serveur Flask pour recevoir les webhooks
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)