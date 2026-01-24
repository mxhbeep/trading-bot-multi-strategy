#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trading Monitor Bot - Version finale propre (24 janvier 2026)
- Exchange : OKX spot
- Watchlist : fixe dans le code (10 paires)
- 4H : MACD (12, 34, 9)
- 1H : Biais EMA(13) vs SMA(34)
- Webhook TradingView pour ST Context / SuperTrend AI
- Alertes Telegram quand alignement
"""

import ccxt
import pandas as pd
import numpy as np
import time
import requests
from datetime import datetime, timezone
import logging
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

# Globales
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
    if df is None or len(df) < 50:
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

def send_alert(symbol: str, signal_type: str, price: float, a4: dict, a1: dict):
    msg = f"{signal_type} - {symbol}\n\n"
    msg += f"Prix : ${price:.2f}\n"
    msg += f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"

    msg += "4H  •  MACD : " + ("Bull" if a4['macd_bull'] else "Bear") + "\n"
    msg += f"     •  MACD line : {a4['macd_line']}\n"
    msg += f"     •  Signal line : {a4['signal_line']}\n\n"

    msg += "1H  •  Biais : " + ("Bull" if a1['bias_bull'] else "Bear") + "\n"
    msg += f"     •  EMA({CONFIG['EMA_1H']}) : {a1['ema']}\n"
    msg += f"     •  SMA({CONFIG['SMA_1H']}) : {a1['sma']}\n\n"

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
# DÉTECTION SIGNAL
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
# WEBHOOK TRADINGVIEW (ST Context)
# ============================================================================

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        logger.warning("Webhook reçu sans JSON")
        return "Données invalides", 400

    logger.info(f"Webhook reçu : {data}")

    global symbols  # ← AJOUTE CETTE LIGNE ICI (résout le NameError)

    try:
        symbol = data['symbol']
        st_signal = data['signal']
        price = data.get('price', 0)

        logger.info(f"Symbole reçu : {symbol}")
        logger.info(f"Signal reçu : {st_signal}")

        if symbol not in symbols:
            logger.warning(f"Symbole {symbol} non dans watchlist")
            return "Symbole non surveillé", 200

        # ... le reste du code (fetch, analyse, etc.)
        logger.info("Symbole trouvé - fetch 4H...")
        df4 = fetch_ohlcv(symbol, CONFIG['TF_4H'])
        logger.info("Fetch 4H terminé - fetch 1H...")
        df1 = fetch_ohlcv(symbol, CONFIG['TF_1H'])

        logger.info("Fetch terminé - analyse 4H...")
        a4 = analyze_tf(df4, '4h')
        logger.info("Analyse 4H terminée - analyse 1H...")
        a1 = analyze_tf(df1, '1h')

        if not a4 or not a1:
            logger.warning(f"Données incomplètes pour {symbol}")
            return "Données incomplètes", 200

        logger.info("Analyse terminée - détection signal...")
        signal_type = 'LONG' if st_signal.lower() == 'buy' else 'SHORT' if st_signal.lower() == 'sell' else None

        if signal_type and get_signal(a4, a1) == signal_type:
            send_alert(symbol, signal_type, a4['price'], a4, a1)
        else:
            logger.info(f"Signal {st_signal} pour {symbol} non aligné")

        return "Webhook traité", 200

    except KeyError as e:
        logger.error(f"Clé manquante dans webhook : {e}")
        return f"Clé manquante : {e}", 400

    except Exception as e:
        logger.error(f"Erreur dans webhook : {type(e).__name__} - {str(e)}", exc_info=True)
        return "Erreur interne du serveur", 500

# ============================================================================
# BOUCLE PRINCIPALE
# ============================================================================

def main_loop():
    global symbols, exchange

    symbols = CONFIG['SYMBOLS']
    logger.info(f"Watchlist chargée : {len(symbols)} actifs ({', '.join(symbols)})")

    # Message de démarrage Telegram
    try:
        start_msg = f"Bot démarré - Surveillance de {len(symbols)} actifs :\n"
        start_msg += "\n".join([f"• {s}" for s in symbols])
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
    print("Module app chargé avec succès")  # Test import

    # Lance la boucle de check en thread
    threading.Thread(target=main_loop, daemon=True).start()

    # Lance Flask
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)