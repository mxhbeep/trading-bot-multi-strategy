#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ST Super Bot - Detection zones ST Context 4H + signaux SuperTrend AI 1H
- Webhook pour ST Context 4H (zone buy/sell + long term context value)
- Webhook pour SuperTrend AI 1H (buy/sell)
- Condition : long term context entre -2 et 2
- Alerte Telegram si aligne
- Watchlist fixe dans le code

Dependencies : pip install flask requests ccxt
Lancement : python st_super_bot.py
Webhook : https://votre-app-heroku.herokuapp.com/webhook
"""

import requests
from datetime import datetime, timezone
import logging
import time
import signal
import sys
from flask import Flask, request, jsonify
import threading
from typing import Optional, Dict

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    'TELEGRAM_BOT_TOKEN': '8110041550:AAHJKAWxIG1ZBjZ8fRfFMKq-4iTeo5v4-Hw',
    'TELEGRAM_CHAT_ID': '6473214015',
    
    # Paires fixes (modifiable ici)
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
    
    # Webhook Flask
    'WEBHOOK_PORT': 5000,
    'WEBHOOK_HOST': '0.0.0.0',
    
    # Parametres bot
    'MIN_TIME_BETWEEN_SAME_ALERT': 1800,  # 30 min anti-spam
}

# ============================================================================
# ETAT GLOBAL
# ============================================================================

# Etat par symbole : zone ST Context 4H et long term context
STATE: Dict[str, Dict] = {}  # {'ETH/USDT': {'zone_4h': 'buy', 'long_term': 1.5, 'timestamp': ...}}

# Derniers signaux envoyes (anti-spam)
LAST_SIGNALS: Dict[str, Dict] = {}

# Flag pour arret propre
shutdown_flag = threading.Event()

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

# Desactiver les logs Flask par defaut (sauf erreurs)
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# ============================================================================
# ENVOI TELEGRAM
# ============================================================================

def send_telegram_alert(symbol: str, signal_type: str, price: float, long_term: float, zone_4h: str, supertrend_1h: str):
    """Envoie une alerte formatee sur Telegram"""
    try:
        msg = f"[SIGNAL {signal_type}] {symbol}\n\n"
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
        
        logger.info(f"Alerte {signal_type} envoyee pour {symbol}")
        return True
        
    except Exception as e:
        logger.error(f"Echec envoi Telegram pour {symbol}: {e}")
        return False

# ============================================================================
# GESTION ANTI-SPAM
# ============================================================================

def should_send_alert(symbol: str, signal_type: str) -> bool:
    """Determine si une alerte doit etre envoyee en fonction de l'anti-spam"""
    now = time.time()
    
    if symbol not in LAST_SIGNALS:
        LAST_SIGNALS[symbol] = {'type': None, 'timestamp': 0}
    
    prev = LAST_SIGNALS[symbol]
    
    # Nouveau type de signal : toujours envoyer
    if signal_type != prev['type']:
        return True
    
    # Meme signal : verifier le cooldown
    time_elapsed = now - prev['timestamp']
    if time_elapsed >= CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']:
        return True
    
    return False

def update_last_signal(symbol: str, signal_type: str):
    """Met a jour l'etat du dernier signal pour un symbole"""
    LAST_SIGNALS[symbol] = {
        'type': signal_type,
        'timestamp': time.time()
    }

# ============================================================================
# DETECTION SIGNAL
# ============================================================================

def check_alignment(symbol: str, supertrend_1h: str) -> Optional[str]:
    """
    Verifie l'alignement entre ST Context 4H et SuperTrend AI 1H
    Retourne 'LONG', 'SHORT', ou None
    """
    if symbol not in STATE:
        logger.debug(f"Pas de zone ST Context 4H pour {symbol}")
        return None

    state = STATE[symbol]
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
# WEBHOOK TRADINGVIEW
# ============================================================================

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    """
    Endpoint pour recevoir les webhooks TradingView
    
    Format ST Context 4H:
    {"symbol": "BTC/USDT", "tf": "4h", "zone": "buy", "long_term": 1.5, "price": 43250}
    
    Format SuperTrend AI 1H:
    {"symbol": "BTC/USDT", "tf": "1h", "supertrend": "buy", "price": 43250}
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
        
        if 'tf' not in data:
            return jsonify({'status': 'error', 'message': 'Champ tf manquant'}), 400

        symbol = data['symbol']
        tf = data.get('tf', 'unknown').lower()
        price = data.get('price', 0)

        # Verification symbole dans watchlist
        if symbol not in CONFIG['SYMBOLS']:
            logger.warning(f"Symbole {symbol} non dans watchlist")
            return jsonify({'status': 'ignored', 'message': 'Symbole non surveille'}), 200

        # Traitement ST Context 4H
        if tf == '4h':
            zone = data.get('zone')
            long_term = data.get('long_term')
            
            if not zone or long_term is None:
                logger.warning(f"Donnees incompletes pour ST Context 4H {symbol}: zone={zone}, long_term={long_term}")
                return jsonify({'status': 'error', 'message': 'Champs zone et long_term requis pour 4h'}), 400
            
            # Mise a jour de l'etat
            STATE[symbol] = {
                'zone_4h': zone.lower(),
                'long_term': float(long_term),
                'timestamp': time.time()
            }
            
            logger.info(f"Zone ST Context 4H mise a jour pour {symbol}: zone={zone}, long_term={long_term}")
            
            return jsonify({
                'status': 'success',
                'message': 'ST Context 4H mis a jour',
                'symbol': symbol,
                'zone': zone,
                'long_term': long_term
            }), 200

        # Traitement SuperTrend AI 1H
        elif tf == '1h':
            supertrend = data.get('supertrend')
            
            if not supertrend:
                logger.warning(f"Donnees incompletes pour SuperTrend AI 1H {symbol}: supertrend={supertrend}")
                return jsonify({'status': 'error', 'message': 'Champ supertrend requis pour 1h'}), 400
            
            # Verification alignement
            signal_type = check_alignment(symbol, supertrend)
            
            if signal_type:
                # Signal aligne
                if should_send_alert(symbol, signal_type):
                    state = STATE[symbol]
                    send_telegram_alert(
                        symbol, 
                        signal_type, 
                        price, 
                        state['long_term'], 
                        state['zone_4h'], 
                        supertrend
                    )
                    update_last_signal(symbol, signal_type)
                    
                    return jsonify({
                        'status': 'success',
                        'message': 'Alerte envoyee',
                        'symbol': symbol,
                        'signal': signal_type
                    }), 200
                else:
                    logger.info(f"Signal {signal_type} pour {symbol} - cooldown actif")
                    return jsonify({
                        'status': 'cooldown',
                        'message': 'Alerte ignoree (cooldown)'
                    }), 200
            else:
                # Non aligne
                return jsonify({
                    'status': 'not_aligned',
                    'message': 'Signal non aligne avec ST Context 4H',
                    'symbol': symbol,
                    'supertrend_1h': supertrend,
                    'zone_4h': STATE.get(symbol, {}).get('zone_4h', 'unknown')
                }), 200

        else:
            logger.warning(f"Timeframe inconnu : {tf}")
            return jsonify({'status': 'error', 'message': f'Timeframe invalide: {tf} (doit etre 4h ou 1h)'}), 400

    except ValueError as e:
        logger.error(f"Erreur de conversion dans webhook: {e}")
        return jsonify({'status': 'error', 'message': f'Erreur de conversion: {e}'}), 400

    except KeyError as e:
        logger.error(f"Cle manquante dans webhook: {e}")
        return jsonify({'status': 'error', 'message': f'Cle manquante: {e}'}), 400

    except Exception as e:
        logger.error(f"Erreur inattendue dans webhook: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'Erreur serveur'}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de sante pour verifier que le bot fonctionne"""
    return jsonify({
        'status': 'running',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'symbols_monitored': len(CONFIG['SYMBOLS']),
        'symbols_with_state': len(STATE),
        'active_signals': len([s for s in LAST_SIGNALS.values() if s.get('type') is not None])
    }), 200

@app.route('/state', methods=['GET'])
def get_state():
    """Endpoint pour voir l'etat actuel de tous les symboles"""
    state_info = {}
    for symbol, data in STATE.items():
        state_info[symbol] = {
            'zone_4h': data.get('zone_4h'),
            'long_term': data.get('long_term'),
            'age_seconds': time.time() - data.get('timestamp', 0)
        }
    
    return jsonify({
        'state': state_info,
        'last_signals': LAST_SIGNALS
    }), 200

@app.route('/state/<symbol>', methods=['GET'])
def get_symbol_state(symbol):
    """Endpoint pour voir l'etat d'un symbole specifique"""
    symbol_formatted = symbol.replace('-', '/')
    
    if symbol_formatted not in CONFIG['SYMBOLS']:
        return jsonify({'error': 'Symbole non surveille'}), 404
    
    if symbol_formatted not in STATE:
        return jsonify({
            'symbol': symbol_formatted,
            'state': 'No ST Context 4H data received yet'
        }), 200
    
    data = STATE[symbol_formatted]
    return jsonify({
        'symbol': symbol_formatted,
        'zone_4h': data.get('zone_4h'),
        'long_term': data.get('long_term'),
        'age_seconds': time.time() - data.get('timestamp', 0),
        'last_signal': LAST_SIGNALS.get(symbol_formatted, {})
    }), 200

# ============================================================================
# FONCTION DEMARRAGE
# ============================================================================

def send_startup_message():
    """Envoie un message de demarrage sur Telegram"""
    try:
        start_msg = f"[BOT DEMARRE]\n\n"
        start_msg += f"Surveillance de {len(CONFIG['SYMBOLS'])} actifs:\n"
        start_msg += "\n".join([f"  - {s}" for s in CONFIG['SYMBOLS']])
        start_msg += f"\n\nAnti-spam : {CONFIG['MIN_TIME_BETWEEN_SAME_ALERT']/60:.0f} min"

        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        response = requests.post(url, json={
            'chat_id': CONFIG['TELEGRAM_CHAT_ID'],
            'text': start_msg
        }, timeout=15)
        
        if response.status_code == 200:
            logger.info("Message de demarrage envoye")
        else:
            logger.warning(f"Echec message demarrage (code {response.status_code})")
            
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
    logger.info("ST Super Bot - Demarrage")
    logger.info("="*60)
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
    
    # Envoi message de demarrage
    send_startup_message()
    
    # Demarrage du serveur Flask (bloquant)
    try:
        logger.info(f"Demarrage serveur webhook sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
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