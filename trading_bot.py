import os, time, json, requests, logging, redis
from flask import Flask, request, jsonify
from datetime import datetime, timezone

# --- CONFIGURATION ---
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
PORT = int(os.environ.get("PORT", 8080))
MIN_INTERVAL = 1800  # Cooldown 30min

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

if not TOKEN or not CHAT_ID:
    logger.warning("⚠️ TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID non définis: envoi Telegram désactivé")

# --- CONNEXION REDIS ---
r = None
try:
    redis_url = os.environ.get('REDIS_URL')
    if redis_url:
        r = redis.from_url(redis_url, decode_responses=True)
        logger.info("✅ Redis connecté")
except Exception as e:
    logger.error(f"❌ Erreur Redis: {e}")

def send_tg(msg):
    if not TOKEN or not CHAT_ID:
        logger.warning("Telegram non configuré")
        return
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, json={'chat_id': CHAT_ID, 'text': msg, 'parse_mode': 'HTML'}, timeout=10)
    except Exception as e:
        logger.error(f"Erreur Telegram: {e}")

def audit_log(data, status="reçu"):
    if not r: return
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "sym": data.get('symbol'),
        "type": data.get('type'),
        "val": data.get('value'),
        "status": status
    }
    try:
        r.lpush('audit_trail', json.dumps(entry))
        r.ltrim('audit_trail', 0, 499) 
    except Exception as e:
        logger.error(f"Erreur audit Redis: {e}")

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data: return "No data", 400
    
    audit_log(data)

    raw_sym = data.get('symbol', '')
    symbol = raw_sym.split(':')[-1].replace('.P', '')
    if 'USDT' in symbol and '/' not in symbol:
        symbol = symbol.replace('USDT', '/USDT')

    tf = str(data.get('tf', '')).strip().lower()
    alert_type = str(data.get('type', '')).strip().lower()
    
    if not alert_type or not tf:
        audit_log(data, status="REJETÉ_CHAMPS_MANQUANTS")
        return "Missing tf/type", 400
    
    try:
        price = float(data.get('price', 0))
    except (TypeError, ValueError):
        audit_log(data, status="REJETÉ_PRICE_INVALIDE")
        return "Invalid price", 400

    val = str(data.get('value', '')).strip().lower()

    if r:
        if alert_type == "macd" and tf == "2d":
            r.hset(f"ctx:{symbol}", "macd", val)
            logger.info(f"MACD {symbol} -> {val}")

        if "ema" in alert_type and tf in ["1h", "60"]:
            ema_val = data.get('value', data.get('ema200', data.get('indicator_value')))
            if ema_val:
                try:
                    r.hset(f"ctx:{symbol}", "ema", float(ema_val))
                except (TypeError, ValueError):
                    logger.warning(f"EMA invalide: {ema_val}")

        if alert_type == "supertrend" and tf in ["1h", "60"] and val == "buy":
            ctx = r.hgetall(f"ctx:{symbol}")
            macd_ok = ctx.get('macd') == "bull"
            ema_val = float(ctx.get('ema', 0))
            ema_ok = ema_val > 0 and price > ema_val

            if macd_ok and ema_ok:
                last_buy = float(r.get(f"last_buy:{symbol}") or 0)
                if time.time() - last_buy > MIN_INTERVAL:
                    send_tg(f"🚀 <b>ACHAT {symbol}</b> @ {price}\n✅ MACD Bull\n✅ Prix > EMA200")
                    r.set(f"last_buy:{symbol}", time.time())
                    audit_log(data, status="SIGNAL_ENVOYÉ")
                else:
                    logger.info(f"Cooldown {symbol}")

    return "OK", 200

@app.route('/audit')
def get_audit():
    if not r: return "Redis non branché", 500
    logs = r.lrange('audit_trail', 0, -1)
    return jsonify([json.loads(l) for l in logs])

@app.route('/')
def health(): return "Bot is alive", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)