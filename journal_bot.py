#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Journal Bot — Bot Telegram de journaling de trades
Séparé du bot principal, utilise SCALP_BOT_TOKEN + même Redis.

Commandes disponibles :
  /entree  — Logger une entrée en position
  /sortie  — Clôturer un trade ouvert
  /trades  — Voir les trades ouverts
  /journal — Historique des trades clôturés
  /stats   — Statistiques globales (winrate, PnL moyen, par stratégie)
  /note    — Ajouter une note à un trade
  /delete  — Supprimer un trade (admin)
  /help    — Aide

Format des clés Redis :
  journal:trade:{trade_id}     — dict trade (ouvert ou clôturé)
  journal:open_ids             — set des IDs de trades ouverts
  journal:closed_ids           — list des IDs de trades clôturés (LIFO)
  journal:counter              — incrémental ID
"""

import os
import json
import time
import logging
import threading
import requests
import redis
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify

# ============================================================================ #
# CONFIGURATION
# ============================================================================ #

CONFIG = {
    'SCALP_BOT_TOKEN': os.environ.get('SCALP_BOT_TOKEN', ''),
    'TELEGRAM_CHAT_ID': os.environ.get('TELEGRAM_CHAT_ID', ''),
    'REDIS_URL': os.environ.get('REDIS_URL', ''),
    'WEBHOOK_PORT': int(os.environ.get('PORT', 5001)),
    'WEBHOOK_HOST': '0.0.0.0',
    # URL publique de CE bot (pour enregistrer le webhook Telegram)
    'JOURNAL_BOT_URL': os.environ.get('JOURNAL_BOT_URL', ''),
}

TZ_DISPLAY = ZoneInfo('Asia/Shanghai')

STRATEGIES = ['CONFLUENCE', 'TREND', 'CONTEXT4H', 'PULSE', 'SWING', 'SCALP', 'AUTRE']
DIRECTIONS = ['LONG', 'SHORT']

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ============================================================================ #
# REDIS
# ============================================================================ #

REDIS_CLIENT = None

def init_redis():
    global REDIS_CLIENT
    url = CONFIG['REDIS_URL']
    if not url:
        logger.warning("⚠️ REDIS_URL non défini — stockage en mémoire uniquement")
        return
    try:
        REDIS_CLIENT = redis.from_url(url, decode_responses=True)
        REDIS_CLIENT.ping()
        logger.info("✅ Redis connecté (Journal Bot)")
    except Exception as e:
        logger.error(f"❌ Redis erreur: {e}")
        REDIS_CLIENT = None


# Fallback mémoire si pas de Redis
_MEM_TRADES = {}
_MEM_OPEN   = set()
_MEM_CLOSED = []
_MEM_CTR    = [0]

def _next_id() -> str:
    if REDIS_CLIENT:
        return str(REDIS_CLIENT.incr('journal:counter'))
    _MEM_CTR[0] += 1
    return str(_MEM_CTR[0])

def _save_trade(trade: dict):
    tid = trade['id']
    if REDIS_CLIENT:
        REDIS_CLIENT.set(f'journal:trade:{tid}', json.dumps(trade))
    else:
        _MEM_TRADES[tid] = trade

def _load_trade(tid: str) -> dict | None:
    if REDIS_CLIENT:
        raw = REDIS_CLIENT.get(f'journal:trade:{tid}')
        return json.loads(raw) if raw else None
    return _MEM_TRADES.get(tid)

def _delete_trade(tid: str):
    if REDIS_CLIENT:
        REDIS_CLIENT.delete(f'journal:trade:{tid}')
        REDIS_CLIENT.srem('journal:open_ids', tid)
        REDIS_CLIENT.lrem('journal:closed_ids', 0, tid)
    else:
        _MEM_TRADES.pop(tid, None)
        _MEM_OPEN.discard(tid)
        if tid in _MEM_CLOSED:
            _MEM_CLOSED.remove(tid)

def _mark_open(tid: str):
    if REDIS_CLIENT:
        REDIS_CLIENT.sadd('journal:open_ids', tid)
    else:
        _MEM_OPEN.add(tid)

def _unmark_open(tid: str):
    if REDIS_CLIENT:
        REDIS_CLIENT.srem('journal:open_ids', tid)
    else:
        _MEM_OPEN.discard(tid)

def _mark_closed(tid: str):
    if REDIS_CLIENT:
        REDIS_CLIENT.lpush('journal:closed_ids', tid)
        REDIS_CLIENT.ltrim('journal:closed_ids', 0, 499)   # garder 500 derniers
    else:
        _MEM_CLOSED.insert(0, tid)
        if len(_MEM_CLOSED) > 500:
            _MEM_CLOSED.pop()

def _get_open_ids() -> list[str]:
    if REDIS_CLIENT:
        return list(REDIS_CLIENT.smembers('journal:open_ids'))
    return list(_MEM_OPEN)

def _get_closed_ids(limit=20) -> list[str]:
    if REDIS_CLIENT:
        return REDIS_CLIENT.lrange('journal:closed_ids', 0, limit - 1)
    return _MEM_CLOSED[:limit]

# ============================================================================ #
# TELEGRAM HELPERS
# ============================================================================ #

def send_msg(text: str, chat_id: str = None, reply_markup: dict = None, parse_mode='HTML'):
    token = CONFIG['SCALP_BOT_TOKEN']
    if not token:
        logger.warning("SCALP_BOT_TOKEN non configuré")
        return None
    cid = chat_id or CONFIG['TELEGRAM_CHAT_ID']
    payload = {'chat_id': cid, 'text': text, 'parse_mode': parse_mode}
    if reply_markup:
        payload['reply_markup'] = reply_markup
    try:
        resp = requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json=payload, timeout=10
        )
        if resp.status_code != 200:
            logger.warning(f"Telegram erreur {resp.status_code}: {resp.text[:200]}")
        return resp
    except Exception as e:
        logger.error(f"send_msg error: {e}")
        return None

def answer_callback(callback_query_id: str, text: str = '✅'):
    token = CONFIG['SCALP_BOT_TOKEN']
    try:
        requests.post(
            f'https://api.telegram.org/bot{token}/answerCallbackQuery',
            json={'callback_query_id': callback_query_id, 'text': text},
            timeout=5
        )
    except Exception:
        pass

def edit_msg(chat_id, message_id, text, parse_mode='HTML'):
    token = CONFIG['SCALP_BOT_TOKEN']
    try:
        requests.post(
            f'https://api.telegram.org/bot{token}/editMessageText',
            json={'chat_id': chat_id, 'message_id': message_id,
                  'text': text, 'parse_mode': parse_mode},
            timeout=10
        )
    except Exception as e:
        logger.error(f"edit_msg error: {e}")

# ============================================================================ #
# SESSION (wizard multi-étapes par utilisateur)
# ============================================================================ #

# user_id -> {'step': str, 'data': dict}
SESSIONS: dict = {}
SESSION_LOCK = threading.Lock()

def get_session(uid: str) -> dict:
    with SESSION_LOCK:
        return SESSIONS.get(uid, {})

def set_session(uid: str, step: str, data: dict = None):
    with SESSION_LOCK:
        SESSIONS[uid] = {'step': step, 'data': data or {}}

def clear_session(uid: str):
    with SESSION_LOCK:
        SESSIONS.pop(uid, None)

# ============================================================================ #
# FORMATTERS
# ============================================================================ #

def fmt_price(p) -> str:
    try:
        p = float(p)
        if p == 0: return 'N/A'
        if p < 0.0001: return f'{p:.8f}'
        if p < 0.01:   return f'{p:.6f}'
        if p < 1:      return f'{p:.4f}'
        return f'{p:.4f}'
    except Exception:
        return str(p)

def fmt_pnl(pnl_pct: float) -> str:
    sign = '+' if pnl_pct >= 0 else ''
    emoji = '🟢' if pnl_pct > 0 else '🔴' if pnl_pct < 0 else '⬜'
    return f"{emoji} {sign}{pnl_pct:.2f}%"

def fmt_ts(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=TZ_DISPLAY)
    return dt.strftime('%d/%m %H:%M')

def calc_pnl_pct(direction: str, entry: float, exit_: float) -> float:
    if entry == 0:
        return 0.0
    if direction == 'LONG':
        return (exit_ - entry) / entry * 100
    else:
        return (entry - exit_) / entry * 100

def trade_summary(t: dict, show_note=True) -> str:
    d = t.get('direction', '?')
    emoji_d = '🟢' if d == 'LONG' else '🔴'
    status = t.get('status', 'open')
    status_emoji = '⏳' if status == 'open' else '✅'

    lines = [
        f"{status_emoji} <b>#{t['id']} — {t.get('symbol','?')} {emoji_d} {d}</b>",
        f"   Stratégie : {t.get('strategy','?')}",
        f"   Entrée    : ${fmt_price(t.get('entry_price', 0))}  ({fmt_ts(t['open_ts'])})",
    ]
    if t.get('entry_count', 1) > 1:
        lines.append(f"   Entrées   : {t['entry_count']} (pyramiding)")

    if status == 'closed':
        pnl = calc_pnl_pct(d, t.get('entry_price', 0), t.get('exit_price', 0))
        lines += [
            f"   Sortie    : ${fmt_price(t.get('exit_price', 0))}  ({fmt_ts(t['close_ts'])})",
            f"   PnL       : {fmt_pnl(pnl)}",
        ]
        dur = t.get('close_ts', 0) - t.get('open_ts', 0)
        h, m = divmod(int(dur // 60), 60)
        lines.append(f"   Durée     : {h}h{m:02d}m")

    if show_note and t.get('note'):
        lines.append(f"   📝 {t['note']}")
    return '\n'.join(lines)

# ============================================================================ #
# COMMANDES — WIZARDS
# ============================================================================ #

def cmd_help(uid, cid):
    send_msg(
        "📓 <b>Journal Bot — Aide</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "/entree — Logger une entrée en position\n"
        "/sortie — Clôturer un trade ouvert\n"
        "/trades — Voir les trades ouverts\n"
        "/journal — Historique des clôtures\n"
        "/stats — Statistiques (winrate, PnL…)\n"
        "/note — Ajouter une note à un trade\n"
        "/delete — Supprimer un trade\n"
        "/annuler — Annuler la saisie en cours",
        chat_id=cid
    )

def cmd_annuler(uid, cid):
    clear_session(uid)
    send_msg("❌ Saisie annulée.", chat_id=cid)

def cmd_entree(uid, cid):
    # Étape 1 : choisir le symbole
    clear_session(uid)
    set_session(uid, 'entree_symbol', {})
    send_msg(
        "📥 <b>Nouvelle entrée</b>\n\nQuel symbole ? (ex: <code>BTC/USDT</code> ou <code>BTC</code>)\n\n/annuler pour interrompre",
        chat_id=cid
    )

def cmd_sortie(uid, cid):
    open_ids = _get_open_ids()
    if not open_ids:
        send_msg("Aucun trade ouvert à clôturer.", chat_id=cid)
        return

    trades = [_load_trade(tid) for tid in open_ids]
    trades = [t for t in trades if t]
    trades.sort(key=lambda t: t.get('open_ts', 0))

    # Boutons inline pour choisir le trade
    buttons = []
    for t in trades:
        d = t.get('direction','?')
        label = f"#{t['id']} {t.get('symbol','?')} {d} @{fmt_price(t.get('entry_price',0))}"
        buttons.append([{"text": label, "callback_data": f"sortie_select:{t['id']}"}])
    buttons.append([{"text": "❌ Annuler", "callback_data": "sortie_cancel"}])

    set_session(uid, 'sortie_select', {})
    send_msg(
        "📤 <b>Clôturer un trade</b>\n\nChoisissez le trade à fermer :",
        chat_id=cid,
        reply_markup={"inline_keyboard": buttons}
    )

def cmd_trades(uid, cid):
    open_ids = _get_open_ids()
    if not open_ids:
        send_msg("✅ Aucun trade ouvert en ce moment.", chat_id=cid)
        return
    trades = [_load_trade(tid) for tid in open_ids]
    trades = [t for t in trades if t]
    trades.sort(key=lambda t: t.get('open_ts', 0))

    parts = [f"⏳ <b>Trades ouverts ({len(trades)})</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for t in trades:
        parts.append(trade_summary(t))
    send_msg('\n\n'.join(parts), chat_id=cid)

def cmd_journal(uid, cid, limit=10):
    closed_ids = _get_closed_ids(limit)
    if not closed_ids:
        send_msg("Aucun trade clôturé dans l'historique.", chat_id=cid)
        return
    trades = [_load_trade(tid) for tid in closed_ids]
    trades = [t for t in trades if t]

    parts = [f"📋 <b>Historique ({len(trades)} derniers)</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for t in trades:
        parts.append(trade_summary(t))
    send_msg('\n\n'.join(parts), chat_id=cid)

def cmd_stats(uid, cid):
    closed_ids = _get_closed_ids(500)
    trades = [_load_trade(tid) for tid in closed_ids]
    trades = [t for t in trades if t and t.get('status') == 'closed']

    if not trades:
        send_msg("Aucun trade clôturé pour calculer les stats.", chat_id=cid)
        return

    total = len(trades)
    pnls = []
    wins = 0
    by_strat: dict[str, list] = {}

    for t in trades:
        pnl = calc_pnl_pct(t.get('direction','LONG'), t.get('entry_price',0), t.get('exit_price',0))
        pnls.append(pnl)
        if pnl > 0:
            wins += 1
        s = t.get('strategy', 'AUTRE')
        by_strat.setdefault(s, []).append(pnl)

    winrate = wins / total * 100
    avg_pnl = sum(pnls) / total
    total_pnl = sum(pnls)
    best  = max(pnls)
    worst = min(pnls)

    lines = [
        "📊 <b>Statistiques Journal</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"📈 Trades clôturés : <b>{total}</b>",
        f"🏆 Winrate         : <b>{winrate:.1f}%</b>  ({wins}W / {total-wins}L)",
        f"💰 PnL moyen       : {fmt_pnl(avg_pnl)}",
        f"💹 PnL cumulé      : {fmt_pnl(total_pnl)}",
        f"⬆️ Meilleur trade  : {fmt_pnl(best)}",
        f"⬇️ Pire trade      : {fmt_pnl(worst)}",
        "",
        "📋 <b>Par stratégie :</b>",
    ]
    for strat, spnls in sorted(by_strat.items()):
        sw = sum(1 for p in spnls if p > 0)
        sa = sum(spnls) / len(spnls)
        lines.append(f"  • {strat}: {len(spnls)} trades | {sw}/{len(spnls)} W | moy {fmt_pnl(sa)}")

    # Trades ouverts en cours
    n_open = len(_get_open_ids())
    if n_open:
        lines.append(f"\n⏳ Trades ouverts : {n_open}")

    send_msg('\n'.join(lines), chat_id=cid)

def cmd_note(uid, cid):
    open_ids  = _get_open_ids()
    closed_ids = _get_closed_ids(20)
    all_ids = list(open_ids) + [i for i in closed_ids if i not in open_ids]
    if not all_ids:
        send_msg("Aucun trade trouvé.", chat_id=cid)
        return
    trades = [_load_trade(tid) for tid in all_ids[:15]]
    trades = [t for t in trades if t]
    trades.sort(key=lambda t: -t.get('open_ts', 0))

    buttons = []
    for t in trades:
        status = '⏳' if t.get('status') == 'open' else '✅'
        label = f"{status} #{t['id']} {t.get('symbol','?')} {t.get('direction','?')}"
        buttons.append([{"text": label, "callback_data": f"note_select:{t['id']}"}])
    buttons.append([{"text": "❌ Annuler", "callback_data": "note_cancel"}])

    set_session(uid, 'note_select', {})
    send_msg("📝 Ajouter une note à quel trade ?",
             chat_id=cid, reply_markup={"inline_keyboard": buttons})

def cmd_delete(uid, cid):
    open_ids   = _get_open_ids()
    closed_ids = _get_closed_ids(10)
    all_ids = list(open_ids) + [i for i in closed_ids if i not in open_ids]
    if not all_ids:
        send_msg("Aucun trade trouvé.", chat_id=cid)
        return
    trades = [_load_trade(tid) for tid in all_ids]
    trades = [t for t in trades if t]
    trades.sort(key=lambda t: -t.get('open_ts', 0))

    buttons = []
    for t in trades[:10]:
        status = '⏳' if t.get('status') == 'open' else '✅'
        label = f"🗑 {status} #{t['id']} {t.get('symbol','?')} {t.get('direction','?')}"
        buttons.append([{"text": label, "callback_data": f"delete_confirm:{t['id']}"}])
    buttons.append([{"text": "❌ Annuler", "callback_data": "delete_cancel"}])

    set_session(uid, 'delete_select', {})
    send_msg("🗑 Supprimer quel trade ?",
             chat_id=cid, reply_markup={"inline_keyboard": buttons})

# ============================================================================ #
# MACHINE D'ÉTAT — SAISIE D'ENTRÉE (wizard texte)
# ============================================================================ #

def _strategies_keyboard():
    row1 = [{"text": s, "callback_data": f"entree_strat:{s}"} for s in STRATEGIES[:3]]
    row2 = [{"text": s, "callback_data": f"entree_strat:{s}"} for s in STRATEGIES[3:6]]
    row3 = [{"text": STRATEGIES[6], "callback_data": f"entree_strat:{STRATEGIES[6]}"},
            {"text": "❌ Annuler",   "callback_data": "entree_cancel"}]
    return {"inline_keyboard": [row1, row2, row3]}

def _direction_keyboard():
    return {"inline_keyboard": [[
        {"text": "🟢 LONG",  "callback_data": "entree_dir:LONG"},
        {"text": "🔴 SHORT", "callback_data": "entree_dir:SHORT"},
        {"text": "❌ Annuler", "callback_data": "entree_cancel"},
    ]]}

def handle_text(uid: str, cid: str, text: str):
    """Gère les messages texte selon la session en cours."""
    sess = get_session(uid)
    if not sess:
        send_msg("Utilisez /help pour la liste des commandes.", chat_id=cid)
        return

    step = sess.get('step', '')
    data = sess.get('data', {})

    # ---- LOG_ENTRY depuis bot principal : saisie du prix réel ----
    if step == 'log_entry_price':
        raw = text.strip()
        signal_price = data.get('signal_price', 0.0)
        if raw == '=':
            real_price = signal_price
        else:
            try:
                real_price = float(raw.replace(',', '.'))
            except ValueError:
                send_msg("❌ Prix invalide. Entrez un nombre ou <code>=</code> pour le prix du signal.", chat_id=cid)
                return
        data['entry_price'] = real_price
        set_session(uid, 'log_entry_note', data)
        send_msg(
            f"Prix d'entrée : <b>${fmt_price(real_price)}</b>\n\n"
            "Note optionnelle ? (<code>-</code> pour ignorer) :",
            chat_id=cid
        )

    # ---- LOG_ENTRY depuis bot principal : note optionnelle ----
    elif step == 'log_entry_note':
        note = '' if text.strip() == '-' else text.strip()
        data['note']        = note
        data['entry_count'] = 1
        _finalize_entree(uid, cid, data)

    # ---- ENTRÉE : saisie du symbole ----
    elif step == 'entree_symbol':
        sym = text.strip().upper()
        if '/' not in sym:
            sym = f"{sym}/USDT"
        data['symbol'] = sym
        set_session(uid, 'entree_strat', data)
        send_msg(f"Symbole : <b>{sym}</b>\n\nQuelle stratégie ?",
                 chat_id=cid, reply_markup=_strategies_keyboard())

    # ---- ENTRÉE : saisie du prix d'entrée ----
    elif step == 'entree_price':
        try:
            price = float(text.replace(',', '.').strip())
            data['entry_price'] = price
            set_session(uid, 'entree_count', data)
            send_msg(
                f"Prix d'entrée : <b>${fmt_price(price)}</b>\n\n"
                "Nombre de lots / entrées (ex: <code>1</code> ou <code>2</code> pour pyramiding) :",
                chat_id=cid
            )
        except ValueError:
            send_msg("❌ Prix invalide. Entrez un nombre (ex: <code>42150.50</code>)", chat_id=cid)

    # ---- ENTRÉE : saisie du nombre d'entrées ----
    elif step == 'entree_count':
        try:
            count = max(1, int(text.strip()))
            data['entry_count'] = count
            set_session(uid, 'entree_note', data)
            send_msg(
                "Note optionnelle ? (raison d'entrée, setup…)\n\n"
                "Envoyez <code>-</code> pour ignorer.",
                chat_id=cid
            )
        except ValueError:
            send_msg("❌ Nombre invalide. Entrez un entier (ex: <code>1</code>)", chat_id=cid)

    # ---- ENTRÉE : note optionnelle ----
    elif step == 'entree_note':
        note = '' if text.strip() == '-' else text.strip()
        data['note'] = note
        _finalize_entree(uid, cid, data)

    # ---- SORTIE : saisie du prix de sortie ----
    elif step == 'sortie_price':
        try:
            price = float(text.replace(',', '.').strip())
            data['exit_price'] = price
            set_session(uid, 'sortie_note', data)
            send_msg(
                f"Prix de sortie : <b>${fmt_price(price)}</b>\n\n"
                "Note optionnelle ? (<code>-</code> pour ignorer) :",
                chat_id=cid
            )
        except ValueError:
            send_msg("❌ Prix invalide. Entrez un nombre (ex: <code>42500.00</code>)", chat_id=cid)

    # ---- SORTIE : note optionnelle ----
    elif step == 'sortie_note':
        note = '' if text.strip() == '-' else text.strip()
        data['exit_note'] = note
        _finalize_sortie(uid, cid, data)

    # ---- NOTE : saisie du texte de note ----
    elif step == 'note_text':
        tid  = data.get('trade_id')
        note = text.strip()
        t = _load_trade(tid)
        if not t:
            send_msg("❌ Trade introuvable.", chat_id=cid)
            clear_session(uid)
            return
        t['note'] = note
        _save_trade(t)
        clear_session(uid)
        send_msg(f"✅ Note enregistrée sur le trade <b>#{tid}</b>.", chat_id=cid)

    else:
        send_msg("Utilisez /help pour la liste des commandes.", chat_id=cid)


def _finalize_entree(uid: str, cid: str, data: dict):
    """Crée le trade ouvert et envoie la confirmation."""
    tid = _next_id()
    now = time.time()
    trade = {
        'id': tid,
        'symbol': data.get('symbol', '?'),
        'strategy': data.get('strategy', 'AUTRE'),
        'direction': data.get('direction', 'LONG'),
        'entry_price': data.get('entry_price', 0.0),
        'entry_count': data.get('entry_count', 1),
        'note': data.get('note', ''),
        'open_ts': now,
        'status': 'open',
    }
    _save_trade(trade)
    _mark_open(tid)
    clear_session(uid)

    d = trade['direction']
    emoji = '🟢' if d == 'LONG' else '🔴'
    send_msg(
        f"✅ <b>Entrée enregistrée #{tid}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{emoji} {trade['symbol']} {d}\n"
        f"Stratégie  : {trade['strategy']}\n"
        f"Prix       : ${fmt_price(trade['entry_price'])}\n"
        f"Entrées    : {trade['entry_count']}\n"
        + (f"📝 {trade['note']}\n" if trade['note'] else '') +
        f"⏰ {fmt_ts(now)} (Shanghai)",
        chat_id=cid
    )
    logger.info(f"[JOURNAL] Entrée #{tid}: {trade['symbol']} {d} @ {trade['entry_price']}")


def _finalize_sortie(uid: str, cid: str, data: dict):
    """Clôture le trade et calcule le PnL."""
    tid  = data.get('trade_id')
    t    = _load_trade(tid)
    if not t:
        send_msg("❌ Trade introuvable.", chat_id=cid)
        clear_session(uid)
        return

    now       = time.time()
    exit_price = data.get('exit_price', 0.0)
    pnl        = calc_pnl_pct(t['direction'], t['entry_price'], exit_price)

    t['exit_price'] = exit_price
    t['close_ts']   = now
    t['status']     = 'closed'
    if data.get('exit_note'):
        existing = t.get('note', '')
        t['note'] = (existing + ' | ' + data['exit_note']).strip(' |')

    _save_trade(t)
    _unmark_open(tid)
    _mark_closed(tid)
    clear_session(uid)

    d = t['direction']
    emoji = '🟢' if d == 'LONG' else '🔴'
    dur   = now - t.get('open_ts', now)
    h, m  = divmod(int(dur // 60), 60)

    send_msg(
        f"✅ <b>Trade clôturé #{tid}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{emoji} {t['symbol']} {d}  ({t['strategy']})\n"
        f"Entrée  : ${fmt_price(t['entry_price'])}  ({fmt_ts(t['open_ts'])})\n"
        f"Sortie  : ${fmt_price(exit_price)}  ({fmt_ts(now)})\n"
        f"Durée   : {h}h{m:02d}m\n"
        f"PnL     : {fmt_pnl(pnl)}\n"
        + (f"📝 {t['note']}" if t.get('note') else ''),
        chat_id=cid
    )
    logger.info(f"[JOURNAL] Clôture #{tid}: {t['symbol']} {d} PnL={pnl:+.2f}%")

# ============================================================================ #
# CALLBACKS INLINE
# ============================================================================ #

def handle_callback(uid: str, cid: str, cbq_id: str, cbq_data: str):
    """Gère les réponses aux boutons inline."""
    answer_callback(cbq_id)

    # ---- ENTRÉE : stratégie ----
    if cbq_data.startswith('entree_strat:'):
        sess = get_session(uid)
        if sess.get('step') not in ('entree_symbol', 'entree_strat'):
            return
        strat = cbq_data.split(':', 1)[1]
        data  = sess.get('data', {})
        data['strategy'] = strat
        set_session(uid, 'entree_dir', data)
        send_msg(
            f"Stratégie : <b>{strat}</b>\n\nDirection ?",
            chat_id=cid, reply_markup=_direction_keyboard()
        )

    # ---- ENTRÉE : direction ----
    elif cbq_data.startswith('entree_dir:'):
        sess = get_session(uid)
        if sess.get('step') != 'entree_dir':
            return
        direction = cbq_data.split(':', 1)[1]
        data = sess.get('data', {})
        data['direction'] = direction
        set_session(uid, 'entree_price', data)
        send_msg(
            f"Direction : <b>{direction}</b>\n\nPrix d'entrée ? (ex: <code>42150.5</code>)",
            chat_id=cid
        )

    # ---- ENTRÉE : annuler ----
    elif cbq_data == 'entree_cancel':
        clear_session(uid)
        send_msg("❌ Saisie annulée.", chat_id=cid)

    # ---- SORTIE : sélection trade ----
    elif cbq_data.startswith('sortie_select:'):
        tid = cbq_data.split(':', 1)[1]
        t   = _load_trade(tid)
        if not t:
            send_msg("❌ Trade introuvable.", chat_id=cid)
            clear_session(uid)
            return
        set_session(uid, 'sortie_price', {'trade_id': tid})
        d = t['direction']
        emoji = '🟢' if d == 'LONG' else '🔴'
        send_msg(
            f"Clôture du trade <b>#{tid}</b>\n"
            f"{emoji} {t['symbol']} {d} @ ${fmt_price(t['entry_price'])}\n\n"
            "Prix de sortie ?",
            chat_id=cid
        )

    elif cbq_data == 'sortie_cancel':
        clear_session(uid)
        send_msg("❌ Annulé.", chat_id=cid)

    # ---- NOTE : sélection trade ----
    elif cbq_data.startswith('note_select:'):
        tid = cbq_data.split(':', 1)[1]
        t   = _load_trade(tid)
        if not t:
            send_msg("❌ Trade introuvable.", chat_id=cid)
            clear_session(uid)
            return
        set_session(uid, 'note_text', {'trade_id': tid})
        current = t.get('note', '')
        send_msg(
            f"Note pour le trade <b>#{tid}</b> ({t['symbol']} {t['direction']})\n"
            + (f"Actuelle : {current}\n\n" if current else "\n") +
            "Entrez votre note :",
            chat_id=cid
        )

    elif cbq_data == 'note_cancel':
        clear_session(uid)
        send_msg("❌ Annulé.", chat_id=cid)

    # ---- DELETE : confirmation ----
    elif cbq_data.startswith('delete_confirm:'):
        tid = cbq_data.split(':', 1)[1]
        t   = _load_trade(tid)
        if not t:
            send_msg("❌ Trade introuvable.", chat_id=cid)
            clear_session(uid)
            return
        set_session(uid, 'delete_confirm', {'trade_id': tid})
        send_msg(
            f"⚠️ Confirmer la suppression du trade <b>#{tid}</b> "
            f"({t['symbol']} {t['direction']}) ?",
            chat_id=cid,
            reply_markup={"inline_keyboard": [[
                {"text": "✅ Confirmer",  "callback_data": f"delete_do:{tid}"},
                {"text": "❌ Annuler",    "callback_data": "delete_cancel"}
            ]]}
        )

    elif cbq_data.startswith('delete_do:'):
        tid = cbq_data.split(':', 1)[1]
        _delete_trade(tid)
        clear_session(uid)
        send_msg(f"🗑 Trade <b>#{tid}</b> supprimé.", chat_id=cid)

    elif cbq_data == 'delete_cancel':
        clear_session(uid)
        send_msg("❌ Suppression annulée.", chat_id=cid)

# ============================================================================ #
# WEBHOOK FLASK
# ============================================================================ #

@app.route('/')
def home():
    n_open = len(_get_open_ids())
    return f"<h1>Journal Bot</h1><p>Trades ouverts: {n_open}</p>"


@app.route('/log_entry', methods=['POST'])
def log_entry():
    """
    Reçoit un trade pré-rempli depuis le bot principal (via relai callback).
    Lance un wizard de confirmation + saisie du prix réel vers l'utilisateur.
    Body JSON attendu :
      { symbol, strategy, direction, price, user_id, chat_id }
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'ok': False, 'error': 'no data'}), 400

    symbol    = str(data.get('symbol', '?')).upper()
    strategy  = str(data.get('strategy', 'AUTRE')).upper()
    direction = str(data.get('direction', 'LONG')).upper()
    price_str = str(data.get('price', '0'))
    cid       = str(data.get('chat_id') or CONFIG['TELEGRAM_CHAT_ID'])
    uid       = str(data.get('user_id') or cid)

    try:
        signal_price = float(price_str)
    except ValueError:
        signal_price = 0.0

    # Vérifier que le chat est autorisé
    allowed_chat = CONFIG.get('TELEGRAM_CHAT_ID', '')
    if allowed_chat and cid != allowed_chat:
        logger.warning(f"[LOG_ENTRY] Chat non autorisé: {cid}")
        return jsonify({'ok': False, 'error': 'unauthorized'}), 403

    d_emoji = '🟢' if direction == 'LONG' else '🔴'

    # Pré-remplir la session avec toutes les infos sauf le prix réel
    set_session(uid, 'log_entry_price', {
        'symbol':         symbol,
        'strategy':       strategy,
        'direction':      direction,
        'signal_price':   signal_price,
    })

    send_msg(
        f"📓 <b>Logger ce trade ?</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{d_emoji} <b>{symbol}</b>  {direction}  —  {strategy}\n"
        f"Prix signal : <code>${fmt_price(signal_price)}</code>\n\n"
        f"Entrez votre <b>prix d'entrée réel</b>\n"
        f"(ou envoyez <code>=</code> pour utiliser le prix du signal)\n\n"
        f"/annuler pour ignorer",
        chat_id=cid
    )
    logger.info(f"[LOG_ENTRY] Wizard lancé: {symbol} {direction} {strategy} @ {signal_price}")
    return jsonify({'ok': True})

@app.route('/webhook', methods=['POST'])
def webhook():
    update = request.get_json(silent=True)
    if not update:
        return jsonify({'ok': True})

    try:
        # Callback query (bouton inline pressé)
        if 'callback_query' in update:
            cbq     = update['callback_query']
            uid     = str(cbq['from']['id'])
            cid     = str(cbq['message']['chat']['id'])
            cbq_id  = cbq['id']
            cbq_data = cbq.get('data', '')
            handle_callback(uid, cid, cbq_id, cbq_data)
            return jsonify({'ok': True})

        # Message texte
        msg = update.get('message', {})
        if not msg:
            return jsonify({'ok': True})

        uid  = str(msg['from']['id'])
        cid  = str(msg['chat']['id'])
        text = msg.get('text', '').strip()

        # Vérification simple : n'accepter que depuis le chat autorisé
        allowed_chat = CONFIG.get('TELEGRAM_CHAT_ID', '')
        if allowed_chat and cid != allowed_chat:
            logger.warning(f"[JOURNAL] Message ignoré depuis chat non autorisé: {cid}")
            return jsonify({'ok': True})

        # Commandes
        if text.startswith('/entree'):   cmd_entree(uid, cid)
        elif text.startswith('/sortie'): cmd_sortie(uid, cid)
        elif text.startswith('/trades'): cmd_trades(uid, cid)
        elif text.startswith('/journal'):cmd_journal(uid, cid)
        elif text.startswith('/stats'):  cmd_stats(uid, cid)
        elif text.startswith('/note'):   cmd_note(uid, cid)
        elif text.startswith('/delete'): cmd_delete(uid, cid)
        elif text.startswith('/annuler'):cmd_annuler(uid, cid)
        elif text.startswith('/help') or text.startswith('/start'):
            cmd_help(uid, cid)
        else:
            # Continuer la session si une est active
            sess = get_session(uid)
            if sess:
                handle_text(uid, cid, text)
            else:
                send_msg("Utilisez /help pour la liste des commandes.", chat_id=cid)

    except Exception as e:
        logger.error(f"[JOURNAL] Erreur webhook: {e}", exc_info=True)

    return jsonify({'ok': True})

# ============================================================================ #
# DÉMARRAGE
# ============================================================================ #

def register_webhook():
    """Enregistre ce bot comme webhook Telegram."""
    token = CONFIG['SCALP_BOT_TOKEN']
    url   = CONFIG.get('JOURNAL_BOT_URL', '')
    if not token or not url:
        logger.warning("⚠️ SCALP_BOT_TOKEN ou JOURNAL_BOT_URL manquant — webhook non configuré")
        return
    wh_url = f"{url.rstrip('/')}/webhook"
    try:
        resp = requests.post(
            f'https://api.telegram.org/bot{token}/setWebhook',
            json={'url': wh_url},
            timeout=10
        )
        if resp.status_code == 200 and resp.json().get('ok'):
            logger.info(f"✅ Webhook enregistré: {wh_url}")
        else:
            logger.warning(f"⚠️ Webhook registration: {resp.text[:200]}")
    except Exception as e:
        logger.error(f"❌ register_webhook: {e}")

def startup():
    init_redis()
    time.sleep(3)
    register_webhook()
    token = CONFIG['SCALP_BOT_TOKEN']
    cid   = CONFIG['TELEGRAM_CHAT_ID']
    if token and cid:
        send_msg(
            "📓 <b>Journal Bot démarré</b>\n"
            f"Redis: {'✅' if REDIS_CLIENT else '⚠️ mémoire uniquement'}\n\n"
            "/entree  — Logger une entrée\n"
            "/sortie  — Clôturer un trade\n"
            "/trades  — Trades ouverts\n"
            "/journal — Historique\n"
            "/stats   — Statistiques",
            chat_id=cid
        )
    logger.info("🚀 Journal Bot prêt")

if os.environ.get('ENABLE_SCHEDULERS', '1') == '1':
    threading.Thread(target=startup, daemon=True).start()

if __name__ == '__main__':
    logger.info(f"Journal Bot sur {CONFIG['WEBHOOK_HOST']}:{CONFIG['WEBHOOK_PORT']}")
    app.run(host=CONFIG['WEBHOOK_HOST'], port=CONFIG['WEBHOOK_PORT'], debug=False)
