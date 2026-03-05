#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import argparse
import redis
from datetime import datetime

def get_redis_client():
    # Récupère l'URL Redis depuis les variables d'environnement (comme sur Railway)
    # Ou utilise localhost par défaut pour un test local avec tunnel
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    try:
        return redis.from_url(redis_url, decode_responses=True)
    except Exception as e:
        print(f"❌ Impossible de se connecter à Redis: {e}")
        return None

def fetch_audit_logs(client, limit=500):
    try:
        # On récupère la liste 'audit_alerts' stockée par le bot
        raw_logs = client.lrange('audit_alerts', 0, limit - 1)
        return [json.loads(l) for l in raw_logs]
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des logs: {e}")
        return []

def print_report(logs, args):
    if not logs:
        print("📭 Aucun log trouvé dans Redis.")
        return

    # Filtrage
    filtered = []
    for log in logs:
        if args.symbol and args.symbol.upper() not in log.get('symbol', '').upper():
            continue
        if args.type and args.type.lower() != log.get('type', '').lower():
            continue
        filtered.append(log)

    print(f"\n📊 AUDIT DES SIGNAUX ({len(filtered)} événements trouvés)")
    print("="*60)
    
    for entry in filtered[:args.show]:
        ts = entry.get('timestamp', 'N/A')
        sym = entry.get('symbol', 'UNKNOWN')
        atype = entry.get('type', 'N/A')
        val = entry.get('value', 'N/A')
        price = entry.get('price', '0')
        
        # Formatage de l'affichage
        print(f"[{ts}] {sym:10} | Type: {atype:12} | Val: {val:5} | Prix: {price}")

def main():
    parser = argparse.ArgumentParser(description="Audit des signaux Trading Bot via Redis")
    parser.add_argument("--symbol", help="Filtrer par symbole (ex: BTC/USDT)")
    parser.add_argument("--type", help="Filtrer par type d'alerte (ex: supertrend, macd, ema)")
    parser.add_argument("--show", type=int, default=50, help="Nombre de lignes à afficher (default: 50)")
    args = parser.parse_args()

    client = get_redis_client()
    if client:
        logs = fetch_audit_logs(client)
        print_report(logs, args)

if __name__ == "__main__":
    main()