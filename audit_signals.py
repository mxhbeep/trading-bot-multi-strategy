import json, os, argparse, redis

def get_redis_client():
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    try:
        return redis.from_url(redis_url, decode_responses=True)
    except Exception as e:
        print(f"❌ Impossible de se connecter à Redis: {e}")
        return None

def fetch_audit_logs(client, limit=500):
    try:
        raw_logs = client.lrange('audit_trail', 0, limit - 1)
        return [json.loads(l) for l in raw_logs]
    except Exception as e:
        print(f"❌ Erreur récupération logs: {e}")
        return []

def print_report(logs, args):
    if not logs:
        print("📭 Aucun log trouvé.")
        return
    
    filtered = []
    for log in logs:
        if args.symbol and args.symbol.upper() not in str(log.get('sym', '')).upper():
            continue
        if args.type and args.type.lower() != str(log.get('type', '')).lower():
            continue
        filtered.append(log)

    print(f"\n📊 AUDIT ({len(filtered)} événements)")
    print("=" * 80)
    for entry in filtered[:args.show]:
        ts, sym = entry.get('ts', 'N/A'), entry.get('sym', 'UNKNOWN')
        atype, val = entry.get('type', 'N/A'), entry.get('val', 'N/A')
        status = entry.get('status', 'N/A')
        print(f"[{ts}] {sym:12} | Type: {atype:12} | Val: {val:8} | Status: {status}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol")
    parser.add_argument("--type")
    parser.add_argument("--show", type=int, default=50)
    args = parser.parse_args()
    client = get_redis_client()
    if client:
        logs = fetch_audit_logs(client)
        print_report(logs, args)

if __name__ == "__main__":
    main()