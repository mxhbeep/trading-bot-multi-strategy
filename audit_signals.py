#!/usr/bin/env python3
import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone


def parse_iso8601(ts_raw):
    if not ts_raw:
        return None
    ts = str(ts_raw).strip()
    if ts.endswith('Z'):
        ts = ts[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_tf(tf_raw):
    tf = str(tf_raw or '').strip().lower()
    aliases = {
        '60': '1h', '1hr': '1h', '1hour': '1h',
        '240': '4h', '4hr': '4h', '4hour': '4h',
        'd': '1d', '1day': '1d',
        '15': '15m',
    }
    return aliases.get(tf, tf)


def get_redis_client():
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    try:
        import redis
    except Exception as exc:
        print(f"❌ Module redis indisponible: {exc}")
        return None
    try:
        return redis.from_url(redis_url, decode_responses=True)
    except Exception as exc:
        print(f"❌ Impossible de se connecter à Redis: {exc}")
        return None


def load_from_redis(client, limit):
    raw_logs = client.lrange('audit_trail', 0, max(0, limit - 1))
    logs = []
    for line in raw_logs:
        try:
            logs.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return logs


def load_from_jsonl(path, limit):
    logs = []
    with open(path, 'r', encoding='utf-8') as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            logs.append(item)
            if len(logs) >= limit:
                break
    return list(reversed(logs))


def standardize_entry(entry):
    ts = parse_iso8601(entry.get('ts') or entry.get('timestamp'))
    symbol = entry.get('sym') or entry.get('symbol') or 'UNKNOWN'
    alert_type = entry.get('type') or entry.get('alert_type') or 'N/A'
    tf = normalize_tf(entry.get('tf') or entry.get('timeframe') or '')
    strategy = entry.get('strategy') or 'N/A'
    status = entry.get('status') or 'N/A'
    value = entry.get('val') if 'val' in entry else entry.get('value')
    return {
        'ts': ts,
        'symbol': str(symbol),
        'type': str(alert_type),
        'tf': tf,
        'strategy': str(strategy),
        'status': str(status),
        'value': value,
        'raw': entry,
    }


def apply_filters(entries, args):
    out = []
    now_utc = datetime.now(timezone.utc)
    for item in entries:
        if args.symbol and args.symbol.upper() not in item['symbol'].upper():
            continue
        if args.type and args.type.lower() != item['type'].lower():
            continue
        if args.tf and normalize_tf(args.tf) != item['tf']:
            continue
        if args.strategy and args.strategy.lower() != item['strategy'].lower():
            continue
        if args.status and args.status.lower() != item['status'].lower():
            continue
        if args.since_hours:
            if not item['ts']:
                continue
            age_seconds = (now_utc - item['ts']).total_seconds()
            if age_seconds > args.since_hours * 3600:
                continue
        out.append(item)
    return out


def print_summary(entries, expected_tfs):
    print(f"\n📊 AUDIT ({len(entries)} événements après filtres)")
    print('=' * 80)

    by_tf = Counter(e['tf'] or 'N/A' for e in entries)
    by_type = Counter(e['type'] for e in entries)
    by_status = Counter(e['status'] for e in entries)
    by_strategy = Counter(e['strategy'] for e in entries)
    by_symbol = Counter(e['symbol'] for e in entries)

    def fmt(counter):
        return ', '.join(f"{k}:{v}" for k, v in counter.most_common()) if counter else 'aucun'

    print(f"TF        : {fmt(by_tf)}")
    print(f"Type      : {fmt(by_type)}")
    print(f"Status    : {fmt(by_status)}")
    print(f"Stratégie : {fmt(by_strategy)}")
    print(f"Top symbols: {fmt(Counter(dict(by_symbol.most_common(10))))}")

    if expected_tfs:
        missing = [tf for tf in expected_tfs if by_tf.get(tf, 0) == 0]
        if missing:
            print("\n🚨 TF attendus absents:")
            for tf in missing:
                print(f"  • {tf.upper()}: jamais reçu dans la fenêtre auditée")
        else:
            print("\n✅ Tous les TF attendus sont présents.")

    last_seen = defaultdict(lambda: None)
    for e in entries:
        tf = e['tf'] or 'N/A'
        if e['ts'] and (last_seen[tf] is None or e['ts'] > last_seen[tf]):
            last_seen[tf] = e['ts']
    if last_seen:
        print("\n🕒 Dernière réception par TF:")
        for tf, dt in sorted(last_seen.items()):
            iso = dt.isoformat() if dt else 'N/A'
            print(f"  • {tf.upper():4} -> {iso}")


def print_events(entries, show):
    print("\n🧾 Derniers événements:")
    for item in entries[:show]:
        ts = item['ts'].isoformat() if item['ts'] else 'N/A'
        print(
            f"[{ts}] {item['symbol']:12} | tf={item['tf'] or 'N/A':4} "
            f"| type={item['type']:12} | strategy={item['strategy']:10} "
            f"| status={item['status']:14} | val={item['value']}"
        )


def main():
    parser = argparse.ArgumentParser(description='Audit des alertes reçues par le bot')
    parser.add_argument('--source', choices=['redis', 'jsonl'], default='redis')
    parser.add_argument('--jsonl-path', default='logs/alerts.jsonl')
    parser.add_argument('--limit', type=int, default=1000)
    parser.add_argument('--show', type=int, default=50)

    parser.add_argument('--symbol')
    parser.add_argument('--type')
    parser.add_argument('--tf')
    parser.add_argument('--strategy')
    parser.add_argument('--status')
    parser.add_argument('--since-hours', type=float)

    parser.add_argument(
        '--expected-tfs',
        default='15m,1h,4h,1d',
        help='Liste TF attendus séparés par virgules',
    )
    args = parser.parse_args()

    if args.source == 'redis':
        client = get_redis_client()
        if not client:
            return
        raw_logs = load_from_redis(client, args.limit)
    else:
        if not os.path.exists(args.jsonl_path):
            print(f"❌ Fichier introuvable: {args.jsonl_path}")
            return
        raw_logs = load_from_jsonl(args.jsonl_path, args.limit)

    entries = [standardize_entry(e) for e in raw_logs]
    entries = apply_filters(entries, args)

    expected_tfs = [normalize_tf(tf) for tf in args.expected_tfs.split(',') if tf.strip()]
    print_summary(entries, expected_tfs)
    print_events(entries, args.show)


if __name__ == '__main__':
    main()