#!/usr/bin/env python3
"""Summarise node-leveler rounds from Loki (independent of local logs, survives Job TTL).

Usage:
    # Loki must be reachable first (either way):
    kubectl -n monitoring port-forward svc/grafana-loki-gateway 13100:80 &
    python3 scripts/rounds.py --hours 24
    python3 scripts/rounds.py --hours 24 --loki http://grafana-loki-gateway.monitoring.svc:80

Output: one row per round (one job pod = one round): start time, moves, bounces, landings
outside the ceilings, aborts.
"""
import argparse
import json
import re

import sys
import urllib.parse
import urllib.request

from collections import defaultdict
from datetime import datetime, timezone

# CRI-formatted container logs arrive as "<ts> stdout F <payload>". promtail keeps the
# prefix unless its pipeline has a `cri` stage, so every JSON parser here would silently
# return nothing (a whole cluster looked like "0 rounds"). Strip it before parsing.
_CRI_PREFIX = re.compile(r"^\S+ (stdout|stderr) [FP] ")


def cri_body(line):
    return _CRI_PREFIX.sub("", line, count=1)

NS = "cluster-ops"


def query(loki, expr, hours, limit=5000):
    now = int(datetime.now(timezone.utc).timestamp())
    start = now - hours * 3600
    qs = urllib.parse.urlencode({
        "query": expr, "start": f"{start}000000000", "end": f"{now}000000000",
        "limit": limit, "direction": "backward",
    })
    with urllib.request.urlopen(f"{loki}/loki/api/v1/query_range?{qs}", timeout=60) as r:
        return json.loads(r.read())["data"]["result"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loki", default="http://127.0.0.1:13100")
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--namespace", default=NS)
    a = ap.parse_args()

    expr = '{namespace="%s"} |= "\\"kind\\":"' % a.namespace
    try:
        streams = query(a.loki, expr, a.hours)
    except Exception as e:
        print(f"Loki query failed ({a.loki}): {type(e).__name__}: {e}", file=sys.stderr)
        print("  start a port-forward first: kubectl -n monitoring port-forward svc/grafana-loki-gateway 13100:80 &", file=sys.stderr)
        return 2

    rounds = defaultdict(lambda: {"start": None, "moves": [], "bounce": 0, "oog": 0,
                                  "abort": 0, "done": None, "mode": None})
    for s in streams:
        pod = s["stream"].get("pod", "?")
        if not pod.startswith("node-leveler-"):
            pass
        for ts, line in s["values"]:
            try:
                d = json.loads(cri_body(line))
            except Exception:
                continue
            r = rounds[pod]
            t = datetime.fromtimestamp(int(ts) / 1e9, timezone.utc).astimezone()
            if r["start"] is None or t < r["start"]:
                r["start"] = t
            k = d.get("kind")
            if k == "config":
                r["mode"] = d.get("mode")
            elif k == "move":
                r["moves"].append(d)
                if d.get("action") == "bounce":
                    r["bounce"] += 1
                if d.get("landed_out_of_guard"):
                    r["oog"] += 1
            elif k == "abort":
                r["abort"] += 1
            elif k == "done":
                r["done"] = d

    rows = sorted(rounds.items(), key=lambda kv: kv[1]["start"] or datetime.min.replace(tzinfo=timezone.utc))
    print(f"{'started':16}{'job':44}{'mode':7}{'moves':>6}{'bounce':>7}{'guard':>6}{'abort':>6}  landings")
    tot = 0
    for pod, r in rows:
        n = len(r["moves"])
        if n == 0 and r["done"] is None:
            continue
        tot += n
        lands = ",".join(sorted({(m.get("landed") or "-") for m in r["moves"]}))[:60]
        print(f"{r['start'].strftime('%m-%d %H:%M:%S') if r['start'] else '?':16}"
              f"{pod[:43]:44}{(r['mode'] or '?'):7}{n:>4}{r['bounce']:>7}{r['oog']:>5}{r['abort']:>6}  {lands}")
    print(f"\n{len(rows)} rounds / {tot} moves (last {a.hours:g}h, from Loki ns={a.namespace})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
