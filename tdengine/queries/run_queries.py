"""Time translated TSBS IoT queries against Arc or TDengine over HTTP.

Both engines are measured from the same client process, one query at a time:
  arc       POST {url}/api/v1/query   {"sql": ..., "format": "json"}
  tdengine  POST {url}/rest/sql/{db}  (taosAdapter REST, body = SQL)

For each query type, the first instance is a cold-ish run (first touch after
load or restart) and is reported separately; the remaining instances are warm.
Writes one JSON line per execution to --out, plus a summary to stdout.

Usage:
  python3 run_queries.py --engine arc --url http://localhost:8000 --token T \
      --db iot --queries q.jsonl --out results.jsonl
  python3 run_queries.py --engine tdengine --url http://localhost:6041 \
      --user root --password taosdata --db iot --queries q.jsonl --out results.jsonl
"""
import argparse
import base64
import json
import statistics
import time
import urllib.request

p = argparse.ArgumentParser()
p.add_argument("--engine", choices=["arc", "tdengine"], required=True)
p.add_argument("--url", required=True)
p.add_argument("--db", required=True)
p.add_argument("--queries", required=True)
p.add_argument("--out", required=True)
p.add_argument("--token", default="")
p.add_argument("--user", default="root")
p.add_argument("--password", default="taosdata")
p.add_argument("--timeout", type=float, default=600)
a = p.parse_args()


def run(sql):
    if a.engine == "arc":
        req = urllib.request.Request(
            a.url + "/api/v1/query",
            data=json.dumps({"sql": sql, "format": "json"}).encode(),
            headers={"Authorization": "Bearer " + a.token, "Content-Type": "application/json"},
        )
    else:
        auth = base64.b64encode(f"{a.user}:{a.password}".encode()).decode()
        req = urllib.request.Request(
            a.url + "/rest/sql/" + a.db, data=sql.encode(),
            headers={"Authorization": "Basic " + auth},
        )
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=a.timeout) as r:
        body = r.read()
    ms = (time.perf_counter() - t0) * 1000
    res = json.loads(body)
    if a.engine == "arc":
        if not res.get("success", False):
            raise RuntimeError(res.get("error"))
        rows = res.get("row_count", len(res.get("data") or []))
    else:
        if res.get("code", 0) != 0:
            raise RuntimeError(res.get("desc"))
        rows = res.get("rows", len(res.get("data") or []))
    return ms, rows


queries = [json.loads(l) for l in open(a.queries)]
key = "arc_sql" if a.engine == "arc" else "tdengine_sql"
seen = set()
by_type = {}
with open(a.out, "w") as out:
    for q in queries:
        sql = q[key].replace("{db}", a.db)
        cold = q["type"] not in seen
        seen.add(q["type"])
        try:
            ms, rows = run(sql)
            err = None
        except Exception as e:  # recorded, never silently dropped
            ms, rows, err = None, None, str(e)[:300]
        rec = {"engine": a.engine, "type": q["type"], "cold": cold, "ms": ms, "rows": rows, "error": err}
        out.write(json.dumps(rec) + "\n")
        by_type.setdefault(q["type"], []).append(rec)

print(f"{'type':38} {'cold ms':>9} {'warm n':>6} {'median':>9} {'p95':>9} {'errors':>6}")
for t, recs in by_type.items():
    cold = [r for r in recs if r["cold"]]
    warm = [r["ms"] for r in recs if not r["cold"] and r["ms"] is not None]
    errs = sum(1 for r in recs if r["error"])
    warm.sort()
    med = statistics.median(warm) if warm else float("nan")
    p95 = warm[max(0, int(round(0.95 * len(warm))) - 1)] if warm else float("nan")
    c = cold[0]["ms"] if cold and cold[0]["ms"] is not None else float("nan")
    print(f"{t:38} {c:9.1f} {len(warm):6d} {med:9.1f} {p95:9.1f} {errs:6d}")
