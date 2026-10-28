#!/usr/bin/env python3
"""Rebuild final.json and summary.txt from the per-run result directories.

  python3 results/aggregate.py

Query figures are warm medians (cold first instance excluded) pooled over every
run of a variant. TDengine's figure is the faster of the REST median and the
median of the native runner's per-run medians, except daily-activity, where the
native client bucketed days in the local time zone and only REST is used.
Storage is reported in decimal MB (du -k x 1024 / 1e6).
"""
import glob
import json
import os
import re
import statistics
from decimal import ROUND_HALF_UP, Decimal

R = os.path.dirname(os.path.abspath(__file__))
ARC_VARIANTS = {
    'arc': 'as shipped, wal.ack_mode = "queued"',
    'arc_durable': 'as shipped, wal.ack_mode = "durable"',
    'arc_sorted': 'ingest.sort_keys = fleet,name, wal.ack_mode = "queued"',
    'arc_sorted_durable': 'ingest.sort_keys = fleet,name, wal.ack_mode = "durable"',
}
# Variants whose query timings are reported (durable runs share the data layout
# of their queued twin; their query timings are in the run directories).
QUERY_VARIANTS = ['arc', 'arc_sorted']


def runs(scale, variant):
    return sorted(glob.glob(f'{R}/scale{scale}/{variant}/run*'))


def warm(scale, variant, qtype):
    v = []
    for d in runs(scale, variant):
        for line in open(f'{d}/queries.jsonl'):
            o = json.loads(line)
            if o['type'] == qtype and not o['cold']:
                v.append(o['ms'])
    return statistics.median(v)


def native(scale, qtype):
    meds = []
    for d in runs(scale, 'tdengine'):
        p = f'{d}/native/{qtype}.out'
        if os.path.exists(p):
            m = re.search(r'med:\s+([\d.]+)ms', open(p).read())
            if m:
                meds.append(float(m.group(1)))
    return statistics.median(meds) if meds else None


def mismatches(scale, variant):
    # The query set is identical in every run, so compare against TDengine run1.
    bad = total = 0
    for d in runs(scale, variant):
        td = [json.loads(l) for l in open(f'{R}/scale{scale}/tdengine/run1/queries.jsonl')]
        ar = [json.loads(l) for l in open(f'{d}/queries.jsonl')]
        for a, t in zip(ar, td):
            total += 1
            bad += a['rows'] != t['rows'] or bool(a.get('error'))
    return bad, total


def loads(scale, variant):
    L = [json.load(open(f'{d}/load.json')) for d in runs(scale, variant)]
    st = [int(open(f'{d}/storage_kb.txt').read()) * 1024 / 1e6 for d in runs(scale, variant)]
    ack = [round(x['load_client_s'], 2) for x in L]
    q = [round(x['load_until_queryable_s'], 2) for x in L]
    return {
        'runs': len(L), 'rows': L[0]['rows'],
        'ack_s': ack, 'ack_med_s': round(statistics.median(ack), 2),
        'queryable_s': q, 'queryable_med_s': round(statistics.median(q), 2),
        'storage_mb_med': round(statistics.median(st), 1),
    }


def f1(x):
    # Half-up to one decimal, matching the page's toFixed(1) on the stored 2-decimal medians.
    return str(Decimal(str(round(x, 2))).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP))


def ratio(arc, td):
    return f'Arc {td / arc:.1f}x' if arc < td else f'TDengine {arc / td:.1f}x'


out, lines = {'variants': ARC_VARIANTS}, []
for scale in (1000, 10000):
    qtypes = sorted({json.loads(l)['type'] for l in open(f'{R}/scale{scale}/tdengine/run1/queries.jsonl')})
    queries = []
    for t in qtypes:
        rest = warm(scale, 'tdengine', t)
        nat = None if t == 'daily-activity' else native(scale, t)
        row = {'type': t, 'td_rest_ms': round(rest, 2), 'td_native_ms': nat and round(nat, 2),
               'td_best_ms': round(min(x for x in (rest, nat) if x is not None), 2)}
        for v in QUERY_VARIANTS:
            row[f'{v}_ms'] = round(warm(scale, v, t), 2)
        queries.append(row)
    ld = {v: loads(scale, v) for v in list(ARC_VARIANTS) + ['tdengine']}
    out[str(scale)] = {'queries': queries, 'loads': ld}

    lines.append(f'=== {scale:,} trucks ({scale * 10:,} tags) ===')
    for v in ARC_VARIANTS:
        bad, total = mismatches(scale, v)
        lines.append(f'{v}: instances={total} row mismatches or errors vs tdengine={bad}')
    lines.append(f"{'query':36}{'sorted':>9}{'shipped':>9}{'TD best':>9}  {'sorted vs TD':>15}  {'shipped vs TD':>15}")
    for q in queries:
        lines.append(f"{q['type']:36}{f1(q['arc_sorted_ms']):>9}{f1(q['arc_ms']):>9}{f1(q['td_best_ms']):>9}  "
                     f"{ratio(q['arc_sorted_ms'], q['td_best_ms']):>15}  {ratio(q['arc_ms'], q['td_best_ms']):>15}")
    for v, x in ld.items():
        lines.append(f"{v:19} ack={x['ack_s']} med={x['ack_med_s']}s queryable med={x['queryable_med_s']}s "
                     f"storage={x['storage_mb_med']} MB")
    lines.append('')

json.dump(out, open(f'{R}/final.json', 'w'), indent=1)
open(f'{R}/summary.txt', 'w').write('\n'.join(lines))
print('\n'.join(lines))
