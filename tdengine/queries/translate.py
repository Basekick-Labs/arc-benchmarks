"""Translate TSBS IoT queries generated for TDengine into Arc SQL.

Input: JSON lines from tsbs_dump_tdengine_queries ({"label", "sql"}).
Output: JSON lines {"type", "label", "tdengine_sql", "arc_sql"} for the
query types this benchmark runs. Parameters (fleet, time range) are taken
from each generated TDengine query, so both databases run the same instances.

TDengine-only constructs map to standard SQL as follows:
  last_row(ts) ... partition by k   ->  DISTINCT ON (k) ... ORDER BY k, time DESC
                                       (last-loc: MAX(time) per k, joined back)
  partition by ... interval(10m)    ->  GROUP BY ..., time_bucket(INTERVAL '10 minutes', time)
  tbname (one subtable per truck)   ->  name
  ts (epoch ms literal)             ->  time compared with epoch_ms(<literal>)
"""
import json
import re
import sys

DB = sys.argv[1] if len(sys.argv) > 1 else "iot"

TYPES = {
    "last location per truck": "last-loc",
    "trucks with low fuel": "low-fuel",
    "trucks with high load": "high-load",
    "stationary trucks": "stationary-trucks",
    "trucks with longer driving sessions": "long-driving-sessions",
    "trucks with longer daily sessions": "long-daily-sessions",
    "average vs projected fuel consumption per fleet": "avg-vs-projected-fuel-consumption",
    "average load per truck model per fleet": "avg-load",
    "daily truck activity per fleet per model": "daily-activity",
}


def fleet(sql):
    return re.search(r"fleet\s*=\s*['\"](\w+)['\"]", sql).group(1)


def ts_range(sql):
    m = re.search(r"ts\s*>=?\s*(\d+)\s+AND\s+ts\s*<=?\s*(\d+)", sql, re.I)
    return int(m.group(1)), int(m.group(2))


def ts_ops(sql):
    m = re.search(r"ts\s*(>=?)\s*\d+\s+AND\s+ts\s*(<=?)\s*\d+", sql, re.I)
    return m.group(1), m.group(2)


def having(sql):
    return int(re.search(r"count\(\*\)\s*>\s*(\d+)", sql, re.I).group(1))


def arc_sql(qtype, sql):
    if qtype == "last-loc":
        # Latest row per (name, driver) via MAX(time) + join: 2.2x faster on
        # Arc than DISTINCT ON at 10k trucks, identical rows.
        f = fleet(sql)
        return (f"WITH l AS (SELECT name, driver, MAX(time) AS t FROM {DB}.readings "
                f"WHERE fleet = '{f}' AND name IS NOT NULL GROUP BY name, driver) "
                f"SELECT r.time, r.name, r.driver, r.latitude, r.longitude FROM {DB}.readings r "
                f"JOIN l ON r.name = l.name AND r.driver = l.driver AND r.time = l.t WHERE r.fleet = '{f}'")
    if qtype == "low-fuel":
        return (f"SELECT DISTINCT ON (name, driver) time, name, driver, fuel_state "
                f"FROM {DB}.diagnostics WHERE fuel_state <= 0.1 AND fleet = '{fleet(sql)}' "
                f"AND name IS NOT NULL ORDER BY name, driver, time DESC")
    if qtype == "high-load":
        return (f"SELECT * FROM (SELECT DISTINCT ON (name, driver) time, name, driver, current_load, "
                f"load_capacity FROM {DB}.diagnostics WHERE fleet = '{fleet(sql)}' "
                f"ORDER BY name, driver, time DESC) WHERE current_load >= 0.9 * load_capacity")
    if qtype == "stationary-trucks":
        a, b = ts_range(sql)
        lo, hi = ts_ops(sql)
        # TDengine's version is "partition by name, driver interval(10m) LIMIT 1":
        # the average of the FIRST aligned 10-minute window per truck in the range.
        return (f"SELECT name, driver FROM (SELECT name, driver, mean_velocity, "
                f"ROW_NUMBER() OVER (PARTITION BY name, driver ORDER BY w) AS rn FROM "
                f"(SELECT name, driver, time_bucket(INTERVAL '10 minutes', time) AS w, "
                f"AVG(velocity) AS mean_velocity FROM {DB}.readings WHERE time {lo} epoch_ms({a}) "
                f"AND time {hi} epoch_ms({b}) AND fleet = '{fleet(sql)}' GROUP BY name, driver, w)) "
                f"WHERE rn = 1 AND mean_velocity < 1")
    if qtype in ("long-driving-sessions", "long-daily-sessions"):
        a, b = ts_range(sql)
        lo, hi = ts_ops(sql)
        return (f"SELECT name, driver FROM (SELECT name, driver, "
                f"time_bucket(INTERVAL '10 minutes', time) AS w, AVG(velocity) AS mean_velocity "
                f"FROM {DB}.readings WHERE fleet = '{fleet(sql)}' AND time {lo} epoch_ms({a}) "
                f"AND time {hi} epoch_ms({b}) GROUP BY name, driver, w) WHERE mean_velocity > 1 "
                f"GROUP BY name, driver HAVING COUNT(*) > {having(sql)}")
    if qtype == "avg-vs-projected-fuel-consumption":
        return (f"SELECT fleet, AVG(fuel_consumption) AS avg_fuel_consumption, "
                f"AVG(nominal_fuel_consumption) AS nominal_fuel_consumption "
                f"FROM {DB}.readings WHERE velocity > 1 GROUP BY fleet")
    if qtype == "avg-load":
        return (f"SELECT fleet, model, load_capacity, AVG(ml / load_capacity) AS avg_load_pct FROM "
                f"(SELECT fleet, model, name, load_capacity, AVG(current_load) AS ml "
                f"FROM {DB}.diagnostics WHERE name IS NOT NULL "
                f"GROUP BY fleet, model, name, load_capacity) GROUP BY fleet, model, load_capacity")
    if qtype == "daily-activity":
        a, b = ts_range(sql)
        lo, hi = ts_ops(sql)
        return (f"SELECT time_bucket(INTERVAL '1 day', w) AS day, model, fleet, COUNT(*) / 144.0 AS active "
                f"FROM (SELECT model, fleet, name, time_bucket(INTERVAL '10 minutes', time) AS w, "
                f"AVG(status) AS ms1 FROM {DB}.diagnostics WHERE time {lo} epoch_ms({a}) "
                f"AND time {hi} epoch_ms({b}) GROUP BY model, fleet, name, w) "
                f"WHERE ms1 < 1 GROUP BY day, model, fleet")
    raise ValueError(qtype)


for line in sys.stdin:
    q = json.loads(line)
    label = q["label"].replace("TDengine ", "", 1).strip()
    qtype = TYPES.get(label)
    if not qtype:
        continue
    print(json.dumps({"type": qtype, "label": label, "tdengine_sql": q["sql"].strip(),
                      "arc_sql": arc_sql(qtype, q["sql"])}))
