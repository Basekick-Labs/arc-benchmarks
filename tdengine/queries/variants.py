import json, statistics, sys, time, urllib.request
URL, TOKEN = "http://localhost:8000", sys.argv[1]
def run(sql):
    req = urllib.request.Request(URL + "/api/v1/query", data=json.dumps({"sql": sql, "format": "json"}).encode(),
        headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"})
    t = time.perf_counter(); r = json.loads(urllib.request.urlopen(req, timeout=600).read())
    if not r.get("success"): raise RuntimeError(r.get("error"))
    return (time.perf_counter() - t) * 1000, r["row_count"]
F = "fleet = 'South'"
V = {
 "last-loc DISTINCT ON": f"SELECT DISTINCT ON (name, driver) time, name, driver, latitude, longitude FROM iot.readings WHERE {F} AND name IS NOT NULL ORDER BY name, driver, time DESC",
 "last-loc ROW_NUMBER": f"SELECT time, name, driver, latitude, longitude FROM (SELECT time, name, driver, latitude, longitude, ROW_NUMBER() OVER (PARTITION BY name, driver ORDER BY time DESC) AS rn FROM iot.readings WHERE {F} AND name IS NOT NULL) WHERE rn = 1",
 "last-loc MAX(time) join": f"WITH l AS (SELECT name, driver, MAX(time) AS t FROM iot.readings WHERE {F} AND name IS NOT NULL GROUP BY name, driver) SELECT r.time, r.name, r.driver, r.latitude, r.longitude FROM iot.readings r JOIN l ON r.name = l.name AND r.driver = l.driver AND r.time = l.t WHERE r.{F}",
 "high-load DISTINCT ON": f"SELECT * FROM (SELECT DISTINCT ON (name, driver) time, name, driver, current_load, load_capacity FROM iot.diagnostics WHERE {F} ORDER BY name, driver, time DESC) WHERE current_load >= 0.9 * load_capacity",
 "low-fuel DISTINCT ON": f"SELECT DISTINCT ON (name, driver) time, name, driver, fuel_state FROM iot.diagnostics WHERE fuel_state <= 0.1 AND {F} AND name IS NOT NULL ORDER BY name, driver, time DESC",
 "low-fuel MAX(time) join": f"WITH l AS (SELECT name, driver, MAX(time) AS t FROM iot.diagnostics WHERE fuel_state <= 0.1 AND {F} AND name IS NOT NULL GROUP BY name, driver) SELECT d.time, d.name, d.driver, d.fuel_state FROM iot.diagnostics d JOIN l ON d.name = l.name AND d.driver = l.driver AND d.time = l.t WHERE d.fuel_state <= 0.1 AND d.{F}",
 "high-load MAX(time) join": f"WITH l AS (SELECT name, driver, MAX(time) AS t FROM iot.diagnostics WHERE {F} GROUP BY name, driver) SELECT d.time, d.name, d.driver, d.current_load, d.load_capacity FROM iot.diagnostics d JOIN l ON d.name = l.name AND d.driver = l.driver AND d.time = l.t WHERE d.{F} AND d.current_load >= 0.9 * d.load_capacity",
}
for k, sql in V.items():
    try:
        ms = [run(sql) for _ in range(7)]
        print(f"{k:28} rows={ms[0][1]:6} median={statistics.median(m for m,_ in ms[1:]):8.1f} ms")
    except Exception as e:
        print(f"{k:28} ERROR {str(e)[:150]}")
