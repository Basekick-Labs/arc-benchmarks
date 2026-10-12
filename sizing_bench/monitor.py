#!/usr/bin/env python3
"""Five-second host/native process, Arc metrics, disk, and Spot interruption samples."""
import json, pathlib, subprocess, time, urllib.request

def request(url, token=None):
    headers = {'Authorization':'Bearer '+token} if token else {}
    with urllib.request.urlopen(urllib.request.Request(url,headers=headers),timeout=3) as r:
        return r.read().decode()

token=pathlib.Path('/srv/arc/token').read_text().strip()
path=pathlib.Path('/srv/arc/results/monitor.jsonl')
while True:
    started=time.monotonic()
    record={'time':time.time()}
    for name, args in [('service',['systemctl','show','arc','-p','MainPID','-p','NRestarts','-p','MemoryCurrent','-p','CPUUsageNSec','-p','ActiveState']),('process',['ps','-C','arc','-o','pid,pcpu,rss,vsz,nlwp,etimes']),('vmstat',['vmstat','1','2']),('iostat',['iostat','-dx','1','2']),('disk',['df','-B1','/srv/arc'])]:
        try:
            record[name]=subprocess.run(args,capture_output=True,text=True,timeout=4).stdout
        except Exception as exc:
            record[name+'_error']=str(exc)
    try:
        record['metrics']=request('http://localhost:8000/metrics',token)
    except Exception as exc:
        record['metrics_error']=str(exc)
    record['meminfo']=pathlib.Path('/proc/meminfo').read_text()
    record['cpustat']=pathlib.Path('/proc/stat').read_text()
    record['netdev']=pathlib.Path('/proc/net/dev').read_text()
    with path.open('a') as out:
        out.write(json.dumps(record)+'\n')
    time.sleep(max(0,5-(time.monotonic()-started)))
