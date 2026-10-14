#!/usr/bin/env python3
"""UTC samples of generator CPU, memory, NIC bytes, and native client processes."""
import json, pathlib, subprocess, time

output=pathlib.Path.home()/'results/driver-monitor.jsonl'
while True:
    record={'time':time.time()}
    for name,path in [('cpustat','/proc/stat'),('meminfo','/proc/meminfo'),('netdev','/proc/net/dev')]:
        record[name]=pathlib.Path(path).read_text()
    record['process']=subprocess.run(['ps','-C','sizing_bench','-o','pid,pcpu,rss,vsz,nlwp,etimes'],text=True,capture_output=True).stdout
    with output.open('a') as stream:stream.write(json.dumps(record)+'\n')
    time.sleep(5)
