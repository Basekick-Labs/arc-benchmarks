#!/usr/bin/env python3
"""Record ENA allowance counters to identify EC2 network throttling."""
import json, pathlib, subprocess, sys, time

output=pathlib.Path(sys.argv[1])
interfaces=[p.name for p in pathlib.Path('/sys/class/net').iterdir() if p.name!='lo' and (p/'device').exists()]
while True:
    record={'time':time.time(),'interfaces':{}}
    for interface in interfaces:
        result=subprocess.run(['ethtool','-S',interface],capture_output=True,text=True,timeout=3)
        counters={}
        for line in result.stdout.splitlines():
            if 'allowance' in line and ':' in line:
                key,value=line.strip().split(':',1)
                try:counters[key]=int(value.strip())
                except ValueError:pass
        record['interfaces'][interface]=counters
    with output.open('a') as stream:stream.write(json.dumps(record)+'\n')
    time.sleep(5)
