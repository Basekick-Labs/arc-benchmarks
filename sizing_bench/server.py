#!/usr/bin/env python3
"""Controls only the native service and disposable benchmark data directory."""
import json, os, pathlib, shutil, subprocess, sys, time, urllib.request

ROOT = pathlib.Path('/srv/arc')
VERSION = '26.09.3'

def run(*args, check=True):
    return subprocess.run(args, text=True, capture_output=True, check=check).stdout

def stop():
    run('systemctl', 'stop', 'arc')

def start():
    data = ROOT/'working'
    data.mkdir(exist_ok=True)
    run('chown', '-R', 'arc:arc', str(data))
    run('systemctl', 'start', 'arc')
    for _ in range(120):
        try:
            with urllib.request.urlopen('http://localhost:8000/health', timeout=2) as response:
                if response.status == 200:
                    print(json.dumps({'status':'ready', 'native_version':VERSION}))
                    return
        except Exception:
            time.sleep(1)
    raise RuntimeError(run('journalctl', '-u', 'arc', '-n', '50', '--no-pager'))

action = sys.argv[1]
if action == 'configure':
    stop()
    token = (ROOT/'token').read_text().strip()
    environment = ROOT/'environment'
    environment.write_text('\n'.join([
        'ARC_STORAGE_BACKEND=local',
        'ARC_STORAGE_LOCAL_PATH=/srv/arc/working/data',
        'ARC_WAL_ENABLED='+sys.argv[2],
        'ARC_WAL_DIRECTORY=/srv/arc/working/wal',
        'ARC_WAL_SYNC_MODE=fdatasync',
        'ARC_COMPACTION_TEMP_DIRECTORY=/srv/arc/working/compaction',
        'ARC_CACHE_ENABLED=false',
        'ARC_TELEMETRY_ENABLED=false',
        'ARC_AUTH_BOOTSTRAP_TOKEN='+token,
    ])+'\n')
    environment.chmod(0o600)
    dropin=pathlib.Path('/etc/systemd/system/arc.service.d')
    dropin.mkdir(exist_ok=True)
    (dropin/'sizing.conf').write_text('[Service]\nWorkingDirectory=/srv/arc/working\nEnvironmentFile=/srv/arc/environment\nRestart=no\n')
    run('systemctl','daemon-reload')
    start()
elif action == 'wal':
    stop()
    env=ROOT/'environment'
    lines=env.read_text().splitlines()
    env.write_text('\n'.join('ARC_WAL_ENABLED='+sys.argv[2] if line.startswith('ARC_WAL_ENABLED=') else line for line in lines)+'\n')
    shutil.rmtree(ROOT/'working', ignore_errors=True)
    start()
elif action == 'reset':
    stop()
    shutil.rmtree(ROOT/'working', ignore_errors=True)
    start()
elif action == 'start':
    start()
elif action == 'snapshot':
    stop()
    shutil.rmtree(ROOT/'seed', ignore_errors=True)
    shutil.copytree(ROOT/'working', ROOT/'seed')
    start()
elif action == 'restore':
    stop()
    shutil.rmtree(ROOT/'working', ignore_errors=True)
    shutil.copytree(ROOT/'seed', ROOT/'working')
    start()
elif action == 'info':
    print(run('systemctl', 'show', 'arc', '-p', 'MainPID', '-p', 'NRestarts', '-p', 'MemoryCurrent', '-p', 'CPUUsageNSec'))
    print(run('dpkg-query', '-W', 'arc'))
    print(run('sha256sum', '/usr/bin/arc'))
    print(run('lsblk', '-J'))
    print(run('lscpu', '-J'))
