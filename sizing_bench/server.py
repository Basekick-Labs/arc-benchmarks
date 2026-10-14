#!/usr/bin/env python3
"""Controls only the native service and disposable benchmark data directory."""
import json, os, pathlib, shutil, subprocess, sys, time, urllib.request

ROOT = pathlib.Path('/srv/arc')
VERSION = 'main-116a318f936c'

def run(*args, check=True):
    return subprocess.run(args, text=True, capture_output=True, check=check).stdout

def stop():
    run('systemctl', 'stop', 'arc')

def clear_working():
    shutil.rmtree(ROOT/'working', ignore_errors=True)
    # Only this campaign's WAL subtree is removed, never the mount itself.
    shutil.rmtree(ROOT/'wal-disk/working', ignore_errors=True)

def start():
    data = ROOT/'working'
    data.mkdir(exist_ok=True)
    run('chown', '-R', 'arc:arc', str(data))
    environment=(ROOT/'environment').read_text()
    if 'ARC_WAL_DIRECTORY=/srv/arc/wal-disk/' in environment:
        wal=ROOT/'wal-disk/working/wal';wal.mkdir(parents=True,exist_ok=True)
        run('chown','-R','arc:arc',str(ROOT/'wal-disk/working'))
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
        'ARC_BACKUP_LOCAL_PATH=/srv/arc/working/backups',
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
    clear_working()
    start()
elif action == 'layout':
    assert sys.argv[2] in ['shared','split']
    stop()
    if sys.argv[2]=='split' and not os.path.ismount(ROOT/'wal-disk'):
        raise RuntimeError('dedicated WAL disk is not mounted')
    directory='/srv/arc/wal-disk/working/wal' if sys.argv[2]=='split' else '/srv/arc/working/wal'
    env=ROOT/'environment';lines=env.read_text().splitlines()
    env.write_text('\n'.join('ARC_WAL_DIRECTORY='+directory if line.startswith('ARC_WAL_DIRECTORY=') else 'ARC_WAL_ENABLED=true' if line.startswith('ARC_WAL_ENABLED=') else line for line in lines)+'\n')
    clear_working();start()
elif action == 'reset':
    stop()
    clear_working()
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
    print((ROOT/'source.json').read_text())
    print(run('sha256sum', '/usr/bin/arc'))
    print(run('lsblk', '-J'))
    print(run('lscpu', '-J'))
