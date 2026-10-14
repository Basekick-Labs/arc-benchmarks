#!/usr/bin/env python3
"""Compare ingestion with a separate, equally provisioned WAL disk."""
import argparse, fcntl, json, time
from controller import Campaign, save

parser=argparse.ArgumentParser();parser.add_argument('--host',required=True);parser.add_argument('--name',required=True)
args=parser.parse_args();args.anchor=0;args.seed_records=20000000;args.ingest_workers=32;args.sweep_seconds=30;args.soak_seconds=600;args.repeats=3
campaign=Campaign(args)
lock=(campaign.root/'split-wal.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
try:
    campaign.wal=True;campaign.wal_path='/srv/arc/wal-disk/working/wal';campaign.control('layout','split')
    for protocol in ['msgpack','lineprotocol']:
        checkpoint=campaign.root/f'wal-split-{protocol}-validated-capacity.json'
        if checkpoint.exists():continue
        campaign.anchor=int(time.time()*1e6)
        save(checkpoint,campaign.qualify_capacity('wal-split',protocol))
    shared=json.loads((campaign.root/'wal-on-msgpack-validated-capacity.json').read_text())
    split=json.loads((campaign.root/'wal-split-msgpack-validated-capacity.json').read_text())
    path=campaign.root/'wal-split-msgpack-matched-shared-ingest.json'
    if shared['target_rps']!=split['target_rps']:
        r=json.loads(path.read_text()) if path.exists() else None
        if not r or not campaign.ingestion_valid(r,shared['target_rps'],15):
            campaign.control('reset')
            r=campaign.stage(path.stem,database='ingest',workers=32,duration=600,warmup=15,rate=shared['target_rps'])
        qualified=campaign.ingestion_valid(r,shared['target_rps'],15)
        campaign.log('split_wal_matched',qualified=qualified,offered_rate=shared['target_rps'])
        if not qualified:raise RuntimeError('separate-WAL matched run did not qualify; preserve server for review')
    campaign.control('layout','shared');campaign.log('split_wal_complete')
    (campaign.root/'SPLIT-WAL-COMPLETE').write_text(time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())+'\n')
except Exception as exc:
    campaign.log('campaign_failed',error=str(exc));raise
