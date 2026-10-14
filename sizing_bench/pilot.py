#!/usr/bin/env python3
"""Functional pilot of native startup, encodings, WAL, seed, SQL, and mixed I/O."""
import argparse, json
from controller import Campaign, api, save

parser=argparse.ArgumentParser();parser.add_argument('--host',required=True)
args=parser.parse_args();args.name='pilot';args.anchor=0;args.seed_records=100000;args.ingest_workers=4
campaign=Campaign(args)
for wal in [True,False]:
    campaign.wal=wal;tag='wal-on' if wal else 'wal-off'
    campaign.control('wal',str(wal).lower())
    for protocol in ['msgpack','lineprotocol']:
        campaign.control('reset')
        result=campaign.stage(tag+'-'+protocol+'-smoke',database='ingest',protocol=protocol,workers=4,duration=3,warmup=0,rate=50000)
        assert result['persistence_validated'] and result['errors_including_warmup']==0
        before=result['server_metrics_before'];after=result['server_metrics_drained']
        assert after.get('arc_wal_dropped_entries_total',0)==before.get('arc_wal_dropped_entries_total',0)
    campaign.control('reset')
    result=campaign.stage(tag+'-seed',workers=4,duration=120,warmup=0,max_records=args.seed_records,history=True)
    assert result['persistence_validated'] and result['persisted_rows']==args.seed_records
    queries=campaign.query_file(10000)
    for query in json.loads(queries.read_text()):assert api(args.host,'/api/v1/query','sizing',query['sql'])['data']
    campaign.control('snapshot');campaign.control('restore')
    q=campaign.stage(tag+'-mixed-queries',mode='query',workers=2,duration=3,warmup=0,rate=5,queries=queries,wait=False)
    i=campaign.stage(tag+'-mixed-ingest',workers=4,duration=3,warmup=0,rate=50000,wait=False)
    qr=campaign.finish(q,tag+'-mixed-queries','query','sizing')
    ir=campaign.finish(i,tag+'-mixed-ingest','ingest','sizing')
    assert qr['errors_including_warmup']==0 and ir['persistence_validated'] and ir['errors_including_warmup']==0
campaign.wal=True;campaign.wal_path='/srv/arc/wal-disk/working/wal';campaign.control('layout','split')
for protocol in ['msgpack','lineprotocol']:
    campaign.control('reset')
    result=campaign.stage('wal-split-'+protocol+'-smoke',database='ingest',protocol=protocol,workers=4,duration=3,warmup=0,rate=50000)
    assert result['persistence_validated'] and result['errors_including_warmup']==0
    assert result['wal_directory']=='/srv/arc/wal-disk/working/wal' and result['wal_bytes']>0
campaign.control('layout','shared');campaign.log('pilot_complete')
(campaign.root/'COMPLETE').write_text('Functional pilot passed\n')
