#!/usr/bin/env python3
"""Qualify lower query demand at the same growing MessagePack write workload."""
import argparse, fcntl, json, pathlib, time
from controller import Campaign, api, save

def main(args):
    args.anchor=0
    args.ingest_workers=32
    campaign=Campaign(args)
    for wal in [True,False]:
        campaign.wal=wal;tag='wal-on' if wal else 'wal-off'
        label=f'{tag}-hosts10000-msgpack-evaluation'
        checkpoint=campaign.root/(label+'-summary.json')
        if checkpoint.exists() and json.loads(checkpoint.read_text()).get('qualified'):
            campaign.log('resume_evaluation',wal=wal)
            continue
        primary=json.loads((campaign.root/f'{tag}-hosts10000-msgpack-mixed-summary.json').read_text())
        target_qps=primary['target_qps']*.4  # 10% of query-only capacity.
        target_rps=primary['target_rps']
        campaign.anchor=int(time.time()*1e6)
        if not wal:
            # Separate capacity selection from overhead: compare WAL on/off at
            # the same MessagePack offered rate as the validated WAL-on soak.
            enabled=json.loads((campaign.root/'wal-on-msgpack-validated-capacity.json').read_text())
            disabled=json.loads((campaign.root/'wal-off-msgpack-validated-capacity.json').read_text())
            match_label='wal-off-msgpack-matched-wal-on-ingest'
            match_file=campaign.root/(match_label+'.json')
            match_valid=match_file.exists() and campaign.ingestion_valid(json.loads(match_file.read_text()),enabled['target_rps'],15)
            if enabled['target_rps']!=disabled['target_rps'] and not match_valid:
                campaign.control('wal','false')
                matched=campaign.stage(match_label,database='ingest',workers=args.ingest_workers,duration=args.soak_seconds,warmup=15,rate=enabled['target_rps'])
                campaign.log('matched_wal_comparison',qualified=campaign.ingestion_valid(matched,enabled['target_rps'],15),offered_rate=enabled['target_rps'])
        campaign.control('wal',str(wal).lower())
        seed=campaign.stage(label+'-seed',hosts=10000,duration=900,warmup=0,max_records=args.seed_records,history=True,workers=args.ingest_workers)
        if not seed['persistence_validated']:raise RuntimeError('evaluation seed mismatch')
        try:save(campaign.root/(label+'-compaction.json'),api(args.host,'/api/v1/compaction/trigger?database=sizing',method='POST'))
        except Exception as exc:campaign.log('evaluation_compaction_error',error=str(exc))
        time.sleep(10)
        campaign.control('snapshot')
        standard=campaign.root/'queries-10000.json'
        previous=standard.read_bytes()
        queries=campaign.query_file(10000)
        definitions=json.loads(queries.read_text())
        queries=campaign.root/(label+'-query-definitions.json');save(queries,definitions)
        standard.write_bytes(previous)
        for query in definitions:
            if not api(args.host,'/api/v1/query','sizing',query['sql']).get('data'):raise RuntimeError('empty evaluation query')
        for attempt in range(3):
            campaign.control('restore')
            run_label=label if attempt==0 else label+f'-retry{attempt}'
            qtask=campaign.stage(run_label+'-queries',mode='query',workers=4,duration=args.soak_seconds,warmup=15,rate=target_qps,queries=queries,wait=False)
            itask=campaign.stage(run_label+'-ingest',workers=args.ingest_workers,duration=args.soak_seconds,warmup=15,rate=target_rps,wait=False)
            qr=campaign.finish(qtask,run_label+'-queries','query','sizing')
            ir=campaign.finish(itask,run_label+'-ingest','ingest','sizing')
            qualified=campaign.ingestion_valid(ir,target_rps,15) and qr.get('errors_including_warmup',qr['errors'])==0 and not any(qr.get('generator_network_allowance_deltas',{}).values()) and qr['requests_per_second']>=target_qps*.95 and qr['seconds']>=args.soak_seconds*.95
            summary={'queries':qr,'ingestion':ir,'target_qps':target_qps,'target_rps':target_rps,'qualified':qualified,'selected_case':run_label}
            save(campaign.root/(run_label+'-summary.json'),summary);save(checkpoint,summary)
            if qualified:break
            campaign.log('reducing_evaluation_query_load',wal=wal,old_qps=target_qps,new_qps=target_qps*.5)
            target_qps*=.5
        if not qualified:raise RuntimeError(f'{tag} combined evaluation did not qualify after three attempts')
    campaign.control('reset')
    campaign.log('evaluation_complete')
    (campaign.root/'EVALUATION-COMPLETE').write_text(time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())+'\n')

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--host',required=True);parser.add_argument('--name',required=True)
    parser.add_argument('--seed-records',type=int,default=20000000)
    parser.add_argument('--soak-seconds',type=int,default=600)
    args=parser.parse_args()
    lockdir=pathlib.Path.home()/'results'/args.name
    lockdir.mkdir(parents=True,exist_ok=True)
    lock=(lockdir/'evaluation.lock').open('w')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    try:main(args)
    except Exception as exc:
        Campaign(args).log('campaign_failed',error=str(exc));raise
