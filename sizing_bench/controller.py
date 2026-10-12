#!/usr/bin/env python3
"""Run reproducible native OSS sizing cases from the dedicated load generator."""
import argparse, concurrent.futures, datetime, json, os, pathlib, statistics, subprocess, time, urllib.error, urllib.request

BASE=pathlib.Path.home()
TOKEN=(BASE/'token').read_text().strip()
ENV=dict(os.environ, ARC_TOKEN=TOKEN)
BIN=BASE/'harness/sizing_bench'

def remote(host, *args):
    return subprocess.run(['ssh','-i',str(BASE/'ssh_key'),'-o','StrictHostKeyChecking=accept-new','-o','ConnectTimeout=10','ubuntu@'+host,*args],check=True,text=True,capture_output=True).stdout

def api(host, path, database='sizing', sql=None, method=None):
    data=json.dumps({'sql':sql}).encode() if sql is not None else None
    req=urllib.request.Request('http://'+host+':8000'+path,data=data,method=method,headers={'Authorization':'Bearer '+TOKEN,'x-arc-database':database,'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=90) as r:
        body=r.read().decode()
    return json.loads(body) if body.startswith('{') else body

def metrics(host):
    text=api(host,'/metrics')
    values={}
    for line in text.splitlines():
        if line and not line.startswith('#'):
            parts=line.split()
            if len(parts)==2 and '{' not in parts[0]:
                try: values[parts[0]]=float(parts[1])
                except ValueError: pass
    return values

def count(host,database):
    return int(api(host,'/api/v1/query',database,'SELECT COUNT(*) FROM cpu')['data'][0][0])

def save(path,value):
    path.write_text(json.dumps(value,indent=2)+'\n')

class Campaign:
    def __init__(self,args):
        self.a=args;self.root=BASE/'results'/args.name;self.root.mkdir(parents=True,exist_ok=True)
        self.anchor=args.anchor or int(time.time()*1e6)
        self.logpath=self.root/'events.jsonl'

    def log(self,event,**data):
        record={'time_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'event':event,**data}
        print(json.dumps(record),flush=True)
        with self.logpath.open('a') as out: out.write(json.dumps(record)+'\n')

    def control(self,action,*args):
        self.log('service',action=action,args=args)
        return remote(self.a.host,'sudo','python3','/srv/arc/server.py',action,*args)

    def stage(self,label,mode='ingest',database='sizing',protocol='msgpack',workers=4,duration=30,warmup=5,rate=0,hosts=10000,max_records=0,history=False,compression='none',queries=None,wait=True):
        warmup=min(warmup,duration//2)
        output=self.root/(label+'.json')
        command=[str(BIN),'--mode',mode,'--host',self.a.host+':8000','--database',database,'--protocol',protocol,'--workers',str(workers),'--duration',str(duration),'--warmup',str(warmup),'--rate',str(rate),'--hosts',str(hosts),'--max-records',str(max_records),'--anchor',str(self.anchor),'--compress',compression,'--output',str(output)]
        if history: command+=['--history']
        if queries: command+=['--queries',str(queries)]
        before=metrics(self.a.host)
        self.log('stage_start',label=label,mode=mode,protocol=protocol,workers=workers,rate=rate,duration=duration,hosts=hosts)
        stream=(self.root/(label+'.log')).open('w')
        proc=subprocess.Popen(command,env=ENV,stdout=stream,stderr=subprocess.STDOUT)
        task=(proc,stream,output,before)
        return self.finish(task,label,mode,database) if wait else task

    def finish(self,task,label,mode,database):
        proc,stream,output,before=task
        code=proc.wait();stream.close()
        if code!=0: raise RuntimeError(f'{label} failed with exit {code}: '+(self.root/(label+'.log')).read_text()[-2000:])
        result=json.loads(output.read_text())
        after=metrics(self.a.host)
        result['server_metrics_before']=before;result['server_metrics_after']=after
        if mode=='query':
            result['query_definitions']=json.loads(pathlib.Path(result['config']['QueryFile']).read_text())
        if mode=='ingest':
            expected=result['acknowledged_records_including_warmup']
            if database=='sizing' and result['config']['MaxRecords']==0:
                expected+=self.a.seed_records
            deadline=time.monotonic()+180;drain_start=time.monotonic()
            persisted=-1
            while time.monotonic()<deadline:
                current=metrics(self.a.host)
                if current.get('arc_buffer_records_written_total',0)-before.get('arc_buffer_records_written_total',0)>=result['acknowledged_records_including_warmup']:
                    persisted=count(self.a.host,database)
                    if persisted==expected: break
                time.sleep(2)
            result['drain_seconds']=time.monotonic()-drain_start
            result['server_metrics_drained']=metrics(self.a.host)
            result['persisted_rows']=persisted if persisted>=0 else count(self.a.host,database)
            result['persistence_validated']=result['persisted_rows']==expected
            result['data_bytes']=int(remote(self.a.host,'sudo','du','-sb','/srv/arc/working/data').split()[0])
            result['wal_bytes']=int(remote(self.a.host,'sudo','du','-sb','/srv/arc/working/wal').split()[0]) if self.wal else 0
        save(output,result)
        self.log('stage_end',label=label,records_per_second=result['records_per_second'],queries_per_second=result['requests_per_second'] if mode=='query' else None,errors=result['errors'],validated=result.get('persistence_validated'),drain_seconds=result.get('drain_seconds'),latencies=result['latencies'])
        return result

    def ingestion_valid(self,result,target,max_drain):
        before=result['server_metrics_before'];after=result['server_metrics_drained']
        counters=['arc_buffer_flush_failures_total','arc_wal_dropped_entries_total','arc_wal_failed_writes_total','arc_ingest_flush_deferred_total','arc_storage_errors_total']
        clean=all(after.get(key,0)==before.get(key,0) for key in counters)
        return clean and result['errors']==0 and result['persistence_validated'] and result['drain_seconds']<max_drain and result['records_per_second']>=target*.95 and result['seconds']>=result['config']['Duration']*.95

    def query_file(self,hosts):
        anchor=datetime.datetime.fromtimestamp(self.anchor/1e6,datetime.timezone.utc)
        lower=(anchor-datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S.%f')
        last_hour=(anchor-datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S.%f')
        defs=[
          {'name':'minute_aggregate_1h','sql':f"SELECT date_trunc('minute', time) AS minute, AVG(value), AVG(cpu_idle), MAX(cpu_user) FROM cpu WHERE time >= TIMESTAMP '{last_hour}' GROUP BY minute ORDER BY minute"},
          {'name':'fleet_aggregate_24h','sql':f"SELECT host, AVG(value) AS mean_value, MAX(cpu_user), COUNT(*) FROM cpu WHERE time >= TIMESTAMP '{lower}' GROUP BY host ORDER BY mean_value DESC LIMIT 100"},
          {'name':'host_aggregate_24h','sql':f"SELECT COUNT(*), AVG(value), MIN(cpu_idle), MAX(cpu_user) FROM cpu WHERE time >= TIMESTAMP '{lower}' AND host = 'server000042'"},
        ]
        path=self.root/f'queries-{hosts}.json';save(path,defs);return path

    def main(self):
        self.log('campaign_start',config=vars(self.a),anchor=self.anchor)
        (self.root/'server-info.txt').write_text(remote(self.a.host,'sudo','python3','/srv/arc/server.py','info'))
        (self.root/'package.sha256').write_text(remote(self.a.host,'sudo','cat','/srv/arc/results/package.sha256'))
        for wal in [True,False]:
            self.wal=wal;tag='wal-on' if wal else 'wal-off'
            self.control('wal',str(wal).lower());self.control('reset')
            capacities={}
            for protocol in ['msgpack','lineprotocol']:
                checkpoint=self.root/f'{tag}-{protocol}-validated-capacity.json'
                if checkpoint.exists():
                    capacities[protocol]=json.loads(checkpoint.read_text())
                    self.log('resume_protocol',wal=wal,protocol=protocol)
                    continue
                self.anchor=int(time.time()*1e6)
                steps=[250000,500000,1000000,2000000,4000000,6000000,8000000] if protocol=='msgpack' else [100000,250000,500000,1000000,2000000,4000000]
                best_rate=0
                for rate in steps:
                    self.control('reset')
                    label=f'{tag}-{protocol}-paced-{rate}'
                    try:
                        r=self.stage(label,database='ingest',protocol=protocol,workers=8,duration=self.a.sweep_seconds,rate=rate)
                        valid=self.ingestion_valid(r,rate,15)
                    except Exception as exc:
                        self.log('ramp_failed',label=label,error=str(exc));valid=False
                    if not valid:
                        self.log('capacity_boundary',wal=wal,protocol=protocol,offered_rate=rate)
                        break
                    best_rate=rate
                if not best_rate:best_rate=steps[0]//2
                # Confirm the highest acceptable offered rate, resetting the
                # disposable database between repetitions to bound memory.
                for repeat in range(self.a.repeats):
                    self.control('reset')
                    label=f'{tag}-{protocol}-confirm-r{repeat}'
                    try:
                        r=self.stage(label,database='ingest',protocol=protocol,workers=8,duration=self.a.sweep_seconds,rate=best_rate)
                        valid=self.ingestion_valid(r,best_rate,15)
                    except Exception as exc:
                        self.log('confirmation_failed',label=label,error=str(exc));valid=False
                    if not valid:best_rate//=2
                target=int(best_rate*.70)
                self.control('reset')
                label=f'{tag}-{protocol}-ingest-soak'
                for attempt in range(6):
                    soak_label=label if attempt==0 else label+f'-retry{attempt}'
                    try:
                        r=self.stage(soak_label,database='ingest',protocol=protocol,workers=8,duration=self.a.soak_seconds,warmup=15,rate=target)
                        valid=self.ingestion_valid(r,target,15)
                    except Exception as exc:
                        self.log('soak_failed',label=soak_label,error=str(exc));valid=False;r={'error':str(exc)}
                    if valid:break
                    self.log('reducing_ingestion_rate',protocol=protocol,wal=wal,old_target=target,new_target=target//2)
                    target//=2;self.control('reset')
                else:raise RuntimeError('no valid sustained ingestion rate for '+tag+' '+protocol)
                capacities[protocol]={'workers':8,'confirmed_rate':best_rate,'target_rps':target,'soak':r}
                save(checkpoint,capacities[protocol])
                self.control('reset')
            save(self.root/(tag+'-capacities.json'),capacities)
            for hosts in [10000,100000]:
                checkpoint=self.root/f'{tag}-hosts{hosts}-COMPLETE.json'
                if checkpoint.exists():
                    self.log('resume_cardinality',wal=wal,hosts=hosts)
                    continue
                self.anchor=int(time.time()*1e6)
                self.control('reset')
                label=f'{tag}-hosts{hosts}-seed'
                seed=self.stage(label,database='sizing',hosts=hosts,duration=900,warmup=0,max_records=self.a.seed_records,history=True,workers=8)
                # Seed validation has no previous rows, unlike a mixed stage.
                seed['persistence_validated']=seed['persisted_rows']==self.a.seed_records
                save(self.root/(label+'-sizing.json'),seed)
                if not seed['persistence_validated']:raise RuntimeError('sizing seed row count mismatch')
                cardinality=api(self.a.host,'/api/v1/query','sizing','SELECT COUNT(DISTINCT host), MIN(time), MAX(time) FROM cpu')
                save(self.root/f'{tag}-hosts{hosts}-seed-shape.json',cardinality)
                # Keep compaction defaults. Capture pre/post trigger metrics;
                # if the released endpoint cannot complete, fail the case.
                try:
                    compaction=api(self.a.host,'/api/v1/compaction/trigger?database=sizing',method='POST')
                    save(self.root/f'{tag}-hosts{hosts}-compaction.json',compaction)
                except urllib.error.HTTPError as exc:
                    self.log('compaction_trigger_error',status=exc.code,body=exc.read().decode()[:500])
                time.sleep(10)
                self.control('snapshot')
                queries=self.query_file(hosts)
                for q in json.loads(queries.read_text()):
                    sanity=api(self.a.host,'/api/v1/query','sizing',q['sql'])
                    if not sanity.get('data'):raise RuntimeError('empty query: '+q['name'])
                q_candidates={}
                for workers in [1,2,4]:
                    rates=[]
                    for repeat in range(self.a.repeats):
                        label=f'{tag}-hosts{hosts}-query-w{workers}-r{repeat}'
                        r=self.stage(label,mode='query',workers=workers,duration=self.a.query_seconds,warmup=5,queries=queries)
                        if r['errors']==0 and max(v['p95_ms'] for v in r['latencies'].values())<=2000:rates.append(r['requests_per_second'])
                    if rates:q_candidates[workers]=statistics.median(rates)
                if not q_candidates:
                    self.log('no_query_candidate_below_2s',hosts=hosts,wal=wal)
                    q_candidates={1:.5}
                q_workers=max(q_candidates,key=q_candidates.get);q_target=q_candidates[q_workers]*.25
                save(self.root/f'{tag}-hosts{hosts}-query-capacity.json',{'qps_by_workers':q_candidates,'mixed_target_qps':q_target,'workers':q_workers})
                protocols=['msgpack','lineprotocol'] if hosts==10000 else ['msgpack']
                # Equal offered writes keep protocol comparisons at the same
                # growing data volume. The cap defines an evaluation scenario,
                # rather than claiming the maximum mixed ingestion rate.
                mixed_rate=min(self.a.mixed_rate,int(min(c['target_rps'] for c in capacities.values())*.5))
                for protocol in protocols:
                    self.control('restore')
                    label=f'{tag}-hosts{hosts}-{protocol}-mixed'
                    duration=self.a.soak_seconds if hosts==10000 else self.a.sensitivity_seconds
                    qtask=self.stage(label+'-queries',mode='query',workers=max(4,q_workers),duration=duration,warmup=15,rate=q_target,queries=queries,wait=False)
                    itask=self.stage(label+'-ingest',workers=8,duration=duration,warmup=15,rate=mixed_rate,protocol=protocol,hosts=hosts,wait=False)
                    qr=self.finish(qtask,label+'-queries','query','sizing')
                    ir=self.finish(itask,label+'-ingest','ingest','sizing')
                    save(self.root/(label+'-summary.json'),{'queries':qr,'ingestion':ir,'target_qps':q_target,'target_rps':mixed_rate})
                save(checkpoint,{'wal':wal,'hosts':hosts,'completed_utc':time.time()})
            self.control('reset')
        self.log('campaign_complete')
        (self.root/'COMPLETE').write_text(time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())+'\n')

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--host',required=True);parser.add_argument('--name',required=True)
    parser.add_argument('--anchor',type=int,default=0)
    parser.add_argument('--seed-records',type=int,default=20000000)
    parser.add_argument('--soak-seconds',type=int,default=600)
    parser.add_argument('--sensitivity-seconds',type=int,default=180)
    parser.add_argument('--sweep-seconds',type=int,default=30)
    parser.add_argument('--query-seconds',type=int,default=30)
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--mixed-rate',type=int,default=100000)
    args=parser.parse_args()
    campaign=Campaign(args)
    try:campaign.main()
    except Exception as exc:
        campaign.log('campaign_failed',error=str(exc));raise
