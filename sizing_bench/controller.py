#!/usr/bin/env python3
"""Run reproducible native OSS sizing cases from the dedicated load generator."""
import argparse, concurrent.futures, datetime, fcntl, json, os, pathlib, statistics, subprocess, time, urllib.error, urllib.request

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

def generator_network():
    counters={}
    for path in pathlib.Path('/sys/class/net').iterdir():
        if path.name=='lo' or not (path/'device').exists():continue
        result=subprocess.run(['ethtool','-S',path.name],capture_output=True,text=True,timeout=5)
        if result.returncode:raise RuntimeError('cannot read generator ENA counters')
        for line in result.stdout.splitlines():
            if '_allowance_exceeded:' in line:
                key,value=line.strip().split(':',1)
                counters[path.name+'.'+key]=int(value.strip())
    if not counters:raise RuntimeError('generator ENA counters missing')
    return counters

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
        network_before=generator_network()
        self.log('stage_start',label=label,mode=mode,protocol=protocol,workers=workers,rate=rate,duration=duration,hosts=hosts)
        stream=(self.root/(label+'.log')).open('w')
        proc=subprocess.Popen(command,env=ENV,stdout=stream,stderr=subprocess.STDOUT)
        task=(proc,stream,output,before,network_before)
        return self.finish(task,label,mode,database) if wait else task

    def finish(self,task,label,mode,database):
        proc,stream,output,before,network_before=task
        code=proc.wait();stream.close()
        if code!=0: raise RuntimeError(f'{label} failed with exit {code}: '+(self.root/(label+'.log')).read_text()[-2000:])
        result=json.loads(output.read_text())
        network_after=generator_network()
        result['generator_network_allowance_deltas']={k:network_after[k]-v for k,v in network_before.items()}
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
            result['wal_directory']=getattr(self,'wal_path','/srv/arc/working/wal') if self.wal else None
            result['wal_bytes']=int(remote(self.a.host,'sudo','du','-sb',result['wal_directory']).split()[0]) if self.wal else 0
        save(output,result)
        self.log('stage_end',label=label,records_per_second=result['records_per_second'],queries_per_second=result['requests_per_second'] if mode=='query' else None,errors=result['errors'],validated=result.get('persistence_validated'),drain_seconds=result.get('drain_seconds'),latencies=result['latencies'])
        return result

    def ingestion_valid(self,result,target,max_drain):
        before=result['server_metrics_before'];after=result['server_metrics_drained']
        counters=['arc_buffer_flush_failures_total','arc_wal_dropped_entries_total','arc_wal_failed_writes_total','arc_wal_failed_syncs_total','arc_ingest_flush_deferred_total','arc_storage_errors_total']
        clean=all(after.get(key,0)==before.get(key,0) for key in counters)
        network_clean=not any(result.get('generator_network_allowance_deltas',{}).values())
        return clean and network_clean and result.get('errors_including_warmup',result['errors'])==0 and result['persistence_validated'] and result['drain_seconds']<max_drain and result['records_per_second']>=target*.95 and result['seconds']>=result['config']['Duration']*.95

    def qualify_capacity(self,tag,protocol):
        steps=([1000000,2000000,3000000,4000000,4500000,5000000,5500000,6000000,7000000,8000000,9000000,10000000,12000000,14000000,16000000,20000000,24000000]
            if protocol=='msgpack' else [100000,250000,500000,750000,1000000,1250000,1500000,1750000,2000000,2500000,3000000,4000000,5000000,6000000,8000000])
        workers=self.a.ingest_workers
        best=0;failed=None;trials=[]
        def trial(rate,label):
            self.control('reset')
            try:
                result=self.stage(label,database='ingest',protocol=protocol,workers=workers,duration=self.a.sweep_seconds,rate=rate)
            except Exception as exc:
                self.log('trial_failed',label=label,error=str(exc))
                trials.append({'rate':rate,'case':label,'valid':False,'error':str(exc)})
                return False
            valid=self.ingestion_valid(result,rate,15)
            trials.append({'rate':rate,'case':label,'valid':valid})
            if any(result['generator_network_allowance_deltas'].values()):
                raise RuntimeError('generator network throttled; capacity selection is invalid')
            return valid
        for rate in steps:
            if not trial(rate,f'{tag}-{protocol}-paced-{rate}'):
                failed=rate;self.log('capacity_boundary',wal=self.wal,protocol=protocol,offered_rate=rate);break
            best=rate
        increment=500000 if protocol=='msgpack' else 125000
        if failed and best:
            for rate in range(best+increment,failed,increment):
                if not trial(rate,f'{tag}-{protocol}-refine-{rate}'):break
                best=rate
        if not best:raise RuntimeError('lowest offered rate failed qualification')
        # Repeat the SAME rate three times. If it fails, lower by one fine step
        # and restart all confirmations rather than mixing different candidates.
        while best>0:
            outcomes=[trial(best,f'{tag}-{protocol}-confirm-{best}-r{i}') for i in range(self.a.repeats)]
            if all(outcomes):break
            best-=increment
        if best<=0:raise RuntimeError('no confirmed capacity')
        target=int(best*.90)
        for attempt in range(10):
            self.control('reset')
            label=f'{tag}-{protocol}-ingest-soak'+(f'-retry{attempt}' if attempt else '')
            try:
                result=self.stage(label,database='ingest',protocol=protocol,workers=workers,duration=self.a.soak_seconds,warmup=15,rate=target)
            except Exception as exc:
                self.log('soak_failed',label=label,error=str(exc))
                target=int(target*.90)
                continue
            if any(result['generator_network_allowance_deltas'].values()):
                raise RuntimeError('generator network throttled during sustained qualification')
            if self.ingestion_valid(result,target,15):
                return {'workers':workers,'confirmed_rate':best,'target_rps':target,'soak':result,'trials':trials,'selection':'fine-step selection, three identical confirmations, 90% offered soak'}
            new_target=int(target*.90)
            self.log('reducing_ingestion_rate',wal=self.wal,protocol=protocol,old_target=target,new_target=new_target)
            target=new_target
        raise RuntimeError('no valid ten-minute operating point')

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
        (self.root/'binary.sha256').write_text(remote(self.a.host,'sudo','cat','/srv/arc/results/binary.sha256'))
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
                capacities[protocol]=self.qualify_capacity(tag,protocol)
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
                seed=self.stage(label,database='sizing',hosts=hosts,duration=900,warmup=0,max_records=self.a.seed_records,history=True,workers=self.a.ingest_workers)
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
                        if r.get('errors_including_warmup',r['errors'])==0 and not any(r.get('generator_network_allowance_deltas',{}).values()) and r['seconds']>=self.a.query_seconds*.95 and max(v['p95_ms'] for v in r['latencies'].values())<=2000:rates.append(r['requests_per_second'])
                    if len(rates)==self.a.repeats:q_candidates[workers]=statistics.median(rates)
                if not q_candidates:
                    self.log('no_query_candidate_below_2s',hosts=hosts,wal=wal)
                    raise RuntimeError(f'{tag} {hosts}-host query profile did not qualify')
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
                    itask=self.stage(label+'-ingest',workers=self.a.ingest_workers,duration=duration,warmup=15,rate=mixed_rate,protocol=protocol,hosts=hosts,wait=False)
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
    parser.add_argument('--ingest-workers',type=int,default=32)
    args=parser.parse_args()
    lockdir=BASE/'results'/args.name;lockdir.mkdir(parents=True,exist_ok=True)
    lock=(lockdir/'controller.lock').open('w')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    campaign=Campaign(args)
    try:campaign.main()
    except Exception as exc:
        campaign.log('campaign_failed',error=str(exc));raise
