#!/usr/bin/env python3
"""Summarize validated runs and match native host samples to measured windows."""
import argparse, datetime, gzip, json, math, pathlib, re, statistics

ROOT=pathlib.Path(__file__).parent
RESULTS=ROOT/'results'

def percentile(values,q):
    values=sorted(values)
    return values[min(len(values)-1,int(q*len(values)))] if values else None

def host_samples(server):
    files=list((RESULTS/server/'native-host').glob('monitor*.jsonl*'))
    if not files:
        file=RESULTS/server/'monitor.jsonl'
        if file.exists():files=[file]
    if not files:return []
    samples=[]
    previous=None
    records=[]
    for file in files:
        stream=gzip.open(file,'rt') if file.suffix=='.gz' else file.open()
        for line in stream:
            try:records.append(json.loads(line))
            except Exception:continue
        stream.close()
    for sample in sorted(records,key=lambda s:s['time']):
        pids=[]
        for row in sample.get('process','').splitlines()[1:]:
            try:fields=row.split();pids.append({'rss_mib':int(fields[2])/1024,'cpu':float(fields[1])})
            except (ValueError,IndexError):pass
        sample['rss_mib']=sum(p['rss_mib'] for p in pids)
        # iostat prints a boot average followed by the one-second interval.
        # Select the final report and sum only the server's EBS NVMe devices.
        io_lines=sample.get('iostat','').splitlines()
        headers=[i for i,line in enumerate(io_lines) if line.startswith('Device')]
        if headers:
            header=io_lines[headers[-1]].split()
            devices=[]
            for line in io_lines[headers[-1]+1:]:
                fields=line.split()
                if fields and fields[0].startswith('nvme'):
                    try:devices.append(dict(zip(header[1:],map(float,fields[1:]))))
                    except ValueError:pass
            if devices:
                sample['disk_write_mibps']=sum(d.get('wkB/s',0) for d in devices)/1024
                sample['disk_read_mibps']=sum(d.get('rkB/s',0) for d in devices)/1024
                sample['disk_write_await_ms']=max(d.get('w_await',0) for d in devices)
        vm={}
        for row in sample.get('meminfo','').splitlines():
            key,value,*_=row.split();vm[key.rstrip(':')]=int(value)*1024
        sample['mem_available_gib']=vm.get('MemAvailable',0)/2**30
        stat=sample.get('cpustat','').splitlines()
        if stat and stat[0].startswith('cpu '):
            values=list(map(int,stat[0].split()[1:9]))
            if previous:
                deltas=[a-b for a,b in zip(values,previous)]
                total=sum(deltas)
                if total>0:
                    sample['host_busy_pct']=100*(total-deltas[3]-deltas[4])/total
                    sample['host_iowait_pct']=100*deltas[4]/total
            previous=values
        prom={}
        for row in sample.get('metrics','').splitlines():
            fields=row.split()
            if len(fields)==2 and not row.startswith('#') and '{' not in fields[0]:
                try:prom[fields[0]]=float(fields[1])
                except ValueError:pass
        sample['arc']=prom
        samples.append(sample)
    return samples

def resource_summary(run,samples):
    start=datetime.datetime.fromisoformat(run['started_utc'].replace('Z','+00:00')).timestamp()
    selected=[s for s in samples if start<=s['time']<=start+run['seconds']]
    summary={'samples':len(selected)}
    if selected:
        summary['coverage_seconds']=selected[-1]['time']-selected[0]['time']
        summary['complete_window']=selected[0]['time']<=start+5 and selected[-1]['time']>=start+run['seconds']-5
    for key in ['rss_mib','host_busy_pct','host_iowait_pct','mem_available_gib','disk_write_mibps','disk_read_mibps','disk_write_await_ms']:
        values=[s[key] for s in selected if key in s]
        if values:summary[key]={'min':min(values),'median':statistics.median(values),'p95':percentile(values,.95),'max':max(values)}
    for key in ['arc_buffer_queue_depth','arc_buffer_records_buffered','arc_buffer_deferred_buffers']:
        values=[s['arc'].get(key,0) for s in selected]
        if values:summary[key]={'first':values[0],'last':values[-1],'max':max(values)}
    return summary

def network_samples(server):
    samples=[]
    for path in (RESULTS/server/'native-host').glob('network*.jsonl*'):
        with (gzip.open(path,'rt') if path.suffix=='.gz' else path.open()) as stream:
            for line in stream:
                try:samples.append(json.loads(line))
                except ValueError:pass
    return sorted(samples,key=lambda s:s['time'])

def network_summary(run,samples):
    start=datetime.datetime.fromisoformat(run['started_utc'].replace('Z','+00:00')).timestamp()
    end=start+run['seconds']
    selected=[s for s in samples if start-5<=s['time']<=end+5]
    if len(selected)<2:return {'samples':len(selected)}
    first=selected[0];last=selected[-1]
    deltas={}
    for interface,counters in first['interfaces'].items():
        after=last['interfaces'].get(interface,{})
        deltas[interface]={key:after[key]-value for key,value in counters.items() if key.endswith('_exceeded') and key in after}
    return dict(samples=len(selected),coverage_seconds=last['time']-first['time'],
        complete_window=first['time']<=start+5 and last['time']>=end-5,allowance_counter_deltas=deltas)

def usable(run):
    reasons=[]
    if run.get('errors',0):reasons.append('HTTP errors')
    if not run.get('persistence_validated',False):reasons.append('persisted row mismatch')
    # Fixed-count historical seeding finishes as soon as all rows are sent.
    if not run['config'].get('MaxRecords') and run.get('seconds',0)<run['config']['Duration']*.95:reasons.append('ended early')
    target=run['config'].get('Rate',0)
    if target and run['records_per_second']<target*.95:reasons.append('offered rate not sustained')
    if not run['config'].get('MaxRecords') and run.get('drain_seconds',999)>15:reasons.append('slow row reconciliation')
    before=run.get('server_metrics_before',{});after=run.get('server_metrics_drained',run.get('server_metrics_after',{}))
    for key in ['arc_buffer_flush_failures_total','arc_wal_dropped_entries_total','arc_wal_failed_writes_total','arc_ingest_flush_deferred_total','arc_storage_errors_total']:
        if after.get(key,0)>before.get(key,0):reasons.append(key+' increased')
    return reasons

def main(quiet=False):
    config=json.loads((ROOT/'instances.json').read_text())
    rows=[]
    for server in config['servers']:
        samples=host_samples(server['name']);network=network_samples(server['name']);directory=RESULTS/server['name']
        for path in sorted(directory.glob('*.json')):
            try:r=json.loads(path.read_text())
            except Exception:continue
            if 'config' not in r or 'seconds' not in r:continue
            host_label=re.search(r'hosts(\d+)',path.stem)
            dataset_hosts=int(host_label.group(1)) if host_label else r['config']['Hosts']
            row={'server':server['name'],'instance_type':server['type'],'vcpus':server['vcpus'],'ram_gib':server['ram_gib'],'case':path.stem,'mode':r['config']['Mode'],'protocol':r['config']['Protocol'] if r['config']['Mode']=='ingest' else 'http-json','wal':'on' if path.name.startswith('wal-on') else 'off','hosts':dataset_hosts,'duration_seconds':r['seconds'],'rps':r['records_per_second'],'qps':r['requests_per_second'] if r['config']['Mode']=='query' else None,'target':r['config']['Rate'],'errors':r['errors'],'latencies':r['latencies'],'resources':resource_summary(r,samples),'minute_windows':r.get('minute_windows',[])}
            row['network']=network_summary(r,network)
            row['payload_mibps']=r.get('bytes',0)/r['seconds']/2**20
            if row['mode']=='ingest':
                row['invalid_reasons']=usable(r)
                row['persisted_rows']=r.get('persisted_rows')
                row['drain_seconds']=r.get('drain_seconds')
                row['data_bytes']=r.get('data_bytes')
                row['wal_bytes']=r.get('wal_bytes')
                row['acknowledged_rows']=r.get('acknowledged_records_including_warmup')
                row['counter_deltas']={key:r.get('server_metrics_drained',{}).get(key,0)-r.get('server_metrics_before',{}).get(key,0) for key in ['arc_buffer_flush_failures_total','arc_wal_dropped_entries_total','arc_wal_failed_writes_total','arc_ingest_flush_deferred_total','arc_storage_errors_total']}
                full_windows=[w for w in r.get('minute_windows',[]) if w['end_seconds']-w['start_seconds']>=60]
                row['minimum_full_minute_rps']=min((w['records_per_second'] for w in full_windows),default=None)
                # Only unique historical data can estimate retention storage.
                if r['config'].get('History') and r['config'].get('MaxRecords') and r.get('persisted_rows'):
                    row['unique_seed_bytes_per_row']=r.get('data_bytes',0)/r['persisted_rows']
            else:
                row['invalid_reasons']=[]
                if r['errors']:row['invalid_reasons'].append('HTTP errors')
                if r['seconds']<r['config']['Duration']*.95:row['invalid_reasons'].append('ended early')
                if row['target'] and row['qps']<row['target']*.95:row['invalid_reasons'].append('offered QPS not sustained')
                row['worst_type_p95_ms']=max((v['p95_ms'] for v in r['latencies'].values()),default=0)
                windows=[w for w in r.get('minute_windows',[]) if w['end_seconds']-w['start_seconds']>=30]
                row['worst_minute_type_p95_ms']=max((v['p95_ms'] for w in windows for v in w['latencies'].values()),default=0)
                row['last_minute_qps']=windows[-1]['requests_per_second'] if windows else None
            rows.append(row)
    summary={'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'complete':(ROOT/'COMPLETE').exists(),'cases':rows}
    (ROOT/'analysis.json').write_text(json.dumps(summary,indent=2)+'\n')
    for row in ([] if quiet else rows):
        if 'ingest-soak' in row['case'] or row['case'].endswith('mixed-ingest'):
            print(json.dumps({k:row.get(k) for k in ['server','case','rps','duration_seconds','invalid_reasons','drain_seconds','data_bytes_per_row']}))

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',type=pathlib.Path,default=ROOT,help='directory containing instances.json and results/')
    parser.add_argument('--quiet',action='store_true')
    args=parser.parse_args();ROOT=args.root;RESULTS=ROOT/'results'
    main(quiet=args.quiet)
