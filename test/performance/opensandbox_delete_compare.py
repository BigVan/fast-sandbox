#!/usr/bin/env python3
# Copyright 2026 The Fast Sandbox Authors
# SPDX-License-Identifier: Apache-2.0
"""Immediately delete ready Alpine sandboxes and observe batch slot restoration.

Runtime cleanup is paired with Controller/Fastlet logs separately. The metric
observer uses direct Fastlet HTTP metrics, so it does not poll the API server.
Set DOMAIN, API_KEY, TEMPLATE_ID, TARGETS_FILE, REPORT_PATH, RUN_NAME.
"""
import asyncio,json,math,os,statistics,time
from datetime import datetime,timedelta,timezone
from pathlib import Path
import httpx
from opensandbox.config import ConnectionConfig
from opensandbox.manager import SandboxManager
from opensandbox.sandbox import Sandbox


def distribution(values):
    ordered=sorted(values)
    if not ordered:return {}
    return {'count':len(ordered),'mean':statistics.mean(ordered),'min':ordered[0],'max':ordered[-1],**{f'p{p}':ordered[math.ceil(p/100*len(ordered))-1] for p in (50,90,99)}}


async def main():
    n=int(os.environ.get('N','300'));c=int(os.environ['CONCURRENCY']);pause=float(os.environ.get('BATCH_PAUSE','5'))
    poll=float(os.environ.get('SLOT_POLL_INTERVAL','0.1'));budget=float(os.environ.get('SLOT_TIMEOUT','15'))
    assert n>0 and c>0 and pause>=0 and poll>0
    targets=json.loads(Path(os.environ['TARGETS_FILE']).read_text());assert len(targets)==32
    config=ConnectionConfig(domain=os.environ['DOMAIN'],protocol=os.environ.get('PROTOCOL','http'),api_key=os.environ['API_KEY'],request_timeout=timedelta(seconds=10),disable_metrics=True)
    manager=await SandboxManager.create(config);attempts=[];batches=[];origin=os.environ.get('BENCH_ORIGIN','delete-compare-20261011');name=os.environ['RUN_NAME'];template=os.environ['TEMPLATE_ID']
    start=time.monotonic();started=datetime.now(timezone.utc).isoformat()
    async with httpx.AsyncClient(timeout=2,trust_env=False,limits=httpx.Limits(max_connections=32,max_keepalive_connections=32)) as observer:
        async def scrape(target):
            begin=time.monotonic()
            try:
                response=await observer.get('http://'+target['ip']+':5758/metrics');response.raise_for_status()
                gauges={}
                for line in response.text.splitlines():
                    if line.startswith(('fast_sandbox_network_slot_available ','fast_sandbox_network_slot_inuse ')):
                        key,val=line.split();gauges[key]=float(val)
                assert len(gauges)==2
                return {'pod':target['pod'],'clean':gauges['fast_sandbox_network_slot_available'],'used':gauges['fast_sandbox_network_slot_inuse'],'start_seconds':begin-start,'end_seconds':time.monotonic()-start,'bytes':len(response.content)}
            except Exception as error:
                return {'pod':target['pod'],'error_type':type(error).__name__,'start_seconds':begin-start,'end_seconds':time.monotonic()-start}
        async def snapshot():
            begin=time.monotonic();parts=await asyncio.gather(*(scrape(t) for t in targets));end=time.monotonic()
            full=all('error_type' not in p and p['clean']==4 and p['used']==0 for p in parts)
            return {'start_seconds':begin-start,'end_seconds':end-start,'all_slots_restored':full,'clean':sum(p.get('clean',0) for p in parts),'used':sum(p.get('used',0) for p in parts),'errors':[p for p in parts if 'error_type' in p],'scrape_duration_seconds':end-begin,'max_target_scrape_seconds':max(p['end_seconds']-p['start_seconds'] for p in parts),'bytes':sum(p.get('bytes',0) for p in parts)}
        async def run(index,batch):
            a={'index':index,'batch':batch,'create_success':False,'delete_accepted':False};sandbox=None;before=time.monotonic()
            try:
                sandbox=await Sandbox.create_from_template(template,timeout=timedelta(hours=1),ready_timeout=timedelta(seconds=180),health_check_polling_interval=timedelta(milliseconds=10),connection_config=config,metadata={'origin':origin,'run':name})
                ready=time.monotonic();a.update(create_success=True,sandbox_id=sandbox.id,ready_seconds=ready-before)
                delete_start=time.monotonic();a['ready_to_delete_call_seconds']=delete_start-ready
                await manager.kill_sandbox(sandbox.id)
                accepted=time.monotonic();a.update(delete_accepted=True,delete_accept_seconds=accepted-delete_start,delete_accepted_seconds=accepted-start)
            except Exception as error:
                a['error_type']=type(error).__name__;a['error_stage']='delete' if sandbox is not None else 'create'
            finally:
                if sandbox is not None:
                    try:await sandbox.close()
                    except Exception as error:a['close_error_type']=type(error).__name__
                a['completed_seconds']=time.monotonic()-start;attempts.append(a);print(json.dumps(a),flush=True)
            return a
        try:
            for batch,first in enumerate(range(1,n+1,c),1):
                before=await snapshot();assert before['all_slots_restored'],'pool not initially clean'
                a=await asyncio.gather(*(run(i,batch) for i in range(first,min(first+c,n+1))))
                tasks_finished=time.monotonic();acks=[x['delete_accepted_seconds'] for x in a if x['delete_accepted']]
                last=max(acks) if acks else tasks_finished-start
                record={'batch':batch,'attempts':len(a),'successful_creates':sum(x['create_success'] for x in a),'delete_acceptances':len(acks),'before':before,'last_delete_accept_seconds':last,'sdk_tasks_finished_seconds':tasks_finished-start,'slot_samples':[],'slot_restored':False}
                first_full=None;previous=None
                while time.monotonic()-start-last<budget:
                    sample=await snapshot();record['slot_samples'].append(sample)
                    if sample['all_slots_restored']:
                        if first_full is None:first_full=sample
                        else:
                            record.update(slot_restored=True,slot_restore_seconds=max(0,first_full['end_seconds']-last),slot_restore_confirmed_seconds=sample['end_seconds']-last,slot_restore_lower_bound_seconds=max(0,(previous['start_seconds'] if previous else last)-last),slot_poll_max_gap_seconds=max((y['end_seconds']-x['end_seconds'] for x,y in zip(record['slot_samples'],record['slot_samples'][1:])),default=0))
                            break
                    else:
                        first_full=None;previous=sample
                    await asyncio.sleep(poll)
                batches.append(record);print('BATCH '+json.dumps({k:v for k,v in record.items() if k not in ['slot_samples','before']}),flush=True)
                if not record['slot_restored']:
                    record['slot_restore_timeout_seconds']=budget
                    if os.environ.get('ALLOW_MANUAL_RECOVERY')!='1':break
                    checkpoint=Path(os.environ['REPORT_PATH']).with_name(name+'-recovery-request.json')
                    checkpoint.write_text(json.dumps({'run':name,'batch':batch,'attempts':attempts,'batches':batches},indent=2)+'\n')
                    resume=checkpoint.with_name(name+'-resume-'+str(batch)+'.json')
                    recovery_deadline=time.monotonic()+600
                    while not resume.exists():
                        if time.monotonic()>recovery_deadline:raise RuntimeError('manual recovery checkpoint was not acknowledged')
                        await asyncio.sleep(1)
                    record['recovery']=json.loads(resume.read_text())
                    assert record['recovery']['physical_reclamation_verified']
                    record['post_recovery_slot_sample']=await snapshot()
                    assert record['post_recovery_slot_sample']['all_slots_restored']
                    checkpoint.unlink()
                if first+c<=n:await asyncio.sleep(max(0,pause-(time.monotonic()-tasks_finished)))
        finally:await manager.close()
    report={'measurement':'SDK guest /ping readiness followed immediately by Delete; batch clean-slot recovery observed via direct Fastlet metrics','started_utc':started,'finished_utc':datetime.now(timezone.utc).isoformat(),'settings':{'n':n,'concurrency':c,'batch_pause_seconds':pause,'warmups':0,'template_id':template,'request_timeout_seconds':10,'ready_timeout_seconds':180,'ping_interval_ms':10,'slot_poll_interval_seconds':poll,'slot_timeout_seconds':budget,'slot_confirmation_samples':2,'total_slots':128,'client_observation_scope':'all 32 Fastlets, metrics only before/after each batch, no Kubernetes API polling on measured path'},'successes':sum(x['create_success'] for x in attempts),'failures':sum(not x['create_success'] for x in attempts),'delete_acceptances':sum(x['delete_accepted'] for x in attempts),'delete_failures':sum(x['create_success'] and not x['delete_accepted'] for x in attempts),'successful_latency_seconds':distribution([x['ready_seconds'] for x in attempts if x['create_success']]),'delete_accept_seconds':distribution([x['delete_accept_seconds'] for x in attempts if x['delete_accepted']]),'batch_slot_restore_seconds':distribution([x['slot_restore_seconds'] for x in batches if x['slot_restored']]),'manual_recovery_count':sum(x.get('recovery',{}).get('manual_recovery_count',0) for x in batches),'slot_restore_timeouts':sum(not x['slot_restored'] for x in batches),'wall_seconds_including_batch_pauses':time.monotonic()-start,'percentile_method':'nearest_rank','targets':targets,'attempts':sorted(attempts,key=lambda x:x['index']),'batches':batches}
    Path(os.environ['REPORT_PATH']).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ['attempts','batches','targets']}),flush=True)
    if len(attempts)!=n or report['failures'] or report['delete_failures'] or report['slot_restore_timeouts']:raise SystemExit(1)

if __name__=='__main__':asyncio.run(main())
