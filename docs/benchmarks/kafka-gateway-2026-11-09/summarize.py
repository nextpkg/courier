import json,pathlib,statistics,csv
p=pathlib.Path(__file__).resolve().parent
rows=[r for r in json.loads((p/'rounds.json').read_text()) if r['included']];out=[]
for n in sorted({r['concurrency'] for r in rows}):
 rs=[r for r in rows if r['concurrency']==n]
 out.append(dict(concurrency=n,rounds=len(rs),rpc_s=statistics.mean(r['successful_calls_per_second'] for r in rs),rpc_min=min(r['successful_calls_per_second'] for r in rs),rpc_max=max(r['successful_calls_per_second'] for r in rs),p50_ms=statistics.mean(r['p50_ms'] for r in rs),p99_ms=statistics.mean(r['p99_ms'] for r in rs),cpu_cores=statistics.mean(r['cpu_cores'] for r in rs),rpc_per_cpu=statistics.mean(r['successful_calls_per_second']/r['cpu_cores'] for r in rs),failures=sum(r['failures'] for r in rs),throttled_periods=sum(r['throttled_periods'] for r in rs)))
low,high=min(r['rpc_s'] for r in out),max(r['rpc_s'] for r in out)
for i,r in enumerate(out):
 r['peak_fraction']=r['rpc_s']/high
 r['knee_score']=(r['rpc_s']-low)/(high-low)-(r['concurrency']-out[0]['concurrency'])/(out[-1]['concurrency']-out[0]['concurrency']) if high>low else 0
 r['marginal_rpc_per_added_concurrent_call']=(r['rpc_s']-out[i-1]['rpc_s'])/(r['concurrency']-out[i-1]['concurrency']) if i else None
 r['marginal_percent_gain']=(r['rpc_s']/out[i-1]['rpc_s']-1)*100 if i else None
(p/'concurrency.json').write_text(json.dumps(out,indent=2))
with (p/'concurrency.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=out[0].keys(),lineterminator='\n');w.writeheader();w.writerows(out)
for r in out:print(r['concurrency'],r['rounds'],round(r['rpc_s']),r['p99_ms'],round(r['cpu_cores'],2),round(r['knee_score'],3))
