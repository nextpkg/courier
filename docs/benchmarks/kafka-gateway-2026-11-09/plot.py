import json,pathlib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
p=pathlib.Path(__file__).resolve().parent
rs=json.loads((p/'concurrency.json').read_text());xs=[r['concurrency'] for r in rs];ys=[r['rpc_s']/1000 for r in rs]
knee=max(rs,key=lambda r:r['knee_score'])['concurrency'];pick=next(r['concurrency'] for r in rs if r['peak_fraction']>=.9)
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
fig,axs=plt.subplots(2,2,figsize=(12,8),layout='constrained')
a=axs[0,0];a.plot(xs,ys,'o-',color='#2067b2',label='Gateway throughput');a.fill_between(xs,[r['rpc_min']/1000 for r in rs],[r['rpc_max']/1000 for r in rs],alpha=.16,color='#2067b2',label='Two-round range');a.axhline(max(ys)*.9,ls=':',color='gray',label='90% of observed peak');a.set_ylabel('Thousand successful RPC/s');a.legend(fontsize=8)
a=axs[0,1];a.plot(xs,[r['p99_ms'] for r in rs],'o-',label='p99',color='#c44234');a.plot(xs,[r['p50_ms'] for r in rs],'o-',label='p50',color='#da9d22');a.set_ylabel('Latency (ms)');a.legend()
a=axs[1,0];a.plot(xs,[r['rpc_per_cpu']/1000 for r in rs],'o-',color='#227a58');a.set_ylabel('Thousand RPC/s per application CPU core')
a=axs[1,1];a.plot(xs[1:],[r['marginal_rpc_per_added_concurrent_call'] for r in rs[1:]],'o-',color='#7b4bac');a.set_ylabel('Added RPC/s per added concurrent call');a.set_title('Marginal return between adjacent tested levels',fontsize=10);a.axhline(0,color='gray',lw=.5)
for a in axs.flat:
 a.axvline(pick,color='#227a58',ls='--',alpha=.6)
 if knee!=pick:a.axvline(knee,color='#da9d22',ls=':',alpha=.7)
 a.set_xlabel('Total outstanding calls (3 clients)');a.grid(alpha=.2);a.set_xticks(xs);a.tick_params(axis='x',labelrotation=45)
fig.suptitle(f'Courier gateway: concurrency ROI | 90%-peak point = {pick}, normalized knee = {knee}\nDefault GC; app 4 CPU / 1 GiB; Kafka 2 CPU / 3 GiB; 2 x 30s per level',fontsize=13)
for ext in ['png','svg','pdf']:fig.savefig(p/f'concurrency.{ext}',dpi=160)

svg=p/"concurrency.svg"
svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines())+"\n")
