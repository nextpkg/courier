"""Render Kafka benchmark logs; requires Python 3 and matplotlib."""
import argparse
import csv
import json
from pathlib import Path
from statistics import median

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("log", type=Path)
parser.add_argument("--data-dir", type=Path, default=Path("docs/benchmarks"))
parser.add_argument("--image-dir", type=Path, default=Path("docs/images"))
args = parser.parse_args()
args.data_dir.mkdir(parents=True, exist_ok=True)
args.image_dir.mkdir(parents=True, exist_ok=True)
rows = {"load": [], "capacity": []}
for line in args.log.read_text().splitlines():
    for kind, marker in [("load", "KAFKA_LOAD "), ("capacity", "KAFKA_CAPACITY ")]:
        if marker in line:
            rows[kind].append(json.loads(line.split(marker, 1)[1]))
for kind, data in rows.items():
    if not data:
        raise SystemExit(f"Missing {kind} measurements")
    with (args.data_dir / f"kafka-{kind}.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=sorted(data[0]))
        writer.writeheader()
        writer.writerows(data)

levels = sorted({r["concurrency"] for r in rows["load"]})
fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), constrained_layout=True)
for ax, field, title, unit in [
    (axes[0], "successful_calls_per_second", "Completed throughput", "Successful RPC/s"),
    (axes[1], "p95_ms", "Completion latency", "Milliseconds"),
    (axes[2], "failures", "Failed or mismatched calls", "Calls per trial"),
]:
    groups = [[r[field] for r in rows["load"] if r["concurrency"] == c] for c in levels]
    mid = [median(g) for g in groups]
    ax.errorbar(levels, mid, yerr=[[m-min(g) for m,g in zip(mid,groups)], [max(g)-m for m,g in zip(mid,groups)]], marker="o", capsize=4, label="p95" if field == "p95_ms" else "median; range")
    if field == "p95_ms":
        for extra, label in [("p50_ms", "p50"), ("p99_ms", "p99")]:
            ax.plot(levels, [median([r[extra] for r in rows["load"] if r["concurrency"] == c]) for c in levels], marker=".", label=label)
    ax.set(xlabel="Concurrent callers (closed loop)", ylabel=unit, title=title)
    if field == "failures":
        ax.set_ylim(bottom=0, top=max(1, max(max(g) for g in groups) * 1.1))
    ax.set_xticks(levels)
    ax.grid(alpha=.25)
    ax.legend(fontsize=8)
fig.suptitle("Kafka 4.4.0 | 3 sessions, 1 server | 5 s/trial, 3 trials/level")
fig.savefig(args.image_dir / "kafka-load.svg")
plt.close(fig)
fig, ax = plt.subplots(figsize=(7, 4), constrained_layout=True)
data = sorted(rows["capacity"], key=lambda r: r["sessions"])
ax.plot([r["sessions"] for r in data], [r["read_amplification"] for r in data], "o-", label="Measured Value bytes ratio")
ax.set(xlabel="Client sessions", ylabel="Read / matched payload bytes", title="Shared reply partition: 64 RPCs per scenario")
ax.set_xticks([r["sessions"] for r in data])
ax.grid(alpha=.25)
ax.legend()
fig.savefig(args.image_dir / "kafka-read-amplification.svg")
