"""Render sustained Kafka load and split-process measurements with matplotlib."""
import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("log", type=Path)
parser.add_argument("samples", type=Path)
parser.add_argument("--split-summary", type=Path)
parser.add_argument("--data-dir", type=Path, default=Path("docs/benchmarks"))
parser.add_argument("--image-dir", type=Path, default=Path("docs/images"))
args = parser.parse_args()
args.data_dir.mkdir(parents=True, exist_ok=True)
args.image_dir.mkdir(parents=True, exist_ok=True)
if args.log.suffix == ".csv":
    with args.log.open() as source:
        rows = [{k: json.loads(v.lower()) if v in ("True", "False") else json.loads(v) for k, v in r.items()} for r in csv.DictReader(source)]
else:
    rows = [json.loads(line.split("KAFKA_LOAD ", 1)[1])
            for line in args.log.read_text().splitlines() if "KAFKA_LOAD " in line]
if not rows:
    raise SystemExit("No measured load rounds")
with (args.data_dir / "kafka-soak.csv").open("w", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=sorted(rows[0]))
    writer.writeheader()
    writer.writerows({k: json.dumps(v, sort_keys=True) if isinstance(v, dict) else v
                     for k, v in row.items()} for row in rows)
if args.samples.suffix == ".csv":
    with args.samples.open() as source:
        samples = [{k: v if k == "role" else float(v) for k, v in r.items()}
                   for r in csv.DictReader(source)]
else:
    samples = []
    for line in args.samples.read_text().splitlines():
        raw = json.loads(line)
        stats = raw["stats"]
        if "FILE:memory.current\n" not in stats:
            continue
        def number(name):
            return int(stats.split("FILE:" + name + "\n", 1)[1].splitlines()[0])
        samples.append({"time": raw["time"],
                        "role": "broker" if raw["container"].endswith("kafka") else "application",
                        "memory_current_mib": number("memory.current") / 2**20,
                        "memory_peak_mib": number("memory.peak") / 2**20})
with (args.data_dir / "kafka-soak-memory.csv").open("w", newline="") as output:
    writer = csv.DictWriter(output, fieldnames=list(samples[0]))
    writer.writeheader()
    writer.writerows(samples)
minutes, cumulative = [], 0
for row in rows:
    cumulative += row["admission_seconds"]
    minutes.append(cumulative / 60)
fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
for ax, fields, title, unit in [
    (axes[0, 0], [("successful_calls_per_second", "successful RPC/s")], "Throughput", "RPC/s"),
    (axes[0, 1], [("p50_ms", "p50"), ("p99_ms", "p99")], "Successful-call latency", "Milliseconds"),
    (axes[1, 0], [("cpu_cores", "application (quota: 4 cores)")], "Application CPU", "Used cores"),
]:
    for field, label in fields:
        ax.plot(minutes, [r[field] for r in rows], label=label)
    ax.set(xlabel="Cumulative request-admission minutes", ylabel=unit, title=title)
    ax.set_ylim(bottom=0)
    ax.grid(alpha=.25)
    ax.legend(fontsize=8)
axes[1, 0].set_ylim(0, 4)
start = min(s["time"] for s in samples)
for role in ["application", "broker"]:
    series = [s for s in samples if s["role"] == role]
    axes[1, 1].plot([(s["time"]-start)/60 for s in series],
                    [s["memory_current_mib"] for s in series], label=role)
axes[1, 1].set(xlabel="Minutes since first resource sample", ylabel="MiB", title="Current cgroup memory (includes cache)")
axes[1, 1].set_ylim(bottom=0)
axes[1, 1].grid(alpha=.25)
axes[1, 1].legend()
failures = sum(r["failures"] for r in rows)
fig.suptitle(f"Kafka mixed-service soak | {len(rows)} rounds | failures/mismatches: {failures}")
fig.savefig(args.image_dir / "kafka-soak.png", dpi=160)
plt.close(fig)
if args.split_summary:
    if args.split_summary.suffix == ".csv":
        with args.split_summary.open() as source:
            data = [{k: v if k == "case" else float(v) for k, v in r.items()}
                    for r in csv.DictReader(source)]
    else:
        data = json.loads(args.split_summary.read_text())
    data = [{k: v for k, v in r.items() if k != "rounds"} for r in data]
    with (args.data_dir / "kafka-split.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), constrained_layout=True)
    labels = [r["case"].replace("client", "C").replace("server", "S") for r in data]
    axes[0].bar(labels, [r["rps"] for r in data])
    axes[0].set(title="Throughput: median of 3 x 20 s", ylabel="Successful RPC/s")
    axes[1].bar(labels, [r["client_cpu"] for r in data], label="clients + load driver")
    axes[1].bar(labels, [r["server_cpu_sampled"] for r in data],
                bottom=[r["client_cpu"] for r in data], label="server")
    axes[1].set(title="Measured CPU usage", ylabel="Used cores")
    axes[1].legend()
    for ax in axes:
        ax.set_xlabel("Client / server CPU quota (cores)")
        ax.grid(axis="y", alpha=.25)
    fig.suptitle("Split processes | Orders.Get | 6144 concurrent calls | Broker: 2 cores / 2 GiB")
    fig.savefig(args.image_dir / "kafka-split.png", dpi=160)
    plt.close(fig)
