#!/usr/bin/env python3
"""Turns the JSON lines written by rdag-bench into a markdown table."""
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]

print("| exp | n | down | crash | batch | offered tx/s | committed tx/s | MB/s | avg ms | p50 ms | p99 ms | certs | consistent |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
for r in rows:
    c = r["config"]
    print(
        f"| {c['label']} | {c['nodes']} | {c['faults']} | {c['crash']} | {c['batch_size'] // 1000} KB "
        f"| {r['offered_tps']:,} | {r['committed_tps']:,.0f} | {r['committed_mb_per_s']:.1f} "
        f"| {r['latency_ms_avg']:.0f} | {r['latency_ms_p50']:.0f} | {r['latency_ms_p99']:.0f} "
        f"| {r['committed_certificates']} | {'yes' if r['consistent'] else 'NO'} |"
    )
