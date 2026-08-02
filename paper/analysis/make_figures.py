from __future__ import annotations
import csv
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "paper" / "generated" / "results"
FIGURES = ROOT / "paper" / "figures"
FIGURES.mkdir(parents=True, exist_ok=True)
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 8, "axes.titlesize": 10, "axes.labelsize": 8})

software = pd.read_csv(RESULTS / "software_release_results.csv")
fig, ax = plt.subplots(figsize=(7.2, 3.8))
x = np.arange(len(software))
ax.bar(x - .18, software.sequence_match_percent, .36, label="Sequence match", color="#2f6f9f")
ax.bar(x + .18, software.timing_match_percent.fillna(0), .36, label="Timing match", color="#d18b35")
ax.axhline(95, color="#2f6f9f", ls="--", lw=.8, alpha=.7)
ax.axhline(90, color="#d18b35", ls="--", lw=.8, alpha=.7)
for i, row in software.iterrows():
    if row.status != "PASS":
        ax.text(i, 76, "candidate", rotation=90, ha="center", va="bottom", fontsize=5.5, color="#9b2c2c")
ax.set_ylim(30, 102); ax.set_ylabel("Match (%)"); ax.set_xlabel("Signal configuration")
ax.set_xticks(x, software.scenario, rotation=65, ha="right", fontsize=6)
ax.set_title("Software-release comparison: 2.15.1 versus 2.18.1")
ax.legend(loc="lower left", frameon=False, ncol=2)
ax.grid(axis="y", alpha=.2); fig.tight_layout()
fig.savefig(FIGURES / "software-release-summary.pdf", bbox_inches="tight"); plt.close(fig)

param = pd.read_csv(RESULTS / "parameter_intervention_results.csv")
fig, ax = plt.subplots(figsize=(7.2, 3.8))
colors = np.where(param.expected_comparison_group.eq("changed"), np.where(param.automatically_flagged, "#b04a4a", "#e6a33b"), "#4f8a62")
x = np.arange(len(param))
ax.scatter(x, param.sequence_match_percent, s=42, c=colors, edgecolor="white", linewidth=.4, zorder=3)
ax.axhline(95, color="#555", ls="--", lw=.8, label="sequence threshold")
ax.set_ylim(30, 103); ax.set_ylabel("Sequence match (%)"); ax.set_xlabel("Signal configuration")
ax.set_xticks(x, param.scenario_id, rotation=65, ha="right", fontsize=6)
ax.set_title("Known parameter intervention: 2.18.1 versus modified timing parameters")
from matplotlib.lines import Line2D
ax.legend(handles=[Line2D([0],[0],marker='o',color='w',markerfacecolor='#b04a4a',label='Changed and flagged',markersize=6), Line2D([0],[0],marker='o',color='w',markerfacecolor='#e6a33b',label='Changed but passed',markersize=6), Line2D([0],[0],marker='o',color='w',markerfacecolor='#4f8a62',label='Unchanged and passed',markersize=6)], frameon=False, loc="lower left")
ax.grid(axis="y", alpha=.2); fig.tight_layout()
fig.savefig(FIGURES / "parameter-intervention-summary.pdf", bbox_inches="tight"); plt.close(fig)

# Small illustrative alignment figure using the committed offline example, not a field result.
ref = pd.read_csv(ROOT / "examples/offline_comparison/reference.csv")
changed = pd.read_csv(ROOT / "examples/offline_comparison/candidate_changed.csv")
fig, axes = plt.subplots(2, 1, figsize=(7.0, 3.6), sharex=True)
for ax, frame, title, color in [(axes[0], ref, "Reference trace", "#2f6f9f"), (axes[1], changed, "Candidate trace with an additional event", "#b04a4a")]:
    for _, row in frame.iterrows():
        ax.vlines(pd.to_datetime(row.timestamp), 0, 1, color=color, lw=2)
        ax.text(pd.to_datetime(row.timestamp), 1.04, str(int(row.event_id)), ha="center", va="bottom", fontsize=7)
    ax.set_ylim(0, 1.28); ax.set_yticks([]); ax.set_title(title, loc="left", fontsize=9); ax.grid(axis="x", alpha=.18)
axes[-1].set_xlabel("Replay time (synthetic example)")
fig.suptitle("Illustrative event grouping before sequence and timing comparison", fontsize=10)
fig.tight_layout(); fig.savefig(FIGURES / "alignment-example.pdf", bbox_inches="tight"); plt.close(fig)

workflow = '''<svg xmlns="http://www.w3.org/2000/svg" width="900" height="180" viewBox="0 0 900 180">
<defs><marker id="a" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="#40566d"/></marker></defs>
<style>text{font-family:Arial,sans-serif;font-size:14px;fill:#203040}.box{fill:#eef3f7;stroke:#40566d;stroke-width:1.5}.arrow{stroke:#40566d;stroke-width:2;marker-end:url(#a)}.small{font-size:11px}</style>
<rect class="box" x="20" y="55" width="130" height="70" rx="8"/><text x="85" y="84" text-anchor="middle">Field event</text><text x="85" y="103" text-anchor="middle">log</text>
<rect class="box" x="190" y="55" width="145" height="70" rx="8"/><text x="262" y="84" text-anchor="middle">NTCIP input</text><text x="262" y="103" text-anchor="middle">replay</text>
<rect class="box" x="375" y="55" width="145" height="70" rx="8"/><text x="447" y="84" text-anchor="middle">Controller or</text><text x="447" y="103" text-anchor="middle">emulator</text>
<rect class="box" x="560" y="55" width="140" height="70" rx="8"/><text x="630" y="84" text-anchor="middle">Event-log</text><text x="630" y="103" text-anchor="middle">adapter</text>
<rect class="box" x="740" y="55" width="140" height="70" rx="8"/><text x="810" y="84" text-anchor="middle">Alignment,</text><text x="810" y="103" text-anchor="middle">review, report</text>
<path class="arrow" d="M150 90 H190"/><path class="arrow" d="M335 90 H375"/><path class="arrow" d="M520 90 H560"/><path class="arrow" d="M700 90 H740"/>
<text class="small" x="450" y="28" text-anchor="middle">Portable NTCIP replay inputs; current implementation uses a MAXTIME output adapter</text>
</svg>'''
(FIGURES / "workflow.svg").write_text(workflow, encoding="utf-8")
print("created", ", ".join(p.name for p in FIGURES.iterdir()))
