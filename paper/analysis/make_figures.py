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
plt.rcParams.update({"font.family": "Times New Roman", "font.size": 11, "axes.titlesize": 12, "axes.labelsize": 11, "xtick.labelsize": 11, "ytick.labelsize": 11})

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

# Simplified timeline view of sequence alignment and separate timing comparison.
fig, ax = plt.subplots(figsize=(7.2, 3.2))
match_color = "#2f6f9f"
extra_color = "#b04a4a"
ref_y = 2.05
cand_y = 0.95
ref_times = [0.0, 4.0, 8.0]
cand_times = [0.2, 4.4, 8.5]
labels = ["Phase 2 green", "Phase 2 yellow", "Phase 2 red"]

ax.set_xlim(-0.6, 9.2)
ax.set_ylim(-0.85, 2.85)
ax.set_yticks([ref_y, cand_y], ["Reference", "Candidate"])
ax.set_xlabel("Elapsed replay time (s)")
ax.spines[["left", "right", "top"]].set_visible(False)
ax.spines["bottom"].set_position(("data", -0.55))
ax.grid(axis="x", alpha=.18)

for t, label in zip(ref_times, labels):
    ax.vlines(t, ref_y - .27, ref_y + .27, color=match_color, lw=2)
    ax.text(t, ref_y + .39, label, ha="center", va="bottom", fontsize=11)
for t, label in zip(cand_times, labels):
    ax.vlines(t, cand_y - .27, cand_y + .27, color=match_color, lw=2)
    ax.text(t, cand_y - .39, label, ha="center", va="top", fontsize=11)

extra_time = 6.1
ax.vlines(extra_time, cand_y - .27, cand_y + .27, color=extra_color, lw=2)
ax.text(extra_time, cand_y - .39, "Phase 6 call\n(additional)", ha="center", va="top", fontsize=11, color=extra_color)

for ref_t, cand_t in zip(ref_times, cand_times):
    ax.plot([ref_t, cand_t], [ref_y - .27, cand_y + .27], color="#778899", linestyle="--", linewidth=1)

ax.text(4.3, 2.72, "DTW aligns event groups by content; timestamps are compared after alignment.", ha="center", va="top", fontsize=11)
fig.tight_layout()
fig.savefig(FIGURES / "alignment-example.pdf", bbox_inches="tight")
plt.close(fig)

workflow = '''<svg xmlns="http://www.w3.org/2000/svg" width="900" height="225" viewBox="0 0 900 225">
<defs><marker id="a" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="#40566d"/></marker></defs>
<style>text{font-family:'Times New Roman',serif;font-size:20px;fill:#203040}.box{fill:#eef3f7;stroke:#40566d;stroke-width:1.5}.arrow{stroke:#40566d;stroke-width:2;marker-end:url(#a)}.small{font-size:20px}</style>
<rect class="box" x="20" y="55" width="130" height="70" rx="8"/><text x="85" y="84" text-anchor="middle">Live controller</text><text x="85" y="103" text-anchor="middle">event log</text>
<rect class="box" x="190" y="55" width="145" height="70" rx="8"/><text x="262" y="84" text-anchor="middle">NTCIP input</text><text x="262" y="103" text-anchor="middle">replay</text>
<rect class="box" x="375" y="55" width="145" height="70" rx="8"/><text x="447" y="84" text-anchor="middle">Controller or</text><text x="447" y="103" text-anchor="middle">emulator</text>
<rect class="box" x="560" y="55" width="140" height="70" rx="8"/><text x="630" y="84" text-anchor="middle">Event log</text><text x="630" y="103" text-anchor="middle">Collector*</text>
<rect class="box" x="740" y="55" width="140" height="70" rx="8"/><text x="810" y="84" text-anchor="middle">Alignment,</text><text x="810" y="103" text-anchor="middle">review, report</text>
<path class="arrow" d="M150 90 H190"/><path class="arrow" d="M335 90 H375"/><path class="arrow" d="M520 90 H560"/><path class="arrow" d="M700 90 H740"/>
<text class="small" x="450" y="28" text-anchor="middle">Detector, pedestrian, and preempt inputs use NTCIP 1202 objects</text>
<text class="small" x="450" y="168" text-anchor="middle">*The current collector reads MAXTIME event logs.</text><text class="small" x="450" y="191" text-anchor="middle">It can be extended to other controller types.</text>
</svg>'''
(FIGURES / "workflow.svg").write_text(workflow, encoding="utf-8")
print("created", ", ".join(p.name for p in FIGURES.iterdir()))
