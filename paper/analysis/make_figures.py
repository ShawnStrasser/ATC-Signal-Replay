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

# Three candidate explanations of sequence alignment for author selection.
from matplotlib.patches import FancyBboxPatch

fig, axes = plt.subplots(3, 1, figsize=(7.2, 8.4), gridspec_kw={"height_ratios": [1.0, 1.0, 1.25]})
match_color = "#2f6f9f"
extra_color = "#b04a4a"
neutral_color = "#eef3f7"

# (a) Order-only view: the information used by the DTW sequence cost.
ax = axes[0]
ax.set_xlim(0, 10); ax.set_ylim(0, 3); ax.axis("off")
ref_items = [(1.5, "Phase 2 green +\nOverlap A green"), (5.0, "Phase 2 yellow"), (8.5, "Phase 2 red")]
cand_items = [(1.2, "Phase 2 green +\nOverlap A green"), (4.0, "Phase 2 yellow"), (6.2, "Phase 6 call\n(candidate only)"), (8.8, "Phase 2 red")]
def draw_box(axis, x, y, label, color):
    box = FancyBboxPatch((x - 0.85, y - 0.32), 1.7, 0.64, boxstyle="round,pad=0.05", facecolor=neutral_color if color == match_color else "#f9e7e7", edgecolor=color, linewidth=1.4)
    axis.add_patch(box)
    axis.text(x, y, label, ha="center", va="center", fontsize=11)
for x, label in ref_items:
    draw_box(ax, x, 2.1, label, match_color)
for x, label in cand_items:
    draw_box(ax, x, 0.8, label, extra_color if "candidate only" in label else match_color)
for xr, xc in zip([1.5, 5.0, 8.5], [1.2, 4.0, 8.8]):
    ax.plot([xr, xc], [1.76, 1.14], color="#778899", linestyle="--", linewidth=1)
ax.text(0.05, 2.1, "Reference", ha="right", va="center", fontsize=11, weight="bold")
ax.text(0.05, 0.8, "Candidate", ha="right", va="center", fontsize=11, weight="bold")
ax.set_title("(a) Ordered event groups used for the sequence comparison", loc="left", fontsize=12, weight="bold")
ax.text(5, 0.05, "DTW aligns matching event groups; the additional phase call remains flagged.", ha="center", fontsize=11)

# (b) Timeline view: the same matches with timestamps restored.
ax = axes[1]
ax.set_xlim(-0.6, 9.2); ax.set_ylim(0, 3)
ax.set_yticks([2.05, 0.85], ["Reference", "Candidate"])
ax.set_xlabel("Elapsed replay time (s)")
ax.spines[["left", "right", "top"]].set_visible(False)
ax.grid(axis="x", alpha=.18)
ref_times = [0.0, 4.0, 8.0]
labels = ["Phase 2 green", "Phase 2 yellow", "Phase 2 red"]
for t, label in zip(ref_times, labels):
    ax.vlines(t, 1.72, 2.38, color=match_color, lw=2)
    ax.text(t, 2.48, label, ha="center", fontsize=11)
for t, label in zip([0.2, 4.4, 8.5], labels):
    ax.vlines(t, 0.52, 1.18, color=match_color, lw=2)
    ax.text(t, 0.37, label, ha="center", va="top", fontsize=11)
ax.vlines(6.1, 0.52, 1.18, color=extra_color, lw=2)
ax.text(6.1, 0.37, "Phase 6 call\nadditional", ha="center", va="top", fontsize=11, color=extra_color)
for a, b in zip(ref_times, [0.2, 4.4, 8.5]):
    ax.plot([a, b], [1.72, 1.18], color="#778899", linestyle="--", linewidth=1)
ax.set_title("(b) Timing comparison after the event groups are aligned", loc="left", fontsize=12, weight="bold")

# (c) Table view: explicit sequence and timing results.
ax = axes[2]
ax.axis("off")
rows = [
    ["1", "Phase 2 green + Overlap A green\n0.0 s", "Phase 2 green + Overlap A green\n0.2 s", "Sequence match; 0.2-s shift"],
    ["2", "Phase 2 yellow\n4.0 s", "Phase 2 yellow\n4.4 s", "Sequence match; 0.4-s shift"],
    ["—", "—", "Phase 6 call\n6.1 s", "Additional candidate event"],
    ["3", "Phase 2 red\n8.0 s", "Phase 2 red\n8.5 s", "Sequence match; 0.5-s shift"],
]
table = ax.table(cellText=rows, colLabels=["Aligned order", "Reference group", "Candidate group", "Result"], cellLoc="left", colLoc="left", loc="center", colWidths=[.13, .27, .27, .33])
table.auto_set_font_size(False)
table.set_fontsize(11)
table.scale(1, 1.65)
for (row, col), cell in table.get_celld().items():
    cell.set_edgecolor("#8796a5")
    cell.set_linewidth(.6)
    if row == 0:
        cell.set_facecolor("#dfe8ef")
        cell.set_text_props(weight="bold")
    elif row == 3:
        cell.set_facecolor("#f9e7e7")
ax.set_title("(c) Alignment table separating sequence and timing findings", loc="left", fontsize=12, weight="bold", pad=6)

fig.tight_layout(h_pad=1.0)
fig.savefig(FIGURES / "alignment-options.pdf", bbox_inches="tight")
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
