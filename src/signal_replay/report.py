import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from jinja2 import Template

from .comparison import DetectorChunkScore, render_sparkline_svg
from .test_suite import FirmwareTestSuite, ScenarioResult, TestType


def load_annotations(path: str) -> Dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {str(k): str(v) for k, v in data.items()}


def _image_to_base64(path: str) -> Optional[str]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return None
    with open(p, "rb") as f:
        content = f.read()
    return base64.b64encode(content).decode("utf-8")


def _coerce_detector_chunk_scores(raw_scores: List[dict]) -> List[DetectorChunkScore]:
    scores: List[DetectorChunkScore] = []
    for item in raw_scores or []:
        if isinstance(item, DetectorChunkScore):
            scores.append(item)
            continue
        scores.append(
            DetectorChunkScore(
                center_seconds=float(item.get("center_seconds", 0.0)),
                window_seconds=float(item.get("window_seconds", 0.0)),
                similarity_percentage=item.get("similarity_percentage"),
                has_activity=bool(item.get("has_activity", False)),
                excluded_from_match=bool(item.get("excluded_from_match", False)),
            )
        )
    return scores


def _build_combined_detector_timeline(
    results: List[ScenarioResult],
    threshold: float,
) -> str:
    aggregate: Dict[tuple, List[float]] = {}

    for row in results:
        if row.test_type != TestType.SIMILARITY:
            continue
    raw_scores = getattr(row, "detector_chunk_scores", getattr(row, "phase_call_chunk_scores", []))
    for score in _coerce_detector_chunk_scores(raw_scores):
            if score.similarity_percentage is None:
                continue
            key = (round(score.center_seconds, 6), round(score.window_seconds, 6))
            aggregate.setdefault(key, []).append(float(score.similarity_percentage))

    combined_scores = [
    DetectorChunkScore(
            center_seconds=center_seconds,
            window_seconds=window_seconds,
            similarity_percentage=sum(values) / len(values),
            has_activity=True,
            excluded_from_match=(sum(values) / len(values)) < threshold,
        )
        for (center_seconds, window_seconds), values in sorted(aggregate.items())
        if values
    ]

    if not combined_scores:
        return ""

    return render_sparkline_svg(
        [],
        detector_chunk_scores=combined_scores,
        detector_threshold=threshold,
        auto_scale_y=True,
        show_exclusion_legend=False,
    )


_REPORT_TEMPLATE = Template("""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ suite.suite_name }} &mdash; Firmware Validation Report</title>
<style>
:root {
  --pass: #1b8a2e;
  --pass-bg: #e6f4ea;
  --fail: #c5221f;
  --fail-bg: #fce8e6;
  --warn: #e8710a;
  --warn-bg: #fef7e0;
  --border: #dadce0;
  --bg: #f8f9fa;
  --card: #ffffff;
  --text: #1f1f1f;
  --muted: #5f6368;
  --link: #1a73e8;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); line-height: 1.5; font-size: 16px; }

/* Header */
.header { background: linear-gradient(135deg, #1a237e 0%, #283593 100%); color: white; padding: 32px 40px; }
.header h1 { font-size: 32px; font-weight: 700; margin-bottom: 4px; }
.header .subtitle { opacity: 0.9; font-size: 17px; }
.header .generated { opacity: 0.72; font-size: 14px; margin-top: 8px; }

/* Layout */
.container { max-width: 1600px; margin: 0 auto; padding: 24px 40px 64px; }

/* Cards */
.card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; margin-bottom: 20px; overflow: hidden; }
.card-header { padding: 16px 20px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 12px; }
.card-header h2 { font-size: 22px; font-weight: 700; }
.card-body { padding: 20px; }

/* Summary tiles */
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
.tile { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; text-align: center; }
.tile .value { font-size: 40px; font-weight: 700; }
.tile .label { font-size: 15px; color: var(--muted); margin-top: 4px; }
.tile.pass .value { color: var(--pass); }
.tile.fail .value { color: var(--fail); }
.tile.neutral .value { color: var(--text); }

/* Match bar */
.match-bar-wrap { display: flex; align-items: center; gap: 8px; }
.match-bar { flex: 1; height: 10px; background: #eee; border-radius: 5px; overflow: hidden; max-width: 120px; }
.match-bar-fill { height: 100%; border-radius: 5px; transition: width .3s; }
.match-bar-fill.high  { background: var(--pass); }
.match-bar-fill.med   { background: var(--warn); }
.match-bar-fill.low   { background: var(--fail); }

/* Tables */
table { width: 100%; border-collapse: collapse; font-size: 15px; }
th { background: var(--bg); padding: 12px 14px; text-align: left; font-weight: 700; border-bottom: 2px solid var(--border); white-space: nowrap; font-size: 15px; }
td { padding: 12px 14px; border-bottom: 1px solid var(--border); vertical-align: middle; }
tr:hover td { background: #f1f3f4; }

/* Badge */
.badge { display: inline-block; padding: 4px 11px; border-radius: 12px; font-size: 13px; font-weight: 700; letter-spacing: 0.3px; }
.badge.pass { background: var(--pass); color: white; }
.badge.fail { background: var(--fail); color: white; }
.badge.warn { background: var(--warn); color: white; }
.badge.info { background: #e8eaed; color: var(--muted); }

/* Scenario detail */
.scenario { margin-bottom: 32px; padding-bottom: 24px; border-bottom: 2px solid var(--border); }
.scenario:last-child { border-bottom: none; }
.scenario-header { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; }
.scenario-header h3 { font-size: 24px; font-weight: 700; }
.meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px 24px; margin-bottom: 16px; font-size: 16px; }
.meta-grid .label { color: var(--muted); }
.meta-grid .val { font-weight: 600; }

/* Phase diff table */
.phase-table { width: 100%; border-collapse: collapse; font-size: 14px; font-family: 'Cascadia Code', 'Consolas', monospace; margin: 8px 0; }
.phase-table th { background: var(--bg); padding: 8px 10px; text-align: left; font-weight: 700; border-bottom: 2px solid var(--border); font-size: 13px; }
.phase-table td { padding: 7px 10px; border-bottom: 1px solid var(--border); }
.phase-table tr:hover td { background: #f1f3f4; }
.phase-table .num { text-align: right; font-variant-numeric: tabular-nums; }
.phase-table .pos { color: var(--pass); }
.phase-table .neg { color: var(--fail); }
.phase-table .warning-yellow { background-color: #fff4e5; font-weight: bold; }

/* Notes */
.notes { background: #f8f9fa; border-left: 3px solid var(--border); padding: 12px 16px; font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 14px; white-space: pre-wrap; word-break: break-word; margin: 12px 0; border-radius: 0 6px 6px 0; max-height: 300px; overflow-y: auto; }
.notes.error { border-left-color: var(--fail); background: var(--fail-bg); }

/* Images */
.plot-gallery { display: flex; flex-direction: column; gap: 16px; margin-top: 12px; }
.plot-gallery img { max-width: 100%; border: 1px solid var(--border); border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.plot-caption { font-size: 14px; color: var(--muted); margin-top: 2px; font-weight: 600; }

/* Footer */
.footer { text-align: center; padding: 24px; font-size: 14px; color: var(--muted); border-top: 1px solid var(--border); margin-top: 32px; }

/* Anchor offset for fixed-ish header */
[id] { scroll-margin-top: 20px; }
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }

/* Config section */
.config-table td:first-child { font-weight: 500; white-space: nowrap; width: 260px; }

/* Sparkline */
.sparkline-wrap {
  display: block;
  width: 100%;
  padding: 14px 16px 12px;
  background: #f8f9fa;
  border: 1px solid var(--border);
  border-radius: 8px;
  overflow-x: auto;
}
.sparkline-wrap svg {
  display: block;
  width: 100%;
  height: auto;
  min-height: 190px;
}
.chart-note { color: var(--muted); font-size: 14px; margin-top: 8px; }
.chart-note strong { color: var(--text); }
.section-copy { color: var(--muted); font-size: 15px; margin-bottom: 14px; max-width: 1050px; }
</style>
</head>
<body>

<div class="header">
  <h1>{{ suite.suite_name }}</h1>
  <div class="subtitle">Baseline <strong>{{ suite.baseline_version }}</strong> &rarr; New firmware <strong>{{ suite.firmware_version }}</strong></div>
  <div class="generated">Generated {{ generated_at }}</div>
</div>

<div class="container">

<!-- ===== Summary tiles ===== -->
<div class="summary-grid">
  <div class="tile {{ 'pass' if total_pass == total_count else 'fail' }}">
    <div class="value">{{ total_pass }}/{{ total_count }}</div>
    <div class="label">Scenarios Passed</div>
  </div>
  {% if similarity|length > 0 %}
  <div class="tile {{ 'pass' if similarity_fail == 0 else 'fail' }}">
    <div class="value">{{ similarity_pass }}/{{ similarity|length }}</div>
    <div class="label">Similarity Tests</div>
  </div>
  {% endif %}
  {% if conflict|length > 0 %}
  <div class="tile {{ 'pass' if conflict_fail == 0 else 'fail' }}">
    <div class="value">{{ conflict_pass }}/{{ conflict|length }}</div>
    <div class="label">Conflict Tests</div>
  </div>
  {% endif %}
  <div class="tile {{ 'pass' if avg_match >= 95 else ('neutral' if avg_match >= 80 else 'fail') }}">
    <div class="value">{{ '%.1f'|format(avg_match) }}%</div>
    <div class="label">Avg Match (Similarity)</div>
  </div>
</div>

<!-- ===== Results table ===== -->
{% if similarity|length > 0 %}
<div class="card">
  <div class="card-header"><h2>Similarity Results</h2></div>
  <div class="card-body" style="padding:0;">
    <table>
      <thead>
        <tr><th>Scenario</th><th>Description</th><th>Match</th><th style="min-width:140px;">Bar</th><th>Divergences</th><th>Status</th></tr>
      </thead>
      <tbody>
      {% for r in sorted_similarity %}
        <tr>
          <td><a href="#{{ r.scenario_id }}">{{ r.scenario_id }}</a></td>
          <td>{{ r.notes.split('\\n')[0][:80] if r.notes else '' }}</td>
          <td style="font-weight:600;{% if r.thrown_out %} color:var(--warn);{% elif r.match_percentage is not none and r.match_percentage >= 95 %} color:var(--pass);{% elif r.match_percentage is not none and r.match_percentage >= 80 %} color:var(--warn);{% elif r.match_percentage is not none %} color:var(--fail);{% endif %}">
            {{ 'Thrown out' if r.thrown_out else ('%.1f%%'|format(r.match_percentage) if r.match_percentage is not none else '&mdash;') }}
          </td>
          <td>
            {% if r.match_percentage is not none and not r.thrown_out %}
            <div class="match-bar-wrap">
              <div class="match-bar"><div class="match-bar-fill {{ 'high' if r.match_percentage >= 95 else ('med' if r.match_percentage >= 80 else 'low') }}" style="width:{{ [r.match_percentage, 100]|min }}%"></div></div>
            </div>
            {% endif %}
          </td>
          <td>{{ r.num_divergences }}</td>
          <td><span class="badge {{ 'warn' if r.thrown_out else ('pass' if r.passed else 'fail') }}">{{ 'THROWN OUT' if r.thrown_out else ('PASS' if r.passed else 'FAIL') }}</span></td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% endif %}

{% if conflict|length > 0 %}
<div class="card">
  <div class="card-header"><h2>Conflict Results</h2></div>
  <div class="card-body" style="padding:0;">
    <table>
      <thead>
        <tr><th>Scenario</th><th>Runs</th><th>New Conflicts</th><th>Status</th></tr>
      </thead>
      <tbody>
      {% for r in conflict %}
        <tr>
          <td><a href="#{{ r.scenario_id }}">{{ r.scenario_id }}</a></td>
          <td>{{ r.runs_completed }} / {{ r.total_runs }}</td>
          <td>{{ r.conflicts_found|length }}</td>
          <td><span class="badge {{ 'pass' if r.passed else 'fail' }}">{{ 'PASS' if r.passed else 'FAIL' }}</span></td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% endif %}

{% if combined_phase_call_timeline_svg %}
<div class="card">
  <div class="card-header"><h2>Combined Timeline</h2></div>
  <div class="card-body">
    <div class="section-copy">
      Black points and the connecting line show the average vehicle-detector input similarity across all similarity devices. This is a system-wide sanity check for network or compute issues that may have affected every device at once, independent of firmware version.
    </div>
    <div class="sparkline-wrap">{{ combined_phase_call_timeline_svg }}</div>
  </div>
</div>
{% endif %}

<!-- ===== Detailed results ===== -->
<div class="card">
  <div class="card-header"><h2>Detailed Results</h2></div>
  <div class="card-body">
  {% for row in detail_rows %}
    <div class="scenario" id="{{ row.scenario_id }}">
      <div class="scenario-header">
        <h3>{{ row.scenario_id }}</h3>
        <span class="badge {{ 'warn' if row.thrown_out else ('pass' if row.passed else 'fail') }}">{{ 'THROWN OUT' if row.thrown_out else ('PASS' if row.passed else 'FAIL') }}</span>
        <span class="badge info">{{ row.test_type }}</span>
      </div>
      {% if row.notes_column %}
        <div style="font-weight:600; margin-bottom:12px; font-size:15px; border-left:4px solid #1a237e; padding-left:12px; color:#1a237e;">{{ row.notes_column }}</div>
      {% endif %}

      <div class="meta-grid">
        {% if row.thrown_out %}
        <div><span class="label">Match:</span> <span class="val" style="color:var(--warn)">Thrown out</span></div>
        <div><span class="label">Reason:</span> <span class="val">All scored chunks fell below the input similarity threshold</span></div>
        {% elif row.match_percentage is not none %}
        <div><span class="label">Match:</span> <span class="val{% if row.match_percentage >= 95 %}" style="color:var(--pass){% elif row.match_percentage >= 80 %}" style="color:var(--warn){% else %}" style="color:var(--fail){% endif %}">{{ '%.1f%%'|format(row.match_percentage) }}</span></div>
        {% endif %}
        <div><span class="label">Divergences:</span> <span class="val">{{ row.num_divergences }}</span></div>
        <div><span class="label">Runs:</span> <span class="val">{{ row.runs_completed }} / {{ row.total_runs }}</span></div>
        {% if row.test_type == 'similarity' and row.sparkline_svg %}
        <div><span class="label">Included chunks:</span> <span class="val">{{ row.included_chunk_count }}</span></div>
        <div><span class="label">Excluded chunks:</span> <span class="val">{{ row.excluded_chunk_count }}</span></div>
        {% endif %}

        {% if row.annotation %}<div><span class="label">Note:</span> <span class="val">{{ row.annotation }}</span></div>{% endif %}
      </div>

      {% if row.sparkline_svg %}
        <div style="margin:12px 0;">
          <div style="font-weight:600;margin-bottom:6px;">Match Timeline</div>
          <div class="sparkline-wrap">{{ row.sparkline_svg }}</div>
          {% if row.test_type == 'similarity' %}
          <div class="chart-note"><strong>Match bars</strong> show the firmware comparison score by chunk. <strong>Black points/line</strong> show vehicle-detector input similarity, which is treated as a simulation-reliability sanity check independent of firmware version. Semi-transparent bars were excluded from the device-level match average because input similarity fell below {{ detector_similarity_threshold }}%.</div>
          {% endif %}
        </div>
      {% endif %}

      {% if row.error %}
        <div class="notes error">{{ row.error }}</div>
      {% endif %}
      {% if row.notes %}
        <details{% if not row.passed %} open{% endif %}>
          <summary style="cursor:pointer;font-weight:600;margin-bottom:6px;">Comparison Details</summary>
          <div class="notes">{{ row.notes }}</div>
        </details>
      {% endif %}

      {% if row.conflicts_found %}
        <details open>
          <summary style="cursor:pointer;font-weight:600;margin-bottom:6px;">Conflicts ({{ row.conflicts_found|length }})</summary>
          <div class="notes error">{{ row.conflicts_found }}</div>
        </details>
      {% endif %}

      {% if row.test_type == 'similarity' and row.timeline_difference_analysis_available %}
        <details{% if not row.passed %} open{% endif %}>
          <summary style="cursor:pointer;font-weight:600;margin-bottom:6px;">Phase / Overlap Differences{% if row.phase_differences %} (top {{ [row.phase_differences|length, 5]|min }} of {{ row.phase_differences|length }}){% endif %}</summary>
          {% if row.phase_differences %}
          <table class="phase-table">
            <thead>
              <tr>
                <th>Phase / Overlap</th>
                <th>State</th>
                <th class="num">Count (Orig)</th>
                <th class="num">Count (New)</th>
                <th class="num">&Delta; Count</th>
                <th class="num">Avg Dur Orig (s)</th>
                <th class="num">Avg Dur New (s)</th>
                <th class="num">&Delta; Avg Dur (s)</th>
              </tr>
            </thead>
            <tbody>
            {% for d in row.phase_differences[:5] %}
              <tr>
                <td>{{ d.label }}</td>
                <td>{{ d.state }}</td>
                <td class="num{% if d.count_a == 0 %} warning-yellow{% endif %}">{{ d.count_a }}</td>
                <td class="num{% if d.count_b == 0 %} warning-yellow{% endif %}">{{ d.count_b }}</td>
                <td class="num {{ 'pos' if d.count_delta > 0 else ('neg' if d.count_delta < 0 else '') }}">{{ '%+d'|format(d.count_delta) if d.count_delta != 0 else '&mdash;' }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_a) }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_b) }}</td>
                <td class="num {{ 'pos' if d.duration_delta > 0 else ('neg' if d.duration_delta < 0 else '') }}">{{ '%+.1f'|format(d.duration_delta) }}</td>
              </tr>
            {% endfor %}
            </tbody>
          </table>
          {% else %}
          <div class="chart-note">No meaningful phase or overlap differences were found.</div>
          {% endif %}
        </details>
      {% endif %}

      {% if row.test_type == 'similarity' and row.timeline_difference_analysis_available %}
        <details{% if not row.passed %} open{% endif %}>
          <summary style="cursor:pointer;font-weight:600;margin-bottom:6px;">Transition / Preempt / Ped Service Differences{% if row.operational_differences %} (top {{ [row.operational_differences|length, 5]|min }} of {{ row.operational_differences|length }}){% endif %}</summary>
          {% if row.operational_differences %}
          <table class="phase-table">
            <thead>
              <tr>
                <th>Category</th>
                <th>State</th>
                <th class="num">Count (Orig)</th>
                <th class="num">Count (New)</th>
                <th class="num">&Delta; Count</th>
                <th class="num">Avg Dur Orig (s)</th>
                <th class="num">Avg Dur New (s)</th>
                <th class="num">&Delta; Avg Dur (s)</th>
              </tr>
            </thead>
            <tbody>
            {% for d in row.operational_differences[:5] %}
              <tr>
                <td>{{ d.label }}</td>
                <td>{{ d.state }}</td>
                <td class="num{% if d.count_a == 0 %} warning-yellow{% endif %}">{{ d.count_a }}</td>
                <td class="num{% if d.count_b == 0 %} warning-yellow{% endif %}">{{ d.count_b }}</td>
                <td class="num {{ 'pos' if d.count_delta > 0 else ('neg' if d.count_delta < 0 else '') }}">{{ '%+d'|format(d.count_delta) if d.count_delta != 0 else '&mdash;' }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_a) }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_b) }}</td>
                <td class="num {{ 'pos' if d.duration_delta > 0 else ('neg' if d.duration_delta < 0 else '') }}">{{ '%+.1f'|format(d.duration_delta) }}</td>
              </tr>
            {% endfor %}
            </tbody>
          </table>
          {% else %}
          <div class="chart-note">No meaningful transition, preempt, or pedestrian-service differences were found.</div>
          {% endif %}
        </details>
      {% endif %}

      {% if row.images %}
        <div class="plot-gallery">
        {% for img in row.images %}
          <div>
            <div class="plot-caption">{{ img.caption }}</div>
            <img src="data:image/png;base64,{{ img.data }}" alt="{{ row.scenario_id }} {{ img.caption }}" loading="lazy" />
          </div>
        {% endfor %}
        </div>
      {% else %}
        {% if row.test_type == 'similarity' and row.passed %}
          <div style="color:var(--pass);font-size:14px;margin-top:8px;">&#10003; Sequences matched within thresholds &mdash; no divergence charts needed.</div>
        {% endif %}
      {% endif %}
    </div>
  {% endfor %}
  </div>
</div>

<!-- ===== Configuration ===== -->
<div class="card">
  <div class="card-header"><h2>Configuration</h2></div>
  <div class="card-body" style="padding:0;">
    <table class="config-table">
      <thead><tr><th>Setting</th><th>Value</th><th>Description</th></tr></thead>
      <tbody>
      <tr><td>Suite name</td><td>{{ suite.suite_name }}</td><td>Name of this validation test suite</td></tr>
      <tr><td>Firmware version</td><td>{{ suite.firmware_version }}</td><td>New firmware version being validated</td></tr>
      <tr><td>Baseline version</td><td>{{ suite.baseline_version }}</td><td>Reference baseline used for comparison</td></tr>
      <tr><td>Collection interval</td><td>{{ suite.collection_interval_minutes }} minutes</td><td>Duration of event log collection per scenario</td></tr>
      <tr><td>Post-replay settle</td><td>{{ suite.post_replay_settle_seconds }} seconds</td><td>Wait time after log replay before collecting events</td></tr>
      <tr><td>Analysis settle window</td><td>{{ suite.analysis_settle_minutes }} minutes</td><td>Initial replay period excluded from reported similarity analysis</td></tr>
      {% if suite.analysis_start_time %}
      <tr><td>Manual analysis start time</td><td>{{ suite.analysis_start_time }}</td><td>Clock time used to clip TOD scenarios before similarity analysis</td></tr>
      {% endif %}
      {% if suite.comparison_thresholds %}
      <tr><td>Sequence DTW threshold</td><td>{{ suite.comparison_thresholds.sequence_threshold }}</td><td>Max normalised DTW distance for event-sequence similarity</td></tr>
      <tr><td>Timing DTW threshold</td><td>{{ suite.comparison_thresholds.timing_threshold }}</td><td>Max normalised DTW distance for timing similarity</td></tr>
      <tr><td>Match threshold</td><td>{{ suite.comparison_thresholds.match_threshold }}%</td><td>Minimum DTW match percentage to pass a scenario</td></tr>
      {% endif %}
      <tr><td>Vehicle detector similarity threshold</td><td>{{ detector_similarity_threshold }}%</td><td>Vehicle-detector input similarity below this value excludes a chunk from the device-level match average while leaving the chunk bar visible as a reliability sanity check</td></tr>
      </tbody>
    </table>
  </div>
</div>

</div><!-- container -->

<div class="footer">
  Firmware Validation Report &mdash; signal_replay v{{ version }} &mdash; {{ generated_at }}
</div>
</body>
</html>
""")


def generate_report(
    results: List[ScenarioResult],
    suite: FirmwareTestSuite,
    output_path: str,
    annotations: Optional[Dict[str, str]] = None,
) -> str:
    """Generate a self-contained HTML firmware validation report.

    The report embeds all plot images as base64 so it is portable as a single file.
    """
    annotations = annotations or {}

    similarity = [r for r in results if r.test_type == TestType.SIMILARITY]
    conflict = [r for r in results if r.test_type == TestType.CONFLICT]

    similarity_pass = sum(1 for r in similarity if r.passed)
    similarity_fail = len(similarity) - similarity_pass

    conflict_pass = sum(1 for r in conflict if r.passed)
    conflict_fail = len(conflict) - conflict_pass

    total_pass = similarity_pass + conflict_pass
    total_count = len(results)

    match_values = [r.match_percentage for r in similarity if r.match_percentage is not None and not getattr(r, "thrown_out", False)]
    avg_match = sum(match_values) / len(match_values) if match_values else 0.0
    detector_similarity_threshold = float(getattr(suite, "detector_similarity_threshold", getattr(suite, "phase_call_similarity_threshold", 90.0)))
    combined_phase_call_timeline_svg = _build_combined_detector_timeline(results, detector_similarity_threshold)

    # Keep input order for both summary table and detail section
    # (caller is expected to pre-sort by scenario name or suite order)
    sorted_similarity = list(similarity)

    detail_rows = []
    for row in results:
        encoded_images = []
        for image_path in row.plot_paths:
            img_data = _image_to_base64(image_path)
            if img_data:
                # Derive a short caption from the filename
                caption = Path(image_path).stem.replace("_", " ")
                encoded_images.append({"caption": caption, "data": img_data})

        detail_rows.append(
            {
                "scenario_id": row.scenario_id,
                "test_type": row.test_type.value,
                "passed": row.passed,
                "match_percentage": row.match_percentage,
                "thrown_out": getattr(row, 'thrown_out', False),
                "num_divergences": row.num_divergences,
                "runs_completed": row.runs_completed,
                "total_runs": row.total_runs,
                "notes": row.notes,
                "notes_column": getattr(row, 'notes_column', ''),
                "error": row.error,
                "conflicts_found": row.conflicts_found,
                "annotation": annotations.get(row.scenario_id, ""),
                "images": encoded_images,
                "phase_differences": getattr(row, 'phase_differences', []),
                "operational_differences": getattr(row, 'operational_differences', []),
                "sparkline_svg": getattr(row, 'sparkline_svg', ''),
                "detector_chunk_scores": getattr(row, 'detector_chunk_scores', getattr(row, 'phase_call_chunk_scores', [])),
                "included_chunk_count": getattr(row, 'included_chunk_count', 0),
                "excluded_chunk_count": getattr(row, 'excluded_chunk_count', 0),
                "timeline_difference_analysis_available": getattr(row, 'timeline_difference_analysis_available', False),
                "temporal_shift_seconds": getattr(row, 'temporal_shift_seconds', 0.0),
            }
        )

    try:
        from . import __version__ as version
    except Exception:
        version = "?"

    html = _REPORT_TEMPLATE.render(
        suite=suite,
        similarity=similarity,
        conflict=conflict,
        similarity_pass=similarity_pass,
        similarity_fail=similarity_fail,
        conflict_pass=conflict_pass,
        conflict_fail=conflict_fail,
        total_pass=total_pass,
        total_count=total_count,
        avg_match=avg_match,
        detector_similarity_threshold=detector_similarity_threshold,
        combined_phase_call_timeline_svg=combined_phase_call_timeline_svg,
        sorted_similarity=sorted_similarity,
        detail_rows=detail_rows,
        version=version,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    return str(output)
