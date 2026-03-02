import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from jinja2 import Template

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
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); line-height: 1.5; }

/* Header */
.header { background: linear-gradient(135deg, #1a237e 0%, #283593 100%); color: white; padding: 32px 40px; }
.header h1 { font-size: 28px; font-weight: 600; margin-bottom: 4px; }
.header .subtitle { opacity: 0.85; font-size: 15px; }
.header .generated { opacity: 0.65; font-size: 13px; margin-top: 8px; }

/* Layout */
.container { max-width: 1200px; margin: 0 auto; padding: 24px 40px 64px; }

/* Cards */
.card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; margin-bottom: 20px; overflow: hidden; }
.card-header { padding: 16px 20px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 12px; }
.card-header h2 { font-size: 18px; font-weight: 600; }
.card-body { padding: 20px; }

/* Summary tiles */
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
.tile { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 20px; text-align: center; }
.tile .value { font-size: 36px; font-weight: 700; }
.tile .label { font-size: 13px; color: var(--muted); margin-top: 4px; }
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
table { width: 100%; border-collapse: collapse; font-size: 14px; }
th { background: var(--bg); padding: 10px 14px; text-align: left; font-weight: 600; border-bottom: 2px solid var(--border); white-space: nowrap; }
td { padding: 10px 14px; border-bottom: 1px solid var(--border); vertical-align: middle; }
tr:hover td { background: #f1f3f4; }

/* Badge */
.badge { display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; letter-spacing: 0.3px; }
.badge.pass { background: var(--pass); color: white; }
.badge.fail { background: var(--fail); color: white; }
.badge.warn { background: var(--warn); color: white; }
.badge.info { background: #e8eaed; color: var(--muted); }

/* Scenario detail */
.scenario { margin-bottom: 32px; padding-bottom: 24px; border-bottom: 2px solid var(--border); }
.scenario:last-child { border-bottom: none; }
.scenario-header { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; }
.scenario-header h3 { font-size: 20px; font-weight: 600; }
.meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px 24px; margin-bottom: 16px; font-size: 14px; }
.meta-grid .label { color: var(--muted); }
.meta-grid .val { font-weight: 500; }

/* Phase diff table */
.phase-table { width: 100%; border-collapse: collapse; font-size: 13px; font-family: 'Cascadia Code', 'Consolas', monospace; margin: 8px 0; }
.phase-table th { background: var(--bg); padding: 6px 10px; text-align: left; font-weight: 600; border-bottom: 2px solid var(--border); font-size: 12px; }
.phase-table td { padding: 5px 10px; border-bottom: 1px solid var(--border); }
.phase-table tr:hover td { background: #f1f3f4; }
.phase-table .num { text-align: right; font-variant-numeric: tabular-nums; }
.phase-table .pos { color: var(--pass); }
.phase-table .neg { color: var(--fail); }

/* Notes */
.notes { background: #f8f9fa; border-left: 3px solid var(--border); padding: 12px 16px; font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 13px; white-space: pre-wrap; word-break: break-word; margin: 12px 0; border-radius: 0 6px 6px 0; max-height: 300px; overflow-y: auto; }
.notes.error { border-left-color: var(--fail); background: var(--fail-bg); }

/* Images */
.plot-gallery { display: flex; flex-direction: column; gap: 16px; margin-top: 12px; }
.plot-gallery img { max-width: 100%; border: 1px solid var(--border); border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.plot-caption { font-size: 12px; color: var(--muted); margin-top: 2px; }

/* Footer */
.footer { text-align: center; padding: 24px; font-size: 12px; color: var(--muted); border-top: 1px solid var(--border); margin-top: 32px; }

/* Anchor offset for fixed-ish header */
[id] { scroll-margin-top: 20px; }
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }

/* Config section */
.config-table td:first-child { font-weight: 500; white-space: nowrap; width: 260px; }
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
          <td style="font-weight:600;{% if r.match_percentage is not none and r.match_percentage >= 95 %} color:var(--pass);{% elif r.match_percentage is not none and r.match_percentage >= 80 %} color:var(--warn);{% elif r.match_percentage is not none %} color:var(--fail);{% endif %}">
            {{ '%.1f%%'|format(r.match_percentage) if r.match_percentage is not none else '&mdash;' }}
          </td>
          <td>
            {% if r.match_percentage is not none %}
            <div class="match-bar-wrap">
              <div class="match-bar"><div class="match-bar-fill {{ 'high' if r.match_percentage >= 95 else ('med' if r.match_percentage >= 80 else 'low') }}" style="width:{{ [r.match_percentage, 100]|min }}%"></div></div>
            </div>
            {% endif %}
          </td>
          <td>{{ r.num_divergences }}</td>
          <td><span class="badge {{ 'pass' if r.passed else 'fail' }}">{{ 'PASS' if r.passed else 'FAIL' }}</span></td>
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

<!-- ===== Detailed results ===== -->
<div class="card">
  <div class="card-header"><h2>Detailed Results</h2></div>
  <div class="card-body">
  {% for row in detail_rows %}
    <div class="scenario" id="{{ row.scenario_id }}">
      <div class="scenario-header">
        <h3>{{ row.scenario_id }}</h3>
        <span class="badge {{ 'pass' if row.passed else 'fail' }}">{{ 'PASS' if row.passed else 'FAIL' }}</span>
        <span class="badge info">{{ row.test_type }}</span>
      </div>

      <div class="meta-grid">
        {% if row.match_percentage is not none %}
        <div><span class="label">Match:</span> <span class="val{% if row.match_percentage >= 95 %}" style="color:var(--pass){% elif row.match_percentage >= 80 %}" style="color:var(--warn){% else %}" style="color:var(--fail){% endif %}">{{ '%.1f%%'|format(row.match_percentage) }}</span></div>
        {% endif %}
        <div><span class="label">Divergences:</span> <span class="val">{{ row.num_divergences }}</span></div>
        <div><span class="label">Runs:</span> <span class="val">{{ row.runs_completed }} / {{ row.total_runs }}</span></div>
        {% if row.annotation %}<div><span class="label">Note:</span> <span class="val">{{ row.annotation }}</span></div>{% endif %}
      </div>

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

      {% if row.phase_differences %}
        <details{% if not row.passed %} open{% endif %}>
          <summary style="cursor:pointer;font-weight:600;margin-bottom:6px;">Phase / Overlap Differences (top {{ [row.phase_differences|length, 10]|min }} of {{ row.phase_differences|length }})</summary>
          <table class="phase-table">
            <thead>
              <tr>
                <th>Phase / Overlap</th>
                <th>State</th>
                <th class="num">Count (Orig)</th>
                <th class="num">Count (New)</th>
                <th class="num">&Delta; Count</th>
                <th class="num">Dur Orig (s)</th>
                <th class="num">Dur New (s)</th>
                <th class="num">&Delta; Dur (s)</th>
              </tr>
            </thead>
            <tbody>
            {% for d in row.phase_differences[:10] %}
              <tr>
                <td>{{ d.label }}</td>
                <td>{{ d.state }}</td>
                <td class="num">{{ d.count_a }}</td>
                <td class="num">{{ d.count_b }}</td>
                <td class="num {{ 'pos' if d.count_delta > 0 else ('neg' if d.count_delta < 0 else '') }}">{{ '%+d'|format(d.count_delta) if d.count_delta != 0 else '&mdash;' }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_a) }}</td>
                <td class="num">{{ '%.1f'|format(d.duration_b) }}</td>
                <td class="num {{ 'pos' if d.duration_delta > 0 else ('neg' if d.duration_delta < 0 else '') }}">{{ '%+.1f'|format(d.duration_delta) }}</td>
              </tr>
            {% endfor %}
            </tbody>
          </table>
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
      <tr><td>Suite name</td><td>{{ suite.suite_name }}</td></tr>
      <tr><td>Firmware version</td><td>{{ suite.firmware_version }}</td></tr>
      <tr><td>Baseline version</td><td>{{ suite.baseline_version }}</td></tr>
      <tr><td>Collection interval</td><td>{{ suite.collection_interval_minutes }} minutes</td></tr>
      <tr><td>Post-replay settle</td><td>{{ suite.post_replay_settle_seconds }} seconds</td></tr>
      {% if suite.comparison_thresholds %}
      <tr><td>Sequence DTW threshold</td><td>{{ suite.comparison_thresholds.sequence_threshold }}</td></tr>
      <tr><td>Timing DTW threshold</td><td>{{ suite.comparison_thresholds.timing_threshold }}</td></tr>
      <tr><td>Match threshold</td><td>{{ suite.comparison_thresholds.match_threshold }}%</td></tr>
      {% endif %}
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

    match_values = [r.match_percentage for r in similarity if r.match_percentage is not None]
    avg_match = sum(match_values) / len(match_values) if match_values else 0.0

    sorted_similarity = sorted(
        similarity,
        key=lambda r: (r.match_percentage is None, r.match_percentage or 999.0),
    )

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
                "num_divergences": row.num_divergences,
                "runs_completed": row.runs_completed,
                "total_runs": row.total_runs,
                "notes": row.notes,
                "error": row.error,
                "conflicts_found": row.conflicts_found,
                "annotation": annotations.get(row.scenario_id, ""),
                "images": encoded_images,
                "phase_differences": getattr(row, 'phase_differences', []),
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
        sorted_similarity=sorted_similarity,
        detail_rows=detail_rows,
        version=version,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    return str(output)
