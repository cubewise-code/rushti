"""HTML report generator for RushTI contention-aware optimization.

Generates a self-contained, interactive HTML report with Chart.js
visualizations that explain the contention analysis: which parameter drives
duration variance, how outliers are detected via IQR statistics, the
predecessor-chain structure, and the max_workers recommendation.

The report follows the same design system as the performance dashboard
(``dashboard.py``): same CSS, colors, fonts, Chart.js charts, and card
layouts.
"""

import json
import logging
import math
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from rushti.contention_analyzer import ContentionAnalysisResult
from rushti.dashboard import _LOGO_SVG

logger = logging.getLogger(__name__)


def _format_duration(seconds: float) -> str:
    """Format seconds into a human-readable duration string.

    :param seconds: Duration in seconds
    :return: Formatted string (e.g., '5m 24s' or '45.2s')
    """
    if seconds >= 60:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}m {secs:.0f}s"
    return f"{seconds:.1f}s"


def _prepare_report_data(
    workflow: str,
    result: ContentionAnalysisResult,
) -> Dict[str, Any]:
    """Prepare all data for the optimization report template.

    Serializes the ``ContentionAnalysisResult`` and its nested dataclasses
    into a plain dict suitable for JSON embedding in the HTML template.

    :param workflow: Workflow identifier
    :param result: Contention analysis result
    :return: JSON-serializable dictionary
    """
    # Serialize groups
    all_groups = [
        {
            "driver_value": g.driver_value,
            "task_count": len(g.task_ids),
            "avg_duration": round(g.avg_duration, 2),
            "is_heavy": g.is_heavy,
        }
        for g in result.all_groups
    ]
    heavy_groups = [g for g in all_groups if g["is_heavy"]]
    light_groups = [g for g in all_groups if not g["is_heavy"]]

    # Serialize parameter analyses
    param_analyses = []
    for pa in result.parameter_analyses:
        param_analyses.append(
            {
                "key": pa.key,
                "distinct_values": pa.distinct_values,
                "range_seconds": round(pa.range_seconds, 2),
                "is_winner": pa.key == result.contention_driver,
                "group_averages": {k: round(v, 2) for k, v in pa.group_averages.items()},
            }
        )

    # Build chain sequences for visualization
    chains = _compute_chain_sequences(result)

    # Worker formula breakdown
    chain_slots = result.fan_out_size
    critical_path = result.critical_path_seconds if result.critical_path_seconds > 0 else 1.0

    light_total_work = 0.0
    for g in result.light_groups:
        light_total_work += g.avg_duration * len(g.task_ids)

    light_slots = math.ceil(light_total_work / critical_path) if critical_path > 0 else 0

    worker_breakdown = {
        "chain_slots": chain_slots,
        "critical_path": round(critical_path, 1),
        "light_total_work": round(light_total_work, 1),
        "light_slots": light_slots,
        "total": result.recommended_workers,
    }

    return {
        "workflow": workflow,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "contention_driver": result.contention_driver or "none",
        "fan_out_keys": result.fan_out_keys,
        "fan_out_size": result.fan_out_size,
        "total_tasks": result.total_tasks,
        "heavy_task_count": result.heavy_task_count,
        "light_task_count": result.light_task_count,
        "sensitivity": result.sensitivity,
        "chain_length": result.chain_length,
        "critical_path_seconds": round(result.critical_path_seconds, 1),
        "recommended_workers": result.recommended_workers,
        "iqr_stats": result.iqr_stats,
        "warnings": result.warnings,
        "all_groups": all_groups,
        "heavy_groups": heavy_groups,
        "light_groups": light_groups,
        "parameter_analyses": param_analyses,
        "chains": chains,
        "worker_breakdown": worker_breakdown,
        "predecessor_count": len(result.predecessor_map),
        "concurrency_ceiling": result.concurrency_ceiling,
        "ceiling_evidence": result.ceiling_evidence,
    }


def _compute_chain_sequences(
    result: ContentionAnalysisResult,
) -> List[Dict[str, Any]]:
    """Compute chain sequences from the analysis result for visualization.

    Reconstructs the chain order (heaviest-first) for each fan-out value.

    :param result: Contention analysis result
    :return: List of chain dicts with fan_out_value and sequence
    """
    if not result.heavy_groups or len(result.heavy_groups) < 2:
        return []

    if not result.predecessor_map:
        return []

    # The chain sequence is simply the heavy groups in order (heaviest first)
    heavy_order = [g.driver_value for g in result.heavy_groups]

    # Find all fan-out values from the predecessor map
    # Group predecessors by their fan-out chain to detect unique chains
    # Since we don't have direct access to fan-out values, we infer from
    # the structure: each chain has chain_length-1 predecessor entries
    # sharing the same fan-out dimension.
    # Simpler approach: just show the chain template repeated fan_out_size times
    chains = []
    for i in range(result.fan_out_size):
        fan_out_label = f"Chain {i + 1}"
        if result.fan_out_keys:
            fan_out_label = f"{', '.join(result.fan_out_keys)} #{i + 1}"
        chains.append(
            {
                "fan_out_value": fan_out_label,
                "sequence": heavy_order,
            }
        )

    return chains


def generate_optimization_report(
    workflow: str,
    result: ContentionAnalysisResult,
    output_path: str,
    open_browser: bool = True,
    dag_url: Optional[str] = None,
) -> str:
    """Generate a self-contained HTML optimization report.

    Creates an interactive HTML report with Chart.js visualizations that
    explain the contention analysis results. The report is self-contained
    (all data embedded as JSON, all styles inline).

    :param workflow: Workflow identifier
    :param result: Contention analysis result
    :param output_path: Output file path for the HTML report
    :param open_browser: Whether to open the report in the default browser
    :param dag_url: Relative URL to DAG visualization HTML (for cross-link)
    :return: Absolute path to the generated HTML file
    """
    data = _prepare_report_data(workflow, result)
    data_json = json.dumps(data, default=str)

    # Format values for static HTML sections
    critical_path_fmt = _format_duration(result.critical_path_seconds)

    # DAG link HTML (conditional)
    dag_link_html = ""
    if dag_url:
        dag_link_html = (
            f'<a href="{dag_url}" style="display:inline-flex;'
            f"align-items:center;gap:6px;padding:8px 16px;"
            f"background:#00AEEF;color:white;border-radius:8px;"
            f"font-size:0.85rem;font-weight:500;text-decoration:none;"
            f'transition:all 0.3s ease;" '
            f"onmouseover=\"this.style.boxShadow='0 4px 12px rgba(0,174,239,0.3)'\" "
            f"onmouseout=\"this.style.boxShadow='none'\">"
            f"View DAG &#8594;</a>"
        )

    # Warnings HTML (conditional)
    warnings_html = ""
    if result.warnings:
        warning_items = "".join(
            f'<div style="padding:8px 0;border-bottom:1px solid #FDE68A;">&#9888; {w}</div>'
            for w in result.warnings
        )
        warnings_html = f"""
        <div class="chart-panel" style="border-color:#F59E0B;background:#FFFBEB;margin-bottom:28px;">
            <h3 style="color:#D97706;">Warnings</h3>
            <div style="font-size:0.85rem;color:#92400E;">{warning_items}</div>
        </div>
        """

    # Concurrency ceiling HTML (conditional)
    ceiling_html = ""
    if result.concurrency_ceiling and result.ceiling_evidence:
        ev = result.ceiling_evidence
        confidence = ev.get("confidence", "unknown")

        if confidence in ("multi_run", "scale_up"):
            levels = ev.get("worker_levels", [])
            rows_html = ""
            for lvl in sorted(levels, key=lambda x: x["max_workers"]):
                is_best = lvl["max_workers"] == ev.get("best_level", {}).get("max_workers")
                row_style = "background:#F0FDF4;font-weight:600;" if is_best else ""
                best_badge = ' <span class="badge badge-info">best</span>' if is_best else ""
                rows_html += (
                    f'<tr style="{row_style}">'
                    f"<td>{lvl['max_workers']}{best_badge}</td>"
                    f"<td>{lvl['wall_clock']:.0f}s</td>"
                    f"<td>{lvl['avg_task_duration']:.1f}s</td>"
                    f"<td>{lvl['effective_parallelism']:.1f}</td>"
                    f"<td>{lvl['efficiency']:.1%}</td>"
                    f"</tr>"
                )
            if confidence == "scale_up":
                section_title = "Scale-Up Opportunity"
                section_color = "#059669"
                badge_color = "#059669"
                badge_text = "Scale-up recommended"
                tip_text = (
                    "Historical runs show that more workers produced faster wall clock times. "
                    "Workers were previously reduced too aggressively. "
                    "Increasing workers back to the best-observed level should improve performance."
                )
                footer_text = (
                    f"Recommended: <strong>{result.concurrency_ceiling}</strong> workers "
                    f"(best observed performance level)"
                )
            else:
                section_title = "Concurrency Ceiling Analysis"
                section_color = "#7C3AED"
                badge_color = "#8B5CF6"
                badge_text = "Multi-run comparison"
                tip_text = (
                    "When too many tasks run simultaneously, server-side resource contention "
                    "causes each task to take longer. The effective parallelism measures how much "
                    "of the worker capacity is actually productive. Lower efficiency means more "
                    'time is wasted on contention. Learn more: <a href="https://en.wikipedia.org/'
                    'wiki/Amdahl%%27s_law" target="_blank" style="color:#00AEEF;">Amdahl\'s Law</a>'
                )
                footer_text = (
                    f"Recommended ceiling: <strong>{result.concurrency_ceiling}</strong> workers "
                    f"(based on effective parallelism of best run)"
                )

            ceiling_html = f"""
        <div class="chart-panel section" style="border-color:{badge_color};">
            <h3 style="color:{section_color};">{section_title}
                <span class="help-icon">?<span class="help-tip">{tip_text}</span></span>
            </h3>
            <div style="margin-bottom:16px;">
                <span class="badge" style="background:{badge_color};color:white;">{badge_text}</span>
                <span style="font-size:0.85rem;color:#64748B;margin-left:8px;">
                    Improvement: {ev.get("wall_clock_improvement", 0):.0f}s
                    ({ev.get("wall_clock_improvement_pct", 0):.1f}%)
                </span>
            </div>
            <table class="details-table">
                <thead><tr>
                    <th>Max Workers</th><th>Wall Clock</th><th>Avg Task Duration</th>
                    <th>Effective Parallelism</th><th>Efficiency</th>
                </tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
            <div style="margin-top:12px;font-size:0.85rem;color:#64748B;">
                {footer_text}
            </div>
        </div>
            """
        elif confidence == "single_run":
            correlation = ev.get("correlation", 0)
            eff_par = ev.get("effective_parallelism", 0)
            max_w = ev.get("max_workers_used", 0)
            efficiency = ev.get("efficiency", 0)

            # Color code correlation strength
            r_color = (
                "#DC2626" if correlation > 0.9 else "#F59E0B" if correlation > 0.8 else "#10B981"
            )

            ceiling_html = f"""
        <div class="chart-panel section" style="border-color:#8B5CF6;">
            <h3 style="color:#7C3AED;">Concurrency Ceiling Analysis
                <span class="help-icon">?<span class="help-tip">When too many tasks run simultaneously, server-side resource contention causes each task to take longer. A high correlation between concurrent task count and task duration indicates the server is overwhelmed. The effective parallelism shows how much of the worker capacity is actually productive. Learn more: <a href="https://en.wikipedia.org/wiki/Amdahl%27s_law" target="_blank" style="color:#00AEEF;">Amdahl's Law</a></span></span>
            </h3>
            <div style="margin-bottom:16px;">
                <span class="badge" style="background:#8B5CF6;color:white;">Single-run inference</span>
                <span style="font-size:0.85rem;color:#64748B;margin-left:8px;">
                    Run another pass at reduced workers to confirm
                </span>
            </div>
            <div class="summary-cards" style="margin-bottom:0;">
                <div class="card" style="border-color:{r_color};">
                    <div class="card-label">Correlation</div>
                    <div class="card-value" style="color:{r_color};">{correlation:.2f}</div>
                    <div class="card-sub">Concurrency &#8596; Duration (<a href="https://en.wikipedia.org/wiki/Pearson_correlation_coefficient" target="_blank" style="color:#00AEEF;">Pearson r</a>)</div>
                </div>
                <div class="card">
                    <div class="card-label">Effective Parallelism</div>
                    <div class="card-value">{eff_par:.1f} / {max_w}</div>
                    <div class="card-sub">Only {efficiency:.0%} of workers productive</div>
                </div>
                <div class="card best">
                    <div class="card-label">Recommended Ceiling</div>
                    <div class="card-value">{result.concurrency_ceiling}</div>
                    <div class="card-sub">workers (rounded to nearest 5)</div>
                </div>
            </div>
        </div>
            """

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RushTI Optimization Report - {workflow}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #F8FAFC;
            color: #1E293B;
            min-height: 100vh;
        }}
        .dashboard {{ max-width: 1440px; margin: 0 auto; padding: 24px; }}

        /* Header */
        .header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 20px 28px;
            margin-bottom: 24px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        }}
        .header-left {{ display: flex; align-items: center; gap: 16px; }}
        .header-left svg {{ height: 32px; width: auto; }}
        .header-title {{ font-size: 1.5rem; font-weight: 700; color: #1E293B; }}
        .header-subtitle {{ font-size: 0.85rem; color: #64748B; margin-top: 2px; }}
        .header-right {{ display: flex; align-items: center; gap: 16px; }}
        .generated-at {{ font-size: 0.75rem; color: #94A3B8; }}

        /* Summary cards */
        .summary-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
            margin-bottom: 28px;
        }}
        .card {{
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 18px 20px;
            transition: box-shadow 0.2s;
        }}
        .card:hover {{ box-shadow: 0 4px 12px rgba(0,133,202,0.08); }}
        .card-label {{
            font-size: 0.75rem; font-weight: 600; text-transform: uppercase;
            letter-spacing: 0.5px; color: #64748B; margin-bottom: 6px;
        }}
        .card-value {{
            font-size: 1.75rem; font-weight: 700; color: #1E293B;
        }}
        .card-sub {{ font-size: 0.75rem; color: #94A3B8; margin-top: 4px; }}
        .card.best {{ border-color: #10B981; background: #F0FDF4; }}
        .card.best .card-value {{ color: #059669; }}

        /* Charts grid */
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(480px, 1fr));
            gap: 20px;
            margin-bottom: 28px;
        }}
        .chart-panel {{
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .chart-panel h3 {{
            font-size: 0.95rem; font-weight: 600; color: #1E293B;
            margin-bottom: 12px;
        }}
        .chart-wrapper {{ position: relative; height: 300px; }}
        .chart-panel.full-width {{ grid-column: 1 / -1; }}

        /* Tables */
        .table-panel {{
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            overflow-x: auto;
        }}
        .table-panel h3 {{
            font-size: 0.95rem; font-weight: 600; color: #1E293B;
            margin-bottom: 12px;
        }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
        th {{
            text-align: left; padding: 10px 12px;
            border-bottom: 2px solid #E2E8F0;
            color: #64748B; font-weight: 600; font-size: 0.75rem;
            text-transform: uppercase; letter-spacing: 0.3px;
            white-space: nowrap;
        }}
        td {{
            padding: 8px 12px; border-bottom: 1px solid #F1F5F9;
            color: #1E293B;
        }}
        tr:hover td {{ background: #F8FAFC; }}
        .mono {{ font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.8rem; }}

        /* Config grid */
        .config-grid {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
        }}
        .config-item {{
            background: #F8FAFC; border-radius: 8px; padding: 12px 16px;
        }}
        .config-item .config-label {{ font-size: 0.75rem; color: #64748B; font-weight: 500; }}
        .config-item .config-value {{ font-size: 0.95rem; font-weight: 600; color: #1E293B; margin-top: 2px; }}

        /* Badges */
        .badge {{
            display: inline-block; padding: 2px 8px;
            border-radius: 4px; font-size: 0.7rem; font-weight: 600;
        }}
        .badge-success {{ background: #D1FAE5; color: #059669; }}
        .badge-fail {{ background: #FEE2E2; color: #DC2626; }}
        .badge-info {{ background: #DBEAFE; color: #2563EB; }}
        .badge-heavy {{ background: #FEE2E2; color: #DC2626; }}
        .badge-light {{ background: #F1F5F9; color: #64748B; }}

        /* Help tooltip */
        .help-icon {{
            display: inline-flex; align-items: center; justify-content: center;
            width: 18px; height: 18px; border-radius: 50%;
            background: #E2E8F0; color: #64748B; font-size: 0.7rem;
            font-weight: 700; cursor: help; margin-left: 6px;
            position: relative; vertical-align: middle;
            flex-shrink: 0;
        }}
        .help-icon:hover {{ background: #00AEEF; color: #FFF; }}
        .help-icon .help-tip {{
            display: none; position: absolute; top: 24px; left: 50%;
            transform: translateX(-50%); background: #1E293B; color: #F1F5F9;
            padding: 10px 14px; border-radius: 8px; font-size: 0.78rem;
            font-weight: 400; line-height: 1.5; width: 320px;
            z-index: 100; box-shadow: 0 4px 16px rgba(0,0,0,0.15);
            pointer-events: none; white-space: normal;
        }}
        .help-icon:hover .help-tip {{ display: block; }}

        /* Chain visualization */
        .chain-row {{
            display: flex; align-items: center; gap: 8px;
            margin-bottom: 12px; flex-wrap: wrap;
        }}
        .chain-label {{
            font-size: 0.78rem; font-weight: 600; color: #64748B;
            min-width: 100px; flex-shrink: 0;
        }}
        .chain-node {{
            display: inline-flex; flex-direction: column;
            align-items: center; justify-content: center;
            padding: 10px 16px; border-radius: 8px;
            font-size: 0.82rem; font-weight: 600; text-align: center;
            min-width: 80px;
        }}
        .chain-node.heavy {{
            background: #FEE2E2; color: #DC2626; border: 2px solid #FECACA;
        }}
        .chain-node small {{
            font-size: 0.7rem; font-weight: 400; margin-top: 2px; opacity: 0.8;
        }}
        .chain-arrow {{
            font-size: 1.2rem; color: #94A3B8; font-weight: 700;
        }}
        .chain-more {{
            font-size: 0.82rem; color: #94A3B8; font-style: italic;
            padding: 8px 0;
        }}

        /* Formula display */
        .formula-panel {{
            display: flex; align-items: center; gap: 12px;
            flex-wrap: wrap; padding: 16px 0;
        }}
        .formula-part {{
            display: flex; flex-direction: column; align-items: center;
            background: #F8FAFC; border-radius: 8px; padding: 12px 20px;
        }}
        .formula-part .formula-value {{
            font-size: 1.5rem; font-weight: 700; color: #1E293B;
        }}
        .formula-part .formula-label {{
            font-size: 0.7rem; color: #64748B; margin-top: 4px; text-align: center;
        }}
        .formula-part.result {{
            background: #F0FDF4; border: 2px solid #10B981;
        }}
        .formula-part.result .formula-value {{ color: #059669; }}
        .formula-operator {{
            font-size: 1.5rem; font-weight: 700; color: #94A3B8;
        }}

        /* Educational links */
        .edu-links {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 12px;
        }}
        .edu-link {{
            display: flex; align-items: flex-start; gap: 12px;
            background: #F8FAFC; border-radius: 8px; padding: 14px 16px;
            text-decoration: none; color: #1E293B;
            transition: box-shadow 0.2s, border-color 0.2s;
            border: 1px solid #E2E8F0;
        }}
        .edu-link:hover {{
            box-shadow: 0 4px 12px rgba(0,133,202,0.08);
            border-color: #00AEEF;
        }}
        .edu-link-icon {{
            font-size: 1.2rem; flex-shrink: 0; margin-top: 2px;
        }}
        .edu-link-title {{
            font-size: 0.85rem; font-weight: 600; color: #00AEEF;
        }}
        .edu-link-desc {{
            font-size: 0.78rem; color: #64748B; margin-top: 2px;
        }}

        /* Footer */
        .footer {{
            text-align: center; padding: 20px;
            font-size: 0.75rem; color: #94A3B8;
        }}
        .footer a {{ color: #00AEEF; text-decoration: none; }}

        /* Section spacer */
        .section {{ margin-bottom: 28px; }}
    </style>
</head>
<body>
    <div class="dashboard">
        <!-- Header -->
        <div class="header">
            <div class="header-left">
                {_LOGO_SVG}
                <div>
                    <div class="header-title">Optimization Report</div>
                    <div class="header-subtitle">Workflow: {workflow}</div>
                </div>
            </div>
            <div class="header-right">
                {dag_link_html}
                <div class="generated-at">Generated: {data["generated_at"]}</div>
            </div>
        </div>

        <!-- Summary cards -->
        <div class="summary-cards">
            <div class="card">
                <div class="card-label">Total Tasks</div>
                <div class="card-value">{result.total_tasks}</div>
                <div class="card-sub">{len(result.all_groups)} groups</div>
            </div>
            <div class="card">
                <div class="card-label">Contention Driver</div>
                <div class="card-value" style="font-size:1.25rem;">
                    <span class="badge badge-info">{result.contention_driver or "none"}</span>
                </div>
                <div class="card-sub">Parameter with highest duration variance</div>
            </div>
            <div class="card" style="border-color:#DC2626;background:#FEF2F2;">
                <div class="card-label">Heavy Groups</div>
                <div class="card-value" style="color:#DC2626;">{len(result.heavy_groups)}</div>
                <div class="card-sub">{result.heavy_task_count} tasks (outliers)</div>
            </div>
            <div class="card">
                <div class="card-label">Light Groups</div>
                <div class="card-value">{len(result.light_groups)}</div>
                <div class="card-sub">{result.light_task_count} tasks</div>
            </div>
            <div class="card">
                <div class="card-label">Chain Length</div>
                <div class="card-value">{result.chain_length}</div>
                <div class="card-sub">{result.fan_out_size} independent chain{"s" if result.fan_out_size != 1 else ""}</div>
            </div>
            <div class="card best">
                <div class="card-label">Recommended Workers</div>
                <div class="card-value">{result.recommended_workers}</div>
                <div class="card-sub">Embedded in optimized taskfile</div>
            </div>
            <div class="card">
                <div class="card-label">Critical Path</div>
                <div class="card-value" style="font-size:1.25rem;">{critical_path_fmt}</div>
                <div class="card-sub">Sum of heavy group durations</div>
            </div>
        </div>

        {warnings_html}

        <!-- Disclaimer -->
        <div class="chart-panel" style="border-color:#F59E0B;background:#FFFBEB;margin-bottom:28px;">
            <h3 style="color:#D97706;">&#9888; Important</h3>
            <div style="font-size:0.85rem;color:#92400E;line-height:1.6;">
                Contention-aware optimization is based on statistical analysis of historical execution data.
                The recommendations follow theoretical reasoning (IQR outlier detection, DAG scheduling,
                concurrency analysis) but actual TM1 server behavior depends on factors beyond task ordering &mdash;
                server load, memory pressure, concurrent users, and data volumes. There is no one-size-fits-all
                solution. Test the optimized taskfile in a non-production environment before deploying.
            </div>
        </div>

        {ceiling_html}

        <!-- Parameter Variance Analysis -->
        <div class="chart-panel section">
            <h3>Parameter Variance Analysis
                <span class="help-icon">?<span class="help-tip">This chart shows how much each varying parameter influences task duration. The parameter with the largest range (difference between the slowest and fastest group averages) is selected as the contention driver. A clear winner means one parameter dominates duration variance. Learn more: <a href="https://en.wikipedia.org/wiki/Interquartile_range" target="_blank" style="color:#00AEEF;">IQR</a></span></span>
            </h3>
            <div class="chart-wrapper"><canvas id="paramVarianceChart"></canvas></div>
        </div>

        <!-- Group Duration Distribution -->
        <div class="chart-panel section">
            <h3>Group Duration Distribution
                <span class="help-icon">?<span class="help-tip">Each bar represents a group of tasks that share the same contention-driver value. Red bars are heavy outliers (above the IQR upper fence, shown as the gold dashed line). These heavy groups will be chained to prevent concurrent execution. The IQR method detects outliers using the spread of the middle 50%% of data. Learn more: <a href="https://en.wikipedia.org/wiki/Box_plot" target="_blank" style="color:#00AEEF;">Box Plot</a></span></span>
            </h3>
            <div class="chart-wrapper"><canvas id="groupDistChart"></canvas></div>
        </div>

        <!-- IQR Statistics Panel -->
        <div class="chart-panel section">
            <h3>IQR Outlier Detection
                <span class="help-icon">?<span class="help-tip">The Interquartile Range (IQR) measures the spread of the middle 50%% of group durations. The upper fence is calculated as Q3 + k &times; IQR, where k is the sensitivity multiplier. Groups above this fence are classified as heavy outliers. A higher sensitivity value means fewer outliers are detected (more conservative). Learn more: <a href="https://en.wikipedia.org/wiki/Interquartile_range" target="_blank" style="color:#00AEEF;">IQR on Wikipedia</a></span></span>
            </h3>
            <div style="background:#F1F5F9;border-radius:8px;padding:16px 20px;margin-bottom:16px;font-family:'SF Mono','Fira Code',monospace;font-size:0.9rem;color:#1E293B;">
                Upper Fence = Q3 + k &times; IQR = {result.iqr_stats.get("q3", 0):.1f} + {result.sensitivity} &times; {result.iqr_stats.get("iqr", 0):.1f} = <strong>{result.iqr_stats.get("upper_fence", 0):.1f}s</strong>
            </div>
            <div class="config-grid">
                <div class="config-item">
                    <div class="config-label">Q1 (25th percentile)</div>
                    <div class="config-value">{result.iqr_stats.get("q1", 0):.1f}s</div>
                </div>
                <div class="config-item">
                    <div class="config-label">Q3 (75th percentile)</div>
                    <div class="config-value">{result.iqr_stats.get("q3", 0):.1f}s</div>
                </div>
                <div class="config-item">
                    <div class="config-label">IQR (Q3 &minus; Q1)</div>
                    <div class="config-value">{result.iqr_stats.get("iqr", 0):.1f}s</div>
                </div>
                <div class="config-item">
                    <div class="config-label">Upper Fence</div>
                    <div class="config-value" style="color:#DC2626;">{result.iqr_stats.get("upper_fence", 0):.1f}s</div>
                </div>
                <div class="config-item">
                    <div class="config-label">Sensitivity (k)</div>
                    <div class="config-value">{result.sensitivity}</div>
                </div>
                <div class="config-item">
                    <div class="config-label">Groups Above Fence</div>
                    <div class="config-value" style="color:#DC2626;">{len(result.heavy_groups)}</div>
                </div>
            </div>
        </div>

        <!-- Chain Structure -->
        <div class="chart-panel section">
            <h3>Predecessor Chain Structure
                <span class="help-icon">?<span class="help-tip">Each row shows one independent chain. Within a chain, heavy groups run sequentially (heaviest first) to prevent resource contention. Light groups have no predecessors and run freely. The number of chains equals the fan-out size (the number of unique values in the non-driver parameters). Learn more: <a href="https://en.wikipedia.org/wiki/Directed_acyclic_graph" target="_blank" style="color:#00AEEF;">DAG Scheduling</a></span></span>
            </h3>
            <div id="chainContainer"></div>
            <div style="font-size:0.82rem;color:#64748B;margin-top:12px;">
                Tasks with predecessors: <strong>{len(result.predecessor_map)}</strong> &nbsp;|&nbsp;
                Fan-out: <strong>{", ".join(result.fan_out_keys) or "none"}</strong> &nbsp;|&nbsp;
                Fan-out size: <strong>{result.fan_out_size}</strong>
            </div>
        </div>

        <!-- Worker Recommendation -->
        <div class="chart-panel section">
            <h3>Worker Recommendation
                <span class="help-icon">?<span class="help-tip">The recommended max_workers ensures enough workers for all chains (one heavy task per chain at a time) plus enough extra workers for light tasks to finish within the chain window. Going higher wastes resources (chains are the bottleneck); going lower starves light-task throughput.</span></span>
            </h3>
            <div class="formula-panel" id="formulaPanel"></div>
        </div>

        <!-- Group Details Table -->
        <div class="table-panel section">
            <h3>All Groups by {result.contention_driver or "Driver"} Value</h3>
            <table>
                <thead>
                    <tr>
                        <th>Group</th>
                        <th>Type</th>
                        <th>Avg Duration</th>
                        <th>Tasks</th>
                    </tr>
                </thead>
                <tbody id="groupTableBody"></tbody>
            </table>
        </div>

        <!-- Educational Links -->
        <div class="chart-panel section">
            <h3>Learn More</h3>
            <p style="font-size:0.82rem;color:#64748B;margin-bottom:16px;">Statistical concepts used in this optimization analysis:</p>
            <div class="edu-links">
                <a href="https://en.wikipedia.org/wiki/Exponential_smoothing" target="_blank" class="edu-link">
                    <span class="edu-link-icon">&#128200;</span>
                    <div>
                        <div class="edu-link-title">Exponential Smoothing (EWMA)</div>
                        <div class="edu-link-desc">The method used to estimate task durations from historical runs, giving more weight to recent observations.</div>
                    </div>
                </a>
                <a href="https://en.wikipedia.org/wiki/Interquartile_range" target="_blank" class="edu-link">
                    <span class="edu-link-icon">&#128202;</span>
                    <div>
                        <div class="edu-link-title">Interquartile Range (IQR)</div>
                        <div class="edu-link-desc">A measure of statistical dispersion used to identify outlier groups whose duration significantly exceeds the norm.</div>
                    </div>
                </a>
                <a href="https://en.wikipedia.org/wiki/Outlier#Detection" target="_blank" class="edu-link">
                    <span class="edu-link-icon">&#128270;</span>
                    <div>
                        <div class="edu-link-title">Outlier Detection</div>
                        <div class="edu-link-desc">Techniques for identifying data points that differ significantly from the majority, applied here to find heavy task groups.</div>
                    </div>
                </a>
                <a href="https://en.wikipedia.org/wiki/Box_plot" target="_blank" class="edu-link">
                    <span class="edu-link-icon">&#128230;</span>
                    <div>
                        <div class="edu-link-title">Box Plot</div>
                        <div class="edu-link-desc">A visual representation of the IQR showing the distribution of group durations and the outlier fence.</div>
                    </div>
                </a>
                <a href="https://en.wikipedia.org/wiki/Directed_acyclic_graph" target="_blank" class="edu-link">
                    <span class="edu-link-icon">&#128736;</span>
                    <div>
                        <div class="edu-link-title">Directed Acyclic Graph (DAG)</div>
                        <div class="edu-link-desc">The scheduling model used by RushTI where tasks form a dependency graph with predecessor chains.</div>
                    </div>
                </a>
            </div>
        </div>

        <!-- Footer -->
        <div class="footer">
            Generated by <a href="https://github.com/cubewise-code/rushti" target="_blank">RushTI</a> Contention-Aware Optimizer
        </div>
    </div>

    <script>
        const DATA = {data_json};

        // --- Parameter Variance Chart ---
        (function() {{
            const ctx = document.getElementById('paramVarianceChart').getContext('2d');
            const analyses = DATA.parameter_analyses;
            if (!analyses || analyses.length === 0) return;

            const labels = analyses.map(a => a.key);
            const values = analyses.map(a => a.range_seconds);
            const colors = analyses.map(a => a.is_winner ? '#00AEEF' : '#CBD5E1');

            new Chart(ctx, {{
                type: 'bar',
                data: {{
                    labels: labels,
                    datasets: [{{
                        label: 'Duration Range (seconds)',
                        data: values,
                        backgroundColor: colors,
                        borderColor: colors,
                        borderWidth: 1,
                        borderRadius: 4,
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(ctx) {{
                                    const a = analyses[ctx.dataIndex];
                                    return [
                                        'Range: ' + ctx.parsed.y.toFixed(1) + 's',
                                        'Distinct values: ' + a.distinct_values,
                                        a.is_winner ? '★ Selected as contention driver' : ''
                                    ].filter(Boolean);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            title: {{ display: true, text: 'Duration Range (seconds)', font: {{ size: 12 }} }},
                            beginAtZero: true,
                            grid: {{ color: '#F1F5F9' }},
                        }},
                        x: {{
                            title: {{ display: true, text: 'Parameter', font: {{ size: 12 }} }},
                            grid: {{ display: false }},
                        }}
                    }}
                }}
            }});
        }})();

        // --- Group Duration Distribution Chart ---
        (function() {{
            const ctx = document.getElementById('groupDistChart').getContext('2d');
            const groups = DATA.all_groups;
            if (!groups || groups.length === 0) return;

            const labels = groups.map(g => g.driver_value);
            const values = groups.map(g => g.avg_duration);
            const colors = groups.map(g => g.is_heavy ? '#DC2626' : '#CBD5E1');

            // Custom plugin: draw upper fence line
            const upperFencePlugin = {{
                id: 'upperFenceLine',
                afterDraw(chart) {{
                    const fence = DATA.iqr_stats.upper_fence;
                    if (!fence || fence <= 0) return;
                    const yAxis = chart.scales.y;
                    const y = yAxis.getPixelForValue(fence);
                    if (y < chart.chartArea.top || y > chart.chartArea.bottom) return;
                    const ctx2 = chart.ctx;
                    ctx2.save();
                    ctx2.setLineDash([6, 4]);
                    ctx2.strokeStyle = '#FBB040';
                    ctx2.lineWidth = 2;
                    ctx2.beginPath();
                    ctx2.moveTo(chart.chartArea.left, y);
                    ctx2.lineTo(chart.chartArea.right, y);
                    ctx2.stroke();
                    // Label
                    ctx2.setLineDash([]);
                    ctx2.fillStyle = '#FBB040';
                    ctx2.font = '600 11px Inter, sans-serif';
                    ctx2.textAlign = 'right';
                    ctx2.fillText('Upper Fence: ' + fence.toFixed(1) + 's', chart.chartArea.right - 8, y - 6);
                    ctx2.restore();
                }}
            }};

            new Chart(ctx, {{
                type: 'bar',
                data: {{
                    labels: labels,
                    datasets: [{{
                        label: 'Avg Duration (seconds)',
                        data: values,
                        backgroundColor: colors,
                        borderColor: colors.map(c => c === '#DC2626' ? '#B91C1C' : '#94A3B8'),
                        borderWidth: 1,
                        borderRadius: 4,
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(ctx) {{
                                    const g = groups[ctx.dataIndex];
                                    return [
                                        'Avg: ' + ctx.parsed.y.toFixed(1) + 's',
                                        'Tasks: ' + g.task_count,
                                        g.is_heavy ? '⚠ Heavy outlier (above fence)' : 'Light group'
                                    ];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            title: {{ display: true, text: 'Avg Duration (seconds)', font: {{ size: 12 }} }},
                            beginAtZero: true,
                            grid: {{ color: '#F1F5F9' }},
                        }},
                        x: {{
                            title: {{ display: true, text: DATA.contention_driver + ' value', font: {{ size: 12 }} }},
                            grid: {{ display: false }},
                            ticks: {{
                                maxRotation: groups.length > 20 ? 90 : 45,
                                autoSkip: groups.length > 40,
                            }}
                        }}
                    }}
                }},
                plugins: [upperFencePlugin]
            }});
        }})();

        // --- Chain Visualization ---
        (function() {{
            const container = document.getElementById('chainContainer');
            const chains = DATA.chains;

            if (!chains || chains.length === 0) {{
                container.innerHTML = '<div style="color:#94A3B8;font-size:0.85rem;padding:16px 0;">No predecessor chains generated (fewer than 2 heavy groups detected).</div>';
                return;
            }}

            const limit = Math.min(chains.length, 6);
            const allGroups = DATA.all_groups;

            chains.slice(0, limit).forEach(function(chain) {{
                const row = document.createElement('div');
                row.className = 'chain-row';

                const label = document.createElement('span');
                label.className = 'chain-label';
                label.textContent = chain.fan_out_value;
                row.appendChild(label);

                chain.sequence.forEach(function(val, i) {{
                    const group = allGroups.find(function(g) {{ return g.driver_value === val; }});
                    const node = document.createElement('div');
                    node.className = 'chain-node heavy';
                    node.innerHTML = val + '<small>' + (group ? group.avg_duration.toFixed(1) + 's' : '') + '</small>';
                    row.appendChild(node);

                    if (i < chain.sequence.length - 1) {{
                        const arrow = document.createElement('span');
                        arrow.className = 'chain-arrow';
                        arrow.innerHTML = '&#8594;';
                        row.appendChild(arrow);
                    }}
                }});

                container.appendChild(row);
            }});

            if (chains.length > limit) {{
                const more = document.createElement('div');
                more.className = 'chain-more';
                more.textContent = '... and ' + (chains.length - limit) + ' more chains (same structure)';
                container.appendChild(more);
            }}
        }})();

        // --- Worker Formula ---
        (function() {{
            const panel = document.getElementById('formulaPanel');
            const wb = DATA.worker_breakdown;
            if (!wb) return;

            panel.innerHTML = '' +
                '<div class="formula-part">' +
                    '<div class="formula-value">' + wb.chain_slots + '</div>' +
                    '<div class="formula-label">Chain slots<br>(fan-out size)</div>' +
                '</div>' +
                '<div class="formula-operator">+</div>' +
                '<div class="formula-part">' +
                    '<div class="formula-value">' + wb.light_slots + '</div>' +
                    '<div class="formula-label">Light slots<br>(' + wb.light_total_work.toFixed(0) + 's / ' + wb.critical_path.toFixed(0) + 's)</div>' +
                '</div>' +
                '<div class="formula-operator">=</div>' +
                '<div class="formula-part result">' +
                    '<div class="formula-value">' + wb.total + '</div>' +
                    '<div class="formula-label">Recommended<br>max_workers</div>' +
                '</div>';
        }})();

        // --- Group Details Table ---
        (function() {{
            const tbody = document.getElementById('groupTableBody');
            const groups = DATA.all_groups;
            if (!groups || groups.length === 0) return;

            groups.forEach(function(g) {{
                const tr = document.createElement('tr');
                const typeClass = g.is_heavy ? 'badge-heavy' : 'badge-light';
                const typeLabel = g.is_heavy ? 'Heavy' : 'Light';
                tr.innerHTML = '' +
                    '<td class="mono">' + g.driver_value + '</td>' +
                    '<td><span class="badge ' + typeClass + '">' + typeLabel + '</span></td>' +
                    '<td>' + g.avg_duration.toFixed(1) + 's</td>' +
                    '<td>' + g.task_count + '</td>';
                tbody.appendChild(tr);
            }});
        }})();
    </script>
</body>
</html>"""

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html_content, encoding="utf-8")

    logger.info(f"Optimization report written to: {output}")

    if open_browser:
        try:
            import os

            abs_path = os.path.abspath(str(output))
            webbrowser.open(f"file://{abs_path}")
        except Exception:
            pass

    return str(output)
