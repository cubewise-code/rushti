"""HTML dashboard generator for RushTI execution statistics.

Generates a self-contained, interactive HTML dashboard with Chart.js
visualizations for analyzing taskfile execution history. The dashboard
embeds all data as JSON and renders charts/tables client-side, with an
in-page run selector for dynamic filtering.
"""

import json
import logging
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# RushTI blue logo SVG (inline)
_LOGO_SVG = (
    '<svg viewBox="0 0 809.52 235.88" xmlns="http://www.w3.org/2000/svg" '
    'style="height:32px;width:auto;">'
    '<path fill="#6eabde" d="M215.98,0H19.9C8.91,0,0,8.91,0,19.9v196.08c0,'
    "10.99,8.91,19.9,19.9,19.9h196.08c10.99,0,19.9-8.91,19.9-19.9V19.9c0-"
    "10.99-8.91-19.9-19.9-19.9ZM151.18,41.46c10.98,0,19.87,8.9,19.87,19.87s"
    "-8.9,19.87-19.87,19.87c-1.13,0-2.23-.12-3.31-.3,9.4-1.58,16.56-9.73,"
    "16.56-19.58s-7.17-18-16.56-19.58c1.08-.18,2.18-.3,3.31-.3ZM178.35,"
    "107.92l-21.44-.21-12.46-20.77c1.63-.41,3.24-.99,4.81-1.73l.6-.28,13.67,"
    "22.78,21.44.21c3.35.03,6.05,2.76,6.05,6.11v7.81c0,3.4-2.78,6.14-6.18,"
    "6.11l-5.73-.06c2.98-.41,5.28-2.95,5.28-6.04v-7.81c0-3.35-2.7-6.07-6.05"
    "-6.11ZM138.02,41.46c10.98,0,19.87,8.9,19.87,19.87s-8.9,19.87-19.87,"
    "19.87c-1.13,0-2.23-.12-3.31-.3,9.4-1.58,16.56-9.73,16.56-19.58s-7.17"
    "-18-16.56-19.58c1.08-.18,2.18-.3,3.31-.3ZM124.83,41.46c10.98,0,19.87,"
    "8.9,19.87,19.87s-8.9,19.87-19.87,19.87c-1.13,0-2.23-.12-3.31-.3,9.4-"
    "1.58,16.56-9.73,16.56-19.58s-7.17-18-16.56-19.58c1.08-.18,2.18-.3,"
    "3.31-.3ZM111.19,41.46c10.98,0,19.87,8.9,19.87,19.87s-8.9,19.87-19.87,"
    "19.87-19.87-8.9-19.87-19.87,8.9-19.87,19.87-19.87ZM190.19,201.74h0c-"
    "2.75,2.1-6.26,2.53-9.31,1.45.94-.34,1.85-.81,2.69-1.45,4.45-3.39,5.21"
    "-9.8,1.67-14.14l-47.76-58.62.42-1.26-6.51-.07-.44,1.33,47.77,58.62c3.54"
    ",4.34,2.78,10.75-1.67,14.14h0c-2.75,2.1-6.26,2.53-9.31,1.45.94-.34,"
    "1.85-.81,2.69-1.45,4.45-3.39,5.21-9.8,1.67-14.14l-47.76-58.62.7-2.1-"
    "4.23-7.04-3.05,9.14,47.77,58.62c3.54,4.34,2.78,10.75-1.67,14.14h0c-"
    "2.75,2.1-6.26,2.53-9.31,1.45.94-.34,1.85-.81,2.69-1.45,4.45-3.39,5.21"
    "-9.8,1.67-14.14l-47.76-58.62,5.41-16.24-4.51-7.51-7.92,23.75,47.76,"
    "58.62c3.54,4.34,2.78,10.75-1.67,14.14h0c-4.24,3.23-10.28,2.53-13.66-"
    "1.59l-16.24-19.75c-.67.11-1.35.18-2.05.18h-6.62c2.62,0,5.05-.78,7.09-"
    "2.1l-2.89-3.52c-2.36,3.39-6.28,5.62-10.73,5.62h-6.62c6.53,0,11.92-4.79"
    ",12.9-11.04l-6.4-7.78v5.74c0,7.22-5.85,13.07-13.07,13.07h-6.62c7.22,0,"
    "13.07-5.85,13.07-13.07v-13.8l-7.01-8.53v22.32c0,7.22-5.85,13.07-13.07,"
    "13.07h-39.69c-3.79,0-6.86-3.07-6.86-6.86v-6.16c0-3.79,3.07-6.86,6.86-"
    "6.86h26.26v-33.12l12.42-33.12h-12.59l-11.99,26.62c-1.71,3.8-6.22,5.44-"
    "9.97,3.63l-4.5-2.17c-3.61-1.74-5.19-6.04-3.55-9.7l17.1-38.26h36.65c"
    "6.85,11.86,21.66,16.48,34.04,10.63l.6-.28,13.67,22.78,10.25.1-.04-.07-"
    "3.19-.03-12.46-20.77c1.63-.41,3.24-.99,4.81-1.73l.6-.28,13.67,22.78,"
    "21.44.21c3.35.03,6.05,2.76,6.05,6.11v7.81c0,3.4-2.78,6.14-6.18,6.11l"
    "-5.73-.06c2.98-.41,5.28-2.95,5.28-6.04v-7.81c0-3.35-2.7-6.07-6.05-6.11"
    "l-18.25-.18.04.07,11.2.11c3.35.03,6.05,2.76,6.05,6.11v7.81c0,3.4-2.78,"
    "6.14-6.18,6.11l-13.52-.15-.4,1.19,47.77,58.62c3.54,4.34,2.78,10.75-"
    "1.67,14.14ZM204.18,121.83c0,3.4-2.78,6.14-6.18,6.11l-5.73-.06c2.98-"
    ".41,5.28-2.95,5.28-6.04v-7.81c0-3.35-2.7-6.07-6.05-6.11l-21.44-.21-"
    "12.46-20.77c1.63-.41,3.24-.99,4.81-1.73l.6-.28,13.67,22.78,21.44.21c"
    '3.35.03,6.05,2.76,6.05,6.11v7.81Z"/>'
    '<path fill="#6eabde" d="M363.22,177.89l-30-53.01v53.01h-33.29V40.83h44.38'
    "c17.94,0,31.37,3.43,40.27,10.27,9.86,7.54,14.79,19.11,14.79,34.73,0,"
    "20.82-8.84,34.73-26.51,41.71l30.21,50.34h-39.86ZM333.22,68.16v38.01h"
    "10.69c6.85,0,12.19-1.64,16.03-4.93,3.83-3.29,5.75-7.94,5.75-13.97s-"
    '1.82-10.38-5.45-13.87c-3.63-3.49-8.39-5.24-14.28-5.24h-12.74Z"/>'
    '<path fill="#6eabde" d="M467.61,142.54v-59.8h31.23v59.8c0,13.43-3.77,'
    "23.22-11.3,29.38-8.08,6.44-18.9,9.66-32.47,9.66-14.79,0-26.1-3.49-"
    "33.9-10.48-6.99-5.89-10.48-15.41-10.48-28.56v-59.8h31.23v57.54c0,10.27"
    ",4.38,15.41,13.15,15.41,3.97,0,7.05-1.16,9.25-3.49,2.19-2.33,3.29-5.55"
    ',3.29-9.66Z"/>'
    '<path fill="#6eabde" d="M580.83,110.9h-28.97v-1.85c0-2.19-.86-4.01-2.57-'
    "5.45-1.71-1.44-3.87-2.16-6.47-2.16-3.56-.13-5.89,1.3-6.99,4.31-1.37,"
    "3.56.13,6.44,4.52,8.63,1.64.82,3.63,1.44,5.96,1.85,13.29,3.84,22.67,"
    "8.22,28.15,13.15,5.48,4.93,8.22,11.58,8.22,19.93,0,9.73-3.53,17.54-"
    "10.58,23.43-7.06,5.89-16.41,8.83-28.05,8.83s-20.89-2.84-27.74-8.53c-"
    "6.85-5.68-10.41-13.53-10.68-23.53h29.59c.68,3.56,1.68,6.06,2.98,7.5,"
    "1.3,1.44,3.25,2.16,5.86,2.16,2.05,0,3.77-.65,5.14-1.95,1.37-1.3,2.05-"
    "2.91,2.05-4.83,0-2.19-1.06-4.01-3.19-5.45-2.12-1.44-6.34-3.19-12.64-"
    "5.24-10.41-3.29-17.98-7.43-22.71-12.43-4.73-5-7.09-11.27-7.09-18.8,0-"
    "9.45,3.52-17.06,10.58-22.81,7.05-5.75,16.34-8.63,27.84-8.63s20.31,2.81"
    ',26.82,8.42c6.5,5.62,9.83,13.43,9.97,23.43Z"/>'
    '<path fill="#6eabde" d="M591.31,40.83h31.23v50.75c7.12-8.35,15.34-12.54,'
    "24.66-12.54s16.78,2.95,23.22,8.84c6.03,5.48,9.04,14.73,9.04,27.74v62.26"
    "h-31.23v-57.12c0-4.79-1.13-8.63-3.39-11.51-2.26-2.88-5.31-4.31-9.14-"
    "4.31-4.11,0-7.33,1.37-9.66,4.11-2.33,2.74-3.49,6.65-3.49,11.71v57.12h"
    '-31.23V40.83Z"/>'
    '<path fill="#6eabde" d="M753.84,67.75h-31.44v110.14h-33.29v-110.14h-31.64'
    'v-26.92h96.37v26.92Z"/>'
    '<path fill="#6eabde" d="M763.09,40.83h33.29v137.06h-33.29V40.83Z"/>'
    "</svg>"
)


def _compute_run_stats(run: Dict[str, Any], tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute statistics for a single run's task results.

    :param run: Run metadata dictionary
    :param tasks: List of task result dictionaries for this run
    :return: Dictionary with computed stats
    """
    durations = [
        t["duration_seconds"]
        for t in tasks
        if t["status"] == "Success" and t["duration_seconds"] is not None
    ]

    if not durations:
        return {
            "run_id": run["run_id"],
            "min": 0,
            "q1": 0,
            "median": 0,
            "q3": 0,
            "max": 0,
            "mean": 0,
            "std_dev": 0,
            "outlier_count": 0,
        }

    sorted_d = sorted(durations)
    n = len(sorted_d)
    q1 = sorted_d[n // 4] if n >= 4 else sorted_d[0]
    q3 = sorted_d[(3 * n) // 4] if n >= 4 else sorted_d[-1]
    iqr = q3 - q1
    outlier_threshold = q3 + 1.5 * iqr

    return {
        "run_id": run["run_id"],
        "min": round(min(sorted_d), 2),
        "q1": round(q1, 2),
        "median": round(statistics.median(sorted_d), 2),
        "q3": round(q3, 2),
        "max": round(max(sorted_d), 2),
        "mean": round(statistics.mean(sorted_d), 2),
        "std_dev": round(statistics.stdev(sorted_d), 2) if len(sorted_d) > 1 else 0,
        "outlier_count": sum(1 for d in sorted_d if d > outlier_threshold),
    }


def _compute_concurrency_timeline(
    run: Dict[str, Any], tasks: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Compute second-by-second concurrency for a run.

    :param run: Run metadata dictionary
    :param tasks: List of task result dictionaries for this run
    :return: List of {second, count} dictionaries
    """
    if not tasks:
        return []

    # Parse task start/end times to compute overlaps
    events = []
    for t in tasks:
        try:
            start = datetime.fromisoformat(t["start_time"])
            end = datetime.fromisoformat(t["end_time"])
            events.append((start, 1))  # task starts
            events.append((end, -1))  # task ends
        except (ValueError, TypeError):
            continue

    if not events:
        return []

    events.sort(key=lambda e: (e[0], -e[1]))  # sort by time, ends before starts at same time

    # Determine the run time window
    try:
        run_start = datetime.fromisoformat(run["start_time"])
    except (ValueError, TypeError):
        run_start = events[0][0]

    # Build timeline: sample every second
    timeline = []
    concurrent = 0
    event_idx = 0
    max_seconds = int(run.get("duration_seconds", 0) or 0) + 1
    # Cap at 3600 seconds to avoid huge arrays
    max_seconds = min(max_seconds, 3600)

    for sec in range(max_seconds):
        current_time = run_start.timestamp() + sec
        while event_idx < len(events) and events[event_idx][0].timestamp() <= current_time:
            concurrent += events[event_idx][1]
            event_idx += 1
        concurrent = max(0, concurrent)
        timeline.append({"second": sec, "count": concurrent})

    return timeline


def _prepare_dashboard_data(
    runs: List[Dict[str, Any]],
    task_results: List[Dict[str, Any]],
    default_runs: int,
) -> Dict[str, Any]:
    """Prepare all data for the dashboard template.

    :param runs: List of run metadata (ordered by start_time DESC)
    :param task_results: List of all task results across all runs
    :param default_runs: Default number of runs to display
    :return: Dictionary with all computed dashboard data
    """
    # Group task results by run_id
    tasks_by_run: Dict[str, List[Dict[str, Any]]] = {}
    for tr in task_results:
        run_id = tr["run_id"]
        if run_id not in tasks_by_run:
            tasks_by_run[run_id] = []
        tasks_by_run[run_id].append(tr)

    # Build enriched run data
    enriched_runs = []
    for run in runs:
        run_tasks = tasks_by_run.get(run["run_id"], [])
        run_stats = _compute_run_stats(run, run_tasks)
        concurrency = _compute_concurrency_timeline(run, run_tasks)

        enriched_runs.append(
            {
                **run,
                "stats": run_stats,
                "concurrency": concurrency,
                "task_count_actual": len(run_tasks),
                "failure_count_actual": sum(1 for t in run_tasks if t["status"] != "Success"),
            }
        )

    # Build per-task aggregate data (across all runs)
    task_data: Dict[str, Dict[str, Any]] = {}
    for tr in task_results:
        sig = tr["task_signature"]
        if sig not in task_data:
            task_data[sig] = {
                "task_signature": sig,
                "task_id": tr["task_id"],
                "instance": tr["instance"],
                "process": tr["process"],
                "durations": [],
                "successes": 0,
                "total": 0,
            }
        task_data[sig]["total"] += 1
        if tr["status"] == "Success":
            task_data[sig]["successes"] += 1
        if tr["duration_seconds"] is not None:
            task_data[sig]["durations"].append(tr["duration_seconds"])

    task_summaries = []
    for sig, data in task_data.items():
        durations = data["durations"]
        task_summaries.append(
            {
                "task_signature": sig,
                "task_id": data["task_id"],
                "instance": data["instance"],
                "process": data["process"],
                "executions": data["total"],
                "success_rate": (
                    round(data["successes"] / data["total"] * 100, 1) if data["total"] > 0 else 0
                ),
                "avg_duration": round(statistics.mean(durations), 2) if durations else 0,
                "min_duration": round(min(durations), 2) if durations else 0,
                "max_duration": round(max(durations), 2) if durations else 0,
                "std_dev": round(statistics.stdev(durations), 2) if len(durations) > 1 else 0,
            }
        )

    # Sort by avg_duration descending
    task_summaries.sort(key=lambda t: t["avg_duration"], reverse=True)

    # Build outliers list (top 20 slowest individual executions)
    all_median = 0
    all_durations = [
        tr["duration_seconds"]
        for tr in task_results
        if tr["duration_seconds"] is not None and tr["status"] == "Success"
    ]
    if all_durations:
        all_median = statistics.median(all_durations)

    outliers = []
    for tr in task_results:
        if tr["duration_seconds"] is not None:
            outliers.append(
                {
                    "task_id": tr["task_id"],
                    "process": tr["process"],
                    "instance": tr["instance"],
                    "run_id": tr["run_id"],
                    "duration": round(tr["duration_seconds"], 2),
                    "status": tr["status"],
                    "vs_median": round(tr["duration_seconds"] - all_median, 2) if all_median else 0,
                }
            )
    outliers.sort(key=lambda o: o["duration"], reverse=True)
    outliers = outliers[:20]

    # Build failures list
    failures = []
    for tr in task_results:
        if tr["status"] != "Success":
            failures.append(
                {
                    "task_id": tr["task_id"],
                    "process": tr["process"],
                    "instance": tr["instance"],
                    "run_id": tr["run_id"],
                    "duration": round(tr["duration_seconds"], 2) if tr["duration_seconds"] else 0,
                    "error_message": tr.get("error_message", ""),
                }
            )

    # Slim task_results for JS (only fields needed for interactive filtering)
    slim_task_results = []
    for tr in task_results:
        slim_task_results.append(
            {
                "run_id": tr["run_id"],
                "task_id": tr["task_id"],
                "task_signature": tr["task_signature"],
                "instance": tr["instance"],
                "process": tr["process"],
                "status": tr["status"],
                "duration_seconds": tr["duration_seconds"],
                "error_message": tr.get("error_message"),
                "stage": tr.get("stage"),
            }
        )

    # Taskfile metadata from the most recent run
    latest = runs[0] if runs else {}

    return {
        "workflow": latest.get("workflow", ""),
        "taskfile_name": latest.get("taskfile_name", ""),
        "taskfile_description": latest.get("taskfile_description", ""),
        "taskfile_author": latest.get("taskfile_author", ""),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "default_runs": min(default_runs, len(runs)),
        "total_runs": len(runs),
        "runs": enriched_runs,
        "task_results": slim_task_results,
        "task_summaries": task_summaries,
        "outliers": outliers,
        "failures": failures,
    }


def generate_dashboard(
    workflow: str,
    runs: List[Dict[str, Any]],
    task_results: List[Dict[str, Any]],
    output_path: str,
    default_runs: int = 5,
    dag_url: Optional[str] = None,
) -> str:
    """Generate an interactive HTML dashboard for workflow execution stats.

    :param workflow: Workflow identifier
    :param runs: List of run metadata dictionaries (ordered by start_time DESC)
    :param task_results: List of all task result dictionaries
    :param output_path: Output file path for the HTML dashboard
    :param default_runs: Default number of runs to display initially
    :param dag_url: Optional relative URL to the DAG visualization HTML
    :return: Path to the generated HTML file
    """
    data = _prepare_dashboard_data(runs, task_results, default_runs)
    data_json = json.dumps(data, default=str)

    # Build conditional DAG link HTML
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

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RushTI Dashboard - {workflow}</title>
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
        .run-selector {{
            display: flex; align-items: center; gap: 8px;
            background: #F1F5F9; border-radius: 8px; padding: 8px 12px;
        }}
        .run-selector label {{ font-size: 0.85rem; color: #64748B; font-weight: 500; white-space: nowrap; }}
        .run-selector select {{
            font-family: inherit; font-size: 0.85rem; font-weight: 600;
            color: #1E293B; background: #FFFFFF;
            border: 1px solid #CBD5E1; border-radius: 6px;
            padding: 4px 24px 4px 8px; cursor: pointer;
            appearance: auto;
        }}
        .generated-at {{ font-size: 0.75rem; color: #94A3B8; }}

        /* Metadata bar */
        .metadata {{
            display: flex; gap: 24px; flex-wrap: wrap;
            margin-bottom: 24px; font-size: 0.85rem; color: #64748B;
        }}
        .metadata span {{ display: flex; align-items: center; gap: 4px; }}
        .metadata strong {{ color: #1E293B; font-weight: 600; }}

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
        }}
        .chart-panel h3 {{
            font-size: 0.95rem; font-weight: 600; color: #1E293B;
            margin-bottom: 12px;
        }}
        .chart-wrapper {{ position: relative; height: 300px; }}
        .chart-panel.full-width {{ grid-column: 1 / -1; }}

        /* Concurrency charts container */
        .concurrency-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 16px;
            margin-top: 12px;
        }}
        .concurrency-item {{ position: relative; height: 200px; }}

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
            cursor: pointer; user-select: none; white-space: nowrap;
        }}
        th:hover {{ color: #00AEEF; }}
        th .sort-arrow {{ font-size: 0.65rem; margin-left: 4px; }}
        td {{
            padding: 8px 12px; border-bottom: 1px solid #F1F5F9;
            color: #1E293B;
        }}
        tr:hover td {{ background: #F8FAFC; }}
        .mono {{ font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.8rem; }}
        .status-success {{ color: #059669; font-weight: 600; }}
        .status-fail {{ color: #DC2626; font-weight: 600; }}
        .status-partial {{ color: #D97706; font-weight: 600; }}
        .high-cv {{ background: #FFF7ED; }}
        .duration-slow {{ color: #DC2626; font-weight: 600; }}

        /* Accordion */
        .accordion {{ margin-bottom: 20px; }}
        .accordion-header {{
            background: #FFFFFF; border: 1px solid #E2E8F0;
            border-radius: 10px; padding: 14px 20px;
            font-weight: 600; font-size: 0.95rem; color: #1E293B;
            cursor: pointer; display: flex; align-items: center;
            justify-content: space-between;
        }}
        .accordion-header:hover {{ border-color: #00AEEF; }}
        .accordion-body {{
            display: none; background: #FFFFFF;
            border: 1px solid #E2E8F0; border-top: none;
            border-radius: 0 0 10px 10px; padding: 16px 20px;
        }}
        .accordion-body.open {{ display: block; }}
        .config-grid {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
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
        .badge-partial {{ background: #FEF3C7; color: #D97706; }}
        .badge-info {{ background: #DBEAFE; color: #2563EB; }}

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
            font-weight: 400; line-height: 1.5; width: 300px;
            z-index: 100; box-shadow: 0 4px 16px rgba(0,0,0,0.15);
            pointer-events: none; white-space: normal;
        }}
        .help-icon:hover .help-tip {{ display: block; }}

        /* Pagination buttons */
        .page-btn {{
            background: #FFFFFF; border: 1px solid #CBD5E1; border-radius: 6px;
            padding: 2px 10px; cursor: pointer; font-size: 1rem; color: #1E293B;
            font-family: inherit; line-height: 1.4;
        }}
        .page-btn:hover {{ border-color: #00AEEF; color: #00AEEF; }}
        .page-btn:disabled {{ opacity: 0.3; cursor: default; border-color: #E2E8F0; color: #94A3B8; }}

        /* No data */
        .no-data {{
            text-align: center; padding: 40px; color: #94A3B8;
            font-size: 0.95rem;
        }}

        /* Footer */
        .footer {{
            text-align: center; padding: 20px;
            font-size: 0.75rem; color: #94A3B8;
        }}
        .footer a {{ color: #00AEEF; text-decoration: none; }}
    </style>
</head>
<body>
    <div class="dashboard">
        <div class="header">
            <div class="header-left">
                {_LOGO_SVG}
                <div>
                    <div class="header-title">Performance Dashboard</div>
                    <div class="header-subtitle" id="headerSubtitle"></div>
                </div>
            </div>
            <div class="header-right">
                {dag_link_html}
                <div class="run-selector">
                    <label for="runCount">Show last</label>
                    <select id="runCount" onchange="updateDashboard()"></select>
                    <label>runs</label>
                </div>
                <div class="generated-at" id="generatedAt"></div>
            </div>
        </div>

        <div class="metadata" id="metadata"></div>

        <div class="summary-cards" id="summaryCards"></div>

        <div class="charts-grid">
            <div class="chart-panel">
                <h3>Run Duration Comparison <span class="help-icon">?<span class="help-tip">How to read: Each bar is one full run. Look for trends — are runs getting faster or slower over time? Green bars finished successfully, yellow had partial failures, and red bars failed. If a bar is much taller than the others, hover over it to check if the configuration (workers, optimize, exclusive) was different. Shorter bars with more workers usually indicate better parallelism.</span></span></h3>
                <div class="chart-wrapper"><canvas id="runDurationChart"></canvas></div>
            </div>
            <div class="chart-panel">
                <h3>Task Duration Distribution <span class="help-icon">?<span class="help-tip">How to read: Each column is a run, split into four colored segments showing how task durations are spread. Gray = fastest 25%%, light blue = below-median tasks, dark blue = above-median tasks, red = slowest 25%%. The orange dot marks the median and the purple triangle marks the mean. A tall red segment means a few tasks took much longer than the rest — these are candidates for optimization. If the mean (triangle) is far above the median (dot), outlier tasks are pulling the average up.</span></span></h3>
                <div class="chart-wrapper"><canvas id="distributionChart"></canvas></div>
            </div>
        </div>

        <div class="chart-panel full-width" style="margin-bottom: 28px;">
            <h3>Concurrency Timeline <span class="help-icon">?<span class="help-tip">How to read: The solid line shows how many tasks were actually running at the same time, second by second. The dashed line is your max_workers target. Ideally the solid line should stay close to the dashed line — if it dips significantly below, workers are sitting idle (possibly waiting for long-running tasks to finish or due to task dependencies). A flat line at 1 means tasks ran sequentially. Look for valleys to identify bottleneck moments in your pipeline.</span></span></h3>
            <p style="font-size:0.8rem;color:#64748B;margin-bottom:8px;">Actual concurrent tasks over time vs. max_workers target (dashed line)</p>
            <div class="concurrency-grid" id="concurrencyGrid"></div>
        </div>

        <div class="charts-grid">
            <div class="chart-panel">
                <h3>Task Duration by Signature <span class="help-icon">?<span class="help-tip">How to read: Each dot is one task execution. Dots at the same horizontal position represent the same task across different runs (matched by signature). If dots at the same X position are tightly clustered vertically, that task runs consistently. If they are spread apart, that task has variable performance — investigate why. Dots sitting much higher than the cluster are outliers worth investigating. Compare colors (runs) to see if a particular run was overall slower or faster.</span></span></h3>
                <div class="chart-wrapper"><canvas id="scatterChart"></canvas></div>
            </div>
            <div class="chart-panel">
                <h3>Success Rate by Run <span class="help-icon">?<span class="help-tip">How to read: Each bar shows the total number of tasks in a run, split into green (succeeded) and red (failed). A fully green bar means all tasks completed without errors. If you see red, scroll down to the Failed Processes table for details. Watch for trends — increasing red across runs may signal a recurring issue. Hover over each segment for exact counts.</span></span></h3>
                <div class="chart-wrapper"><canvas id="successChart"></canvas></div>
            </div>
        </div>

        <div id="stageSection" style="display:none;">
            <div class="charts-grid">
                <div class="chart-panel">
                    <h3>Stage Duration Breakdown <span class="help-icon">?<span class="help-tip">How to read: Each stacked bar is a run, with colored segments for each pipeline stage. The tallest segment is where most execution time is spent — that is your bottleneck stage. Compare across runs: if one stage consistently dominates, focus optimization there. If the proportions shift between runs, check whether task counts or configurations changed.</span></span></h3>
                    <div class="chart-wrapper"><canvas id="stageDurationChart"></canvas></div>
                </div>
                <div class="chart-panel">
                    <h3>Stage Avg Duration per Task <span class="help-icon">?<span class="help-tip">How to read: Each bar shows the average time a single task takes within a given stage. A stage with a high average means its individual tasks are slow (optimize the tasks themselves). Compare this with the Stage Duration Breakdown: if a stage has low average but high total, it has many fast tasks — the bottleneck is volume, not individual task speed. Focus optimization on stages where both metrics are high.</span></span></h3>
                    <div class="chart-wrapper"><canvas id="stageAvgChart"></canvas></div>
                </div>
            </div>
        </div>

        <div class="table-panel">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
                <h3 style="margin-bottom:0;">Per-Task Performance <span class="help-icon">?<span class="help-tip">How to read: Each row is a unique task (grouped by signature) with stats computed across the selected runs. Look for rows highlighted in orange — these have a high coefficient of variation (CV), meaning the task runs inconsistently and may need investigation. Sort by "Max" to find the slowest tasks, or by "Std Dev" to find the most unpredictable ones. A low success rate signals a flaky task. Click any column header to sort.</span></span></h3>
                <div style="display:flex;align-items:center;gap:8px;">
                    <span id="taskTableInfo" style="font-size:0.8rem;color:#64748B;"></span>
                    <div class="run-selector" style="padding:4px 8px;">
                        <label for="taskPageSize" style="font-size:0.8rem;">Show</label>
                        <select id="taskPageSize" onchange="taskTablePage=0;updateTaskTable(getSelectedRuns());" style="font-size:0.8rem;padding:2px 8px;">
                            <option value="10">10</option>
                            <option value="25">25</option>
                            <option value="50">50</option>
                            <option value="0">All</option>
                        </select>
                    </div>
                    <button onclick="taskTablePage=Math.max(0,taskTablePage-1);updateTaskTable(getSelectedRuns());" class="page-btn" id="taskPrevBtn" title="Previous">&lsaquo;</button>
                    <button onclick="taskTablePage++;updateTaskTable(getSelectedRuns());" class="page-btn" id="taskNextBtn" title="Next">&rsaquo;</button>
                </div>
            </div>
            <table id="taskTable">
                <thead><tr>
                    <th onclick="sortTaskTable(0)">Task ID <span class="sort-arrow"></span></th>
                    <th onclick="sortTaskTable(1)">Instance <span class="sort-arrow"></span></th>
                    <th onclick="sortTaskTable(2)">Process <span class="sort-arrow"></span></th>
                    <th onclick="sortTaskTable(3)">Avg (s) <span class="sort-arrow"></span></th>
                    <th onclick="sortTaskTable(4)">Min (s) <span class="sort-arrow"></span></th>
                    <th onclick="sortTaskTable(5)">Max (s) <span class="sort-arrow"></span></th>
                    <th onclick="sortTaskTable(6)">Std Dev <span class="sort-arrow"></span></th>
                    <th onclick="sortTaskTable(7)">Success % <span class="sort-arrow"></span></th>
                    <th onclick="sortTaskTable(8)">Runs <span class="sort-arrow"></span></th>
                </tr></thead>
                <tbody id="taskTableBody"></tbody>
            </table>
        </div>

        <div class="table-panel">
            <h3>Top 10 Slowest Executions <span class="help-icon">?<span class="help-tip">How to read: These are the 10 longest-running individual task executions across the selected runs. The "vs Median" column tells you how many times slower each one was compared to the typical task — a high multiplier (e.g., 5×) means that execution was significantly slower than usual and may be worth investigating. Check whether the same task appears multiple times — if so, it is a consistent bottleneck.</span></span></h3>
            <table id="outlierTable">
                <thead><tr>
                    <th>Task ID</th><th>Process</th><th>Instance</th>
                    <th>Run</th><th>Duration (s)</th><th>vs Median</th><th>Status</th>
                </tr></thead>
                <tbody id="outlierTableBody"></tbody>
            </table>
        </div>

        <div class="table-panel" id="failuresPanel" style="display:none;">
            <h3>Failed Processes <span class="help-icon">?<span class="help-tip">How to read: Every task execution that did not succeed is listed here. Look for patterns — does the same task fail repeatedly across runs? That points to a systemic issue. If failures are scattered across different tasks, the problem may be environmental (server load, connectivity). Hover over the Error column to see the full error message for each failure.</span></span></h3>
            <table id="failureTable">
                <thead><tr>
                    <th>Task ID</th><th>Process</th><th>Instance</th>
                    <th>Run</th><th>Duration (s)</th><th>Error</th>
                </tr></thead>
                <tbody id="failureTableBody"></tbody>
            </table>
        </div>

        <div class="accordion">
            <div class="accordion-header" onclick="toggleAccordion(this)">
                Run Configuration Details
                <span>&#9660;</span>
            </div>
            <div class="accordion-body" id="configBody"></div>
        </div>

        <div class="footer">
            Generated by <a href="https://github.com/cubewise-code/rushti" target="_blank">RushTI</a>
        </div>
    </div>

    <script>
    const DATA = {data_json};

    // Chart instances for cleanup on re-render
    let charts = {{}};
    const RUN_COLORS = [
        '#00AEEF', '#FBB040', '#10B981', '#8B5CF6', '#EC4899',
        '#F59E0B', '#06B6D4', '#6366F1', '#EF4444', '#14B8A6',
        '#F97316', '#A855F7', '#0EA5E9'
    ];

    function init() {{
        // Build run count selector options
        const sel = document.getElementById('runCount');
        const total = DATA.total_runs;
        const options = [];
        for (let i = 1; i <= Math.min(total, 5); i++) options.push(i);
        if (total > 5) options.push(5);
        if (total > 10) options.push(10);
        if (total > 20) options.push(20);
        if (total > 5) options.push(total);
        // Deduplicate and sort
        const unique = [...new Set(options)].sort((a, b) => a - b);
        unique.forEach(n => {{
            const opt = document.createElement('option');
            opt.value = n;
            opt.textContent = n === total ? `All (${{total}})` : n;
            sel.appendChild(opt);
        }});
        sel.value = DATA.default_runs;

        document.getElementById('generatedAt').textContent = 'Generated: ' + DATA.generated_at;
        updateDashboard();
    }}

    function getSelectedRuns() {{
        const n = parseInt(document.getElementById('runCount').value);
        // Runs are ordered DESC (newest first); take last N and reverse for chronological display
        return DATA.runs.slice(0, n).reverse();
    }}

    function statusBadge(status) {{
        const cls = status === 'Success' ? 'badge-success' : status === 'Partial' ? 'badge-partial' : 'badge-fail';
        return `<span class="badge ${{cls}}">${{status}}</span>`;
    }}

    function formatTime(s) {{
        if (s == null) return '-';
        if (s < 60) return s.toFixed(1) + 's';
        const m = Math.floor(s / 60);
        const sec = (s % 60).toFixed(0);
        return `${{m}}m ${{sec}}s`;
    }}

    function formatDate(iso) {{
        if (!iso) return '-';
        try {{
            const d = new Date(iso);
            return d.toLocaleString(undefined, {{ month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }});
        }} catch {{ return iso; }}
    }}

    function destroyCharts() {{
        Object.values(charts).forEach(c => {{ if (c && c.destroy) c.destroy(); }});
        charts = {{}};
    }}

    function updateDashboard() {{
        destroyCharts();
        const runs = getSelectedRuns();

        // Header subtitle
        document.getElementById('headerSubtitle').textContent =
            `${{DATA.workflow}}${{DATA.taskfile_name ? ' — ' + DATA.taskfile_name : ''}}`;

        // Metadata
        const meta = document.getElementById('metadata');
        let metaHtml = '';
        if (DATA.taskfile_description) metaHtml += `<span>${{DATA.taskfile_description}}</span>`;
        if (DATA.taskfile_author) metaHtml += `<span>Author: <strong>${{DATA.taskfile_author}}</strong></span>`;
        metaHtml += `<span>Total runs in DB: <strong>${{DATA.total_runs}}</strong></span>`;
        if (runs.length > 0) {{
            metaHtml += `<span>Date range: <strong>${{formatDate(runs[0].start_time)}} — ${{formatDate(runs[runs.length-1].start_time)}}</strong></span>`;
        }}
        meta.innerHTML = metaHtml;

        // Summary cards
        updateSummaryCards(runs);

        // Charts
        renderRunDurationChart(runs);
        renderDistributionChart(runs);
        renderConcurrencyCharts(runs);
        renderScatterChart(runs);
        renderSuccessChart(runs);
        renderStageCharts(runs);

        // Tables
        updateTaskTable(runs);
        updateOutlierTable(runs);
        updateFailureTable(runs);
        updateConfigDetails(runs);
    }}

    function updateSummaryCards(runs) {{
        const cards = document.getElementById('summaryCards');
        if (runs.length === 0) {{ cards.innerHTML = '<div class="no-data">No runs available</div>'; return; }}

        const durations = runs.filter(r => r.duration_seconds).map(r => r.duration_seconds);
        const best = durations.length ? Math.min(...durations) : 0;
        const avg = durations.length ? durations.reduce((a,b) => a+b, 0) / durations.length : 0;
        const totalTasks = runs.reduce((s, r) => s + (r.task_count || 0), 0);
        const totalSuccess = runs.reduce((s, r) => s + (r.success_count || 0), 0);
        const successRate = totalTasks > 0 ? (totalSuccess / totalTasks * 100) : 0;
        const workers = [...new Set(runs.map(r => r.max_workers).filter(Boolean))];

        cards.innerHTML = `
            <div class="card">
                <div class="card-label">Runs Shown</div>
                <div class="card-value">${{runs.length}}</div>
                <div class="card-sub">of ${{DATA.total_runs}} total</div>
            </div>
            <div class="card">
                <div class="card-label">Tasks per Run</div>
                <div class="card-value">${{runs[runs.length-1].task_count || 0}}</div>
                <div class="card-sub">${{totalTasks}} total executions</div>
            </div>
            <div class="card">
                <div class="card-label">Success Rate</div>
                <div class="card-value">${{successRate.toFixed(1)}}%</div>
                <div class="card-sub">${{totalSuccess}} / ${{totalTasks}} tasks</div>
            </div>
            <div class="card best">
                <div class="card-label">Best Run</div>
                <div class="card-value">${{formatTime(best)}}</div>
                <div class="card-sub">fastest wall-clock</div>
            </div>
            <div class="card">
                <div class="card-label">Avg Duration</div>
                <div class="card-value">${{formatTime(avg)}}</div>
                <div class="card-sub">across ${{runs.length}} runs</div>
            </div>
            <div class="card">
                <div class="card-label">Max Workers</div>
                <div class="card-value">${{workers.join(', ') || '-'}}</div>
                <div class="card-sub">concurrency level</div>
            </div>
        `;
    }}

    function renderRunDurationChart(runs) {{
        const ctx = document.getElementById('runDurationChart').getContext('2d');
        const labels = runs.map(r => formatDate(r.start_time));
        const data = runs.map(r => r.duration_seconds || 0);
        const colors = runs.map(r =>
            r.status === 'Success' ? '#10B981' : r.status === 'Partial' ? '#F59E0B' : '#EF4444'
        );

        charts.runDuration = new Chart(ctx, {{
            type: 'bar',
            data: {{
                labels: labels,
                datasets: [{{
                    label: 'Duration (s)',
                    data: data,
                    backgroundColor: colors,
                    borderColor: colors.map(c => c),
                    borderWidth: 1,
                    borderRadius: 4,
                }}]
            }},
            options: {{
                responsive: true, maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            afterBody: function(items) {{
                                const idx = items[0].dataIndex;
                                const run = runs[idx];
                                return [
                                    `Status: ${{run.status}}`,
                                    `Workers: ${{run.max_workers || '-'}}`,
                                    `Tasks: ${{run.task_count}}`,
                                    `Optimize: ${{run.optimize ? 'Yes' : 'No'}}`,
                                    `Exclusive: ${{run.exclusive ? 'Yes' : 'No'}}`
                                ];
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{ beginAtZero: true, title: {{ display: true, text: 'Seconds' }} }},
                    x: {{ title: {{ display: true, text: 'Run' }} }}
                }}
            }}
        }});
    }}

    function renderDistributionChart(runs) {{
        const ctx = document.getElementById('distributionChart').getContext('2d');
        const labels = runs.map(r => formatDate(r.start_time));

        // Horizontal stacked bar: each segment = range between stats
        // Min→Q1 (whisker), Q1→Median (IQR lower), Median→Q3 (IQR upper), Q3→Max (whisker)
        // Use floating bars: each segment = [start, end]
        charts.distribution = new Chart(ctx, {{
            type: 'bar',
            data: {{
                labels: labels,
                datasets: [
                    {{
                        label: 'Min',
                        data: runs.map(r => [0, r.stats.min]),
                        backgroundColor: 'transparent',
                        borderWidth: 0,
                        barPercentage: 0.5,
                    }},
                    {{
                        label: 'Min → Q1',
                        data: runs.map(r => [r.stats.min, r.stats.q1]),
                        backgroundColor: '#E2E8F0',
                        borderColor: '#CBD5E1',
                        borderWidth: 1,
                        borderRadius: {{ topLeft: 4, bottomLeft: 4, topRight: 0, bottomRight: 0 }},
                        borderSkipped: false,
                        barPercentage: 0.5,
                    }},
                    {{
                        label: 'Q1 → Median',
                        data: runs.map(r => [r.stats.q1, r.stats.median]),
                        backgroundColor: 'rgba(0, 174, 239, 0.35)',
                        borderColor: '#00AEEF',
                        borderWidth: 1,
                        borderSkipped: false,
                        barPercentage: 0.5,
                    }},
                    {{
                        label: 'Median → Q3',
                        data: runs.map(r => [r.stats.median, r.stats.q3]),
                        backgroundColor: 'rgba(0, 174, 239, 0.55)',
                        borderColor: '#00AEEF',
                        borderWidth: 1,
                        borderSkipped: false,
                        barPercentage: 0.5,
                    }},
                    {{
                        label: 'Q3 → Max',
                        data: runs.map(r => [r.stats.q3, r.stats.max]),
                        backgroundColor: '#FEE2E2',
                        borderColor: '#FECACA',
                        borderWidth: 1,
                        borderRadius: {{ topLeft: 0, bottomLeft: 0, topRight: 4, bottomRight: 4 }},
                        borderSkipped: false,
                        barPercentage: 0.5,
                    }},
                    {{
                        label: 'Median',
                        data: runs.map(r => r.stats.median),
                        type: 'line',
                        borderColor: '#FBB040',
                        backgroundColor: '#FBB040',
                        pointRadius: 6,
                        pointStyle: 'rectRounded',
                        borderWidth: 2,
                        fill: false,
                        order: -1,
                    }},
                    {{
                        label: 'Mean',
                        data: runs.map(r => r.stats.mean),
                        type: 'line',
                        borderColor: '#8B5CF6',
                        backgroundColor: '#8B5CF6',
                        pointRadius: 5,
                        pointStyle: 'triangle',
                        borderWidth: 1,
                        borderDash: [4, 4],
                        fill: false,
                        order: -1,
                    }}
                ]
            }},
            options: {{
                responsive: true, maintainAspectRatio: false,
                indexAxis: 'x',
                plugins: {{
                    legend: {{
                        labels: {{
                            filter: function(item) {{
                                // Hide the invisible spacer and show meaningful labels
                                return item.text !== 'Min';
                            }}
                        }}
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(item) {{
                                const idx = item.dataIndex;
                                const s = runs[idx].stats;
                                if (item.dataset.label === 'Median') return `Median: ${{s.median.toFixed(2)}}s`;
                                if (item.dataset.label === 'Mean') return `Mean: ${{s.mean.toFixed(2)}}s`;
                                return `${{item.dataset.label}}: ${{Array.isArray(item.raw) ? item.raw[0].toFixed(2) + 's → ' + item.raw[1].toFixed(2) + 's' : ''}}`;
                            }},
                            afterBody: function(items) {{
                                const idx = items[0].dataIndex;
                                const s = runs[idx].stats;
                                return [
                                    `───────────────`,
                                    `Min: ${{s.min.toFixed(2)}}s   Q1: ${{s.q1.toFixed(2)}}s`,
                                    `Median: ${{s.median.toFixed(2)}}s   Q3: ${{s.q3.toFixed(2)}}s`,
                                    `Max: ${{s.max.toFixed(2)}}s   Mean: ${{s.mean.toFixed(2)}}s`,
                                    `Std Dev: ${{s.std_dev.toFixed(2)}}s   Outliers: ${{s.outlier_count}}`
                                ];
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        stacked: true,
                        title: {{ display: true, text: 'Duration (seconds)' }},
                    }},
                    x: {{
                        stacked: true,
                        title: {{ display: true, text: 'Run' }},
                    }}
                }}
            }}
        }});
    }}

    function renderConcurrencyCharts(runs) {{
        const grid = document.getElementById('concurrencyGrid');
        // Remove old canvases
        grid.innerHTML = '';

        runs.forEach((run, idx) => {{
            if (!run.concurrency || run.concurrency.length === 0) return;
            const wrapper = document.createElement('div');
            wrapper.className = 'concurrency-item';
            const canvas = document.createElement('canvas');
            canvas.id = `concurrency_${{idx}}`;
            wrapper.appendChild(canvas);
            grid.appendChild(wrapper);

            const maxWorkers = run.max_workers || 0;
            const labels = run.concurrency.map(c => c.second);
            const data = run.concurrency.map(c => c.count);

            charts[`concurrency_${{idx}}`] = new Chart(canvas.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: labels,
                    datasets: [
                        {{
                            label: 'Concurrent Tasks',
                            data: data,
                            borderColor: RUN_COLORS[idx % RUN_COLORS.length],
                            backgroundColor: RUN_COLORS[idx % RUN_COLORS.length] + '20',
                            fill: true,
                            tension: 0.2,
                            pointRadius: 0,
                            borderWidth: 1.5,
                        }},
                        ...(maxWorkers > 0 ? [{{
                            label: `Max Workers (${{maxWorkers}})`,
                            data: labels.map(() => maxWorkers),
                            borderColor: '#94A3B8',
                            borderDash: [6, 4],
                            pointRadius: 0,
                            borderWidth: 1,
                            fill: false,
                        }}] : [])
                    ]
                }},
                options: {{
                    responsive: true, maintainAspectRatio: false,
                    plugins: {{
                        title: {{
                            display: true,
                            text: `${{formatDate(run.start_time)}} (${{formatTime(run.duration_seconds)}})`,
                            font: {{ size: 11, weight: '500' }},
                            color: '#64748B',
                        }},
                        legend: {{ display: false }},
                    }},
                    scales: {{
                        y: {{ beginAtZero: true, title: {{ display: true, text: 'Tasks', font: {{ size: 10 }} }} }},
                        x: {{ title: {{ display: true, text: 'Seconds', font: {{ size: 10 }} }},
                               ticks: {{ maxTicksLimit: 10 }} }}
                    }}
                }}
            }});
        }});
    }}

    function renderScatterChart(runs) {{
        const ctx = document.getElementById('scatterChart').getContext('2d');
        const runIds = new Set(runs.map(r => r.run_id));
        const relevant = DATA.task_results.filter(tr => runIds.has(tr.run_id));

        // Build a stable signature-to-index map so the same task always appears
        // at the same X position across all runs (sorted by signature for consistency)
        const signatures = [...new Set(relevant.map(tr => tr.task_signature))].sort();
        const sigIndex = {{}};
        signatures.forEach((sig, i) => {{ sigIndex[sig] = i; }});

        // Also store a lookup for tooltip labels
        const sigLabel = {{}};
        relevant.forEach(tr => {{
            if (!sigLabel[tr.task_signature]) sigLabel[tr.task_signature] = tr.process;
        }});

        const datasets = runs.map((run, idx) => {{
            const runTasks = relevant.filter(tr => tr.run_id === run.run_id);
            return {{
                label: formatDate(run.start_time),
                data: runTasks.map(t => ({{ x: sigIndex[t.task_signature], y: t.duration_seconds || 0, _sig: t.task_signature, _task: t.task_id, _proc: t.process }})),
                backgroundColor: RUN_COLORS[idx % RUN_COLORS.length] + '80',
                borderColor: RUN_COLORS[idx % RUN_COLORS.length],
                pointRadius: 3,
                pointHoverRadius: 5,
            }};
        }});

        charts.scatter = new Chart(ctx, {{
            type: 'scatter',
            data: {{ datasets }},
            options: {{
                responsive: true, maintainAspectRatio: false,
                plugins: {{
                    tooltip: {{
                        callbacks: {{
                            label: function(item) {{
                                const pt = item.raw;
                                return `${{pt._task}} (${{pt._proc}}): ${{pt.y.toFixed(2)}}s`;
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{ beginAtZero: true, title: {{ display: true, text: 'Duration (s)' }} }},
                    x: {{ title: {{ display: true, text: 'Task (by signature)' }},
                           ticks: {{ display: false }},
                           min: -0.5, max: signatures.length - 0.5 }}
                }}
            }}
        }});
    }}

    function renderSuccessChart(runs) {{
        const ctx = document.getElementById('successChart').getContext('2d');
        const labels = runs.map(r => formatDate(r.start_time));
        const successData = runs.map(r => r.success_count || 0);
        const failData = runs.map(r => (r.failure_count || 0));

        charts.success = new Chart(ctx, {{
            type: 'bar',
            data: {{
                labels: labels,
                datasets: [
                    {{
                        label: 'Success',
                        data: successData,
                        backgroundColor: '#10B981',
                        borderRadius: 3,
                    }},
                    {{
                        label: 'Failed',
                        data: failData,
                        backgroundColor: '#EF4444',
                        borderRadius: 3,
                    }}
                ]
            }},
            options: {{
                responsive: true, maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'top' }} }},
                scales: {{
                    y: {{ beginAtZero: true, stacked: true, title: {{ display: true, text: 'Tasks' }} }},
                    x: {{ stacked: true, title: {{ display: true, text: 'Run' }} }}
                }}
            }}
        }});
    }}

    const STAGE_COLORS = {{
        'extract': '#3B82F6',
        'load': '#10B981',
        'transform': '#F59E0B',
        'export': '#EC4899',
        'import': '#06B6D4',
        'input': '#8B5CF6',
        'analysis': '#6366F1',
        'reporting': '#14B8A6',
    }};
    const STAGE_FALLBACK_COLORS = ['#0EA5E9', '#F97316', '#A855F7', '#EF4444', '#84CC16', '#E879F9', '#FB923C', '#2DD4BF'];

    function getStageColor(stage, idx) {{
        return STAGE_COLORS[stage.toLowerCase()] || STAGE_FALLBACK_COLORS[idx % STAGE_FALLBACK_COLORS.length];
    }}

    function renderStageCharts(runs) {{
        const section = document.getElementById('stageSection');
        const runIds = new Set(runs.map(r => r.run_id));
        const relevant = DATA.task_results.filter(tr => runIds.has(tr.run_id));

        // Detect stages
        const stages = [...new Set(relevant.map(tr => tr.stage).filter(Boolean))];
        if (stages.length === 0) {{
            section.style.display = 'none';
            return;
        }}
        section.style.display = 'block';

        // Sort stages in a logical pipeline order if possible
        const stageOrder = ['extract', 'input', 'transform', 'analysis', 'load', 'export', 'reporting'];
        stages.sort((a, b) => {{
            const ai = stageOrder.indexOf(a.toLowerCase());
            const bi = stageOrder.indexOf(b.toLowerCase());
            if (ai !== -1 && bi !== -1) return ai - bi;
            if (ai !== -1) return -1;
            if (bi !== -1) return 1;
            return a.localeCompare(b);
        }});

        // --- Chart 1: Stage total duration per run (stacked bar) ---
        const labels = runs.map(r => formatDate(r.start_time));
        const durationDatasets = stages.map((stage, idx) => {{
            const data = runs.map(run => {{
                const tasks = relevant.filter(tr => tr.run_id === run.run_id && tr.stage === stage && tr.duration_seconds != null);
                return tasks.reduce((sum, t) => sum + t.duration_seconds, 0);
            }});
            return {{
                label: stage,
                data: data.map(d => Math.round(d * 100) / 100),
                backgroundColor: getStageColor(stage, idx),
                borderRadius: 2,
            }};
        }});

        charts.stageDuration = new Chart(document.getElementById('stageDurationChart').getContext('2d'), {{
            type: 'bar',
            data: {{ labels, datasets: durationDatasets }},
            options: {{
                responsive: true, maintainAspectRatio: false,
                plugins: {{
                    legend: {{ position: 'top' }},
                    tooltip: {{
                        callbacks: {{
                            label: function(item) {{
                                return `${{item.dataset.label}}: ${{item.raw.toFixed(1)}}s total`;
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{ beginAtZero: true, stacked: true, title: {{ display: true, text: 'Total Duration (s)' }} }},
                    x: {{ stacked: true, title: {{ display: true, text: 'Run' }} }}
                }}
            }}
        }});

        // --- Chart 2: Stage avg duration per task (grouped bar) ---
        const avgDatasets = stages.map((stage, idx) => {{
            const tasks = relevant.filter(tr => tr.stage === stage && tr.duration_seconds != null && tr.status === 'Success');
            const avgDur = tasks.length > 0 ? tasks.reduce((s, t) => s + t.duration_seconds, 0) / tasks.length : 0;
            return {{
                stage: stage,
                avg: Math.round(avgDur * 100) / 100,
                count: tasks.length,
                color: getStageColor(stage, idx),
            }};
        }});

        charts.stageAvg = new Chart(document.getElementById('stageAvgChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: avgDatasets.map(d => d.stage),
                datasets: [{{
                    label: 'Avg Duration (s)',
                    data: avgDatasets.map(d => d.avg),
                    backgroundColor: avgDatasets.map(d => d.color),
                    borderRadius: 4,
                }}]
            }},
            options: {{
                responsive: true, maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            afterLabel: function(item) {{
                                const d = avgDatasets[item.dataIndex];
                                return `Tasks: ${{d.count}}`;
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{ beginAtZero: true, title: {{ display: true, text: 'Avg Duration per Task (s)' }} }},
                    x: {{ title: {{ display: true, text: 'Stage' }} }}
                }}
            }}
        }});
    }}

    // Task table state
    let taskTablePage = 0;
    let taskTableRows = [];
    let taskTableSortCol = 3; // default sort by Avg
    let taskTableSortAsc = false; // default descending

    function computeTaskRows(runs) {{
        const runIds = new Set(runs.map(r => r.run_id));
        const taskData = {{}};
        DATA.task_results.filter(tr => runIds.has(tr.run_id)).forEach(tr => {{
            const sig = tr.task_signature;
            if (!taskData[sig]) {{
                taskData[sig] = {{
                    task_id: tr.task_id, instance: tr.instance, process: tr.process,
                    durations: [], successes: 0, total: 0
                }};
            }}
            taskData[sig].total++;
            if (tr.status === 'Success') taskData[sig].successes++;
            if (tr.duration_seconds != null) taskData[sig].durations.push(tr.duration_seconds);
        }});

        return Object.values(taskData).map(t => {{
            const d = t.durations;
            const avg = d.length ? d.reduce((a,b) => a+b, 0) / d.length : 0;
            const mn = d.length ? Math.min(...d) : 0;
            const mx = d.length ? Math.max(...d) : 0;
            const stdDev = d.length > 1 ? Math.sqrt(d.map(x => (x-avg)**2).reduce((a,b) => a+b, 0) / (d.length-1)) : 0;
            const successRate = t.total > 0 ? (t.successes / t.total * 100) : 0;
            const cv = avg > 0 ? stdDev / avg : 0;
            return {{ ...t, avg, mn, mx, stdDev, successRate, cv }};
        }});
    }}

    function applyTaskSort(rows) {{
        const fields = ['task_id', 'instance', 'process', 'avg', 'mn', 'mx', 'stdDev', 'successRate', 'total'];
        const field = fields[taskTableSortCol];
        rows.sort((a, b) => {{
            const av = a[field], bv = b[field];
            if (typeof av === 'number' && typeof bv === 'number') return taskTableSortAsc ? av - bv : bv - av;
            return taskTableSortAsc ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
        }});
        return rows;
    }}

    function sortTaskTable(colIdx) {{
        if (taskTableSortCol === colIdx) {{
            taskTableSortAsc = !taskTableSortAsc;
        }} else {{
            taskTableSortCol = colIdx;
            taskTableSortAsc = (colIdx < 3); // text cols ascending, numeric descending by default
        }}
        taskTablePage = 0;
        // Update sort arrows
        document.querySelectorAll('#taskTable th').forEach((h, i) => {{
            const arrow = h.querySelector('.sort-arrow');
            if (arrow) arrow.textContent = i === colIdx ? (taskTableSortAsc ? '\\u25B2' : '\\u25BC') : '';
        }});
        renderTaskTablePage();
    }}

    function updateTaskTable(runs) {{
        taskTableRows = computeTaskRows(runs);
        taskTablePage = 0;
        renderTaskTablePage();
    }}

    function renderTaskTablePage() {{
        const tbody = document.getElementById('taskTableBody');
        const info = document.getElementById('taskTableInfo');
        const pageSize = parseInt(document.getElementById('taskPageSize').value) || 0;
        const sorted = applyTaskSort([...taskTableRows]);
        const total = sorted.length;

        let pageRows;
        if (pageSize === 0) {{
            pageRows = sorted;
            taskTablePage = 0;
        }} else {{
            const maxPage = Math.max(0, Math.ceil(total / pageSize) - 1);
            taskTablePage = Math.min(taskTablePage, maxPage);
            const start = taskTablePage * pageSize;
            pageRows = sorted.slice(start, start + pageSize);
        }}

        // Update info and buttons
        if (pageSize === 0) {{
            info.textContent = `${{total}} tasks`;
        }} else {{
            const start = taskTablePage * pageSize + 1;
            const end = Math.min(start + pageSize - 1, total);
            info.textContent = `${{start}}-${{end}} of ${{total}} tasks`;
        }}
        document.getElementById('taskPrevBtn').disabled = (taskTablePage <= 0 || pageSize === 0);
        document.getElementById('taskNextBtn').disabled = (pageSize === 0 || (taskTablePage + 1) * pageSize >= total);

        tbody.innerHTML = pageRows.map(r => {{
            const cvClass = r.cv > 0.5 ? 'high-cv' : '';
            const statusCls = r.successRate < 100 ? 'status-fail' : 'status-success';
            return `<tr class="${{cvClass}}">
                <td class="mono">${{r.task_id}}</td>
                <td>${{r.instance}}</td>
                <td>${{r.process}}</td>
                <td class="mono">${{r.avg.toFixed(2)}}</td>
                <td class="mono">${{r.mn.toFixed(2)}}</td>
                <td class="mono">${{r.mx.toFixed(2)}}</td>
                <td class="mono">${{r.stdDev.toFixed(2)}}</td>
                <td class="${{statusCls}}">${{r.successRate.toFixed(1)}}%</td>
                <td>${{r.total}}</td>
            </tr>`;
        }}).join('');
    }}

    function updateOutlierTable(runs) {{
        const tbody = document.getElementById('outlierTableBody');
        const runIds = new Set(runs.map(r => r.run_id));
        const relevant = DATA.task_results.filter(tr => runIds.has(tr.run_id) && tr.duration_seconds != null);

        // Compute median for selected runs
        const durations = relevant.filter(tr => tr.status === 'Success').map(tr => tr.duration_seconds).sort((a,b) => a-b);
        const median = durations.length > 0 ? durations[Math.floor(durations.length / 2)] : 0;

        // Sort by duration desc and take top 10
        const sorted = [...relevant].sort((a,b) => b.duration_seconds - a.duration_seconds).slice(0, 10);

        tbody.innerHTML = sorted.map(o => {{
            const statusCls = o.status === 'Success' ? 'status-success' : 'status-fail';
            const vsMedian = o.duration_seconds - median;
            return `<tr>
                <td class="mono">${{o.task_id}}</td>
                <td>${{o.process}}</td>
                <td>${{o.instance}}</td>
                <td class="mono">${{o.run_id}}</td>
                <td class="mono duration-slow">${{o.duration_seconds.toFixed(2)}}</td>
                <td class="mono">+${{vsMedian.toFixed(2)}}</td>
                <td class="${{statusCls}}">${{o.status}}</td>
            </tr>`;
        }}).join('');

        if (sorted.length === 0) {{
            tbody.innerHTML = '<tr><td colspan="7" class="no-data">No data available</td></tr>';
        }}
    }}

    function updateFailureTable(runs) {{
        const panel = document.getElementById('failuresPanel');
        const tbody = document.getElementById('failureTableBody');
        const runIds = new Set(runs.map(r => r.run_id));
        const filtered = DATA.task_results.filter(tr => runIds.has(tr.run_id) && tr.status !== 'Success');

        if (filtered.length === 0) {{
            panel.style.display = 'none';
            return;
        }}

        panel.style.display = 'block';
        tbody.innerHTML = filtered.map(f => `<tr>
            <td class="mono">${{f.task_id}}</td>
            <td>${{f.process}}</td>
            <td>${{f.instance}}</td>
            <td class="mono">${{f.run_id}}</td>
            <td class="mono">${{f.duration_seconds != null ? f.duration_seconds.toFixed(2) : '-'}}</td>
            <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"
                title="${{(f.error_message || '').replace(/"/g, '&quot;')}}">${{f.error_message || '-'}}</td>
        </tr>`).join('');
    }}

    function updateConfigDetails(runs) {{
        const body = document.getElementById('configBody');
        body.innerHTML = runs.map(r => `
            <div style="margin-bottom:16px;">
                <div style="font-weight:600;margin-bottom:8px;">
                    ${{formatDate(r.start_time)}} — ${{r.run_id}} ${{statusBadge(r.status)}}
                </div>
                <div class="config-grid">
                    <div class="config-item"><div class="config-label">Duration</div><div class="config-value">${{formatTime(r.duration_seconds)}}</div></div>
                    <div class="config-item"><div class="config-label">Max Workers</div><div class="config-value">${{r.max_workers || '-'}}</div></div>
                    <div class="config-item"><div class="config-label">Tasks</div><div class="config-value">${{r.task_count}} (${{r.success_count}} ok, ${{r.failure_count || 0}} fail)</div></div>
                    <div class="config-item"><div class="config-label">Retries</div><div class="config-value">${{r.retries != null ? r.retries : '-'}}</div></div>
                    <div class="config-item"><div class="config-label">Optimize</div><div class="config-value">${{r.optimize ? 'Yes' : 'No'}}</div></div>
                    <div class="config-item"><div class="config-label">Exclusive</div><div class="config-value">${{r.exclusive ? 'Yes' : 'No'}}</div></div>
                    <div class="config-item"><div class="config-label">Taskfile Path</div><div class="config-value" style="font-size:0.8rem;word-break:break-all;">${{r.taskfile_path || '-'}}</div></div>
                </div>
            </div>
        `).join('');
    }}

    function toggleAccordion(header) {{
        const body = header.nextElementSibling;
        body.classList.toggle('open');
        header.querySelector('span').textContent = body.classList.contains('open') ? '\\u25B2' : '\\u25BC';
    }}

    function sortTable(tableId, colIdx) {{
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        if (rows.length === 0) return;

        // Determine current sort direction
        const th = table.querySelectorAll('th')[colIdx];
        const asc = th.dataset.sort !== 'asc';
        // Reset all arrows
        table.querySelectorAll('th').forEach(h => {{
            h.dataset.sort = '';
            const arrow = h.querySelector('.sort-arrow');
            if (arrow) arrow.textContent = '';
        }});
        th.dataset.sort = asc ? 'asc' : 'desc';
        const arrow = th.querySelector('.sort-arrow');
        if (arrow) arrow.textContent = asc ? '\\u25B2' : '\\u25BC';

        rows.sort((a, b) => {{
            const aVal = a.cells[colIdx].textContent.replace('%', '').trim();
            const bVal = b.cells[colIdx].textContent.replace('%', '').trim();
            const aNum = parseFloat(aVal);
            const bNum = parseFloat(bVal);
            if (!isNaN(aNum) && !isNaN(bNum)) {{
                return asc ? aNum - bNum : bNum - aNum;
            }}
            return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
        }});

        rows.forEach(row => tbody.appendChild(row));
    }}

    // Initialize
    init();
    </script>
</body>
</html>"""

    # Ensure output directory exists
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html_content, encoding="utf-8")

    return str(output)
