#!/usr/bin/env python3
"""Record daily feed results and render the GitHub Pages health dashboard."""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HISTORY = ROOT / "hosted-data" / "feed-health"

SOURCE_LABELS = {
    "books/GB/adultBooks": "Books · UK adult (NIQ annual snapshot)",
    "books/GB/childrensBooks": "Books · UK children & YA (NIQ annual snapshot)",
    "mediaTelevision/GB": "TV · UK (BARB annual audience)",
    "mediaTelevision/ES": "TV · Spain (RTVE annual audience)",
}


def source_label(source: str) -> str:
    return SOURCE_LABELS.get(source, source)


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return value


def record_snapshot(args: argparse.Namespace) -> None:
    manifest = read_json(args.manifest)
    statuses = manifest.get("runStatus")
    if not isinstance(statuses, list):
        raise ValueError(f"Manifest has no runStatus list: {args.manifest}")
    entries = []
    for entry in statuses:
        if not isinstance(entry, dict) or not isinstance(entry.get("source"), str):
            continue
        status = entry.get("status")
        if status not in {"ok", "error"}:
            continue
        item: dict[str, str] = {"source": entry["source"], "status": status}
        if entry.get("detail"):
            item["detail"] = str(entry["detail"])
        entries.append(item)

    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    snapshot = {
        "date": args.date,
        "generatedAt": str(manifest.get("generatedAt") or now.isoformat().replace("+00:00", "Z")),
        "workflowRunURL": os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        + "/"
        + os.environ.get("GITHUB_REPOSITORY", "andrewpmoore/kins-data")
        + "/actions/runs/"
        + os.environ.get("GITHUB_RUN_ID", ""),
        "runStatus": sorted(entries, key=lambda item: item["source"]),
    }
    args.history.mkdir(parents=True, exist_ok=True)
    path = args.history / f"{args.date}.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)
    print(f"Recorded {len(entries)} feeds in {path}")


def latest_run_status_by_source(snapshot: dict[str, Any]) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for entry in snapshot.get("runStatus", []):
        if isinstance(entry, dict) and isinstance(entry.get("source"), str):
            if entry.get("status") in {"ok", "error"}:
                result[entry["source"]] = entry
    return result


def render(args: argparse.Namespace) -> None:
    today = dt.date.fromisoformat(args.today) if args.today else dt.datetime.now(dt.timezone.utc).date()
    dates = [today - dt.timedelta(days=offset) for offset in range(27, -1, -1)]
    snapshots: dict[str, dict[str, Any]] = {}
    feeds: set[str] = set()
    for day in dates:
        path = args.history / f"{day.isoformat()}.json"
        if not path.exists():
            continue
        snapshot = read_json(path)
        snapshots[day.isoformat()] = snapshot
        feeds.update(latest_run_status_by_source(snapshot))

    # Before the UK books had their own rows, the daily manifest exposed only
    # annual/GB. Hide that legacy aggregate once its more useful split is present.
    if any(source.startswith("books/GB/") for source in feeds):
        feeds.discard("annual/GB")

    rows: list[str] = []
    latest_date = dates[-1].isoformat()
    latest_statuses = latest_run_status_by_source(snapshots.get(latest_date, {}))
    for source in sorted(feeds, key=str.casefold):
        cells = []
        for day in dates:
            date_key = day.isoformat()
            entry = latest_run_status_by_source(snapshots.get(date_key, {})).get(source)
            if entry is None:
                cells.append('<td class="empty" aria-label="No feed result">·</td>')
                continue
            run_url = snapshots[date_key].get("workflowRunURL")
            detail = entry.get("detail") or ("Feed succeeded" if entry["status"] == "ok" else "Feed failed")
            label = f"{source_label(source)} — {detail}"
            symbol = "✓" if entry["status"] == "ok" else "×"
            css = "success" if entry["status"] == "ok" else "failure"
            mark = f'<span class="{css}" title="{html.escape(label, quote=True)}" aria-label="{html.escape(label, quote=True)}">{symbol}</span>'
            if run_url:
                mark = f'<a href="{html.escape(str(run_url), quote=True)}" title="{html.escape(label, quote=True)}">{mark}</a>'
            cells.append(f'<td class="{css}-cell">{mark}</td>')
        latest = latest_statuses.get(source)
        state = '<span class="state-good">Healthy</span>' if latest and latest["status"] == "ok" else (
            '<span class="state-bad">Failed</span>' if latest and latest["status"] == "error" else
            '<span class="state-quiet">No run today</span>'
        )
        rows.append(f'<tr><th scope="row" title="{html.escape(source, quote=True)}">{html.escape(source_label(source))}</th><td class="current">{state}</td>{"".join(cells)}</tr>')

    headers = "".join(
        f'<th scope="col" title="{day.strftime("%A, %d %B %Y")}"><span>{day.strftime("%a")}</span><br>{day.strftime("%d")}</th>'
        for day in dates
    )
    if not rows:
        body_rows = '<tr><td colspan="30" class="empty-state">No feed history has been recorded yet. The grid will fill in after the next successful harvest.</td></tr>'
    else:
        body_rows = "".join(rows)
    generated = dt.datetime.now(dt.timezone.utc).strftime("%d %b %Y at %H:%M UTC")
    today_snapshot = snapshots.get(latest_date)
    today_run = f'<a href="{html.escape(str(today_snapshot["workflowRunURL"]), quote=True)}">Open today’s harvest run</a>' if today_snapshot and today_snapshot.get("workflowRunURL") else ""
    title_dates = f'{dates[0].strftime("%d %b")} – {dates[-1].strftime("%d %b %Y")} UTC'
    document = f'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light">
  <title>Kins data feed health</title>
  <style>
    :root {{ color-scheme: light; --ink: #17231f; --muted: #66736d; --line: #e3e9e5; --paper: #fff; --canvas: #f4f7f5; --green: #18714c; --red: #b53f46; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: var(--canvas); color: var(--ink); font: 15px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    main {{ max-width: 1500px; margin: 0 auto; padding: 38px 28px 56px; }}
    .eyebrow {{ margin: 0 0 8px; color: var(--green); font-size: 12px; font-weight: 750; letter-spacing: .12em; text-transform: uppercase; }}
    h1 {{ margin: 0; font-size: clamp(28px, 4vw, 42px); letter-spacing: -.04em; }}
    .intro {{ margin: 9px 0 25px; color: var(--muted); }}
    .overview {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: stretch; margin-bottom: 18px; }}
    .stat {{ min-width: 150px; padding: 14px 17px; background: var(--paper); border: 1px solid var(--line); border-radius: 13px; }}
    .stat strong {{ display: block; font-size: 23px; line-height: 1.1; }}
    .stat span {{ display: block; margin-top: 5px; color: var(--muted); font-size: 12px; }}
    .key {{ display: flex; flex-wrap: wrap; gap: 16px; margin: 0 0 13px; color: var(--muted); font-size: 13px; }}
    .key b {{ font-size: 16px; margin-right: 4px; }}
    .success {{ color: var(--green); font-weight: 800; }} .failure {{ color: var(--red); font-weight: 800; }} .empty {{ color: #a5afaa; }}
    .table-wrap {{ overflow: auto; max-height: calc(100vh - 275px); background: var(--paper); border: 1px solid var(--line); border-radius: 13px; box-shadow: 0 8px 24px #243b3010; }}
    table {{ width: max-content; min-width: 100%; border-collapse: separate; border-spacing: 0; font-variant-numeric: tabular-nums; }}
    th, td {{ height: 42px; padding: 8px 9px; border-right: 1px solid var(--line); border-bottom: 1px solid var(--line); text-align: center; }}
    thead th {{ position: sticky; top: 0; z-index: 2; height: 53px; background: #f8faf9; color: var(--muted); font-size: 11px; font-weight: 650; }}
    thead th span {{ font-size: 10px; text-transform: uppercase; }}
    th:first-child {{ position: sticky; left: 0; z-index: 3; min-width: 255px; max-width: 360px; background: #fff; text-align: left; }}
    thead th:first-child {{ z-index: 4; background: #f8faf9; }}
    th:nth-child(2), td.current {{ position: sticky; left: 255px; z-index: 2; min-width: 118px; background: #fff; }}
    thead th:nth-child(2) {{ z-index: 3; background: #f8faf9; }}
    td.current {{ font-size: 12px; }}
    td {{ min-width: 42px; }}
    tbody tr:hover th, tbody tr:hover td {{ background-color: #f7faf8; }}
    tbody tr:hover th:first-child, tbody tr:hover td.current {{ background-color: #f7faf8; }}
    td a {{ display: block; text-decoration: none; }}
    td a:hover {{ transform: scale(1.15); }}
    .state-good {{ color: var(--green); }} .state-bad {{ color: var(--red); font-weight: 700; }} .state-quiet {{ color: var(--muted); }}
    .empty-state {{ padding: 34px; color: var(--muted); text-align: center; }}
    footer {{ display: flex; flex-wrap: wrap; justify-content: space-between; gap: 10px; margin-top: 14px; color: var(--muted); font-size: 12px; }}
    footer a {{ color: var(--green); }}
    @media (max-width: 700px) {{ main {{ padding: 26px 14px 40px; }} .table-wrap {{ max-height: calc(100vh - 310px); }} th:first-child {{ min-width: 205px; }} th:nth-child(2), td.current {{ left: 205px; }} .stat {{ min-width: 125px; }} }}
  </style>
</head>
<body>
  <main>
    <p class="eyebrow">Kins · hosted data</p>
    <h1>Data feed health</h1>
    <p class="intro">Daily results for the 28 most recent UTC dates · {html.escape(title_dates)}. Hover over a mark for its detail or open the run.</p>
    <section class="overview" aria-label="Summary">
      <div class="stat"><strong>{len(feeds)}</strong><span>tracked feeds</span></div>
      <div class="stat"><strong>{sum(1 for entry in latest_statuses.values() if entry["status"] == "ok")}</strong><span>successful today</span></div>
      <div class="stat"><strong>{sum(1 for entry in latest_statuses.values() if entry["status"] == "error")}</strong><span>failed today</span></div>
      <div class="stat"><strong>{len(snapshots)} / 28</strong><span>days with a recorded run</span></div>
    </section>
    <div class="key"><span><b class="success">✓</b> Feed succeeded</span><span><b class="failure">×</b> Feed failed</span><span><b class="empty">·</b> No result recorded</span></div>
    <div class="table-wrap" role="region" aria-label="Feed status by day" tabindex="0">
      <table>
        <thead><tr><th scope="col">Data feed</th><th scope="col">Latest</th>{headers}</tr></thead>
        <tbody>{body_rows}</tbody>
      </table>
    </div>
    <footer><span>Generated {html.escape(generated)} · Dates use UTC.</span><span>{today_run}</span></footer>
  </main>
</body>
</html>
'''
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(document, encoding="utf-8")
    print(f"Rendered dashboard for {len(feeds)} feeds and {len(snapshots)} recorded days: {args.output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    snapshot = subparsers.add_parser("snapshot", help="save the current manifest for a UTC date")
    snapshot.add_argument("--manifest", type=Path, default=ROOT / "hosted-data" / "manifest.json")
    snapshot.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    snapshot.add_argument("--date", default=dt.datetime.now(dt.timezone.utc).date().isoformat())
    snapshot.set_defaults(handler=record_snapshot)
    page = subparsers.add_parser("render", help="render the static 28-day dashboard")
    page.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    page.add_argument("--output", type=Path, default=ROOT / "_site" / "index.html")
    page.add_argument("--today", help="override the final date (YYYY-MM-DD), for deterministic previews")
    page.set_defaults(handler=render)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
