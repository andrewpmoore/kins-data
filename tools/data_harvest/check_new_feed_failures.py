#!/usr/bin/env python3
"""Report feeds that have newly changed into an error state.

The previous committed manifest is compared with the manifest produced by the
current harvest. A feed that was already failing is deliberately quiet; if it
recovers and later fails again, it becomes a new failure again.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any


def read_manifest(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict) or not isinstance(value.get("runStatus"), list):
        raise ValueError(f"Manifest has no runStatus list: {path}")
    return value


def statuses(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    for entry in manifest.get("runStatus", []):
        if isinstance(entry, dict) and isinstance(entry.get("source"), str):
            result[entry["source"]] = entry
    return result


def newly_failing(
    previous: dict[str, Any], current: dict[str, Any]
) -> list[dict[str, str]]:
    before = statuses(previous)
    failures = []
    for source, entry in statuses(current).items():
        if entry.get("status") != "error":
            continue
        if before.get(source, {}).get("status") == "error":
            continue
        failures.append(
            {
                "source": source,
                "detail": str(entry.get("detail") or "No error detail was supplied"),
            }
        )
    return sorted(failures, key=lambda value: value["source"])


def report(failures: list[dict[str, str]], date: str) -> str:
    lines = [
        f"The {date} data harvest found {len(failures)} newly failing feed(s).",
        "",
    ]
    for failure in failures:
        lines.append(f"- **{failure['source']}** — {failure['detail']}")
    lines.extend(
        [
            "",
            "This alert is state-aware: the same feed will stay quiet while it remains failed, "
            "and will alert again only after it recovers and subsequently fails.",
        ]
    )
    return "\n".join(lines)


def write_github_output(values: dict[str, str]) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    delimiter = "KINS_FEED_FAILURE_OUTPUT"
    with Path(output_path).open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            if "\n" in value:
                handle.write(f"{key}<<{delimiter}\n{value}\n{delimiter}\n")
            else:
                handle.write(f"{key}={value}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous", required=True, type=Path)
    parser.add_argument("--current", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    previous = read_manifest(args.previous)
    current = read_manifest(args.current)
    failures = newly_failing(previous, current)
    date = str(current.get("latestDate") or current.get("generatedAt") or "unknown date")
    sources = ", ".join(value["source"] for value in failures)
    key_value = hashlib.sha256(f"{date}|{sources}".encode()).hexdigest()[:16]
    text = report(failures, date) if failures else "No newly failing feeds."
    write_github_output(
        {
            "count": str(len(failures)),
            "sources": sources,
            "alert_key": key_value,
            "date": date,
            "report": text,
        }
    )
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
