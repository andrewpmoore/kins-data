#!/usr/bin/env python3
"""Convert defensible legacy Kins data into the hosted v1 schema.

Only rank-one music rows and text-only birthday records are imported.  A chart
date with conflicting rank-one rows is omitted instead of guessing which old
scrape was correct.  Each chart observation is valid for at most seven days,
which matches the weekly lists represented by the legacy files without filling
large gaps with stale values.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
COUNTRIES = {
    "AU": ("au", "https://en.wikipedia.org/wiki/List_of_number-one_singles_in_Australia"),
    "CA": ("ca", "https://en.wikipedia.org/wiki/List_of_number-one_singles_in_Canada"),
    "DE": ("de", "https://en.wikipedia.org/wiki/List_of_number-one_hits_(Germany)"),
    "ES": ("es", "https://en.wikipedia.org/wiki/List_of_number-one_hits_(Spain)"),
    "FR": ("fr", "https://en.wikipedia.org/wiki/List_of_number-one_singles_in_France"),
    "GB": ("uk", "https://en.wikipedia.org/wiki/Lists_of_UK_Singles_Chart_number_ones"),
    "IE": ("ie", "https://en.wikipedia.org/wiki/List_of_songs_that_reached_number_one_on_the_Irish_Singles_Chart"),
    "IT": ("it", "https://en.wikipedia.org/wiki/List_of_number-one_hits_(Italy)"),
    "NZ": ("nz", "https://en.wikipedia.org/wiki/List_of_number-one_singles_in_New_Zealand"),
    "US": ("us", "https://en.wikipedia.org/wiki/Lists_of_Billboard_number-one_singles"),
}
MONTH_KEYS = [
    "january_birthdays", "february_birthdays", "march_birthdays", "april_birthdays",
    "may_birthdays", "june_birthdays", "july_birthdays", "august_birthdays",
    "september_birthdays", "october_birthdays", "november_birthdays", "december_birthdays",
]
BIRTHDAY_SOURCE_URL = (
    "https://github.com/andrewpmoore/kins-data/blob/main/"
    "data/birthdays/birthdays_processed.json"
)


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
        handle.write(payload)
        temporary = Path(handle.name)
    temporary.replace(path)


def clean_text(value: str | None) -> str:
    return " ".join((value or "").strip().strip('"').split())


def identity(title: str, artist: str) -> tuple[str, str]:
    return title.casefold(), artist.casefold()


def read_rank_one_rows(path: Path) -> tuple[list[tuple[dt.date, str, str]], int]:
    """Return unambiguous date/title/artist rows and the ambiguity count."""
    by_date: dict[dt.date, dict[tuple[str, str], tuple[str, str]]] = defaultdict(dict)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if clean_text(row.get("rank")) != "1":
                continue
            try:
                date = dt.date.fromisoformat(clean_text(row.get("date")))
            except ValueError:
                continue
            title = clean_text(row.get("song") or row.get("title"))
            artist = clean_text(row.get("artist"))
            if not title or not artist or date.year < 1940 or date > dt.date.today():
                continue
            by_date[date][identity(title, artist)] = (title, artist)

    accepted = []
    ambiguous = 0
    for date, values in sorted(by_date.items()):
        if len(values) != 1:
            ambiguous += 1
            continue
        title, artist = next(iter(values.values()))
        accepted.append((date, title, artist))
    return accepted, ambiguous


def chart_periods(rows: list[tuple[dt.date, str, str]]) -> list[dict[str, Any]]:
    periods: list[dict[str, Any]] = []
    for index, (start, title, artist) in enumerate(rows):
        end = start + dt.timedelta(days=6)
        if index + 1 < len(rows):
            end = min(end, rows[index + 1][0] - dt.timedelta(days=1))
        if end < start:
            continue
        item = {"rank": 1, "title": title, "artist": artist}
        if (
            periods
            and periods[-1]["items"] == [item]
            and dt.date.fromisoformat(periods[-1]["endDate"]) + dt.timedelta(days=1) == start
        ):
            periods[-1]["endDate"] = end.isoformat()
        else:
            periods.append({"startDate": start.isoformat(), "endDate": end.isoformat(), "items": [item]})
    return periods


def split_periods_by_year(periods: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    result: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for period in periods:
        start = dt.date.fromisoformat(period["startDate"])
        end = dt.date.fromisoformat(period["endDate"])
        for year in range(start.year, end.year + 1):
            clipped_start = max(start, dt.date(year, 1, 1))
            clipped_end = min(end, dt.date(year, 12, 31))
            result[year].append(
                {
                    "startDate": clipped_start.isoformat(),
                    "endDate": clipped_end.isoformat(),
                    "items": period["items"],
                }
            )
    return result


def content_hash(value: Any) -> str:
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def import_music(data_root: Path, output: Path) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    for country, (legacy_code, source_url) in COUNTRIES.items():
        country_output = output / "v1" / "history" / country.lower()
        for stale in country_output.glob("*.json"):
            stale.unlink()
        rows, ambiguous = read_rank_one_rows(data_root / "music" / f"music_{legacy_code}.csv")
        periods = chart_periods(rows)
        by_year = split_periods_by_year(periods)
        observed_at = rows[-1][0].isoformat() if rows else "unknown"
        for year, year_periods in by_year.items():
            chart = {
                "id": "legacy-national-number-one-single",
                "title": "Historical number-one single",
                "source": {
                    "provider": "Wikipedia contributors",
                    "url": source_url,
                    "territory": country,
                    "metric": "historical number-one singles chart; legacy rows with conflicting dates omitted",
                    "observedAt": observed_at,
                    "kind": "nationalSinglesChart",
                },
                "periods": year_periods,
            }
            value = {
                "schemaVersion": SCHEMA_VERSION,
                "country": country.lower(),
                "year": year,
                "charts": {"music": chart},
            }
            value["contentHash"] = content_hash(value["charts"])
            atomic_json(country_output / f"{year}.json", value)
        summary[country] = {
            "acceptedAnchors": len(rows),
            "ambiguousDatesOmitted": ambiguous,
            "periods": len(periods),
            "years": len(by_year),
        }
    return summary


def import_birthdays(path: Path, output: Path) -> int:
    raw = json.loads(path.read_text(encoding="utf-8"))
    birthday_output = output / "v1" / "editorial" / "birthday-twins"
    for stale in birthday_output.glob("*.json"):
        stale.unlink()
    occurrences: dict[tuple[str, int], set[tuple[int, str]]] = defaultdict(set)
    if isinstance(raw, dict):
        for month, month_key in enumerate(MONTH_KEYS, start=1):
            for day, records in raw.get(month_key, {}).items():
                for record in records if isinstance(records, list) else []:
                    if not isinstance(record, dict) or not isinstance(record.get("birth_year"), int):
                        continue
                    name = clean_text(record.get("name"))
                    occurrences[(name.casefold(), record["birth_year"])].add((month, str(day).zfill(2)))
    total = 0
    for month, key in enumerate(MONTH_KEYS, start=1):
        days: dict[str, list[dict[str, Any]]] = {}
        source_days = raw.get(key, {}) if isinstance(raw, dict) else {}
        for day, records in sorted(source_days.items()):
            accepted = []
            seen: set[tuple[str, int]] = set()
            for record in records if isinstance(records, list) else []:
                name = clean_text(record.get("name")) if isinstance(record, dict) else ""
                occupation = clean_text(record.get("occupation")) if isinstance(record, dict) else ""
                born_year = record.get("birth_year") if isinstance(record, dict) else None
                if not name or not occupation or not isinstance(born_year, int) or not 1000 <= born_year <= dt.date.today().year:
                    continue
                key_value = (name.casefold(), born_year)
                # The legacy set contains a few people on more than one day. We
                # cannot infer which date is right, so omit every conflicting copy.
                if key_value in seen or len(occurrences[key_value]) != 1:
                    continue
                seen.add(key_value)
                accepted.append(
                    {
                        "name": name,
                        "bornYear": born_year,
                        "description": occupation,
                        "sourceURL": BIRTHDAY_SOURCE_URL,
                    }
                )
            if accepted:
                days[day.zfill(2)] = accepted
                total += len(accepted)
        value = {
            "schemaVersion": SCHEMA_VERSION,
            "month": month,
            "provenanceStatus": "legacyEditorialUnverified",
            "source": {
                "provider": "Kins legacy editorial dataset",
                "url": BIRTHDAY_SOURCE_URL,
                "metric": "selected people by birthday; text fields only",
                "observedAt": "legacy-import",
                "kind": "birthdayReference",
            },
            "days": days,
        }
        value["contentHash"] = content_hash(value["days"])
        atomic_json(birthday_output / f"{month:02d}.json", value)
    return total


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=root / "data")
    parser.add_argument("--output", type=Path, default=root / "hosted-data")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = import_music(args.data_root, args.output)
    birthdays = import_birthdays(args.data_root / "birthdays" / "birthdays_processed.json", args.output)
    for country, values in summary.items():
        print(f"{country}: {values['periods']} periods across {values['years']} years; "
              f"{values['ambiguousDatesOmitted']} ambiguous dates omitted")
    print(f"Birthday records: {birthdays} text-only entries")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
