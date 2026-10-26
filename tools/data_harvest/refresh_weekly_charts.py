#!/usr/bin/env python3
"""Append newly published weekly number-one singles and films to the legacy CSVs.

The reviewed CSVs in ``data/music`` and ``data/movies`` feed
``import_legacy_data.py``, which publishes ``v1/history/{country}/{year}.json``.
This script keeps those CSVs growing every week without a code change for a new
year:

* music: the current (and, early in January, previous) year's Wikipedia
  number-one list for each country's official singles chart;
* films: Box Office Mojo's country weekend index for the same years.

Rows are append-only. A row is accepted only when it is newer than the last
reviewed row, uses the same weekday anchor as the existing file and has a
non-empty title. Existing history is never rewritten, so a transient upstream
mistake cannot damage reviewed years. Each source reports ok/error into the
manifest; a feed that stops producing weeks for ``STALE_DAYS`` is an error so the
daily workflow raises an alert instead of silently freezing the archive.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import re
import urllib.parse
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable

try:
    from . import harvest
except ImportError:  # Executed directly from the workflow.
    import harvest

STALE_DAYS = 24
MONTHS = {name.lower(): index for index, name in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
MONTHS.update({name[:3]: index for name, index in list(MONTHS.items())})
MONTHS["sept"] = 9

MUSIC_SOURCES: dict[str, tuple[str, Callable[[int], str]]] = {
    "us": ("US", lambda year: f"List of Billboard Hot 100 number ones of {year}"),
    "uk": ("GB", lambda year: f"List of UK singles chart number ones of the {year // 10 * 10}s"),
    "fr": ("FR", lambda year: f"List of number-one hits of {year} (France)"),
    "de": ("DE", lambda year: f"List of number-one hits of {year} (Germany)"),
    "it": ("IT", lambda year: f"List of number-one hits of {year} (Italy)"),
    "es": ("ES", lambda year: f"List of number-one singles of {year} (Spain)"),
}
MOVIE_SOURCES: dict[str, tuple[str, str]] = {
    "us": ("US", ""),
    "uk": ("GB", "GB"),
    "fr": ("FR", "FR"),
    "de": ("DE", "DE"),
    "it": ("IT", "IT"),
    "es": ("ES", "ES"),
}


# --------------------------------------------------------------------------- text


def clean(value: str) -> str:
    text = html.unescape(value or "")
    text = re.sub(r"\[(?:\d+|[a-zA-Z]{1,3}|nb\s*\d+|note\s*\d+)\]", "", text)
    text = re.sub(r"[†‡♦♪◁*¤§]+", "", text)
    text = " ".join(text.split()).strip()
    text = re.sub(r'"\s*/\s*"', " / ", text)
    return text.strip().strip('"“”«»').strip()


def clean_film(value: str) -> str:
    """Box Office Mojo appends re-release labels directly to some titles."""
    text = clean(value)
    text = re.sub(r"\s*\d{4}\s*Re-release$", "", text)
    text = re.sub(r"\s*Re-release$", "", text)
    return text.strip()


# --------------------------------------------------------------------- csv files


def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), [row for row in reader]


def write_rows(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def last_anchor(rows: list[dict[str, str]]) -> dt.date | None:
    dates = []
    for row in rows:
        try:
            dates.append(dt.date.fromisoformat(row.get("date", "")))
        except ValueError:
            continue
    return max(dates) if dates else None


def append_new(
    path: Path,
    parsed: dict[dt.date, dict[str, str]],
    title_field: str,
) -> tuple[int, dt.date | None]:
    """Append parsed weeks newer than the file's last anchor. Returns (added, last)."""
    fields, rows = read_rows(path)
    last = last_anchor(rows)
    if last is None:
        raise harvest.HarvestError(f"{path.name} has no dated rows to extend")
    weekday = last.weekday()
    added = 0
    for date in sorted(parsed):
        if date <= last or date.weekday() != weekday:
            continue
        value = parsed[date]
        if not value.get(title_field):
            continue
        rows.append({"date": date.isoformat(), "rank": "1", **value})
        added += 1
        last = date
    if added:
        write_rows(path, fields, rows)
    return added, last


# ------------------------------------------------------------------ html tables


class TableParser(HTMLParser):
    """Collect wikitable rows, expanding rowspan/colspan into a rectangular grid.

    Each table is returned as ``(header_row_count, grid)``; leading rows made
    only of ``<th>`` cells are headers (charts often use two header rows).
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[tuple[int, list[list[str]]]] = []
        self._depth = 0
        self._rows: list[list[tuple[str, int, int]]] | None = None
        self._header_flags: list[bool] = []
        self._row: list[tuple[str, int, int]] | None = None
        self._row_all_th = True
        self._cell: list[str] | None = None
        self._span = (1, 1)
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if tag == "table":
            self._depth += 1
            if self._depth == 1 and "wikitable" in (values.get("class") or ""):
                self._rows, self._header_flags = [], []
        elif self._rows is None or self._depth != 1:
            return
        elif tag == "tr":
            self._row, self._row_all_th = [], True
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []
            self._row_all_th = self._row_all_th and tag == "th"
            self._span = (_int(values.get("rowspan")), _int(values.get("colspan")))
        elif tag in ("sup", "style") and self._cell is not None:
            self._skip += 1
        elif tag == "br" and self._cell is not None:
            self._cell.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag == "table":
            if self._depth == 1 and self._rows is not None:
                headers = 0
                for flag in self._header_flags:
                    if not flag:
                        break
                    headers += 1
                self.tables.append((headers, _expand(self._rows)))
                self._rows = None
            self._depth -= 1
        elif self._rows is None or self._depth != 1:
            return
        elif tag in ("sup", "style") and self._skip:
            self._skip -= 1
        elif tag in ("td", "th") and self._cell is not None and self._row is not None:
            self._row.append((clean("".join(self._cell)), *self._span))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self._rows.append(self._row)
                self._header_flags.append(self._row_all_th)
            self._row = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None and not self._skip:
            self._cell.append(data)


def _int(value: str | None) -> int:
    try:
        return max(1, min(int(str(value).strip()), 60))
    except (TypeError, ValueError):
        return 1


def _expand(raw_rows: list[list[tuple[str, int, int]]]) -> list[list[str]]:
    grid: list[list[str]] = []
    pending: dict[int, tuple[str, int]] = {}
    for raw in raw_rows:
        row: list[str] = []
        cells = list(raw)
        column = 0
        while cells or column in pending:
            if column in pending:
                text, remaining = pending.pop(column)
                row.append(text)
                if remaining > 1:
                    pending[column] = (text, remaining - 1)
                column += 1
                continue
            text, rowspan, colspan = cells.pop(0)
            for _ in range(colspan):
                row.append(text)
                if rowspan > 1:
                    pending[column] = (text, rowspan - 1)
                column += 1
        grid.append(row)
    return grid


def wikipedia_tables(title: str) -> list[tuple[int, list[list[str]]]]:
    url = "https://en.wikipedia.org/w/api.php?" + urllib.parse.urlencode(
        {"action": "parse", "page": title, "prop": "text", "format": "json",
         "redirects": 1, "formatversion": 2}
    )
    payload = json.loads(harvest.fetch(url))
    if "error" in payload:
        raise harvest.HarvestError(f"Wikipedia: {payload['error'].get('info', title)}")
    parser = TableParser()
    parser.feed(payload["parse"]["text"])
    return parser.tables


# ------------------------------------------------------------------ date parsing


def parse_date(text: str, year: int) -> dt.date | None:
    value = clean(text).replace(",", " ")
    full = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", value)
    if full:
        try:
            return dt.date(*map(int, full.groups()))
        except ValueError:
            return None
    explicit_year = re.search(r"\b(19|20)\d{2}\b", value)
    use_year = int(explicit_year.group(0)) if explicit_year else year
    day_month = re.search(r"\b(\d{1,2})\s+([A-Za-z]{3,9})\b", value)
    month_day = re.search(r"\b([A-Za-z]{3,9})\.?\s+(\d{1,2})\b", value)
    for match, day_index, month_index in ((day_month, 1, 2), (month_day, 2, 1)):
        if not match:
            continue
        month = MONTHS.get(match.group(month_index).lower().rstrip("."))
        if not month:
            continue
        try:
            return dt.date(use_year, month, int(match.group(day_index)))
        except ValueError:
            return None
    return None


def first_weekday(year: int, weekday: int) -> dt.date:
    start = dt.date(year, 1, 1)
    return start + dt.timedelta(days=(weekday - start.weekday()) % 7)


def column(headers: list[str], *needles: str, exclude: tuple[str, ...] = ()) -> int | None:
    for needle in needles:  # Needles are in priority order.
        for index, header in enumerate(headers):
            lower = header.lower()
            if needle in lower and not any(bad in lower for bad in exclude):
                return index
    return None


def music_weeks(
    tables: list[tuple[int, list[list[str]]]], year: int, anchor_weekday: int
) -> dict[dt.date, dict[str, str]]:
    """Read the page's main singles table into {anchor date: song/artist}."""
    result: dict[dt.date, dict[str, str]] = {}
    for header_count, table in tables:
        header_count = max(1, header_count)
        if len(table) <= header_count:
            continue
        width = max(len(row) for row in table[:header_count])
        headers = [
            " ".join(row[index] for row in table[:header_count] if index < len(row))
            for index in range(width)
        ]
        song = column(headers, "song", "single", "title", exclude=("album",))
        artist = column(headers, "artist", "performer")
        date = column(headers, "issue date", "date", "issue", "week", "chart",
                      exclude=("weeks at", "weeks on", "number of"))
        weeks = column(headers, "weeks at", "weeks on", "number of weeks")
        if song is None or artist is None or date is None:
            continue
        data = table[header_count:]
        for index, row in enumerate(data):
            if max(song, artist, date) >= len(row):
                continue
            title, performer, raw_date = clean(row[song]), clean(row[artist]), row[date]
            if not title or not performer or title.lower() in {"song", "single", "title", "no release"}:
                continue
            start = parse_date(raw_date, year)
            if start is not None and not re.search(r"\b(19|20)\d{2}\b", raw_date):
                # Year-less dates at the edges of an annual list belong to the
                # neighbouring year (e.g. a 2026 list opening on 26 December).
                if start.month == 12 and index < 3 and len(data) > 10:
                    start = start.replace(year=year - 1)
                elif start.month == 1 and index >= len(data) - 3 and len(data) > 10:
                    start = start.replace(year=year + 1)
            if start is None and re.fullmatch(r"\d{1,2}", raw_date.strip()):
                start = first_weekday(year, anchor_weekday) + dt.timedelta(weeks=int(raw_date) - 1)
            if start is None:
                continue
            count = 1
            if weeks is not None and weeks < len(row):
                found = re.search(r"\d+", row[weeks])
                count = max(1, min(int(found.group(0)), 60)) if found else 1
            for offset in range(count):
                result[start + dt.timedelta(weeks=offset)] = {"song": title, "artist": performer}
        if result:
            break  # The first matching table is the main singles chart.
    return result
# ------------------------------------------------------------- box office mojo


def box_office_weeks(area: str, year: int) -> dict[dt.date, dict[str, str]]:
    """Box Office Mojo's weekend index anchored on each weekend's Sunday."""
    url = f"https://www.boxofficemojo.com/weekend/by-year/{year}/"
    if area:
        url += f"?area={area}"
    page = harvest.fetch(url).decode("utf-8", "replace")
    result: dict[dt.date, dict[str, str]] = {}
    for raw in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.I | re.S):
        link = re.search(
            r'href="[^"]*/weekend/(\d{4})W(\d+)[^"]*"[^>]*>\s*([A-Z][a-z]{2})\s+(\d{1,2})', raw
        )
        release = re.search(
            r'mojo-field-type-release[^>]*>\s*<a[^>]*>(.*?)</a>', raw, re.I | re.S
        )
        if not link or not release:
            continue
        title = clean_film(re.sub(r"<[^>]+>", "", release.group(1)))
        if not title or title == "-":
            continue
        chart_year, week, month_name, day = link.groups()
        start_year = int(chart_year)
        # Week 1 can begin in the previous December (e.g. 2026W01 = Dec 30-Jan 1).
        if month_name == "Dec" and int(week) <= 1:
            start_year -= 1
        try:
            start = dt.date(start_year, MONTHS[month_name.lower()], int(day))
        except (KeyError, ValueError):
            continue
        sunday = start + dt.timedelta(days=(6 - start.weekday()) % 7)
        result[sunday] = {"film": title}
    return result


# ----------------------------------------------------------------------- runner


def years_to_check(today: dt.date) -> list[int]:
    # Early January lists can still be adding the previous December's weeks.
    return [today.year - 1, today.year] if today.timetuple().tm_yday <= 21 else [today.year]


def refresh(data_root: Path, today: dt.date) -> list[dict[str, str]]:
    status: list[dict[str, str]] = []
    years = years_to_check(today)

    for code, (country, page_title) in MUSIC_SOURCES.items():
        path = data_root / "music" / f"music_{code}.csv"
        source = f"weeklyMusic/{country}"
        try:
            _, rows = read_rows(path)
            last = last_anchor(rows)
            if last is None:
                raise harvest.HarvestError("no reviewed rows")
            parsed: dict[dt.date, dict[str, str]] = {}
            for year in years:
                parsed.update(music_weeks(wikipedia_tables(page_title(year)), year, last.weekday()))
            if not parsed:
                raise harvest.HarvestError("no number-one rows could be parsed")
            added, newest = append_new(path, parsed, "song")
            status.append(_status(source, added, newest, today))
        except (harvest.HarvestError, ValueError, KeyError, OSError) as error:
            status.append({"source": source, "status": "error", "detail": str(error)})

    for code, (country, area) in MOVIE_SOURCES.items():
        path = data_root / "movies" / f"movies_{code}.csv"
        source = f"weeklyMovies/{country}"
        try:
            parsed = {}
            for year in years:
                parsed.update(box_office_weeks(area, year))
            if not parsed:
                raise harvest.HarvestError("Box Office Mojo returned no weekends")
            added, newest = append_new(path, parsed, "film")
            status.append(_status(source, added, newest, today))
        except (harvest.HarvestError, ValueError, KeyError, OSError) as error:
            status.append({"source": source, "status": "error", "detail": str(error)})
    return status


def _status(source: str, added: int, newest: dt.date | None, today: dt.date) -> dict[str, str]:
    if newest is None or (today - newest).days > STALE_DAYS:
        return {"source": source, "status": "error",
                "detail": f"latest weekly row is {newest}; no new week for over {STALE_DAYS} days"}
    detail = f"added {added} week(s); latest {newest.isoformat()}" if added else f"up to date ({newest.isoformat()})"
    return {"source": source, "status": "ok", "detail": detail}


def record_status(output: Path, status: list[dict[str, str]]) -> None:
    manifest_path = output / "manifest.json"
    manifest = harvest.read_json(manifest_path, {})
    if not manifest:
        return
    manifest["runStatus"] = [
        entry for entry in manifest.get("runStatus", [])
        if not str(entry.get("source", "")).startswith(("weeklyMusic/", "weeklyMovies/"))
    ] + status
    harvest.atomic_json(manifest_path, manifest)


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=root / "data")
    parser.add_argument("--output", type=Path, default=root / "hosted-data")
    parser.add_argument("--date", default=dt.date.today().isoformat())
    args = parser.parse_args()
    status = refresh(args.data_root, dt.date.fromisoformat(args.date))
    record_status(args.output, status)
    for entry in status:
        print(f"{entry['status'].upper():7} {entry['source']}: {entry.get('detail', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
