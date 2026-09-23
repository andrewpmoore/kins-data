#!/usr/bin/env python3
"""Build versioned, static JSON snapshots for Kins.

The output is deliberately boring: it can be served by GitHub Pages, S3,
Cloudflare Pages, or any other static host.  Sources are kept semantically
separate so an Apple Music chart can never silently become a national sales
chart, and a popularity feed can never be presented as box office.
"""

from __future__ import annotations

import argparse
import copy
import csv
import datetime as dt
import gzip
import hashlib
import http.client
import html
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from xml.etree import ElementTree
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
USER_AGENT = "KinsDataHarvester/1.0 (+https://github.com/andrewpmoore/kins-data)"
APPLE_CHART_URL = (
    "https://rss.marketingtools.apple.com/api/v2/"
    "{country}/music/most-played/{limit}/songs.json"
)
SSA_NAMES_URL = "https://www.ssa.gov/oact/babynames/names.zip"
NYT_LIST_URL = "https://api.nytimes.com/svc/books/v3/lists/current/{list_name}.json"
APPLE_STORE_FEEDS = {
    "movies": ("topmovies", None, "Top Movies", "Apple Store chart position"),
    "adultBooks": ("toppaidebooks", None, "Top Paid Books", "paid ebook chart position"),
    "childrensBooks": (
        "toppaidebooks",
        "9010",
        "Top Paid Children's Books",
        "paid Children & Teens ebook chart position",
    ),
    "games": ("toppaidapplications", "6014", "Top Paid Games", "paid Games app chart position"),
}


class HarvestError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
        handle.write(payload)
        temporary = Path(handle.name)
    temporary.replace(path)


def fetch(url: str, *, attempts: int = 3, timeout: int = 25) -> bytes:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json, application/zip;q=0.9, */*;q=0.1", "User-Agent": USER_AGENT},
    )
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                if getattr(response, "status", 200) != 200:
                    raise HarvestError(f"HTTP {response.status} from {url}")
                value = response.read()
                return gzip.decompress(value) if value.startswith(b"\x1f\x8b") else value
        except urllib.error.HTTPError as error:
            last_error = error
            if 400 <= error.code < 500:
                break
            if attempt + 1 < attempts:
                time.sleep(0.5 * (2**attempt))
        except (OSError, http.client.IncompleteRead, urllib.error.URLError, HarvestError) as error:
            last_error = error
            if attempt + 1 < attempts:
                time.sleep(0.5 * (2**attempt))
    # Some government/CDN endpoints reject the older TLS/HTTP stack in the Python
    # bundled with Xcode while curl succeeds. Keep the normal path dependency-free,
    # with a bounded system-curl fallback for transport errors and truncated files.
    curl = shutil.which("curl")
    retry_with_curl = not (
        isinstance(last_error, urllib.error.HTTPError) and 400 <= last_error.code < 500
    )
    if curl and retry_with_curl:
        try:
            result = subprocess.run(
                [
                    curl,
                    "--fail",
                    "--location",
                    "--silent",
                    "--show-error",
                    "--retry",
                    "2",
                    "--max-time",
                    "120",
                    "--user-agent",
                    USER_AGENT,
                    url,
                ],
                check=True,
                capture_output=True,
                timeout=130,
            )
            if result.stdout:
                return gzip.decompress(result.stdout) if result.stdout.startswith(b"\x1f\x8b") else result.stdout
        except (OSError, subprocess.SubprocessError) as error:
            last_error = error
    raise HarvestError(f"Unable to fetch {url}: {last_error}")


def fetch_json(url: str) -> dict[str, Any]:
    try:
        value = json.loads(fetch(url))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise HarvestError(f"Invalid JSON from {url}: {error}") from error
    if not isinstance(value, dict):
        raise HarvestError(f"Expected a JSON object from {url}")
    return value


def source(
    provider: str,
    url: str,
    territory: str,
    metric: str,
    observed_at: str,
    kind: str,
) -> dict[str, Any]:
    return {
        "provider": provider,
        "url": url,
        "territory": territory,
        "metric": metric,
        "observedAt": observed_at,
        "kind": kind,
    }


def apple_music_chart(country: str, limit: int, observed_at: str) -> dict[str, Any]:
    url = APPLE_CHART_URL.format(country=country.lower(), limit=limit)
    payload = fetch_json(url)
    feed = payload.get("feed")
    if not isinstance(feed, dict) or not isinstance(feed.get("results"), list):
        raise HarvestError(f"Apple chart schema changed for {country}")
    results = []
    for rank, item in enumerate(feed["results"][:limit], start=1):
        if not isinstance(item, dict) or not item.get("name") or not item.get("artistName"):
            continue
        results.append(
            {
                "rank": rank,
                "id": str(item.get("id", "")),
                "title": item["name"],
                "artist": item["artistName"],
                "releaseDate": item.get("releaseDate"),
                "url": item.get("url"),
                "artworkURL": item.get("artworkUrl100"),
            }
        )
    if not results:
        raise HarvestError(f"Apple returned no usable songs for {country}")
    provider_time = feed.get("updated") if isinstance(feed.get("updated"), str) else observed_at
    return {
        "id": "apple-music-most-played",
        "title": feed.get("title") or "Top Songs",
        "source": source("Apple Music", url, country, "most-played", provider_time, "streamingChart"),
        "items": results,
    }


def nested_label(value: Any, key: str = "label") -> str | None:
    return value.get(key) if isinstance(value, dict) and isinstance(value.get(key), str) else None


def html_text(value: str) -> str:
    """Return compact visible text from a small HTML fragment."""
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", value)).split())


def apple_store_chart(country: str, output_key: str, limit: int, observed_at: str) -> dict[str, Any]:
    feed_name, genre, title, metric = APPLE_STORE_FEEDS[output_key]
    genre_path = f"/genre={genre}" if genre else ""
    url = (
        f"https://itunes.apple.com/{country.lower()}/rss/{feed_name}/"
        f"limit={limit}{genre_path}/json"
    )
    payload = fetch_json(url)
    feed = payload.get("feed")
    entries = feed.get("entry") if isinstance(feed, dict) else None
    if not isinstance(entries, list):
        raise HarvestError(f"Apple Store chart schema changed for {output_key}/{country}")
    items = []
    for rank, item in enumerate(entries[:limit], start=1):
        if not isinstance(item, dict):
            continue
        name = nested_label(item.get("im:name"))
        if not name:
            continue
        identity = item.get("id") if isinstance(item.get("id"), dict) else {}
        identity_attributes = identity.get("attributes") if isinstance(identity.get("attributes"), dict) else {}
        links = item.get("link")
        if isinstance(links, dict):
            links = [links]
        item_url = next(
            (
                link.get("attributes", {}).get("href")
                for link in links or []
                if isinstance(link, dict) and link.get("attributes", {}).get("rel") == "alternate"
            ),
            nested_label(identity),
        )
        images = item.get("im:image", [])
        if isinstance(images, dict):
            images = [images]
        category = item.get("category", {}).get("attributes", {}) if isinstance(item.get("category"), dict) else {}
        record = {
            "rank": rank,
            "id": str(identity_attributes.get("im:id", "")),
            "title": name,
            "creator": nested_label(item.get("im:artist")),
            "category": category.get("label"),
            "releaseDate": nested_label(item.get("im:releaseDate")),
            "url": item_url,
            "artworkURL": nested_label(images[-1]) if images else None,
        }
        items.append({key: value for key, value in record.items() if value is not None})
    if not items:
        raise HarvestError(f"Apple Store returned no usable {output_key} for {country}")
    return {
        "id": f"apple-store-{feed_name}" + (f"-{genre}" if genre else ""),
        "title": title,
        "source": source("Apple", url, country, metric, observed_at, "storefrontChart"),
        "items": items,
    }


def nyt_book_chart(list_name: str, audience: str, api_key: str, observed_at: str) -> dict[str, Any]:
    url = NYT_LIST_URL.format(list_name=urllib.parse.quote(list_name, safe=""))
    request_url = f"{url}?api-key={urllib.parse.quote(api_key, safe='')}"
    payload = fetch_json(request_url)
    result = payload.get("results")
    books = result.get("books") if isinstance(result, dict) else None
    if not isinstance(books, list):
        raise HarvestError(f"New York Times schema changed for {list_name}")
    items = []
    for book in books[:10]:
        if not isinstance(book, dict) or not book.get("title"):
            continue
        rank = book.get("rank")
        items.append(
            {
                "rank": int(rank) if isinstance(rank, int) else len(items) + 1,
                "title": str(book["title"]).title(),
                "author": book.get("author"),
                "isbn13": next((x for x in book.get("isbns", []) if x.get("isbn13")), {}).get("isbn13"),
                "description": book.get("description"),
                "url": book.get("amazon_product_url"),
                "coverURL": book.get("book_image"),
            }
        )
    if not items:
        raise HarvestError(f"New York Times returned no usable books for {list_name}")
    display_name = result.get("display_name") or list_name.replace("-", " ").title()
    published_date = result.get("published_date") or observed_at[:10]
    public_url = f"https://www.nytimes.com/books/best-sellers/{list_name}/"
    return {
        "id": f"nyt-{list_name}",
        "title": display_name,
        "audience": audience,
        "source": source(
            "The New York Times", public_url, "US", "weekly editorial bestseller list",
            published_date, "editorialBestseller"
        ),
        "items": items,
    }


def bfi_report_links(page: str, on_or_before: dt.date) -> list[tuple[dt.date, str, str]]:
    pattern = re.compile(
        r'<a href="([^"]+)"[^>]*download[^>]*>.*?'
        r'<span[^>]*file-title[^>]*>(.*?)</span>',
        re.IGNORECASE | re.DOTALL,
    )
    reports = []
    for url, raw_title in pattern.findall(page):
        title = html.unescape(re.sub(r"<[^>]+>", "", raw_title)).strip()
        match = re.search(r"to\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", title)
        if not match:
            continue
        try:
            end_date = dt.datetime.strptime(" ".join(match.groups()), "%d %B %Y").date()
        except ValueError:
            continue
        if end_date <= on_or_before:
            reports.append((end_date, html.unescape(url), title))
    return sorted(reports, reverse=True)


def ods_rows(document: bytes) -> list[list[str]]:
    table_ns = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
    try:
        with zipfile.ZipFile(io.BytesIO(document)) as zipped:
            root = ElementTree.fromstring(zipped.read("content.xml"))
    except (zipfile.BadZipFile, KeyError, ElementTree.ParseError) as error:
        raise HarvestError(f"Invalid OpenDocument spreadsheet: {error}") from error
    table = root.find(f".//{{{table_ns}}}table")
    if table is None:
        raise HarvestError("OpenDocument spreadsheet contains no table")
    rows = []
    for row in table.findall(f"{{{table_ns}}}table-row"):
        values = []
        for cell in row.findall(f"{{{table_ns}}}table-cell"):
            value = " ".join("".join(cell.itertext()).split())
            repeat = min(int(cell.attrib.get(f"{{{table_ns}}}number-columns-repeated", "1")), 20)
            values.extend([value] * repeat)
        rows.append(values)
    return rows


def bfi_box_office_chart(
    page_url: str, date: str, limit: int, observed_at: str
) -> dict[str, Any]:
    page = fetch(page_url).decode("utf-8")
    reports = bfi_report_links(page, dt.date.fromisoformat(date))
    if not reports:
        raise HarvestError(f"BFI has no weekend report on or before {date}")
    end_date, data_url, report_title = reports[0]
    rows = ods_rows(fetch(data_url))
    header_index = next(
        (index for index, row in enumerate(rows) if row[:2] == ["Rank", "Film"]), None
    )
    if header_index is None:
        raise HarvestError("BFI report schema changed")
    items = []
    for row in rows[header_index + 1 :]:
        if len(row) < 5 or not row[0].isdigit():
            continue
        rank = int(row[0])
        if rank > limit:
            continue
        items.append(
            {
                "rank": rank,
                "title": row[1],
                "countryOfOrigin": row[2],
                "weekendGross": row[3],
                "distributor": row[4],
                "weeksOnRelease": row[6] if len(row) > 6 else None,
            }
        )
    if not items:
        raise HarvestError("BFI report contains no ranked films")
    return {
        "id": f"bfi-weekend-box-office-{end_date.isoformat()}",
        "title": "UK weekend theatrical box office",
        "source": source(
            "British Film Institute", page_url, "GB",
            "Friday-to-Sunday theatrical box-office gross in pounds sterling",
            observed_at, "theatricalBoxOffice"
        ),
        "reportTitle": report_title,
        "reportURL": data_url,
        "items": items,
    }


def cinetel_box_office_chart(page_url: str, date: str, limit: int) -> dict[str, Any]:
    page = fetch(page_url).decode("utf-8")
    period = re.search(
        r"Settimana\s+Dal\s+(\d{2}/\d{2}/\d{4})\s+Al\s+(\d{2}/\d{2}/\d{4})",
        page,
        re.IGNORECASE,
    )
    if not period:
        raise HarvestError("Cinetel weekly period was not found")
    start_date = dt.datetime.strptime(period.group(1), "%d/%m/%Y").date()
    end_date = dt.datetime.strptime(period.group(2), "%d/%m/%Y").date()
    if end_date > dt.date.fromisoformat(date):
        raise HarvestError(f"Cinetel's completed week ends after requested date {date}")

    def cell_text(value: str) -> str:
        return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", value)).split())

    items = []
    for raw_row in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.IGNORECASE | re.DOTALL):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", raw_row, re.IGNORECASE | re.DOTALL)
        if len(cells) < 9:
            continue
        rank_text = cell_text(cells[0])
        if not rank_text.isdigit():
            continue
        rank = int(rank_text)
        if rank > limit:
            continue
        release_match = re.search(r"'(\d{4}-\d{2}-\d{2})'", cells[2])
        try:
            weekend_gross = float(cell_text(cells[5]))
            admissions = int(cell_text(cells[6]))
        except ValueError:
            continue
        items.append(
            {
                "rank": rank,
                "title": cell_text(cells[1]),
                "releaseDate": release_match.group(1) if release_match else None,
                "countryOfOrigin": cell_text(cells[3]),
                "distributor": cell_text(cells[4]),
                "weekendGrossEUR": weekend_gross,
                "admissions": admissions,
            }
        )
    if not items:
        raise HarvestError("Cinetel report contains no ranked films")
    return {
        "id": f"cinetel-national-week-{end_date.isoformat()}",
        "title": "Italy national weekly theatrical box office",
        "source": source(
            "Cinetel", page_url, "IT",
            "national weekly theatrical gross in euros",
            end_date.isoformat(), "theatricalBoxOffice"
        ),
        "reportPeriod": f"{start_date.isoformat()}/{end_date.isoformat()}",
        "items": items,
    }


def box_office_mojo_weekend_link(
    page: bytes, page_url: str, on_or_before: dt.date
) -> tuple[dt.date, str]:
    """Select the latest populated country weekend on or before a requested date."""
    candidates = []
    text_value = page.decode("utf-8", "replace")
    for raw_row in re.findall(r"<tr[^>]*>(.*?)</tr>", text_value, re.I | re.DOTALL):
        match = re.search(
            r'href=["\']([^"\']*/weekend/(\d{4})W\d+[^"\']*)["\'][^>]*>'
            r"\s*([A-Z][a-z]{2})\s+\d{1,2}-(\d{1,2})\s*</a>",
            raw_row,
            re.I | re.DOTALL,
        )
        release = re.search(
            r'<td[^>]*mojo-field-type-release mojo-cell-wide[^>]*>\s*'
            r'<a[^>]+href=["\'][^"\']+["\'][^>]*>(.*?)</a>',
            raw_row,
            re.I | re.DOTALL,
        )
        if not match or not release:
            continue
        relative_url, year, month, end_day = match.groups()
        try:
            end_date = dt.datetime.strptime(f"{month} {end_day} {year}", "%b %d %Y").date()
        except ValueError:
            continue
        if end_date <= on_or_before and html_text(release.group(1)) not in {"", "-"}:
            candidates.append((end_date, urllib.parse.urljoin(page_url, html.unescape(relative_url))))
    if not candidates:
        raise HarvestError("Box Office Mojo has no populated weekend on or before the requested date")
    return max(candidates)


def box_office_mojo_chart(
    page_url: str, country: str, date: str, limit: int
) -> dict[str, Any]:
    """Read a country-specific theatrical weekend chart from public HTML."""
    end_date, weekend_url = box_office_mojo_weekend_link(
        fetch(page_url), page_url, dt.date.fromisoformat(date)
    )
    page = fetch(weekend_url).decode("utf-8", "replace")
    period_match = re.search(r"<h4[^>]*>(.*?)</h4>", page, re.I | re.DOTALL)
    report_period = html_text(period_match.group(1)) if period_match else end_date.isoformat()
    items = []
    for raw_row in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.I | re.DOTALL):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", raw_row, re.I | re.DOTALL)
        if len(cells) < 4:
            continue
        rank_text = html_text(cells[0])
        title_match = re.search(
            r'href=["\']([^"\']*/release/[^"\']+)["\'][^>]*>(.*?)</a>',
            cells[2],
            re.I | re.DOTALL,
        )
        if not rank_text.isdigit() or not title_match:
            continue
        rank = int(rank_text)
        if rank > limit:
            continue
        item: dict[str, Any] = {
            "rank": rank,
            "title": html_text(title_match.group(2)),
            "url": urllib.parse.urljoin(weekend_url, html.unescape(title_match.group(1))),
            "weekendGross": html_text(cells[3]),
        }
        if len(cells) > 9:
            item["weeksOnRelease"] = html_text(cells[9])
        if len(cells) > 10:
            distributor = html_text(cells[10])
            if distributor:
                item["distributor"] = distributor
        items.append(item)
    items.sort(key=lambda item: item["rank"])
    if not items or items[0]["rank"] != 1:
        raise HarvestError("Box Office Mojo weekend page contains no ranked films")
    return {
        "id": f"box-office-mojo-{country.lower()}-{end_date.isoformat()}",
        "title": f"{country} weekend theatrical box office",
        "source": source(
            "Box Office Mojo",
            page_url,
            country,
            "country weekend theatrical gross in local currency",
            end_date.isoformat(),
            "theatricalBoxOffice",
        ),
        "reportPeriod": report_period,
        "reportURL": weekend_url,
        "items": items,
    }


def media_control_germany_box_office_chart(
    page_url: str, date: str, limit: int
) -> dict[str, Any]:
    """Read Media Control's official German admissions-based cinema top five."""
    page = fetch(page_url).decode("utf-8", "replace")
    period = re.search(
        r"Erhebungszeitraum:\s*(\d{2}\.\d{2}\.)\s*-\s*(\d{2}\.\d{2}\.\d{4})",
        html_text(page),
        re.I,
    )
    if not period:
        raise HarvestError("Media Control cinema chart period was not found")
    end_date = dt.datetime.strptime(period.group(2), "%d.%m.%Y").date()
    start_year = end_date.year - (1 if period.group(1).startswith("12.") and end_date.month == 1 else 0)
    start_date = dt.datetime.strptime(f"{period.group(1)}{start_year}", "%d.%m.%Y").date()
    if end_date > dt.date.fromisoformat(date):
        raise HarvestError(f"Media Control's completed period ends after requested date {date}")
    table_match = re.search(
        r'<tbody[^>]*class=["\'][^"\']*chart-table[^"\']*["\'][^>]*>(.*?)</tbody>',
        page,
        re.I | re.DOTALL,
    )
    if not table_match:
        raise HarvestError("Media Control cinema chart table was not found")
    items = []
    for raw_row in re.findall(r"<tr[^>]*>(.*?)</tr>", table_match.group(1), re.I | re.DOTALL):
        rank_match = re.search(
            r'class=["\'][^"\']*pos-span[^"\']*["\'][^>]*>\s*(\d+)', raw_row, re.I
        )
        title_match = re.search(
            r'class=["\'][^"\']*info1-span[^"\']*["\'][^>]*>(.*?)</span>',
            raw_row,
            re.I | re.DOTALL,
        )
        if not rank_match or not title_match:
            continue
        rank = int(rank_match.group(1))
        if rank > limit:
            continue
        genre_match = re.search(
            r'class=["\'][^"\']*info2-span[^"\']*["\'][^>]*>(.*?)</span>',
            raw_row,
            re.I | re.DOTALL,
        )
        image_match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw_row, re.I)
        item: dict[str, Any] = {"rank": rank, "title": html_text(title_match.group(1))}
        if genre_match and html_text(genre_match.group(1)):
            item["genre"] = html_text(genre_match.group(1))
        if image_match:
            item["artworkURL"] = html.unescape(image_match.group(1))
        items.append(item)
    items.sort(key=lambda item: item["rank"])
    if not items or items[0]["rank"] != 1:
        raise HarvestError("Media Control cinema chart contains no ranked films")
    return {
        "id": f"media-control-germany-{end_date.isoformat()}",
        "title": "Official German cinema chart",
        "source": source(
            "Media Control",
            page_url,
            "DE",
            "official national weekly cinema chart based on representative admissions",
            end_date.isoformat(),
            "theatricalBoxOffice",
        ),
        "reportPeriod": f"{start_date.isoformat()}/{end_date.isoformat()}",
        "items": items,
    }


def the_numbers_us_box_office_chart(page_url: str, date: str, limit: int) -> dict[str, Any]:
    """Read the latest completed US weekend chart from The Numbers."""
    requested = dt.date.fromisoformat(date)
    days_since_friday = (requested.weekday() - 4) % 7
    if days_since_friday < 2:  # A Friday or Saturday weekend is not complete yet.
        days_since_friday += 7
    friday = requested - dt.timedelta(days=days_since_friday)
    weekend_url = urllib.parse.urljoin(page_url, f"/box-office-chart/weekend/{friday:%Y/%m/%d}")
    page = fetch(weekend_url).decode("utf-8", "replace")
    items = []
    for raw_row in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.I | re.DOTALL):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", raw_row, re.I | re.DOTALL)
        if len(cells) < 4:
            continue
        rank_text = html_text(cells[0])
        title_match = re.search(
            r'href=["\']([^"\']*/movie/[^"\']+)["\'][^>]*>(.*?)</a>',
            cells[2],
            re.I | re.DOTALL,
        )
        if not rank_text.isdigit() or not title_match:
            continue
        rank = int(rank_text)
        if rank > limit:
            continue
        item: dict[str, Any] = {
            "rank": rank,
            "title": html_text(title_match.group(2)),
            "url": urllib.parse.urljoin(weekend_url, html.unescape(title_match.group(1))),
            "weekendGross": html_text(cells[3]),
        }
        if len(cells) > 7:
            item["totalGross"] = html_text(cells[7])
        items.append(item)
    if not items or items[0]["rank"] != 1:
        raise HarvestError("The Numbers weekend page contains no ranked films")
    sunday = friday + dt.timedelta(days=2)
    return {
        "id": f"the-numbers-us-{friday.isoformat()}",
        "title": "US weekend theatrical box office",
        "source": source(
            "The Numbers",
            page_url,
            "US",
            "US theatrical weekend gross",
            sunday.isoformat(),
            "theatricalBoxOffice",
        ),
        "reportPeriod": f"{friday.isoformat()}/{sunday.isoformat()}",
        "reportURL": weekend_url,
        "items": items,
    }


def publishers_weekly_book_chart(
    page_url: str, country: str, title: str, audience: str, date: str, limit: int
) -> dict[str, Any]:
    """Read a public Publishers Weekly Circana BookScan ranking."""
    page = fetch(page_url).decode("utf-8", "replace")
    requested = dt.date.fromisoformat(date)
    published_dates = []
    for month, day, year in re.findall(r"\b(\d{2})/(\d{2})/(\d{4})\b", page):
        try:
            value = dt.date(int(year), int(month), int(day))
        except ValueError:
            continue
        if value <= requested:
            published_dates.append(value)
    observed = max(published_dates).isoformat() if published_dates else date
    items = []
    for raw_row in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.I | re.DOTALL):
        rank_match = re.search(
            r'class=["\'][^"\']*nielsen-rank[^"\']*["\'][^>]*>\s*(\d+)', raw_row, re.I
        )
        title_match = re.search(
            r'class=["\'][^"\']*nielsen-booktitle[^"\']*["\'][^>]*>(.*?)</div>',
            raw_row,
            re.I | re.DOTALL,
        )
        if not rank_match or not title_match:
            continue
        rank = int(rank_match.group(1))
        if rank > limit:
            continue
        raw_title = title_match.group(1)
        link_match = re.search(r'href=["\']([^"\']+)["\']', raw_title, re.I)
        remainder = raw_row[title_match.end() :]
        creator_match = re.search(r"<div[^>]*>(.*?)</div>", remainder, re.I | re.DOTALL)
        isbn_match = re.search(r"\b(97[89][\d-]{10,17})\b", raw_row)
        item: dict[str, Any] = {
            "rank": rank,
            "title": html_text(raw_title),
        }
        if creator_match:
            creator = re.sub(r",?\s+(Author|Illustrator|Foreword by)\b", "", html_text(creator_match.group(1)), flags=re.I)
            if creator:
                item["creator"] = creator
        if isbn_match:
            item["isbn13"] = re.sub(r"\D", "", isbn_match.group(1))
        if link_match:
            item["url"] = urllib.parse.urljoin(page_url, html.unescape(link_match.group(1)))
        items.append(item)
    items.sort(key=lambda item: item["rank"])
    if not items or items[0]["rank"] != 1:
        raise HarvestError("Publishers Weekly page contains no ranked books")
    return {
        "id": f"publishers-weekly-{audience}-{observed}",
        "title": title,
        "audience": audience,
        "source": source(
            "Publishers Weekly / Circana BookScan",
            page_url,
            country,
            title,
            observed,
            "publicBookChart",
        ),
        "items": items,
    }


def aba_australia_book_chart(page_url: str, date: str, limit: int) -> dict[str, Any]:
    page = fetch(page_url).decode("utf-8", "replace")
    period = re.search(r"Week Ending\s+(\d{1,2}\s+[A-Za-z]+,?\s+\d{4})", html_text(page), re.I)
    if not period:
        raise HarvestError("ABA chart week was not found")
    observed = dt.datetime.strptime(period.group(1).replace(",", ""), "%d %B %Y").date()
    if observed > dt.date.fromisoformat(date):
        raise HarvestError("ABA chart is newer than the requested date")
    pattern = re.compile(
        r'badge[^>]*>\s*(\d+)\s*</span>.*?card-title[^>]*>.*?'
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>.*?'
        r'(?:</br>|<br\s*/?>)\s*(.*?)</p>',
        re.I | re.DOTALL,
    )
    items = []
    for rank_text, relative_url, raw_title, raw_creator in pattern.findall(page):
        rank = int(rank_text)
        if rank > limit:
            continue
        items.append(
            {
                "rank": rank,
                "title": html_text(raw_title),
                "creator": html_text(raw_creator),
                "url": urllib.parse.urljoin(page_url, html.unescape(relative_url)),
            }
        )
    items.sort(key=lambda item: item["rank"])
    if not items or items[0]["rank"] != 1:
        raise HarvestError("ABA page contains no ranked books")
    return {
        "id": f"aba-buying-group-{observed.isoformat()}",
        "title": "ABA Buying Group Top 10",
        "audience": "adult",
        "source": source(
            "Australian Booksellers Association / NielsenIQ BookScan",
            page_url,
            "AU",
            "bestsellers in ABA Buying Group stores",
            observed.isoformat(),
            "publicBookChart",
        ),
        "items": items,
    }


def bookhub_new_zealand_chart(page_url: str, date: str, limit: int) -> dict[str, Any]:
    page = fetch(page_url).decode("utf-8", "replace")
    match = re.search(r"['\"]impressions['\"]\s*:\s*(\[.*?\])", page, re.DOTALL)
    if not match:
        raise HarvestError("BookHub Nielsen ranking was not found")
    try:
        records = json.loads(match.group(1))
    except json.JSONDecodeError as error:
        raise HarvestError(f"BookHub ranking JSON changed: {error}") from error
    items = []
    for record in sorted(records, key=lambda value: int(value.get("position", 999)))[:limit]:
        rank = int(record["position"])
        items.append(
            {
                "rank": rank,
                "title": str(record["name"]),
                "isbn13": str(record.get("id", "")),
                "category": record.get("category"),
            }
        )
    if not items or items[0]["rank"] != 1:
        raise HarvestError("BookHub ranking contains no ranked books")
    return {
        "id": f"bookhub-nielsen-indie-{date}",
        "title": "Official Nielsen Indie Top 10",
        "audience": "adult",
        "source": source(
            "Booksellers Aotearoa / Nielsen BookScan",
            page_url,
            "NZ",
            "weekly sales through participating independent booksellers",
            date,
            "publicBookChart",
        ),
        "items": items,
    }


def booksellers_nz_category_chart(
    page_url: str, heading: str, title: str, audience: str, date: str, limit: int
) -> dict[str, Any]:
    page = fetch(page_url).decode("utf-8", "replace")
    modified_match = re.search(r'article:modified_time["\']\s+content=["\']([^"\']+)', page, re.I)
    observed = modified_match.group(1)[:10] if modified_match else date
    if dt.date.fromisoformat(observed) > dt.date.fromisoformat(date):
        raise HarvestError("Booksellers NZ chart is newer than the requested date")
    section = re.search(
        rf"<h2[^>]*>\s*{heading}\s*</h2>.*?<table[^>]*>(.*?)</table>",
        page,
        re.I | re.DOTALL,
    )
    if not section:
        raise HarvestError(f"Booksellers NZ section was not found: {heading}")
    items = []
    for raw_row in re.findall(r"<tr[^>]*>(.*?)</tr>", section.group(1), re.I | re.DOTALL):
        cells = [html_text(value) for value in re.findall(r"<td[^>]*>(.*?)</td>", raw_row, re.I | re.DOTALL)]
        if len(cells) < 4 or not cells[0].isdigit():
            continue
        rank = int(cells[0])
        if rank > limit:
            continue
        items.append({"rank": rank, "isbn13": cells[1], "title": cells[2], "creator": cells[3]})
    if not items or items[0]["rank"] != 1:
        raise HarvestError("Booksellers NZ section contains no ranked books")
    return {
        "id": f"booksellers-nz-{audience}-{observed}",
        "title": title,
        "audience": audience,
        "source": source(
            "Booksellers Aotearoa / Nielsen BookScan",
            page_url,
            "NZ",
            title,
            observed,
            "publicBookChart",
        ),
        "items": items,
    }


def dice_game_of_year(page_template: str, award_year: int, observed_at: str) -> dict[str, Any]:
    page_url = page_template.format(year=award_year)
    try:
        page = fetch(page_url).decode("iso-8859-1")
    except UnicodeDecodeError as error:
        raise HarvestError(f"D.I.C.E. page encoding changed: {error}") from error
    match = re.search(
        r"<h2>\s*Game of the Year\s*</h2>.*?"
        r'<div[^>]*aias-award-details-label[^>]*>\s*Winner:\s*</div>.*?'
        r'<a href="([^"]+)">(.*?)</a>',
        page,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise HarvestError(f"D.I.C.E. has no published Game of the Year for {award_year}")
    winner_url, raw_title = match.groups()
    title = html.unescape(re.sub(r"<[^>]+>", "", raw_title)).strip()
    if not title:
        raise HarvestError("D.I.C.E. Game of the Year winner is empty")
    return {
        "id": f"dice-game-of-the-year-{award_year}",
        "title": "D.I.C.E. Awards · Game of the Year",
        "year": award_year,
        "source": source(
            "Academy of Interactive Arts & Sciences",
            page_url,
            "WORLD",
            "Game of the Year award winner voted by Academy members",
            observed_at,
            "awardWinner",
        ),
        "items": [{"rank": 1, "title": title, "url": html.unescape(winner_url)}],
    }


def ssa_top_names(archive: bytes, limit: int) -> tuple[int, list[dict[str, Any]]]:
    """Read the newest birth year in SSA's national archive."""
    with tempfile.TemporaryDirectory() as folder:
        archive_path = Path(folder) / "names.zip"
        archive_path.write_bytes(archive)
        try:
            with zipfile.ZipFile(archive_path) as zipped:
                candidates = []
                for name in zipped.namelist():
                    stem = Path(name).stem
                    if stem.startswith("yob") and stem[3:].isdigit():
                        candidates.append((int(stem[3:]), name))
                if not candidates:
                    raise HarvestError("SSA archive contains no yobYYYY files")
                year, member = max(candidates)
                rows = zipped.read(member).decode("utf-8").splitlines()
        except (zipfile.BadZipFile, KeyError, UnicodeDecodeError) as error:
            raise HarvestError(f"Invalid SSA names archive: {error}") from error

    by_category: dict[str, list[tuple[str, int]]] = {"girl": [], "boy": []}
    for row in rows:
        columns = row.split(",")
        if len(columns) != 3 or columns[1] not in {"F", "M"}:
            continue
        category = "girl" if columns[1] == "F" else "boy"
        by_category[category].append((columns[0], int(columns[2])))
    items = []
    for category in ("girl", "boy"):
        ranked = sorted(by_category[category], key=lambda pair: (-pair[1], pair[0]))[:limit]
        items.extend(
            {"rank": rank, "name": name, "count": count, "category": category, "rankGroup": category}
            for rank, (name, count) in enumerate(ranked, start=1)
        )
    return year, items


def cached_ssa_names(cache_dir: Path, refresh_days: int) -> bytes:
    cache_file = cache_dir / "ssa" / "names.zip"
    if cache_file.exists():
        age = dt.datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < refresh_days * 86_400:
            return cache_file.read_bytes()
    archive = fetch(SSA_NAMES_URL)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_bytes(archive)
    return archive


def cached_download(url: str, cache_file: Path, refresh_days: int) -> bytes:
    if cache_file.exists():
        age = dt.datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < refresh_days * 86_400:
            return cache_file.read_bytes()
    value = fetch(url)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_bytes(value)
    return value


def csv_member(archive: bytes, *, exclude: str = "") -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
            names = [
                name
                for name in zipped.namelist()
                if name.lower().endswith(".csv")
                and (not exclude or exclude.lower() not in name.lower())
            ]
    except zipfile.BadZipFile as error:
        raise HarvestError(f"Invalid ZIP archive: {error}") from error
    if not names:
        raise HarvestError("Archive contains no matching CSV file")
    return names[0]


def zip_csv_rows(archive: bytes, member: str, delimiter: str = ",") -> list[dict[str, str]]:
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
            text_value = zipped.read(member).decode("utf-8-sig")
    except (zipfile.BadZipFile, KeyError, UnicodeDecodeError) as error:
        raise HarvestError(f"Invalid names archive: {error}") from error
    return list(csv.DictReader(io.StringIO(text_value), delimiter=delimiter))


def statcan_top_names(archive: bytes, limit: int) -> tuple[int, list[dict[str, Any]]]:
    rows = zip_csv_rows(archive, csv_member(archive, exclude="metadata"))
    years = [int(row["REF_DATE"]) for row in rows if row.get("REF_DATE", "").isdigit()]
    if not years:
        raise HarvestError("Statistics Canada data contains no years")
    year = max(years)
    frequency = {
        (row.get("Sex at birth"), row.get("First name at birth")): int(float(row["VALUE"]))
        for row in rows
        if row.get("REF_DATE") == str(year)
        and row.get("Indicator") == "Frequency"
        and row.get("VALUE", "").replace(".", "", 1).isdigit()
    }
    records = []
    for row in rows:
        if row.get("REF_DATE") != str(year) or row.get("Indicator") != "Rank":
            continue
        try:
            rank = int(float(row["VALUE"]))
        except (TypeError, ValueError):
            continue
        if rank > limit:
            continue
        sex = row.get("Sex at birth")
        name = row.get("First name at birth")
        if sex not in {"Female", "Male"} or not name:
            continue
        records.append(
            {
                "rank": rank,
                "name": name.title(),
                "count": frequency.get((sex, name)),
                "category": "girl" if sex == "Female" else "boy",
                "rankGroup": "girl" if sex == "Female" else "boy",
            }
        )
    records.sort(key=lambda item: (item["category"], item["rank"]))
    return year, records


def insee_top_names(archive: bytes, limit: int) -> tuple[int, list[dict[str, Any]]]:
    rows = zip_csv_rows(archive, csv_member(archive), delimiter=";")
    years = [int(row["periode"]) for row in rows if row.get("periode", "").isdigit()]
    if not years:
        raise HarvestError("INSEE data contains no years")
    year = max(years)
    records = []
    for row in rows:
        if row.get("periode") != str(year):
            continue
        try:
            rank = int(row["rang"])
        except (TypeError, ValueError):
            continue
        if rank > limit or row.get("sexe") not in {"1", "2"}:
            continue
        records.append(
            {
                "rank": rank,
                "name": row["prenom"].title(),
                "count": int(row["valeur"]),
                "category": "boy" if row["sexe"] == "1" else "girl",
                "rankGroup": "boy" if row["sexe"] == "1" else "girl",
            }
        )
    records.sort(key=lambda item: (item["category"], item["rank"]))
    return year, records


def ons_top_names(workbook: bytes, limit: int) -> tuple[int, list[dict[str, Any]]]:
    """Read the latest England-and-Wales rankings from the official ONS XLSX."""
    spreadsheet_ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    relationships_ns = "http://schemas.openxmlformats.org/package/2006/relationships"
    document_rel_ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    try:
        with zipfile.ZipFile(io.BytesIO(workbook)) as zipped:
            shared_strings = []
            if "xl/sharedStrings.xml" in zipped.namelist():
                root = ElementTree.fromstring(zipped.read("xl/sharedStrings.xml"))
                for item in root.findall(f"{{{spreadsheet_ns}}}si"):
                    shared_strings.append(
                        "".join(node.text or "" for node in item.iter(f"{{{spreadsheet_ns}}}t"))
                    )

            workbook_root = ElementTree.fromstring(zipped.read("xl/workbook.xml"))
            relationships_root = ElementTree.fromstring(
                zipped.read("xl/_rels/workbook.xml.rels")
            )
            targets = {
                item.attrib["Id"]: item.attrib["Target"].lstrip("/")
                for item in relationships_root.findall(f"{{{relationships_ns}}}Relationship")
            }

            sheets: list[tuple[str, str]] = []
            for sheet in workbook_root.findall(
                f".//{{{spreadsheet_ns}}}sheet"
            ):
                name = sheet.attrib.get("name", "")
                if name not in {"Table_1", "Table_2"}:
                    continue
                relation = sheet.attrib.get(f"{{{document_rel_ns}}}id")
                target = targets.get(relation or "")
                if target:
                    sheets.append((name, target if target.startswith("xl/") else f"xl/{target}"))

            records = []
            latest_year: int | None = None
            for sheet_name, target in sheets:
                sheet_root = ElementTree.fromstring(zipped.read(target))
                rows = sheet_root.findall(
                    f".//{{{spreadsheet_ns}}}sheetData/{{{spreadsheet_ns}}}row"
                )
                cells_by_row: list[dict[str, str]] = []
                for row in rows:
                    cells: dict[str, str] = {}
                    for cell in row.findall(f"{{{spreadsheet_ns}}}c"):
                        reference = cell.attrib.get("r", "")
                        column = "".join(character for character in reference if character.isalpha())
                        value_node = cell.find(f"{{{spreadsheet_ns}}}v")
                        value = "" if value_node is None else value_node.text or ""
                        if cell.attrib.get("t") == "s" and value.isdigit():
                            value = shared_strings[int(value)]
                        elif cell.attrib.get("t") == "inlineStr":
                            value = "".join(
                                node.text or ""
                                for node in cell.iter(f"{{{spreadsheet_ns}}}t")
                            )
                        cells[column] = value
                    cells_by_row.append(cells)

                header = next(
                    (row for row in cells_by_row if row.get("A") == "Name" and row.get("B", "").endswith(" Rank")),
                    None,
                )
                if not header:
                    raise HarvestError(f"ONS {sheet_name} sheet has no recognisable header")
                year_text = header["B"].split()[0]
                if not year_text.isdigit():
                    raise HarvestError(f"ONS {sheet_name} latest year is invalid")
                year = int(year_text)
                if latest_year is not None and latest_year != year:
                    raise HarvestError("ONS girls and boys sheets have different latest years")
                latest_year = year
                category = "girl" if sheet_name == "Table_1" else "boy"
                for row in cells_by_row:
                    if not row.get("A") or not row.get("B", "").isdigit():
                        continue
                    rank = int(row["B"])
                    if rank > limit:
                        continue
                    count = int(row["C"]) if row.get("C", "").isdigit() else None
                    records.append(
                        {
                            "rank": rank,
                            "name": row["A"],
                            "count": count,
                            "category": category,
                            "rankGroup": category,
                        }
                    )
    except (zipfile.BadZipFile, KeyError, ElementTree.ParseError) as error:
        raise HarvestError(f"Invalid ONS names workbook: {error}") from error

    if latest_year is None:
        raise HarvestError("ONS workbook contains no supported name sheets")
    records.sort(key=lambda item: (item["category"], item["rank"], item["name"]))
    return latest_year, records


def json_stat_category_ids(category: dict[str, Any]) -> list[str]:
    index = category.get("index", [])
    if isinstance(index, list):
        return [str(value) for value in index]
    if isinstance(index, dict):
        return [str(key) for key, _ in sorted(index.items(), key=lambda item: item[1])]
    raise HarvestError("JSON-stat category index has an unsupported shape")


def cso_ireland_top_names(
    boys_payload: bytes, girls_payload: bytes, limit: int
) -> tuple[int, list[dict[str, Any]]]:
    """Read the latest national baby-name rank and count from CSO JSON-stat."""
    records: list[dict[str, Any]] = []
    latest_year: int | None = None
    for category, raw_payload in (("boy", boys_payload), ("girl", girls_payload)):
        try:
            payload = json.loads(raw_payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise HarvestError(f"Invalid CSO Ireland JSON-stat: {error}") from error
        dimensions = payload.get("dimension") if isinstance(payload, dict) else None
        dimension_ids = payload.get("id") if isinstance(payload, dict) else None
        sizes = payload.get("size") if isinstance(payload, dict) else None
        values = payload.get("value") if isinstance(payload, dict) else None
        if (
            not isinstance(dimensions, dict)
            or not isinstance(dimension_ids, list)
            or not isinstance(sizes, list)
            or not isinstance(values, list)
            or len(dimension_ids) != 3
            or len(sizes) != 3
        ):
            raise HarvestError("CSO Ireland JSON-stat schema changed")
        statistic_id, year_id, name_id = dimension_ids
        statistic = dimensions.get(statistic_id, {}).get("category", {})
        year_dimension = dimensions.get(year_id, {}).get("category", {})
        name_dimension = dimensions.get(name_id, {}).get("category", {})
        statistic_ids = json_stat_category_ids(statistic)
        years = json_stat_category_ids(year_dimension)
        names = json_stat_category_ids(name_dimension)
        labels = statistic.get("label", {})
        name_labels = name_dimension.get("label", {})
        numeric_years = [int(value) for value in years if value.isdigit()]
        if not numeric_years:
            raise HarvestError("CSO Ireland data contains no years")
        year = max(numeric_years)
        if latest_year is not None and latest_year != year:
            raise HarvestError("CSO Ireland boys and girls datasets have different latest years")
        latest_year = year
        year_index = years.index(str(year))
        rank_index = next(
            (
                index
                for index, identity in enumerate(statistic_ids)
                if "rank" in str(labels.get(identity, "")).casefold()
            ),
            None,
        )
        count_index = next(
            (
                index
                for index, identity in enumerate(statistic_ids)
                if "rank" not in str(labels.get(identity, "")).casefold()
            ),
            None,
        )
        if rank_index is None or count_index is None:
            raise HarvestError("CSO Ireland statistic dimensions changed")
        stride = len(years) * len(names)
        ranked: list[tuple[int, str, int | None]] = []
        for name_index, name_identity in enumerate(names):
            rank_value = values[rank_index * stride + year_index * len(names) + name_index]
            count_value = values[count_index * stride + year_index * len(names) + name_index]
            if not isinstance(rank_value, (int, float)) or rank_value < 1:
                continue
            name = name_labels.get(name_identity)
            if not isinstance(name, str) or not name.strip():
                continue
            ranked.append(
                (
                    int(rank_value),
                    name.strip(),
                    int(count_value) if isinstance(count_value, (int, float)) else None,
                )
            )
        ranked.sort(key=lambda value: (value[0], value[1]))
        for display_rank, (source_rank, name, count) in enumerate(ranked[:limit], start=1):
            records.append(
                {
                    "rank": display_rank,
                    "sourceRank": source_rank,
                    "name": name,
                    "count": count,
                    "category": category,
                    "rankGroup": category,
                }
            )
    if latest_year is None:
        raise HarvestError("CSO Ireland data contains no supported name records")
    records.sort(key=lambda item: (item["category"], item["rank"], item["name"]))
    return latest_year, records


def xlsx_sheet_rows(document: bytes, wanted_sheet: str) -> list[dict[str, str]]:
    spreadsheet_ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    relationships_ns = "http://schemas.openxmlformats.org/package/2006/relationships"
    document_rel_ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    try:
        with zipfile.ZipFile(io.BytesIO(document)) as zipped:
            shared_strings: list[str] = []
            if "xl/sharedStrings.xml" in zipped.namelist():
                root = ElementTree.fromstring(zipped.read("xl/sharedStrings.xml"))
                for item in root.findall(f"{{{spreadsheet_ns}}}si"):
                    shared_strings.append(
                        "".join(node.text or "" for node in item.iter(f"{{{spreadsheet_ns}}}t"))
                    )
            workbook = ElementTree.fromstring(zipped.read("xl/workbook.xml"))
            relationships = ElementTree.fromstring(zipped.read("xl/_rels/workbook.xml.rels"))
            targets = {
                item.attrib["Id"]: item.attrib["Target"].lstrip("/")
                for item in relationships.findall(f"{{{relationships_ns}}}Relationship")
            }
            target = None
            for sheet in workbook.findall(f".//{{{spreadsheet_ns}}}sheet"):
                if sheet.attrib.get("name") != wanted_sheet:
                    continue
                relation = sheet.attrib.get(f"{{{document_rel_ns}}}id", "")
                target = targets.get(relation)
                break
            if not target:
                raise HarvestError(f"XLSX contains no {wanted_sheet} sheet")
            if not target.startswith("xl/"):
                target = f"xl/{target}"
            sheet_root = ElementTree.fromstring(zipped.read(target))
            rows = []
            for row in sheet_root.findall(
                f".//{{{spreadsheet_ns}}}sheetData/{{{spreadsheet_ns}}}row"
            ):
                values: dict[str, str] = {}
                for cell in row.findall(f"{{{spreadsheet_ns}}}c"):
                    reference = cell.attrib.get("r", "")
                    column = "".join(character for character in reference if character.isalpha())
                    value_node = cell.find(f"{{{spreadsheet_ns}}}v")
                    value = "" if value_node is None else value_node.text or ""
                    if cell.attrib.get("t") == "s" and value.isdigit():
                        value = shared_strings[int(value)]
                    elif cell.attrib.get("t") == "inlineStr":
                        value = "".join(
                            node.text or "" for node in cell.iter(f"{{{spreadsheet_ns}}}t")
                        )
                    values[column] = value
                rows.append(values)
            return rows
    except (zipfile.BadZipFile, KeyError, ElementTree.ParseError, IndexError) as error:
        raise HarvestError(f"Invalid XLSX workbook: {error}") from error


def ine_spain_workbook_url(page_url: str, page: bytes) -> tuple[int, str]:
    text_value = page.decode("utf-8", "replace")
    matches = re.findall(r'href=["\']([^"\']*/nomnac(\d{2})\.xlsx)["\']', text_value, re.I)
    if not matches:
        raise HarvestError("INE newborn-name workbook link was not found")
    relative_url, short_year = matches[0]
    year = 2000 + int(short_year)
    return year, urllib.parse.urljoin(page_url, html.unescape(relative_url))


def ine_spain_top_names(workbook: bytes, year: int, limit: int) -> tuple[int, list[dict[str, Any]]]:
    rows = xlsx_sheet_rows(workbook, "TOTAL")
    records = []
    for category, name_column, count_column in (("boy", "A", "B"), ("girl", "D", "E")):
        ranked = []
        for row in rows:
            name = row.get(name_column, "").strip()
            count = row.get(count_column, "").strip()
            if not name or name == "TOTAL" or not count.replace(".0", "").isdigit():
                continue
            ranked.append((name.title(), int(float(count))))
        for rank, (name, count) in enumerate(ranked[:limit], start=1):
            records.append(
                {
                    "rank": rank,
                    "name": name,
                    "count": count,
                    "category": category,
                    "rankGroup": category,
                }
            )
    if len(records) < 2 * limit:
        raise HarvestError("INE workbook contains too few national name rows")
    records.sort(key=lambda item: (item["category"], item["rank"]))
    return year, records


def jsonp_payload(value: bytes) -> Any:
    text_value = value.decode("utf-8-sig")
    match = re.fullmatch(r"\s*[A-Za-z_$][\w$]*\((.*)\);?\s*", text_value, re.DOTALL)
    if not match:
        raise HarvestError("Expected a JSONP response")
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError as error:
        raise HarvestError(f"Invalid JSONP response: {error}") from error


def istat_italy_top_names(endpoint: str, limit: int) -> tuple[int, list[dict[str, Any]]]:
    years_url = f"{endpoint}?{urllib.parse.urlencode({'type': 'years', 'callback': 'callbackY'})}"
    years = jsonp_payload(fetch(years_url))
    numeric_years = [int(value) for value in years if str(value).isdigit()] if isinstance(years, list) else []
    if not numeric_years:
        raise HarvestError("Istat name service contains no years")
    year = max(numeric_years)
    list_url = f"{endpoint}?{urllib.parse.urlencode({'type': 'list', 'limit': limit, 'year': year, 'callback': 'callback'})}"
    payload = jsonp_payload(fetch(list_url))
    if not isinstance(payload, dict):
        raise HarvestError("Istat name service schema changed")
    records = []
    for key, category in (("0", "boy"), ("1", "girl")):
        values = payload.get(key)
        if not isinstance(values, list):
            raise HarvestError("Istat name ranking is missing a sex category")
        for rank, item in enumerate(values[:limit], start=1):
            if not isinstance(item, dict) or not item.get("name"):
                continue
            records.append(
                {
                    "rank": rank,
                    "name": str(item["name"]).title(),
                    "count": int(item["count"]),
                    "sharePercent": round(float(item["percent"]), 4),
                    "category": category,
                    "rankGroup": category,
                }
            )
    records.sort(key=lambda item: (item["category"], item["rank"]))
    return year, records


def gfds_germany_top_names(page: bytes, year: int, limit: int) -> tuple[int, list[dict[str, Any]]]:
    text_value = page.decode("utf-8", "replace")

    def clean_cell(value: str) -> str:
        return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", value)).split())

    records = []
    for raw_row in re.findall(r"<tr[^>]*>(.*?)</tr>", text_value, re.I | re.DOTALL):
        cells = [clean_cell(value) for value in re.findall(r"<td[^>]*>(.*?)</td>", raw_row, re.I | re.DOTALL)]
        if len(cells) < 4:
            continue
        parsed = []
        for value in (cells[0], cells[3]):
            match = re.match(r"(\d+)\.\s+(.+?)(?:\s+\(\d+\))?$", value)
            parsed.append((int(match.group(1)), match.group(2)) if match else None)
        if not all(parsed):
            continue
        for category, parsed_value, share_cell in (
            ("girl", parsed[0], cells[2]),
            ("boy", parsed[1], cells[5] if len(cells) > 5 else ""),
        ):
            rank, name = parsed_value
            if rank > limit:
                continue
            record: dict[str, Any] = {
                "rank": rank,
                "name": name,
                "category": category,
                "rankGroup": category,
            }
            share_match = re.search(r"\d+(?:[,.]\d+)?", share_cell)
            if share_match:
                record["sharePercent"] = float(share_match.group(0).replace(",", "."))
            records.append(record)
    if len(records) < 2 * limit:
        raise HarvestError("GfdS page contains too few first-name ranking rows")
    records.sort(key=lambda item: (item["category"], item["rank"]))
    return year, records


def smartstart_new_zealand_top_names(
    page_url: str, page: bytes, limit: int
) -> tuple[int, list[dict[str, Any]]]:
    page_text = page.decode("utf-8", "replace")
    bundle_match = re.search(r'<script[^>]+src=["\']([^"\']*/main\.[^"\']+\.bundle\.js)["\']', page_text, re.I)
    if not bundle_match:
        raise HarvestError("SmartStart main application bundle was not found")
    bundle_url = urllib.parse.urljoin(page_url, html.unescape(bundle_match.group(1)))
    bundle = fetch(bundle_url).decode("utf-8", "replace")
    dataset = re.search(
        r"Mp=\{girls:\{(.*?)\},boys:\{(.*?)\}\};function", bundle, re.DOTALL
    )
    if not dataset:
        raise HarvestError("SmartStart baby-name dataset was not found")

    def parse_category(value: str, category: str) -> tuple[int, list[dict[str, Any]]]:
        years = list(re.finditer(r"(\d{4}):\[(.*?)\](?:,|$)", value, re.DOTALL))
        if not years:
            raise HarvestError(f"SmartStart contains no {category} name years")
        latest = max(years, key=lambda match: int(match.group(1)))
        year = int(latest.group(1))
        items = re.findall(r'\{name:"((?:[^"\\]|\\.)*)",amount:(\d+)\}', latest.group(2))
        records = [
            {
                "rank": rank,
                "name": json.loads(f'"{raw_name}"'),
                "count": int(count),
                "category": category,
                "rankGroup": category,
            }
            for rank, (raw_name, count) in enumerate(items[:limit], start=1)
        ]
        return year, records

    girls_year, girls = parse_category(dataset.group(1), "girl")
    boys_year, boys = parse_category(dataset.group(2), "boy")
    if girls_year != boys_year or len(girls) < limit or len(boys) < limit:
        raise HarvestError("SmartStart boys and girls datasets are incomplete or mismatched")
    records = [*girls, *boys]
    records.sort(key=lambda item: (item["category"], item["rank"]))
    return girls_year, records


def manual_charts(path: Path, date: str, country: str) -> dict[str, Any]:
    value = read_json(path, {})
    if not isinstance(value, dict):
        raise HarvestError(f"Manual source must be an object: {path}")
    dated = value.get(date, {})
    country_value = dated.get(country, {}) if isinstance(dated, dict) else {}
    if not isinstance(country_value, dict):
        raise HarvestError(f"Manual source {date}/{country} must be an object")
    allowed = {"movies", "adultBooks", "childrensBooks", "games", "names"}
    unexpected = set(country_value) - allowed
    if unexpected:
        raise HarvestError(f"Unsupported manual chart keys: {', '.join(sorted(unexpected))}")
    for key, chart in country_value.items():
        validate_chart(chart, key)
    return country_value


def annual_charts(path: Path, year: int, country: str) -> dict[str, Any]:
    value = read_json(path, {})
    if not isinstance(value, dict):
        raise HarvestError(f"Annual source must be an object: {path}")
    year_value = value.get(str(year), {})
    if not isinstance(year_value, dict):
        raise HarvestError(f"Annual source {year} must be an object")
    result: dict[str, Any] = {}
    for territory in ("WORLD", country):
        charts = year_value.get(territory, {})
        if not isinstance(charts, dict):
            raise HarvestError(f"Annual source {year}/{territory} must be an object")
        result.update(copy.deepcopy(charts))
    allowed = {"movies", "adultBooks", "childrensBooks", "games", "names"}
    unexpected = set(result) - allowed
    if unexpected:
        raise HarvestError(f"Unsupported annual chart keys: {', '.join(sorted(unexpected))}")
    for key, chart in result.items():
        validate_chart(chart, key)
    return result


def validate_chart(chart: Any, label: str) -> None:
    if not isinstance(chart, dict) or not isinstance(chart.get("items"), list) or not chart["items"]:
        raise HarvestError(f"{label} must contain a non-empty items array")
    if not isinstance(chart.get("id"), str) or not isinstance(chart.get("title"), str):
        raise HarvestError(f"{label} must contain string id and title fields")
    record_source = chart.get("source")
    required = {"provider", "url", "territory", "metric", "observedAt", "kind"}
    if not isinstance(record_source, dict) or not required.issubset(record_source):
        raise HarvestError(f"{label}.source must contain {', '.join(sorted(required))}")
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in chart["items"]:
        if not isinstance(item, dict):
            raise HarvestError(f"{label} items must be objects")
        groups.setdefault(str(item.get("rankGroup", "all")), []).append(item)
    for category, items in groups.items():
        for expected_rank, item in enumerate(items, start=1):
            if item.get("rank") != expected_rank:
                raise HarvestError(
                    f"{label}/{category} ranks must be consecutive and start at 1"
                )


def validate_name_meanings(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if payload.get("schemaVersion") != SCHEMA_VERSION or not isinstance(entries, list):
        raise HarvestError(f"Invalid name meanings dataset: {path}")
    spellings: set[str] = set()
    for entry in entries:
        required = {"names", "meaning", "origin", "note", "sourceURL"}
        if not isinstance(entry, dict) or not required.issubset(entry):
            raise HarvestError("Every name meaning needs names, meaning, origin, note and sourceURL")
        for name in entry["names"]:
            key = str(name).casefold()
            if key in spellings:
                raise HarvestError(f"Duplicate name spelling: {name}")
            spellings.add(key)
        if not str(entry["sourceURL"]).startswith("https://"):
            raise HarvestError(f"Name source must be HTTPS: {entry['sourceURL']}")
    payload["entryCount"] = len(entries)
    payload["spellingCount"] = len(spellings)
    return payload


def combined_name_meanings(curated_path: Path, bulk_path: Path) -> dict[str, Any]:
    curated = validate_name_meanings(curated_path)
    bulk = validate_name_meanings(bulk_path) if bulk_path.exists() else {"entries": []}
    used: set[str] = set()
    entries = []
    bulk_entries = [
        entry for entry in bulk["entries"]
        if not str(entry.get("meaning", "")).startswith("Etymology tree")
    ]
    for entry in [*curated["entries"], *bulk_entries]:
        available = [name for name in entry["names"] if str(name).casefold() not in used]
        if not available:
            continue
        value = copy.deepcopy(entry)
        value["names"] = available
        entries.append(value)
        used.update(str(name).casefold() for name in available)
    return {
        "schemaVersion": SCHEMA_VERSION,
        "reviewedAt": curated.get("reviewedAt"),
        "policy": curated.get("policy"),
        "licenseNotice": "Curated summaries plus attributed CC BY-SA 4.0 Wiktionary material",
        "licenseURL": "https://creativecommons.org/licenses/by-sa/4.0/",
        "attribution": "English Wiktionary contributors; bulk extraction by Kaikki/Wiktextract",
        "entryCount": len(entries),
        "spellingCount": len(used),
        "entries": entries,
    }


def name_meaning_shard(name: str) -> str:
    folded = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().casefold()
    return folded[0] if folded and "a" <= folded[0] <= "z" else "other"


def content_hash(value: Any) -> str:
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def harvest(config_path: Path, output: Path, cache_dir: Path, date: str, strict: bool) -> int:
    config = read_json(config_path)
    if not isinstance(config, dict):
        raise HarvestError(f"Invalid config: {config_path}")
    countries = [str(value).upper() for value in config.get("countries", [])]
    if not countries:
        raise HarvestError("Config must contain at least one country")
    observed_at = utc_now()
    status: list[dict[str, str]] = []
    snapshots: dict[str, dict[str, Any]] = {
        country: {
            "schemaVersion": SCHEMA_VERSION,
            "date": date,
            "country": country,
            "generatedAt": observed_at,
            "charts": {},
        }
        for country in countries
    }

    music = config.get("appleMusic", {})
    if music.get("enabled", True):
        limit = max(1, min(int(music.get("limit", 10)), 100))
        for country in countries:
            try:
                snapshots[country]["charts"]["music"] = apple_music_chart(country, limit, observed_at)
                status.append({"source": f"appleMusic/{country}", "status": "ok"})
            except HarvestError as error:
                status.append({"source": f"appleMusic/{country}", "status": "error", "detail": str(error)})

    store = config.get("appleStore", {})
    if store.get("enabled", True):
        limit = max(1, min(int(store.get("limit", 10)), 200))
        enabled_charts = store.get("charts", list(APPLE_STORE_FEEDS))
        for country in countries:
            for output_key in enabled_charts:
                if output_key not in APPLE_STORE_FEEDS:
                    raise HarvestError(f"Unknown Apple Store chart: {output_key}")
                try:
                    snapshots[country]["charts"][output_key] = apple_store_chart(
                        country, output_key, limit, observed_at
                    )
                    status.append({"source": f"appleStore/{output_key}/{country}", "status": "ok"})
                except HarvestError as error:
                    status.append(
                        {
                            "source": f"appleStore/{output_key}/{country}",
                            "status": "error",
                            "detail": str(error),
                        }
                    )

    games = config.get("gameOfYear", {})
    if games.get("enabled", False):
        requested_award_year = int(date[:4])
        errors = []
        for award_year in (requested_award_year, requested_award_year - 1):
            try:
                chart = dice_game_of_year(str(games["pageTemplate"]), award_year, observed_at)
                for snapshot in snapshots.values():
                    snapshot["charts"]["games"] = copy.deepcopy(chart)
                detail = None if award_year == requested_award_year else f"retained {award_year} winner"
                record = {"source": "gameOfYear/WORLD", "status": "ok"}
                if detail:
                    record["detail"] = detail
                status.append(record)
                break
            except (HarvestError, ValueError) as error:
                errors.append(str(error))
        else:
            status.append(
                {"source": "gameOfYear/WORLD", "status": "error", "detail": "; ".join(errors)}
            )

    box_office = config.get("theatricalBoxOffice", {})
    if box_office.get("enabled", False) and "GB" in snapshots:
        try:
            snapshots["GB"]["charts"]["movies"] = bfi_box_office_chart(
                str(box_office["bfiPage"]),
                date,
                max(1, min(int(box_office.get("limit", 10)), 15)),
                observed_at,
            )
            status.append({"source": "theatricalBoxOffice/GB", "status": "ok"})
        except (HarvestError, UnicodeDecodeError) as error:
            status.append(
                {"source": "theatricalBoxOffice/GB", "status": "error", "detail": str(error)}
            )
    if box_office.get("enabled", False) and "IT" in snapshots and box_office.get("cinetelPage"):
        try:
            snapshots["IT"]["charts"]["movies"] = cinetel_box_office_chart(
                str(box_office["cinetelPage"]),
                date,
                max(1, min(int(box_office.get("limit", 10)), 10)),
            )
            status.append({"source": "theatricalBoxOffice/IT", "status": "ok"})
        except (HarvestError, UnicodeDecodeError, ValueError) as error:
            status.append(
                {"source": "theatricalBoxOffice/IT", "status": "error", "detail": str(error)}
            )
    for country, page_url in box_office.get("boxOfficeMojoCountries", {}).items():
        country = str(country).upper()
        if not box_office.get("enabled", False) or country not in snapshots:
            continue
        try:
            snapshots[country]["charts"]["movies"] = box_office_mojo_chart(
                str(page_url),
                country,
                date,
                max(1, min(int(box_office.get("limit", 10)), 10)),
            )
            status.append({"source": f"theatricalBoxOffice/{country}", "status": "ok"})
        except (HarvestError, UnicodeDecodeError, ValueError) as error:
            status.append(
                {"source": f"theatricalBoxOffice/{country}", "status": "error", "detail": str(error)}
            )
    if box_office.get("enabled", False) and "DE" in snapshots and box_office.get("mediaControlGermanyPage"):
        try:
            snapshots["DE"]["charts"]["movies"] = media_control_germany_box_office_chart(
                str(box_office["mediaControlGermanyPage"]),
                date,
                max(1, min(int(box_office.get("limit", 10)), 5)),
            )
            status.append({"source": "theatricalBoxOffice/DE", "status": "ok"})
        except (HarvestError, UnicodeDecodeError, ValueError) as error:
            status.append(
                {"source": "theatricalBoxOffice/DE", "status": "error", "detail": str(error)}
            )
    if box_office.get("enabled", False) and "US" in snapshots and box_office.get("theNumbersUSPage"):
        try:
            snapshots["US"]["charts"]["movies"] = the_numbers_us_box_office_chart(
                str(box_office["theNumbersUSPage"]),
                date,
                max(1, min(int(box_office.get("limit", 10)), 10)),
            )
            status.append({"source": "theatricalBoxOffice/US", "status": "ok"})
        except (HarvestError, UnicodeDecodeError, ValueError) as error:
            status.append(
                {"source": "theatricalBoxOffice/US", "status": "error", "detail": str(error)}
            )

    books = config.get("nytBooks", {})
    api_key = os.environ.get(str(books.get("apiKeyEnvironment", "NYT_API_KEY")))
    if books.get("enabled", False) and api_key and "US" in snapshots:
        for list_config in books.get("lists", []):
            list_name = str(list_config["name"])
            key = str(list_config["outputKey"])
            try:
                snapshots["US"]["charts"][key] = nyt_book_chart(
                    list_name, str(list_config["audience"]), api_key, observed_at
                )
                status.append({"source": f"nytBooks/{list_name}", "status": "ok"})
            except HarvestError as error:
                status.append({"source": f"nytBooks/{list_name}", "status": "error", "detail": str(error)})
    elif books.get("enabled", False):
        status.append({"source": "nytBooks", "status": "skipped", "detail": "API key not configured or US disabled"})

    public_books = config.get("publicBookCharts", {})
    public_limit = max(1, min(int(public_books.get("limit", 10)), 10))
    if public_books.get("enabled", False):
        for chart_config in public_books.get("charts", []):
            country = str(chart_config["country"]).upper()
            output_key = str(chart_config["outputKey"])
            if country not in snapshots:
                continue
            chart_type = str(chart_config["type"])
            try:
                if chart_type == "publishersWeekly":
                    chart = publishers_weekly_book_chart(
                        str(chart_config["url"]),
                        country,
                        str(chart_config["title"]),
                        str(chart_config["audience"]),
                        date,
                        public_limit,
                    )
                elif chart_type == "abaAustralia":
                    chart = aba_australia_book_chart(str(chart_config["url"]), date, public_limit)
                elif chart_type == "bookhubNZ":
                    chart = bookhub_new_zealand_chart(str(chart_config["url"]), date, public_limit)
                elif chart_type == "booksellersNZCategory":
                    chart = booksellers_nz_category_chart(
                        str(chart_config["url"]),
                        str(chart_config["heading"]),
                        str(chart_config["title"]),
                        str(chart_config["audience"]),
                        date,
                        public_limit,
                    )
                else:
                    raise HarvestError(f"Unknown public book chart type: {chart_type}")
                snapshots[country]["charts"][output_key] = chart
                status.append({"source": f"publicBooks/{output_key}/{country}", "status": "ok"})
            except (HarvestError, UnicodeDecodeError, ValueError) as error:
                status.append(
                    {
                        "source": f"publicBooks/{output_key}/{country}",
                        "status": "error",
                        "detail": str(error),
                    }
                )

    names = config.get("ssaNames", {})
    if names.get("enabled", False) and "US" in snapshots:
        try:
            archive = cached_ssa_names(cache_dir, int(names.get("refreshDays", 30)))
            year, items = ssa_top_names(archive, max(1, min(int(names.get("limit", 10)), 1000)))
            snapshots["US"]["charts"]["names"] = {
                "id": "ssa-national-baby-names",
                "title": f"Popular baby names for {year} births",
                "year": year,
                "source": source(
                    "U.S. Social Security Administration",
                    "https://www.ssa.gov/oact/babynames/limits.html",
                    "US",
                    "national birth-name count; exact spellings; fewer than five occurrences suppressed",
                    observed_at,
                    "nameRanking",
                ),
                "items": items,
            }
            status.append({"source": "ssaNames/US", "status": "ok"})
        except (HarvestError, ValueError) as error:
            status.append({"source": "ssaNames/US", "status": "error", "detail": str(error)})

    for names_config in config.get("governmentNames", []):
        country = str(names_config["country"]).upper()
        if not names_config.get("enabled", True) or country not in snapshots:
            continue
        provider_type = str(names_config["type"])
        try:
            limit = max(1, min(int(names_config.get("limit", 10)), 1000))
            if provider_type == "statcan":
                archive = cached_download(
                    str(names_config["url"]),
                    cache_dir / "names" / f"{country.lower()}-{provider_type}.zip",
                    int(names_config.get("refreshDays", 30)),
                )
                year, items = statcan_top_names(archive, limit)
            elif provider_type == "insee":
                archive = cached_download(
                    str(names_config["url"]),
                    cache_dir / "names" / f"{country.lower()}-{provider_type}.zip",
                    int(names_config.get("refreshDays", 30)),
                )
                year, items = insee_top_names(archive, limit)
            elif provider_type == "ons":
                archive = cached_download(
                    str(names_config["url"]),
                    cache_dir / "names" / f"{country.lower()}-{provider_type}.xlsx",
                    int(names_config.get("refreshDays", 30)),
                )
                year, items = ons_top_names(archive, limit)
            elif provider_type == "csoIreland":
                boys = cached_download(
                    str(names_config["boysURL"]),
                    cache_dir / "names" / "ie-cso-boys.json",
                    int(names_config.get("refreshDays", 30)),
                )
                girls = cached_download(
                    str(names_config["girlsURL"]),
                    cache_dir / "names" / "ie-cso-girls.json",
                    int(names_config.get("refreshDays", 30)),
                )
                year, items = cso_ireland_top_names(boys, girls, limit)
            elif provider_type == "ineSpain":
                source_page = str(names_config["sourcePage"])
                discovered_year, workbook_url = ine_spain_workbook_url(
                    source_page, fetch(source_page)
                )
                workbook = cached_download(
                    workbook_url,
                    cache_dir / "names" / f"es-ine-{discovered_year}.xlsx",
                    int(names_config.get("refreshDays", 30)),
                )
                year, items = ine_spain_top_names(workbook, discovered_year, limit)
            elif provider_type == "istatItaly":
                year, items = istat_italy_top_names(str(names_config["url"]), limit)
            elif provider_type == "gfdsGermany":
                requested_year = int(date[:4]) - 1
                errors = []
                for candidate_year in (requested_year, requested_year - 1):
                    try:
                        detail_url = str(names_config["pageTemplate"]).format(year=candidate_year)
                        year, items = gfds_germany_top_names(
                            fetch(detail_url), candidate_year, limit
                        )
                        break
                    except HarvestError as error:
                        errors.append(str(error))
                else:
                    raise HarvestError("; ".join(errors))
            elif provider_type == "smartstartNZ":
                source_page = str(names_config["sourcePage"])
                year, items = smartstart_new_zealand_top_names(
                    source_page, fetch(source_page), limit
                )
            else:
                raise HarvestError(f"Unsupported government names adapter: {provider_type}")
            if not items:
                raise HarvestError(f"No names found in {provider_type} data")
            snapshots[country]["charts"]["names"] = {
                "id": str(names_config["id"]),
                "title": f"Popular baby names for {year} births",
                "year": year,
                "source": source(
                    str(names_config["provider"]),
                    str(names_config["sourcePage"]),
                    country,
                    str(names_config["metric"]),
                    observed_at,
                    "nameRanking",
                ),
                "items": items,
            }
            status.append({"source": f"governmentNames/{country}", "status": "ok"})
        except (HarvestError, ValueError, zipfile.BadZipFile) as error:
            status.append(
                {"source": f"governmentNames/{country}", "status": "error", "detail": str(error)}
            )

    seed_path = config_path.parent / str(config.get("seedData", "seed-names.json"))
    seed_data = read_json(seed_path, {})
    if not isinstance(seed_data, dict):
        raise HarvestError(f"Seed source must be an object: {seed_path}")
    for country in countries:
        if "names" in snapshots[country]["charts"]:
            continue
        latest_path = output / "v1" / "latest" / f"{country.lower()}.json"
        previous = read_json(latest_path, {})
        previous_charts = previous.get("charts", {}) if isinstance(previous, dict) else {}
        previous_names = previous_charts.get("names") if isinstance(previous_charts, dict) else None
        if isinstance(previous_names, dict) and previous_names.get("source", {}).get("kind") != "nameRanking":
            previous_names = None
        seeded_names = seed_data.get(country) if isinstance(seed_data.get(country), dict) else None
        fallback = previous_names or seeded_names
        if fallback:
            validate_chart(fallback, f"names/{country}")
            snapshots[country]["charts"]["names"] = copy.deepcopy(fallback)
            status.append(
                {
                    "source": f"namesFallback/{country}",
                    "status": "carried" if previous_names else "seeded",
                    "detail": "latest published annual chart retained until its official replacement",
                }
            )

    annual_path = config_path.parent / str(config.get("annualData", "annual.json"))
    for country in countries:
        try:
            annual = annual_charts(annual_path, int(date[:4]), country)
            snapshots[country]["charts"].update(annual)
            if annual:
                status.append({"source": f"annual/{country}", "status": "ok"})
        except (HarvestError, ValueError) as error:
            status.append({"source": f"annual/{country}", "status": "error", "detail": str(error)})

    manual_path = config_path.parent / str(config.get("manualData", "manual.json"))
    for country in countries:
        try:
            manual = manual_charts(manual_path, date, country)
            snapshots[country]["charts"].update(copy.deepcopy(manual))
            if manual:
                status.append({"source": f"manual/{country}", "status": "ok"})
        except HarvestError as error:
            status.append({"source": f"manual/{country}", "status": "error", "detail": str(error)})

    for country, snapshot in snapshots.items():
        if not snapshot["charts"]:
            status.append({"source": f"snapshot/{country}", "status": "skipped", "detail": "no charts available"})
            continue
        for label, chart in snapshot["charts"].items():
            validate_chart(chart, label)
        snapshot["contentHash"] = content_hash(snapshot["charts"])
        dated_path = output / "v1" / "dates" / date / f"{country.lower()}.json"
        latest_path = output / "v1" / "latest" / f"{country.lower()}.json"
        atomic_json(dated_path, snapshot)
        atomic_json(latest_path, snapshot)

    meanings_path = config_path.parent / str(config.get("nameMeanings", "name-meanings.json"))
    bulk_meanings_path = config_path.parent / str(
        config.get("bulkNameMeanings", "wiktionary-name-meanings.json")
    )
    meanings = combined_name_meanings(meanings_path, bulk_meanings_path)
    meanings["generatedAt"] = observed_at
    atomic_json(output / "v1" / "editorial" / "name-meanings.json", meanings)
    shards: dict[str, list[dict[str, Any]]] = {
        key: [] for key in [*(chr(value) for value in range(ord("a"), ord("z") + 1)), "other"]
    }
    for entry in meanings["entries"]:
        for key in {name_meaning_shard(str(name)) for name in entry["names"]}:
            shards[key].append(entry)
    for key, entries in shards.items():
        shard = {field: value for field, value in meanings.items() if field != "entries"}
        shard["entryCount"] = len(entries)
        shard["entries"] = entries
        atomic_json(output / "v1" / "editorial" / "name-meanings" / f"{key}.json", shard)

    errors = [entry for entry in status if entry["status"] == "error"]
    manifest = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": observed_at,
        "latestDate": date,
        "countries": sorted(
            country for country, snapshot in snapshots.items() if snapshot["charts"]
        ),
        "endpoints": {
            "latest": "v1/latest/{country}.json",
            "byDate": "v1/dates/{yyyy-mm-dd}/{country}.json",
            "historyYear": "v1/history/{country}/{year}.json",
            "nameMeanings": "v1/editorial/name-meanings.json",
            "nameMeaningShard": "v1/editorial/name-meanings/{initial}.json",
            "birthdayTwinMonth": "v1/editorial/birthday-twins/{month}.json",
        },
        "runStatus": status,
    }
    atomic_json(output / "manifest.json", manifest)
    for entry in status:
        detail = f": {entry['detail']}" if entry.get("detail") else ""
        print(f"{entry['status'].upper():7} {entry['source']}{detail}")
    print(f"Wrote {len(manifest['countries'])} country snapshots to {output}")
    return 1 if strict and errors else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[2]
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.json"))
    parser.add_argument("--output", type=Path, default=root / "hosted-data")
    parser.add_argument("--cache", type=Path, default=root / ".cache" / "data-harvest")
    parser.add_argument("--date", default=dt.date.today().isoformat(), help="snapshot date (YYYY-MM-DD)")
    parser.add_argument("--strict", action="store_true", help="fail if any enabled source fails")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        dt.date.fromisoformat(args.date)
        return harvest(args.config.resolve(), args.output.resolve(), args.cache.resolve(), args.date, args.strict)
    except (HarvestError, ValueError, KeyError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
