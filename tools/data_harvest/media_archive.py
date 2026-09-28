#!/usr/bin/env python3
"""Publish annual TV/game facts and attributable artwork without an app release.

Run after harvest.py. Reviewed seeds and previous valid years remain available;
only exact-year BARB broadcasts and officially published D.I.C.E. winners update.
TVmaze URL metadata is CC BY-SA (https://www.tvmaze.com/api#licensing).
"""
from __future__ import annotations
import argparse
import copy
import datetime as dt
import html
import json
import re
import urllib.parse
from pathlib import Path
from typing import Any
try:
    from . import harvest
except ImportError:
    import harvest


def text(value: str) -> str:
    return html.unescape(re.sub(r"<[^>]*>", "", value)).strip()


def normalized(value: str) -> str:
    return re.sub(r"[^\w]+", "", value.lower().replace("&", "and"))


def barb_television(year: int, observed: str) -> dict[str, Any]:
    url = f"https://www.barb.co.uk/tv-since-1981/{year}/top10/"
    page = harvest.fetch(url).decode("utf-8")
    table = re.search(r'<table\b[^>]*id=["\']top_programmes["\'][^>]*>(.*?)</table>', page, re.S | re.I)
    if not table:
        raise harvest.HarvestError(f"BARB has no annual table for {year}")
    headers = [text(cell) for cell in re.findall(r"<th\b[^>]*>(.*?)</th>", table[1], re.S | re.I)]
    rows = re.findall(r"<tr\b[^>]*>(.*?)</tr>", table[1], re.S | re.I)
    cells = next(([text(cell) for cell in re.findall(r"<td\b[^>]*>(.*?)</td>", row, re.S | re.I)]
                  for row in rows if re.search(r"<td\b[^>]*>\s*1\s*</td>", row, re.I)), [])
    if len(cells) < 5 or len(headers) != len(cells) or cells[0] != "1":
        raise harvest.HarvestError("BARB rank-one row is incomplete")
    try:
        broadcast = dt.datetime.strptime(cells[2], "%d %b %Y")
    except ValueError as error:
        raise harvest.HarvestError("BARB broadcast date is invalid") from error
    if broadcast.year != year or not re.fullmatch(r"\d+(?:\.\d+)?", cells[-1]):
        raise harvest.HarvestError("BARB returned another year or an invalid audience")
    four_screen = "four-screen" in headers[-1].lower()
    audience = cells[-1] + "m" + (" four-screen" if four_screen else "")
    return {"id": f"barb-television-{year}", "title": "UK most-watched broadcast", "year": year,
            "source": harvest.source("BARB", url, "GB", "Annual rank 1 · consolidated audience", observed, "annualTelevisionAudience"),
            "items": [{"rank": 1, "title": cells[1], "broadcastDate": cells[2], "channel": cells[3], "audience": audience}]}


def television_artwork(title: str) -> dict[str, str]:
    # Episode suffixes can be removed; sporting/royal broadcasts never borrow a
    # vaguely related series poster. Require one exact named UK series match.
    query = re.split(r":\s*Series\s+\d", title, flags=re.I)[0]
    query = {"Britain's Got Talent Final Result": "Britain's Got Talent", "The X Factor Results": "The X Factor"}.get(query, query)
    url = "https://api.tvmaze.com/search/shows?" + urllib.parse.urlencode({"q": query})
    matches = []
    for result in json.loads(harvest.fetch(url)):
        show = result.get("show", {})
        network = show.get("network") or show.get("webChannel") or {}
        if normalized(show.get("name", "")) == normalized(query) and (network.get("country") or {}).get("code") == "GB":
            matches.append(show)
    if len(matches) != 1:
        return {}
    show = matches[0]
    image = (show.get("image") or {}).get("medium")
    if not image or not image.startswith("https://") or not show.get("url", "").startswith("https://"):
        return {}
    return {"artworkURL": image, "artworkSourceURL": show["url"], "artworkProviderName": "TVmaze"}


def game_artwork(chart: dict[str, Any]) -> dict[str, str]:
    # The official category page carries the winning game's named image, so this
    # works for future games without a Steam ID, API key or fuzzy title search.
    url = chart["source"]["url"]
    page = harvest.fetch(url).decode("iso-8859-1")
    title = chart["items"][0]["title"]
    for image in re.findall(r"<img\b[^>]*>", page, re.I):
        alt = re.search(r'\balt=(["\'])(.*?)\1', image, re.I)
        src = re.search(r'\bsrc=(["\'])(.*?)\1', image, re.I)
        if alt and src and normalized(html.unescape(alt[2])) == normalized(title):
            image_url = urllib.parse.urljoin(url, html.unescape(src[2]))
            if image_url.startswith("https://"):
                return {"artworkURL": image_url, "artworkSourceURL": url, "artworkProviderName": chart["source"]["provider"]}
    return {}


def matching_item(archive: dict[str, Any], kind: str, title: str) -> dict[str, Any]:
    for territories in archive.values():
        for charts in territories.values():
            for item in charts.get(kind, {}).get("items", []):
                if normalized(item.get("title", "")) == normalized(title):
                    return item
    return {}


def update_archive(output: Path, seed: Path, countries: list[str], date: str) -> dict[str, Any]:
    archive: dict[str, Any] = {}
    # Restore newly discovered years before applying explicitly reviewed seeds.
    for path in sorted((output / "v1/media").glob("*/*.json")):
        value = harvest.read_json(path, {})
        year, country = value.get("year"), value.get("country", "").upper()
        if value.get("schemaVersion") != 1 or str(year) != path.parent.name:
            continue
        charts = {key: chart for key, chart in value.get("charts", {}).items()
                  if (key == "games" and country == "WORLD") or (key == "television" and country == "GB")}
        if charts:
            archive.setdefault(str(year), {}).setdefault(country, {}).update(charts)
    for year, territories in harvest.read_json(seed, {}).items():
        for country, charts in territories.items():
            archive.setdefault(year, {}).setdefault(country, {}).update(copy.deepcopy(charts))
    observed = date + "T00:00:00Z"
    status = []
    current_year = int(date[:4])
    try:
        chart = barb_television(current_year - 1, observed)
        item = chart["items"][0]
        previous = matching_item(archive, "television", item["title"])
        artwork = {key: previous[key] for key in ("artworkURL", "artworkSourceURL", "artworkProviderName") if key in previous}
        try:
            item.update(artwork or television_artwork(item["title"]))
        except (harvest.HarvestError, ValueError) as error:
            status.append({"source": "mediaArtwork/GB", "status": "error", "detail": str(error)})
        archive.setdefault(str(chart["year"]), {}).setdefault("GB", {})["television"] = chart
        status.append({"source": "mediaTelevision/GB", "status": "ok"})
    except (harvest.HarvestError, ValueError) as error:
        status.append({"source": "mediaTelevision/GB", "status": "error", "detail": str(error)})
    # Daily harvest already checks this year's ceremony and last year's published
    # winner. Archive under the actual award year, never the snapshot's year.
    games = []
    for path in sorted((output / "v1/latest").glob("*.json")):
        chart = harvest.read_json(path, {}).get("charts", {}).get("games")
        if chart and chart.get("source", {}).get("kind") == "awardWinner" and chart.get("year"):
            games.append(chart)
    if games:
        chart = copy.deepcopy(max(games, key=lambda chart: chart["year"]))
        year = int(chart["year"])
        existing = archive.get(str(year), {}).get("WORLD", {}).get("games")
        # Preserve the historical TGA/BAFTA selection; D.I.C.E. is the automated
        # source only for 2026 onward, matching the app's existing year boundary.
        if 2026 <= year <= current_year:
            item = chart["items"][0]
            previous = matching_item(archive, "games", item["title"])
            for key in ("artworkURL", "artworkSourceURL", "artworkProviderName", "trailerURL", "releaseYear", "releaseDate"):
                if key in previous and key not in item:
                    item[key] = previous[key]
            if not item.get("artworkURL"):
                try:
                    item.update(game_artwork(chart))
                except (harvest.HarvestError, ValueError) as error:
                    status.append({"source": "mediaArtwork/WORLD", "status": "error", "detail": str(error)})
            if existing and normalized(existing["items"][0]["title"]) == normalized(item["title"]):
                for key in ("artworkURL", "artworkSourceURL", "artworkProviderName", "trailerURL", "releaseYear", "releaseDate"):
                    if key in existing["items"][0] and key not in item:
                        item[key] = existing["items"][0][key]
            archive.setdefault(str(year), {}).setdefault("WORLD", {})["games"] = chart
            status.append({"source": "mediaGames/WORLD", "status": "ok"})
    for year, territories in archive.items():
        for country in sorted(set(["WORLD", *countries])):
            charts = copy.deepcopy(territories.get("WORLD", {}))
            charts.update(copy.deepcopy(territories.get(country, {})))
            if not charts:
                continue
            for key, chart in charts.items():
                harvest.validate_chart(chart, key)
                if chart.get("year") != int(year):
                    raise harvest.HarvestError("Annual media chart year does not match its endpoint")
            value = {"schemaVersion": 1, "year": int(year), "country": country.lower(), "charts": charts,
                     "contentHash": harvest.content_hash(charts)}
            harvest.atomic_json(output / f"v1/media/{year}/{country.lower()}.json", value)
    manifest = harvest.read_json(output / "manifest.json", {})
    if manifest:
        manifest.setdefault("endpoints", {})["mediaYear"] = "v1/media/{year}/{country}.json"
        manifest["runStatus"] = [entry for entry in manifest.get("runStatus", [])
                                 if not entry.get("source", "").startswith(("mediaArtwork/", "mediaTelevision/", "mediaGames/"))] + status
        harvest.atomic_json(output / "manifest.json", manifest)
    for entry in status:
        print(f"{entry['status'].upper():7} {entry['source']}: {entry.get('detail', 'annual archive refreshed')}")
    return archive


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[2] / "hosted-data")
    parser.add_argument("--date", default=dt.datetime.now(dt.timezone.utc).date().isoformat())
    args = parser.parse_args()
    config = harvest.read_json(Path(__file__).with_name("config.json"), {})
    update_archive(args.output, Path(__file__).with_name("media-seed.json"), config.get("countries", []), args.date)

if __name__ == "__main__":
    main()
