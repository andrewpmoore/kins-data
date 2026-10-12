#!/usr/bin/env python3
"""Build a weekly movie/game trailer index from explicitly qualified Wikidata links.

Movie titles come from the existing static API. Current and previous D.I.C.E.
Game of the Year winners are read from the awards body's public winner pages.
YouTube pages and APIs are never accessed; video content is never downloaded.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import re
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "KinsDataTrailerIndex/1.0 (https://github.com/andrewpmoore/kins-data/issues)"
VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
BATCH_SIZE = 20
CLASSES = {"movies": "Q11424", "games": "Q7889"}
DICE_PAGE = "https://www.interactive.org/awards/award_category_details.asp?idAward={year}&idGameAwardType=65"


def load_movie_targets(data_dir: Path, start_year: int, end_year: int) -> set[tuple[str, str, int]]:
    targets: set[tuple[str, str, int]] = set()
    paths = sorted((data_dir / "v1/latest").glob("*.json"))
    paths += sorted((data_dir / "v1/dates").glob("*/*.json"))
    for path in paths:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        chart = data.get("charts", {}).get("movies")
        if not isinstance(chart, dict):
            continue
        year_value = chart.get("year") or data.get("year") or str(data.get("date", ""))[:4]
        try:
            year = int(year_value)
        except (TypeError, ValueError):
            continue
        if not start_year <= year <= end_year:
            continue
        for item in chart.get("items", []):
            title = item.get("title") if isinstance(item, dict) else None
            if isinstance(title, str) and title.strip():
                targets.add(("movies", title.strip(), year))
    return targets


def fetch_dice_winner(year: int) -> str | None:
    request = urllib.request.Request(DICE_PAGE.format(year=year), headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            page = response.read().decode("iso-8859-1")
    except (OSError, UnicodeDecodeError):
        return None
    match = re.search(
        r"<h2>\s*Game of the Year\s*</h2>.*?"
        r'<div[^>]*aias-award-details-label[^>]*>\s*Winner:\s*</div>.*?'
        r'<a href="[^"]+">(.*?)</a>',
        page, re.I | re.S,
    )
    title = html.unescape(re.sub(r"<[^>]*>", "", match.group(1))).strip() if match else ""
    return title or None


def sparql_literal(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def query_batch(kind: str, titles: list[str]) -> list[dict[str, str]]:
    class_id = CLASSES[kind]
    values = " ".join(f"{sparql_literal(title)}@en" for title in titles)
    query = f"""SELECT ?title ?film ?video ?releaseYear WHERE {{
      VALUES ?title {{ {values} }}
      ?film wdt:P31/wdt:P279* wd:{class_id} ; rdfs:label ?title ; p:P1651 ?statement .
      ?statement ps:P1651 ?video ; pq:P3831 wd:Q622550 .
      OPTIONAL {{ ?film wdt:P577 ?date . BIND(YEAR(?date) AS ?releaseYear) }}
    }}"""
    request = urllib.request.Request(
        ENDPOINT,
        data=urllib.parse.urlencode({"query": query, "format": "json"}).encode("utf-8"),
        headers={"Accept": "application/sparql-results+json", "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "User-Agent": USER_AGENT},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    rows = []
    for binding in payload.get("results", {}).get("bindings", []):
        title = binding.get("title", {}).get("value", "")
        film_uri = binding.get("film", {}).get("value", "")
        video = binding.get("video", {}).get("value", "")
        year = binding.get("releaseYear", {}).get("value", "")
        if title and VIDEO_ID.fullmatch(video):
            rows.append({"title": title, "film": film_uri, "videoID": video, "releaseYear": year})
    return rows


def resolve(targets: set[tuple[str, str, int]]) -> list[dict[str, Any]]:
    titles_by_kind: dict[str, set[str]] = defaultdict(set)
    for kind, title, _ in targets:
        titles_by_kind[kind].add(title)
    candidates: dict[tuple[str, str], set[tuple[str, str, str]]] = defaultdict(set)
    for kind, title_set in sorted(titles_by_kind.items()):
        titles = sorted(title_set)
        for index in range(0, len(titles), BATCH_SIZE):
            for row in query_batch(kind, titles[index:index + BATCH_SIZE]):
                candidates[(kind, row["title"])].add((row["videoID"], row["releaseYear"], row["film"]))
            if index + BATCH_SIZE < len(titles):
                time.sleep(1.2)

    records: list[dict[str, Any]] = []
    for kind, title, chart_year in sorted(targets, key=lambda item: (item[0], item[2], item[1].casefold())):
        matches = []
        for video_id, release_year_text, film_uri in candidates.get((kind, title), set()):
            if not release_year_text.isdigit():
                continue
            release_year = int(release_year_text)
            # Movies top charts in their release year; award winners can be a
            # year older than the D.I.C.E. ceremony. Keep only close matches.
            max_distance = 1 if kind == "movies" else 2
            if abs(release_year - chart_year) <= max_distance:
                matches.append((abs(release_year - chart_year), video_id, release_year, film_uri))
        if not matches:
            if kind == "games":
                records.append({
                    "kind": kind,
                    "title": title,
                    "chartYear": chart_year,
                    "releaseYear": None,
                    "trailerURL": None,
                    "titleSourceURL": DICE_PAGE.format(year=chart_year),
                    "wikidataSources": [],
                })
            continue
        closest_distance = min(match[0] for match in matches)
        closest = {match[1:] for match in matches if match[0] == closest_distance}
        video_ids = {item[0] for item in closest}
        if len(video_ids) != 1:
            if kind == "games":
                records.append({
                    "kind": kind,
                    "title": title,
                    "chartYear": chart_year,
                    "releaseYear": None,
                    "trailerURL": None,
                    "titleSourceURL": DICE_PAGE.format(year=chart_year),
                    "wikidataSources": [],
                })
            continue
        video_id = next(iter(video_ids))
        release_years = sorted({item[1] for item in closest})
        film_uris = sorted({item[2] for item in closest})
        record = {
            "kind": kind,
            "title": title,
            "chartYear": chart_year,
            "releaseYear": release_years[0],
            "trailerURL": f"https://www.youtube.com/watch?v={video_id}",
            "wikidataSources": film_uris,
        }
        if kind == "games":
            record["titleSourceURL"] = DICE_PAGE.format(year=chart_year)
        records.append(record)
    return records


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("api"))
    parser.add_argument("--output", type=Path, default=Path("api/media-trailers.json"))
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--max-age-days", type=int, help="skip if the existing index was generated within this many days")
    args = parser.parse_args()
    current_time = dt.datetime.now(dt.timezone.utc)
    if args.max_age_days is not None and args.output.exists():
        try:
            previous = json.loads(args.output.read_text(encoding="utf-8"))
            generated_at = dt.datetime.fromisoformat(previous["generatedAt"].replace("Z", "+00:00"))
            if current_time - generated_at < dt.timedelta(days=max(args.max_age_days, 0)):
                print(f"Index refreshed {generated_at.isoformat()}; skipping until it is older than {args.max_age_days} days")
                return
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            pass
    current_year = current_time.year
    end_year = args.end_year or current_year
    start_year = args.start_year or end_year - 9
    targets = load_movie_targets(args.data_dir, start_year, end_year)
    # Recheck the most recent and preceding award year so a newly announced winner
    # enters the next weekly index. Keep the last known game record if the award
    # page temporarily fails or has not published the current winner yet.
    previous_games: list[dict[str, Any]] = []
    if args.output.exists():
        try:
            previous = json.loads(args.output.read_text(encoding="utf-8"))
            for target in previous.get("targets", []):
                if target.get("kind") == "movies":
                    try:
                        year = int(target.get("chartYear"))
                    except (TypeError, ValueError):
                        continue
                    title = target.get("title")
                    if start_year <= year <= end_year and isinstance(title, str) and title.strip():
                        targets.add(("movies", title.strip(), year))
            previous_games = [record for record in previous.get("records", [])
                              if record.get("kind") == "games"
                              and record.get("chartYear") in (current_year - 1, current_year)]
        except (OSError, json.JSONDecodeError, AttributeError):
            previous_games = []
    dice_winners: dict[int, str] = {}
    for award_year in (current_year - 1, current_year):
        winner = fetch_dice_winner(award_year)
        if winner:
            dice_winners[award_year] = winner
            targets.add(("games", winner, award_year))
    if not targets:
        raise SystemExit(f"No movie or game titles found for {start_year}-{end_year}; leaving previous output intact")
    records = resolve(targets)
    current_games = {(record["title"], record["chartYear"]): record
                     for record in records if record["kind"] == "games"}
    for previous_game in previous_games:
        key = (previous_game.get("title"), previous_game.get("chartYear"))
        award_year = previous_game.get("chartYear")
        if award_year in dice_winners:
            # The official page confirms the current title; keep a previously
            # indexed link if the Wikidata statement was temporarily unavailable.
            current_game = current_games.get((dice_winners[award_year], award_year))
            if current_game is not None and not current_game.get("trailerURL") and previous_game.get("trailerURL"):
                current_game["trailerURL"] = previous_game["trailerURL"]
                current_game["releaseYear"] = previous_game.get("releaseYear")
                current_game["wikidataSources"] = previous_game.get("wikidataSources", [])
            continue
        if key not in current_games:
            records.append(previous_game)
    value = {
        "schemaVersion": 1,
        "generatedAt": current_time.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "targets": [
            {"kind": kind, "title": title, "chartYear": chart_year}
            for kind, title, chart_year in sorted(targets, key=lambda item: (item[0], item[2], item[1].casefold()))
        ],
        "coverage": {
            "startYear": start_year,
            "endYear": end_year,
            "targetCount": len(targets),
            "recordCount": len(records),
            "matchedTrailerCount": sum(bool(record.get("trailerURL")) for record in records),
        },
        "source": "Wikidata (CC0)",
        "attribution": "Wikidata contributors",
        "records": records,
    }
    atomic_json(args.output, value)
    by_kind = defaultdict(int)
    for record in records:
        by_kind[record["kind"]] += 1
    print(f"Wrote {len(records)} trailer matches for {len(targets)} title/year pairs ({start_year}-{end_year}): {dict(by_kind)}")


if __name__ == "__main__":
    main()
