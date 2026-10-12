#!/usr/bin/env python3
"""Build a weekly movie/game trailer index from Wikidata and public APIs.

Movie titles come from hosted box-office charts. Current and previous D.I.C.E.
Game of the Year winners are read from the awards body's public winner pages.
YouTube gaps are searched with the official Data API when configured; pages are
not scraped and video content is never downloaded.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
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
YOUTUBE_API = "https://www.googleapis.com/youtube/v3"
YOUTUBE_SEARCH_LIMIT = 90
YOUTUBE_DATA_MAX_AGE_DAYS = 28


def normalized(value: str) -> str:
    return re.sub(r"[^\w]+", "", value.lower().replace("&", "and"))


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


def video_id_from_url(value: str) -> str | None:
    parsed = urllib.parse.urlparse(value)
    host = (parsed.hostname or "").lower()
    if host == "youtu.be":
        video_id = parsed.path.strip("/").split("/")[0]
    elif host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        parts = [part for part in parsed.path.split("/") if part]
        if parsed.path == "/watch":
            video_id = urllib.parse.parse_qs(parsed.query).get("v", [""])[0]
        elif len(parts) == 2 and parts[0] in {"embed", "shorts", "live"}:
            video_id = parts[1]
        else:
            return None
    else:
        return None
    return video_id if VIDEO_ID.fullmatch(video_id) else None


def load_manual_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"Cannot read manual trailer list {path}: {error}") from error
    if not isinstance(value, dict) or value.get("schemaVersion") != 1 or not isinstance(value.get("records"), list):
        raise SystemExit(f"Manual trailer list {path} must have schemaVersion 1 and a records array")
    records = []
    seen = set()
    for number, entry in enumerate(value["records"], 1):
        if not isinstance(entry, dict):
            raise SystemExit(f"Manual trailer record {number} must be an object")
        kind, title = entry.get("kind"), entry.get("title")
        try:
            chart_year = int(entry.get("chartYear"))
        except (TypeError, ValueError) as error:
            raise SystemExit(f"Manual trailer record {number} needs an integer chartYear") from error
        video_id = video_id_from_url(str(entry.get("trailerURL", "")))
        source_url = str(entry.get("sourceURL", ""))
        source = urllib.parse.urlparse(source_url)
        if kind not in {"movies", "games"} or not isinstance(title, str) or not title.strip():
            raise SystemExit(f"Manual trailer record {number} needs kind movies/games and a title")
        if video_id is None or source.scheme not in {"https", "http"} or not source.netloc:
            raise SystemExit(f"Manual trailer record {number} needs a valid YouTube trailerURL and http(s) sourceURL")
        key = (kind, title.strip().casefold(), chart_year)
        if key in seen:
            raise SystemExit(f"Duplicate manual trailer record for {kind} {title!r} ({chart_year})")
        seen.add(key)
        release_year = entry.get("releaseYear")
        if release_year is not None:
            try:
                release_year = int(release_year)
            except (TypeError, ValueError) as error:
                raise SystemExit(f"Manual trailer record {number} has an invalid releaseYear") from error
        record = {
            "kind": kind,
            "title": title.strip(),
            "chartYear": chart_year,
            "releaseYear": release_year,
            "trailerURL": f"https://www.youtube.com/watch?v={video_id}",
            "curationSourceURL": source_url,
            "wikidataSources": [],
        }
        for optional in ("titleSourceURL",):
            if isinstance(entry.get(optional), str):
                record[optional] = entry[optional]
        records.append(record)
    return records


def youtube_request(path: str, params: dict[str, str], api_key: str) -> dict[str, Any] | None:
    query = urllib.parse.urlencode({**params, "key": api_key})
    request = urllib.request.Request(
        f"{YOUTUBE_API}/{path}?{query}",
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload if isinstance(payload, dict) else None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        status = getattr(error, "code", None)
        print(f"WARNING YouTube API {path} request failed" + (f" (HTTP {status})" if status else ""))
        return None


def video_id_from_record(record: dict[str, Any]) -> str | None:
    return video_id_from_url(str(record.get("trailerURL", "")))


def refresh_api_records(
    previous_records: list[dict[str, Any]], api_key: str | None, now: dt.datetime
) -> tuple[list[dict[str, Any]], set[tuple[str, str, int]]]:
    api_records = [record for record in previous_records if record.get("matchSource") == "youtubeDataAPI"]
    if not api_records:
        return [], set()
    retained = []
    refreshed_targets = set()
    for record in api_records:
        target = (record.get("kind"), str(record.get("title", "")).casefold(), record.get("chartYear"))
        refreshed_at = record.get("apiVerifiedAt")
        try:
            age = now - dt.datetime.fromisoformat(str(refreshed_at).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            age = dt.timedelta(days=YOUTUBE_DATA_MAX_AGE_DAYS + 1)
        if age <= dt.timedelta(days=YOUTUBE_DATA_MAX_AGE_DAYS):
            retained.append(record)
            refreshed_targets.add(target)
    if not api_key:
        print("WARNING YOUTUBE_API_KEY is not configured; keeping only API matches verified within 28 days")
        return retained, refreshed_targets

    ids = sorted({video_id for record in api_records if (video_id := video_id_from_record(record))})
    available: set[str] | None = set()
    for offset in range(0, len(ids), 50):
        payload = youtube_request("videos", {"part": "id,status", "id": ",".join(ids[offset:offset + 50])}, api_key)
        if payload is None:
            available = None
            break
        for item in payload.get("items", []):
            status = item.get("status", {})
            if status.get("privacyStatus") == "public" and status.get("embeddable") is True:
                available.add(str(item.get("id", "")))
    if available is None:
        print("WARNING Could not revalidate saved YouTube matches; retaining only those younger than 28 days")
        return retained, refreshed_targets

    timestamp = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    refreshed = []
    refreshed_targets = set()
    for record in api_records:
        video_id = video_id_from_record(record)
        if video_id and video_id in available:
            record = {**record, "apiVerifiedAt": timestamp}
            refreshed.append(record)
            refreshed_targets.add((record["kind"], record["title"].casefold(), record["chartYear"]))
    return refreshed, refreshed_targets


def youtube_search_trailer(kind: str, title: str, year: int, api_key: str) -> tuple[bool, str | None]:
    query = f'"{title}" official trailer {year}'
    payload = youtube_request("search", {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": "10",
        "order": "relevance",
        "videoEmbeddable": "true",
        "videoSyndicated": "true",
    }, api_key)
    if payload is None:
        return False, None
    title_key = normalized(title)
    candidates = set()
    for item in payload.get("items", []):
        video_id = str(item.get("id", {}).get("videoId", ""))
        video_title = str(item.get("snippet", {}).get("title", ""))
        video_title_key = normalized(video_title)
        if (VIDEO_ID.fullmatch(video_id)
                and title_key in video_title_key
                and re.search(r"\b(trailer|teaser)\b", video_title, re.I)):
            candidates.add(video_id)
    # Ambiguous results should remain a search fallback rather than guess.
    return True, next(iter(candidates)) if len(candidates) == 1 else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("hosted-data"))
    parser.add_argument("--output", type=Path, default=Path("hosted-data/media-trailers.json"))
    parser.add_argument("--manual-input", type=Path, default=Path("tools/data_harvest/manual-media-trailers.json"))
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    args = parser.parse_args()
    current_time = dt.datetime.now(dt.timezone.utc)
    current_year = current_time.year
    end_year = args.end_year or current_year
    start_year = args.start_year or end_year - 9
    targets = load_movie_targets(args.data_dir, start_year, end_year)
    manual_records = load_manual_records(args.manual_input)
    # On each weekly refresh, check current/prior D.I.C.E. winners. Keep known
    # game records if the award page temporarily fails or has no new winner.
    previous_games: list[dict[str, Any]] = []
    previous_records: list[dict[str, Any]] = []
    youtube_search_cursor: dict[str, Any] | None = None
    if args.output.exists():
        try:
            previous = json.loads(args.output.read_text(encoding="utf-8"))
            previous_records = [record for record in previous.get("records", []) if isinstance(record, dict)]
            stored_cursor = previous.get("youtubeSearchCursor")
            if isinstance(stored_cursor, dict):
                youtube_search_cursor = stored_cursor
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
                              and record.get("matchSource") != "youtubeDataAPI"
                              and record.get("chartYear") in (current_year - 1, current_year)]
        except (OSError, json.JSONDecodeError, AttributeError):
            previous_games = []
    dice_winners: dict[int, str] = {}
    for award_year in (current_year - 1, current_year):
        winner = fetch_dice_winner(award_year)
        if winner:
            dice_winners[award_year] = winner
            targets.add(("games", winner, award_year))
    for record in manual_records:
        targets.add((record["kind"], record["title"], record["chartYear"]))
    if not targets:
        raise SystemExit(f"No movie or game titles found for {start_year}-{end_year}; leaving previous output intact")
    manual_keys = {(record["kind"], record["title"].casefold(), record["chartYear"])
                   for record in manual_records}
    unresolved_manual_targets = {
        target for target in targets
        if (target[0], target[1].casefold(), target[2]) not in manual_keys
    }
    records = resolve(unresolved_manual_targets)
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
    api_key = os.environ.get("YOUTUBE_API_KEY")
    refreshed_api_records, _ = refresh_api_records(previous_records, api_key, current_time)
    existing_link_keys = {(record["kind"], record["title"].casefold(), record["chartYear"])
                          for record in records if record.get("trailerURL")}
    records.extend(record for record in refreshed_api_records
                   if (record["kind"], record["title"].casefold(), record["chartYear"]) not in existing_link_keys)
    by_key = {(record["kind"], record["title"].casefold(), record["chartYear"]): record for record in records}
    by_key.update({(record["kind"], record["title"].casefold(), record["chartYear"]): record
                   for record in manual_records})
    if api_key:
        occupied = {key for key, record in by_key.items() if record.get("trailerURL")}
        search_queue = [target for target in targets
                        if (target[0], target[1].casefold(), target[2]) not in occupied]
        search_queue.sort(key=lambda item: (item[0], item[1].casefold(), item[2]))
        if search_queue and youtube_search_cursor:
            cursor = (
                str(youtube_search_cursor.get("kind", "")),
                str(youtube_search_cursor.get("title", "")).casefold(),
                youtube_search_cursor.get("chartYear", 0),
            )
            after_cursor = [target for target in search_queue
                            if (target[0], target[1].casefold(), target[2]) > cursor]
            before_cursor = [target for target in search_queue
                             if (target[0], target[1].casefold(), target[2]) <= cursor]
            search_queue = after_cursor + before_cursor
        searched = matched = 0
        for kind, title, chart_year in search_queue[:YOUTUBE_SEARCH_LIMIT]:
            succeeded, video_id = youtube_search_trailer(kind, title, chart_year, api_key)
            if not succeeded:
                break
            searched += 1
            youtube_search_cursor = {"kind": kind, "title": title, "chartYear": chart_year}
            if video_id:
                key = (kind, title.casefold(), chart_year)
                by_key[key] = {
                    **by_key.get(key, {}),
                    "kind": kind,
                    "title": title,
                    "chartYear": chart_year,
                    "releaseYear": None,
                    "trailerURL": f"https://www.youtube.com/watch?v={video_id}",
                    "matchSource": "youtubeDataAPI",
                    "apiVerifiedAt": current_time.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    "wikidataSources": [],
                }
                matched += 1
            time.sleep(0.15)
        print(f"YouTube Data API: searched {searched} gap targets; accepted {matched} unambiguous trailer matches")
    else:
        print("YouTube Data API gap search skipped; configure the YOUTUBE_API_KEY Actions secret to enable it")
    records = sorted(by_key.values(), key=lambda record: (record["kind"], record["chartYear"], record["title"].casefold()))
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
        "source": "Wikidata (CC0), YouTube Data API, and manual curation",
        "attribution": "Wikidata contributors; YouTube data refreshed weekly",
        "youtubeSearchCursor": youtube_search_cursor,
        "records": records,
    }
    atomic_json(args.output, value)
    by_kind = defaultdict(int)
    for record in records:
        by_kind[record["kind"]] += 1
    print(f"Wrote {len(records)} trailer matches for {len(targets)} title/year pairs ({start_year}-{end_year}): {dict(by_kind)}")


if __name__ == "__main__":
    main()
