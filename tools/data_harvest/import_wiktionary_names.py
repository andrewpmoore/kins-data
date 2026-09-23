#!/usr/bin/env python3
"""Extract given-name etymologies from a Wiktextract/Kaikki JSONL dump.

The input remains a Wikimedia-derived CC BY-SA dataset. The output keeps a
source URL and licence on every record and is intended for the hosted extension,
not the small hand-reviewed Swift fallback.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterable


DEFAULT_URL = (
    "https://kaikki.org/dictionary/English/pos-name/"
    "kaikki.org-dictionary-English-by-pos-name.jsonl"
)
NAME_RE = re.compile(r"^[^\W\d_][^\d_]{0,39}$", re.UNICODE)
WHITESPACE_RE = re.compile(r"\s+")
ORIGIN_PATTERNS = (
    re.compile(r"given name (?:originally )?from ([^.;,()]+)", re.IGNORECASE),
    re.compile(r"given name of ([^.;,()]+) origin", re.IGNORECASE),
)


def text_lines(path: Path | None, url: str) -> Iterable[str]:
    if path:
        raw = path.open("rb")
    else:
        request = urllib.request.Request(url, headers={"User-Agent": "KinsDataHarvester/1.0"})
        raw = urllib.request.urlopen(request, timeout=120)
    stream = gzip.GzipFile(fileobj=raw) if (path and path.suffix == ".gz") or url.endswith(".gz") else raw
    return io.TextIOWrapper(stream, encoding="utf-8")


def clean_text(value: str, limit: int = 420) -> str:
    value = WHITESPACE_RE.sub(" ", value).strip()
    if len(value) <= limit:
        return value
    cut = value.rfind(". ", 0, limit)
    return value[: cut + 1 if cut > 100 else limit].rstrip() + ("" if cut > 100 else "…")


def given_name_gloss(record: dict[str, Any]) -> str | None:
    for sense in record.get("senses", []):
        if not isinstance(sense, dict):
            continue
        tags = {str(tag) for tag in sense.get("tags", [])}
        categories = {
            str(category.get("name", ""))
            for category in sense.get("categories", [])
            if isinstance(category, dict)
        }
        glosses = [str(value) for value in sense.get("glosses", [])]
        combined = " ".join(glosses)
        if "given-name" in tags or any("given names" in value.casefold() for value in categories) or "given name" in combined.casefold():
            return combined
    return None


def origin_from(gloss: str, etymology: str) -> str:
    for value in (gloss, etymology):
        for pattern in ORIGIN_PATTERNS:
            match = pattern.search(value)
            if match:
                return clean_text(match.group(1), 80)
    match = re.match(r"(?:Ultimately )?From ([A-Z][A-Za-z -]+)", etymology)
    return clean_text(match.group(1), 80) if match else "Wiktionary etymology"


def entry_from(record: dict[str, Any]) -> dict[str, Any] | None:
    word = record.get("word")
    if record.get("pos") != "name" or record.get("lang_code") != "en" or not isinstance(word, str):
        return None
    word = word.strip()
    if not NAME_RE.fullmatch(word) or not word[:1].isupper() or " " in word:
        return None
    gloss = given_name_gloss(record)
    etymology = record.get("etymology_text")
    if not gloss or not isinstance(etymology, str):
        return None
    etymology = clean_text(etymology)
    if (
        not etymology
        or etymology.casefold().startswith(("unknown", "uncertain"))
        or etymology.startswith("Etymology tree")
    ):
        return None
    return {
        "names": [word],
        "meaning": etymology,
        "origin": origin_from(gloss, etymology),
        "note": clean_text(gloss, 240),
        "sourceURL": "https://en.wiktionary.org/wiki/" + urllib.parse.quote(word.replace(" ", "_")),
        "license": "CC BY-SA 4.0",
        "sourceProvider": "English Wiktionary via Kaikki/Wiktextract",
    }


def extract(lines: Iterable[str], maximum: int | None = None) -> list[dict[str, Any]]:
    entries: dict[str, dict[str, Any]] = {}
    for line_number, line in enumerate(lines, start=1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid JSONL at line {line_number}: {error}") from error
        if not isinstance(record, dict):
            continue
        entry = entry_from(record)
        if not entry:
            continue
        key = entry["names"][0].casefold()
        current = entries.get(key)
        if current is None or len(entry["meaning"]) < len(current["meaning"]):
            entries[key] = entry
        if maximum and len(entries) >= maximum:
            break
    return sorted(entries.values(), key=lambda value: value["names"][0].casefold())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--input", type=Path, help="local JSONL or JSONL.gz dump")
    source.add_argument("--url", default=DEFAULT_URL, help="Kaikki/Wiktextract JSONL URL")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).with_name("wiktionary-name-meanings.json"),
    )
    parser.add_argument("--max-entries", type=int, help="bounded development import")
    args = parser.parse_args()
    try:
        entries = extract(text_lines(args.input, args.url), args.max_entries)
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    if not entries:
        print("ERROR: no usable given-name etymologies found", file=sys.stderr)
        return 2
    payload = {
        "schemaVersion": 1,
        "license": "CC BY-SA 4.0",
        "attribution": "English Wiktionary contributors; extracted by Kaikki/Wiktextract",
        "sourceURL": args.url if not args.input else DEFAULT_URL,
        "entries": entries,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(entries)} Wiktionary name etymologies to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
