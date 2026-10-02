"""Validated, attributed supplemental meanings shared by hosted data and quiz builds."""
from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

RESEARCH_FILES = (
    "baby-name-meaning-lexical-roots.json",
    "baby-name-meaning-research-a-m.json",
    "baby-name-meaning-research-n-z.json",
)


def load_research(directory: Path) -> dict[str, dict[str, object]]:
    additions = {}
    for filename in RESEARCH_FILES:
        path = directory / filename
        if not path.exists():
            continue
        records = json.loads(path.read_text())
        if not isinstance(records, dict):
            raise ValueError(f"Invalid meaning research: {path}")
        for name, record in records.items():
            if not isinstance(record, dict):
                raise ValueError(f"Invalid researched record: {name}")
            meaning = record.get("meaning")
            reference = urlparse(str(record.get("sourceURL", "")))
            if (not isinstance(meaning, str) or not 1 <= len(meaning) <= 120
                    or record.get("hasMeaning") is not True
                    or reference.scheme != "https" or reference.hostname != "en.wiktionary.org"
                    or not reference.path.startswith("/wiki/")
                    or not record.get("evidence") or record.get("license") != "CC BY-SA 4.0"):
                raise ValueError(f"Unattributed or invalid researched meaning: {name}")
            # Individually checked entries supersede a bulk lexical summary.
            additions[name] = record
    return additions


def enrich_entries(entries: list[dict], research: dict[str, dict], interpretations: dict[str, dict] | None = None, overrides: dict[str, dict] | None = None) -> list[dict]:
    result = []
    for entry in entries:
        correction = research.get(entry["names"][0])
        value = dict(entry)
        if correction and "behindthename.com" not in entry["sourceURL"]:
            value.update(
                meaning=correction["meaning"], sourceURL=correction["sourceURL"],
                sourceEtymology=entry["meaning"], nameSourceURL=entry["sourceURL"],
                license=correction["license"],
                meaningEvidence=correction["evidence"],
                sourceProvider="English Wiktionary contributors; Kins sourced summary",
            )
        result.append(value)
    if interpretations:
        try:
            from .clarify_baby_name_meanings import rewrite_meaning
        except ImportError:
            from clarify_baby_name_meanings import rewrite_meaning
        roots = {spelling.casefold(): (entry['meaning'], 'behindthename.com' in entry['sourceURL']) for entry in result for spelling in entry['names']}
        for name, correction in (overrides or {}).items():
            if correction['hasMeaning']:
                roots[name.casefold()] = (correction['meaning'], False)
        for entry in result:
            name = entry['names'][0]
            if name not in interpretations or name in research or 'behindthename.com' in entry['sourceURL']:
                continue
            supported = (overrides or {}).get(name, {}).get('hasMeaning')
            if supported is None:
                supported = rewrite_meaning(name, entry['meaning'], False, roots).usable_in_quiz
            if supported:
                continue
            interpretation = interpretations[name]
            entry.update(
                sourceEtymology=entry['meaning'], meaning=interpretation['meaning'],
                meaningStatus='ai-assisted', meaningConfidence=interpretation['confidence'],
                meaningEvidence=interpretation['evidence'],
                sourceProvider='Kins AI-assisted interpretation; Wiktionary etymology reference',
            )
    return result

AI_FILES = (
    'baby-name-meaning-ai-a-h.json',
    'baby-name-meaning-ai-i-m.json',
    'baby-name-meaning-ai-n-z.json',
)


def load_interpretations(directory: Path) -> dict[str, dict]:
    """AI meanings are stored distinctly and never presented as verified sources."""
    interpretations = {}
    for filename in AI_FILES:
        path = directory / filename
        if not path.exists():
            continue
        for name, record in json.loads(path.read_text()).items():
            if name in interpretations:
                raise ValueError(f'Duplicate AI meaning: {name}')
            meaning = record.get('meaning')
            if (not isinstance(meaning, str) or not 1 <= len(meaning) <= 120
                    or record.get('hasMeaning') is not True
                    or record.get('confidence') not in {'high', 'medium'}
                    or record.get('meaningStatus') != 'ai-assisted'
                    or not record.get('evidence')):
                raise ValueError(f'Invalid AI meaning interpretation: {name}')
            interpretations[name] = record
    return interpretations
