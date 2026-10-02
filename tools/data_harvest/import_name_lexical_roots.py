#!/usr/bin/env python3
"""Attach English lexical glosses to explicitly borrowed name roots.

Inputs are Kaikki's English-glossed per-language Wiktionary JSONL downloads.
Only a single explicit borrowing/transliteration with an exact native spelling
(or explicitly recorded canonical form) qualifies. This never transliterates,
removes accents, combines name components, or treats a root as a personality.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import quote
import unicodedata

ROOT = Path(__file__).resolve().parents[2]
BORROWING = re.compile(
    r"^(?:Borrowed from|From|Transliteration of) (?P<language>Sanskrit|Yoruba|Hindi|Arabic|Persian|Hawaiian|Maori) "
    r"(?P<term>[^\s.(),]+)(?: \([^)]*\))?\.$"
)

# These native entries have lexical senses that do not establish the modern
# personal name's interpretation; an individually sourced correction may apply.
NEEDS_CONTEXT_REVIEW = {"Neeraj", "Gauri", "Samita", "Bharata", "Ikshvaku", "Saed"}


def native_key(word: str) -> str:
    return unicodedata.normalize("NFC", word)


def lexical_gloss(record: dict) -> str | None:
    if record.get("pos") not in {"adj", "noun", "name"}:
        return None
    senses = [sense for sense in record.get("senses", []) if not sense.get("form_of") and not set(sense.get("tags", [])) & {"form-of", "obsolete", "archaic"}]
    # Several lexical senses need individual editorial disambiguation. Selecting
    # a shorter later definition can silently change the name's interpretation.
    if not senses or (record.get("pos") == "noun" and len(senses) != 1):
        return None
    for sense in senses[:1]:
        if sense.get("form_of") or set(sense.get("tags", [])) & {"form-of", "obsolete", "archaic"}:
            continue
        for gloss in sense.get("glosses", []):
            explicit = re.search(r'\bmeaning [“"]([^”"]+)[”"]', gloss)
            if record.get("pos") == "name" and not explicit:
                continue
            value = explicit[1] if explicit else gloss
            if re.match(r"^[A-Z][a-z]+(?:,|$)", value):
                continue
            if (not 2 <= len(value) <= 65 or not re.match(r"[A-Za-z]", value)
                    or re.search(r"[()\[\]{}]|\b(?:name of|given name|surname|alternative|variant|inflection|plural|of a |of the |epithet|someone|something|used to|literally)\b", value, re.I)):
                continue
            return value.rstrip(".;")
    return None


def extract(entries: list[dict], dictionaries: dict[str, list[dict]]) -> dict[str, dict]:
    indexes = {}
    for language, records in dictionaries.items():
        index = {}
        for record in records:
            for word in [record.get("word", ""), *(f["form"] for f in record.get("forms", []) if "canonical" in f.get("tags", []))]:
                index.setdefault(native_key(word), []).append(record)
        indexes[language] = index
    additions = {}
    for entry in entries:
        if entry["names"][0] in NEEDS_CONTEXT_REVIEW:
            continue
        if re.search('[“"”]', entry["meaning"]):
            continue  # The name's own etymology supplies the stronger gloss.
        match = BORROWING.fullmatch(entry["meaning"])
        if not match or match["language"] not in indexes:
            continue
        records = indexes[match["language"]].get(native_key(match["term"]), [])
        lexical = [record for record in records if record.get("pos") in {"adj", "noun"}]
        interpretations = {lexical_gloss(record) for record in lexical}
        if lexical and (None in interpretations or len(interpretations) != 1):
            continue
        for record in records:
            gloss = lexical_gloss(record)
            if not gloss:
                continue
            language = match["language"]
            article = "an" if language[0].lower() in "aeiou" else "a"
            meaning = f'Linked to {article} {language} word meaning “{gloss}”.'
            if len(meaning) > 120:
                continue
            term = record["word"]
            additions[entry["names"][0]] = {
                "meaning": meaning, "hasMeaning": True,
                "sourceURL": "https://en.wiktionary.org/wiki/" + quote(term) + "#" + language,
                "nameSourceURL": entry["sourceURL"], "nativeTerm": match["term"],
                "language": language, "evidence": gloss,
                "license": "CC BY-SA 4.0",
                "datasetURL": f"https://kaikki.org/dictionary/{language}/kaikki.org-dictionary-{language}.jsonl",
            }
            break
    return dict(sorted(additions.items(), key=lambda item: item[0].casefold()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dictionary-dir", type=Path, required=True)
    parser.add_argument("--source", type=Path, default=ROOT / "tools/data_harvest/wiktionary-name-meanings.json")
    parser.add_argument("--output", type=Path, default=ROOT / "tools/data_harvest/baby-name-meaning-lexical-roots.json")
    args = parser.parse_args()
    entries = json.loads(args.source.read_text())["entries"]
    wanted = {}
    for entry in entries:
        match = BORROWING.fullmatch(entry["meaning"])
        if match:
            wanted.setdefault(match["language"], set()).add(native_key(match["term"]))
    dictionaries = {}
    for path in args.dictionary_dir.glob("*.jsonl"):
        if path.stem not in wanted:
            continue
        retained = []
        for line in path.open():
            record = json.loads(line)
            spellings = [record.get("word", ""), *(f["form"] for f in record.get("forms", []) if "canonical" in f.get("tags", []))]
            if any(native_key(word) in wanted[path.stem] for word in spellings):
                retained.append(record)
        dictionaries[path.stem] = retained
    additions = extract(entries, dictionaries)
    args.output.write_text(json.dumps(additions, ensure_ascii=False, indent=2) + "\n")
    print(f"Extracted {len(additions)} explicitly linked lexical meanings")


if __name__ == "__main__":
    main()
