#!/usr/bin/env python3
"""Generate the offline Swift name dictionary from the reviewed JSON source."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import urlparse


HEADER = '''import Foundation

/// An offline editorial dictionary generated from
/// `tools/data_harvest/name-meanings.json`. Spellings are explicit: never infer
/// a meaning from a prefix, compound name, gender, or similar spelling.
struct ReworkNameMeaning: Equatable, Sendable {
    let names: [String]
    let meaning: String
    let origin: String
    let note: String
    let sourceSlug: String
    var sourceOverrideURL: URL? = nil

    var sourceURL: URL { sourceOverrideURL ?? URL(string: "https://www.behindthename.com/name/\\(sourceSlug)")! }
    var sourceProviderName: String {
        sourceURL.host == "en.wiktionary.org" ? "Wiktionary · CC BY-SA 4.0" : "Behind the Name"
    }

    var localizedMeaning: String { localized("meaning", meaning) }
    var localizedOrigin: String { localized("origin", origin) }
    var localizedNote: String { localized("note", note) }

    private func localized(_ field: String, _ fallback: String) -> String {
        NSLocalizedString("rework.name_dictionary.\\(names[0].lowercased()).\\(field)", value: fallback, comment: "Name etymology")
    }
}

enum ReworkNameMeanings {
    static func lookup(_ name: String) -> ReworkNameMeaning? {
        index[key(name)]
    }

    private static func key(_ name: String) -> String {
        name.trimmingCharacters(in: .whitespacesAndNewlines)
            .precomposedStringWithCanonicalMapping
            .lowercased(with: Locale(identifier: "en_US_POSIX"))
    }

    private static let index: [String: ReworkNameMeaning] = {
        Dictionary(uniqueKeysWithValues: entries.flatMap { entry in
            entry.names.map { (key($0), entry) }
        })
    }()

'''


def swift_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'


def render(dataset: dict) -> str:
    lines = [HEADER]
    lines.append(
        f"    // Reviewed {dataset['reviewedAt']}. See docs/name-meanings.md for coverage and policy.\n"
    )
    lines.append("    static let entries: [ReworkNameMeaning] = [\n")
    for entry in dataset["entries"]:
        names = ", ".join(swift_string(name) for name in entry["names"])
        slug = urlparse(entry["sourceURL"]).path.rstrip("/").split("/")[-1]
        fields = ", ".join(
            [
                f"names: [{names}]",
                f"meaning: {swift_string(entry['meaning'])}",
                f"origin: {swift_string(entry['origin'])}",
                f"note: {swift_string(entry['note'])}",
                f"sourceSlug: {swift_string(slug)}",
            ]
        )
        lines.append(f"        .init({fields}),\n")
    lines.append("    ]\n}\n")
    return "".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[2]
    parser.add_argument("--input", type=Path, default=Path(__file__).with_name("name-meanings.json"))
    parser.add_argument("--output", type=Path, default=root / "Kins/Rework/Data/ReworkNameMeanings.swift")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    dataset = json.loads(args.input.read_text(encoding="utf-8"))
    generated = render(dataset)
    if args.check:
        if not args.output.exists() or args.output.read_text(encoding="utf-8") != generated:
            print(f"Generated file is stale: {args.output}")
            return 1
        print(f"PASS: {args.output} matches {args.input}")
        return 0
    args.output.write_text(generated, encoding="utf-8")
    print(f"Wrote {len(dataset['entries'])} name entries to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
