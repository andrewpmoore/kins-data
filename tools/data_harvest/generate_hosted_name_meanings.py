#!/usr/bin/env python3
"""Refresh only the sourced hosted name dictionary and its alphabetical shards."""
from __future__ import annotations
import argparse
from pathlib import Path
from harvest import atomic_json, combined_name_meanings, name_meaning_shard, utc_now

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT/'hosted-data')
    args = parser.parse_args()
    source = Path(__file__).resolve().parent
    dataset = combined_name_meanings(source/'name-meanings.json', source/'wiktionary-name-meanings.json')
    dataset['generatedAt'] = utc_now()
    editorial = args.output/'v1/editorial'
    atomic_json(editorial/'name-meanings.json', dataset)
    shards = {letter: [] for letter in 'abcdefghijklmnopqrstuvwxyz'}
    shards['other'] = []
    for entry in dataset['entries']:
        for letter in {name_meaning_shard(name) for name in entry['names']}:
            shards[letter].append(entry)
    for letter, entries in shards.items():
        payload = {key: value for key, value in dataset.items() if key != 'entries'}
        payload.update(entryCount=len(entries), entries=entries)
        atomic_json(editorial/'name-meanings'/f'{letter}.json', payload)
    print(f"Hosted meanings: {dataset['entryCount']} entries, {dataset['spellingCount']} explicit spellings")


if __name__ == '__main__':
    main()
