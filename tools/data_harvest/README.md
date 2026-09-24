# Kins hosted-data harvester

This job starts a date-addressed daily archive at `hosted-data/v1/dates/YYYY-MM-DD/cc.json`
and maintains `hosted-data/v1/latest/cc.json`. It publishes the name dictionary as a
complete file and small `name-meanings/a.json` … `z.json` shards. Every chart retains
its provider URL, territory, metric, semantic kind, observation time and ranks.

## Run it

```sh
python3 -m unittest tools/data_harvest/test_harvest.py
python3 tools/data_harvest/generate_name_meanings_swift.py --check
python3 tools/data_harvest/import_legacy_data.py
python3 tools/data_harvest/harvest.py
```

Refresh the hosted Wiktionary extension (normally done monthly by the workflow):

```sh
curl -L -o /tmp/kaikki-names.jsonl \
  https://kaikki.org/dictionary/English/pos-name/kaikki.org-dictionary-English-by-pos-name.jsonl
python3 tools/data_harvest/import_wiktionary_names.py \
  --input /tmp/kaikki-names.jsonl --max-entries 10000
```

Use `--date YYYY-MM-DD` for a deterministic backfill date and `--strict` when every
enabled upstream must respond. The normal scheduled run is deliberately non-strict:
one unavailable provider must not erase valid charts from the other countries.
Downloads of slow annual government files are cached under `.cache/data-harvest`.

The workflow in `.github/workflows/harvest-hosted-data.yml` runs daily at 03:17 UTC
and commits the new static snapshot to this public repository. The app reads it from
`https://raw.githubusercontent.com/andrewpmoore/kins-data/main/hosted-data/`, so no
paid hosting plan or cross-repository write token is required.

The weekly `keep-scheduled-workflows-active.yml` job records repository activity so
GitHub does not disable schedules after 60 inactive days. The daily harvest compares
each feed with the prior committed manifest. A newly failing feed creates an issue
assigned to the repository owner so GitHub can email the account's configured
notification address. The harvest itself remains successful and the issue carries the
failed workflow link. A feed that remains down does not send repeated alerts; after
recovery, a later failure is considered new again.

The superseded NYT scraper and `api-data` generator workflows are intentionally
retired. Neither output is consumed by the app, and the former amended and
force-pushed `main`, which could race with the supported hosted-data harvest.

The app first asks for an exact date snapshot, then checks the compact country/year
history for an exact containing period. It caches valid responses and falls back to
bundled facts if the host or schema is wrong. It never substitutes the current chart
for an older birthday. Hosted name meanings extend the bundled
dictionary and are accepted only from Behind the Name or English Wiktionary HTTPS
references.

## Sources and boundaries

- Apple Marketing Tools supplies localized most-played songs. Adapters for Apple
  movie, paid-ebook and paid-game storefront charts remain available but are disabled:
  they are not box office, national book sales or Game of the Year, and the app does
  not consume them.
- BFI supplies the latest completed UK theatrical weekend top ten from its official
  Friday-to-Sunday gross spreadsheet; Cinetel supplies Italy's national weekly
  theatrical chart. Box Office Mojo supplies separate Australia and New Zealand
  country weekend charts, and Media Control supplies Germany's official admissions
  top five. D.I.C.E. supplies the global annual Game of the Year winner.
- SSA, ONS, Statistics Canada, INSEE, Ireland's CSO, New Zealand DIA/SmartStart,
  Spain's INE and Italy's Istat supply annual name rankings. Germany's GfdS public
  first-name table supplies a representative national top ten. Annual data is retained
  in daily snapshots until a newer release replaces it. The SSA download sometimes
  rejects automated clients, so a reviewed 2025 official top-ten seed is used only
  until the full archive succeeds.
- The New York Times adapter is available but disabled. Its lists are editorial
  bestsellers, not national unit-sales totals, so the app does not use them as the
  requested national book-sales result.
- Public candidates are harvested when their page is current and readable: The
  Numbers for US theatrical weekends, Publishers Weekly/Circana for US overall and
  children's books, ABA/NielsenIQ for Australian buying-group sales, and Booksellers
  Aotearoa/Nielsen for New Zealand indie and children's charts. Their narrower scope
  remains in `source.metric`; the app accepts the `publicBookChart` semantic kind.
- `manual.json` is an audited escape hatch for a licensed or curated chart. Copy the
  structure from `manual.example.json`; a source, territory, metric and consecutive
  ranks are mandatory.
- `annual.json` carries persistent annual records such as the current NIQ UK book
  sales leaders. Updating it changes hosted results without an app release.
- `source-candidates.json` is the audited backlog of public movie, book and game
  pages. It records why each source is active, still a candidate, limited, stale or
  unavailable so a public webpage is not mistaken for a national measured chart.
- `import_legacy_data.py` converts the old country music CSVs into year-addressed
  periods. It keeps only rank-one rows, collapses exact duplicates, rejects every
  date with conflicting rank-one values and never stretches one observation beyond
  seven days. The old movie files are excluded because their three country copies
  are effectively identical and include invalid 1900 dates. The NYT archive is
  excluded because it is not national book-sales data and redistribution rights are
  unclear. Birthday records are text-only, explicitly marked as unverified legacy
  editorial data, and are used by the app only if its live Wikipedia result is empty;
  legacy images are not republished.

Do not relabel Apple charts as national sales or combine Australian state baby-name
lists into a fabricated national rank. Public-page adapters retain only the small set
of facts the app displays and never bypass authentication, paywalls, CAPTCHAs or other
access controls. Add a provider only after its metric, geography and stable identifier
are understood; keep the source URL attached to every harvested chart.

## Editing name meanings

Edit `name-meanings.json`, retaining explicit spellings, uncertainty and a direct
reference URL. It is the compact hand-reviewed fallback and overrides bulk records.
Then regenerate and test the bundled fallback:

```sh
python3 tools/data_harvest/generate_name_meanings_swift.py
python3 tools/data_harvest/generate_name_meanings_swift.py --check
```

`import_wiktionary_names.py` builds the much larger hosted extension from the
English Wiktionary proper-name dump generated by Kaikki/Wiktextract. Every imported
record retains its source URL and CC BY-SA 4.0 attribution. Behind the Name is never
scraped: its terms do not permit bulk redistribution.
