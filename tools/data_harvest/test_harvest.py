import importlib.util
import csv
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("harvest.py")
SPEC = importlib.util.spec_from_file_location("kins_harvest", MODULE_PATH)
harvest = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(harvest)

IMPORTER_PATH = Path(__file__).with_name("import_wiktionary_names.py")
IMPORTER_SPEC = importlib.util.spec_from_file_location("kins_wiktionary_import", IMPORTER_PATH)
wiktionary = importlib.util.module_from_spec(IMPORTER_SPEC)
assert IMPORTER_SPEC.loader
IMPORTER_SPEC.loader.exec_module(wiktionary)

LEGACY_PATH = Path(__file__).with_name("import_legacy_data.py")
LEGACY_SPEC = importlib.util.spec_from_file_location("kins_legacy_import", LEGACY_PATH)
legacy = importlib.util.module_from_spec(LEGACY_SPEC)
assert LEGACY_SPEC.loader
LEGACY_SPEC.loader.exec_module(legacy)


class HarvestTests(unittest.TestCase):
    def test_legacy_music_omits_conflicts_and_caps_weekly_periods(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "music.csv"
            path.write_text(
                "date,rank,song,artist\n"
                "2020-01-01,1,Song A,Artist A\n"
                "2020-01-01,1,Song B,Artist B\n"
                "2020-01-08,1,Song C,Artist C\n"
                "2020-02-01,2,Not number one,Artist D\n",
                encoding="utf-8",
            )
            rows, ambiguous = legacy.read_rank_one_rows(path)
            periods = legacy.chart_periods(rows)
            self.assertEqual(ambiguous, 1)
            self.assertEqual(len(periods), 1)
            self.assertEqual(periods[0]["startDate"], "2020-01-08")
            self.assertEqual(periods[0]["endDate"], "2020-01-14")

    def test_legacy_birthday_import_is_text_only(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            value = {key: {} for key in legacy.MONTH_KEYS}
            value["january_birthdays"] = {
                "01": [{"name": "Example Person", "birth_year": 1980, "occupation": "Writer", "image": "unlicensed.jpg"}]
            }
            source = root / "birthdays.json"
            source.write_text(json.dumps(value), encoding="utf-8")
            self.assertEqual(legacy.import_birthdays(source, root / "output"), 1)
            result = json.loads((root / "output/v1/editorial/birthday-twins/01.json").read_text())
            person = result["days"]["01"][0]
            self.assertEqual(person["bornYear"], 1980)
            self.assertNotIn("image", person)
            self.assertNotIn("imageURL", person)

    def test_ssa_names_are_ranked_by_count_then_spelling(self):
        value = io.BytesIO()
        with zipfile.ZipFile(value, "w") as archive:
            archive.writestr("yob2024.txt", "Older,F,99\nOlderboy,M,99\n")
            archive.writestr(
                "yob2025.txt",
                "Zoe,F,10\nAnna,F,20\nAda,F,10\nNoah,M,30\nLiam,M,40\n",
            )
        year, entries = harvest.ssa_top_names(value.getvalue(), 2)
        self.assertEqual(year, 2025)
        self.assertEqual([entry["name"] for entry in entries], ["Anna", "Ada", "Liam", "Noah"])
        self.assertEqual([entry["rank"] for entry in entries], [1, 2, 1, 2])

    def test_manual_data_requires_provenance_and_consecutive_ranks(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "manual.json"
            path.write_text(
                json.dumps(
                    {
                        "2026-09-23": {
                            "GB": {
                            "movies": {
                                "id": "example-movies",
                                "title": "Example movies",
                                "source": {
                                        "provider": "Provider",
                                        "url": "https://example.com",
                                        "territory": "GB",
                                        "metric": "weekend gross",
                                        "kind": "theatricalBoxOffice",
                                        "observedAt": "2026-09-23",
                                    },
                                    "items": [{"rank": 1, "title": "Film"}],
                                }
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            charts = harvest.manual_charts(path, "2026-09-23", "GB")
            self.assertEqual(charts["movies"]["items"][0]["title"], "Film")

    def test_reviewed_name_dictionary_has_unique_spellings(self):
        path = Path(__file__).with_name("name-meanings.json")
        dataset = harvest.validate_name_meanings(path)
        self.assertGreaterEqual(dataset["entryCount"], 90)
        self.assertGreater(dataset["spellingCount"], dataset["entryCount"])
        noa = next(entry for entry in dataset["entries"] if "Noa" in entry["names"])
        self.assertEqual(noa["meaning"], "Motion")

    def test_insee_names_use_source_rank(self):
        value = io.BytesIO()
        with zipfile.ZipFile(value, "w") as archive:
            archive.writestr(
                "names.csv",
                "sexe;prenom;periode;valeur;rang\n1;GABRIEL;2025;100;1\n2;ALMA;2025;95;1\n",
            )
        year, entries = harvest.insee_top_names(value.getvalue(), 10)
        self.assertEqual(year, 2025)
        self.assertEqual({entry["name"] for entry in entries}, {"Gabriel", "Alma"})

    def test_statcan_names_join_frequency_and_rank(self):
        fields = ["REF_DATE", "Sex at birth", "First name at birth", "Indicator", "VALUE"]
        text = io.StringIO()
        writer = csv.DictWriter(text, fieldnames=fields)
        writer.writeheader()
        writer.writerows(
            [
                {"REF_DATE": "2024", "Sex at birth": "Male", "First name at birth": "NOAH", "Indicator": "Frequency", "VALUE": "100"},
                {"REF_DATE": "2024", "Sex at birth": "Male", "First name at birth": "NOAH", "Indicator": "Rank", "VALUE": "1"},
                {"REF_DATE": "2024", "Sex at birth": "Female", "First name at birth": "OLIVIA", "Indicator": "Frequency", "VALUE": "90"},
                {"REF_DATE": "2024", "Sex at birth": "Female", "First name at birth": "OLIVIA", "Indicator": "Rank", "VALUE": "1"},
            ]
        )
        value = io.BytesIO()
        with zipfile.ZipFile(value, "w") as archive:
            archive.writestr("table.csv", text.getvalue())
        year, entries = harvest.statcan_top_names(value.getvalue(), 10)
        self.assertEqual(year, 2024)
        self.assertEqual({entry["count"] for entry in entries}, {90, 100})

    def test_ons_names_use_latest_rank_columns(self):
        spreadsheet_ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
        document_rel_ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
        package_rel_ns = "http://schemas.openxmlformats.org/package/2006/relationships"
        workbook = f'''<workbook xmlns="{spreadsheet_ns}" xmlns:r="{document_rel_ns}"><sheets><sheet name="Table_1" sheetId="1" r:id="rId1"/><sheet name="Table_2" sheetId="2" r:id="rId2"/></sheets></workbook>'''
        relationships = f'''<Relationships xmlns="{package_rel_ns}"><Relationship Id="rId1" Target="worksheets/girls.xml"/><Relationship Id="rId2" Target="worksheets/boys.xml"/></Relationships>'''

        def worksheet(first, second):
            return f'''<worksheet xmlns="{spreadsheet_ns}"><sheetData>
            <row r="1"><c r="A1" t="inlineStr"><is><t>Heading</t></is></c></row>
            <row r="5"><c r="A5" t="inlineStr"><is><t>Name</t></is></c><c r="B5" t="inlineStr"><is><t>2025 Rank</t></is></c><c r="C5" t="inlineStr"><is><t>2025 Count</t></is></c></row>
            <row r="6"><c r="A6" t="inlineStr"><is><t>{first}</t></is></c><c r="B6"><v>1</v></c><c r="C6"><v>100</v></c></row>
            <row r="7"><c r="A7" t="inlineStr"><is><t>{second}</t></is></c><c r="B7"><v>2</v></c><c r="C7"><v>90</v></c></row>
            </sheetData></worksheet>'''

        value = io.BytesIO()
        with zipfile.ZipFile(value, "w") as archive:
            archive.writestr("xl/workbook.xml", workbook)
            archive.writestr("xl/_rels/workbook.xml.rels", relationships)
            archive.writestr("xl/worksheets/girls.xml", worksheet("Olivia", "Amelia"))
            archive.writestr("xl/worksheets/boys.xml", worksheet("Muhammad", "Noah"))
        year, entries = harvest.ons_top_names(value.getvalue(), 2)
        self.assertEqual(year, 2025)
        self.assertEqual([entry["name"] for entry in entries], ["Muhammad", "Noah", "Olivia", "Amelia"])

    def test_cso_ireland_names_read_latest_json_stat_values(self):
        def payload(prefix, names, ranks, counts):
            return json.dumps(
                {
                    "id": ["STATISTIC", "YEAR", "NAME"],
                    "size": [2, 1, len(names)],
                    "dimension": {
                        "STATISTIC": {
                            "category": {
                                "index": [f"{prefix}R", f"{prefix}C"],
                                "label": {
                                    f"{prefix}R": "Baby Names Rank",
                                    f"{prefix}C": "Baby Names Occurrences",
                                },
                            }
                        },
                        "YEAR": {"category": {"index": ["2025"]}},
                        "NAME": {
                            "category": {
                                "index": [identity for identity, _ in names],
                                "label": dict(names),
                            }
                        },
                    },
                    "value": [*ranks, *counts],
                }
            ).encode()

        boys = payload("B", [("n", "Noah"), ("l", "Luca")], [1, 2], [244, 236])
        girls = payload("G", [("i", "Isla"), ("c", "Charlotte")], [1, 2], [179, 178])
        year, entries = harvest.cso_ireland_top_names(boys, girls, 2)
        self.assertEqual(year, 2025)
        self.assertEqual([entry["name"] for entry in entries], ["Noah", "Luca", "Isla", "Charlotte"])
        self.assertEqual([entry["rank"] for entry in entries], [1, 2, 1, 2])

    def test_ine_spain_names_read_national_workbook(self):
        spreadsheet_ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
        document_rel_ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
        package_rel_ns = "http://schemas.openxmlformats.org/package/2006/relationships"
        workbook = f'''<workbook xmlns="{spreadsheet_ns}" xmlns:r="{document_rel_ns}"><sheets><sheet name="TOTAL" sheetId="1" r:id="rId1"/></sheets></workbook>'''
        relationships = f'''<Relationships xmlns="{package_rel_ns}"><Relationship Id="rId1" Target="worksheets/total.xml"/></Relationships>'''
        worksheet = f'''<worksheet xmlns="{spreadsheet_ns}"><sheetData>
        <row r="1"><c r="A1" t="inlineStr"><is><t>TOTAL</t></is></c><c r="B1"><v>1000</v></c><c r="D1" t="inlineStr"><is><t>TOTAL</t></is></c><c r="E1"><v>900</v></c></row>
        <row r="2"><c r="A2" t="inlineStr"><is><t>MATEO</t></is></c><c r="B2"><v>100</v></c><c r="D2" t="inlineStr"><is><t>SOFIA</t></is></c><c r="E2"><v>90</v></c></row>
        <row r="3"><c r="A3" t="inlineStr"><is><t>HUGO</t></is></c><c r="B3"><v>80</v></c><c r="D3" t="inlineStr"><is><t>LUCIA</t></is></c><c r="E3"><v>70</v></c></row>
        </sheetData></worksheet>'''
        value = io.BytesIO()
        with zipfile.ZipFile(value, "w") as archive:
            archive.writestr("xl/workbook.xml", workbook)
            archive.writestr("xl/_rels/workbook.xml.rels", relationships)
            archive.writestr("xl/worksheets/total.xml", worksheet)
        year, entries = harvest.ine_spain_top_names(value.getvalue(), 2024, 2)
        self.assertEqual(year, 2024)
        self.assertEqual([entry["name"] for entry in entries], ["Mateo", "Hugo", "Sofia", "Lucia"])

    def test_istat_names_use_live_year_and_list_endpoints(self):
        years = b'callbackY([2023,2024]);'
        ranking = b'''callback({"years":[2024],"0":[{"name":"LEONARDO","count":100,"percent":3.1}],"1":[{"name":"SOFIA","count":90,"percent":2.8}]});'''
        with mock.patch.object(harvest, "fetch", side_effect=[years, ranking]):
            year, entries = harvest.istat_italy_top_names("https://example.com/names", 1)
        self.assertEqual(year, 2024)
        self.assertEqual([entry["name"] for entry in entries], ["Leonardo", "Sofia"])

    def test_gfds_names_parse_public_first_name_table(self):
        page = b'''<table>
        <tr><td>Maedchen</td><td>Gesamtliste</td><td>%</td><td>Jungen</td><td>Gesamtliste</td><td>%</td></tr>
        <tr><td>1. Sophia/Sofia (1)</td><td>4</td><td>1,33</td><td>1. Noah (1)</td><td>1</td><td>1,48</td></tr>
        <tr><td>2. Emma (3)</td><td>6</td><td>1,25</td><td>2. Matteo (2)</td><td>2</td><td>1,32</td></tr>
        </table>'''
        year, entries = harvest.gfds_germany_top_names(page, 2025, 2)
        self.assertEqual(year, 2025)
        self.assertEqual([entry["name"] for entry in entries], ["Noah", "Matteo", "Sophia/Sofia", "Emma"])

    def test_smartstart_names_parse_official_site_bundle(self):
        page = b'<script defer src="/main.abc.bundle.js"></script>'
        bundle = b'''Mp={girls:{2024:[{name:"Old",amount:1}],2025:[{name:"Isla",amount:179},{name:"Charlotte",amount:178}]},boys:{2024:[{name:"Old",amount:1}],2025:[{name:"Noah",amount:244},{name:"Luca",amount:236}]}};function xp(e){}'''
        with mock.patch.object(harvest, "fetch", return_value=bundle):
            year, entries = harvest.smartstart_new_zealand_top_names(
                "https://smartstart.example/news/baby-names", page, 2
            )
        self.assertEqual(year, 2025)
        self.assertEqual([entry["name"] for entry in entries], ["Noah", "Luca", "Isla", "Charlotte"])

    def test_wiktionary_import_keeps_given_names_with_etymology(self):
        records = [
            {
                "word": "Alice",
                "lang_code": "en",
                "pos": "name",
                "etymology_text": "From Old French Adelais, from a Germanic root meaning noble.",
                "senses": [{"tags": ["given-name"], "glosses": ["A female given name from French."]}],
            },
            {
                "word": "London",
                "lang_code": "en",
                "pos": "name",
                "etymology_text": "From Latin.",
                "senses": [{"glosses": ["The capital city of the United Kingdom."]}],
            },
        ]
        entries = wiktionary.extract(json.dumps(value) for value in records)
        self.assertEqual([entry["names"] for entry in entries], [["Alice"]])
        self.assertEqual(entries[0]["license"], "CC BY-SA 4.0")

    def test_bfi_report_selects_latest_completed_weekend(self):
        page = '''
        <a href="https://example.com/new" download><span class="file-title">Weekend box office report: 18 to 20 September 2026</span></a>
        <a href="https://example.com/old" download><span class="file-title">Weekend box office report: 11 to 13 September 2026</span></a>
        '''
        reports = harvest.bfi_report_links(page, harvest.dt.date(2026, 9, 16))
        self.assertEqual(reports[0][1], "https://example.com/old")

    def test_dice_game_of_year_is_an_award_not_a_store_chart(self):
        page = b'''<h2>Game of the Year</h2>
        <div class="aias-award-details-label">Winner:</div>
        <div><a href="https://example.com/game">Clair Obscur: Expedition 33</a></div>'''
        with mock.patch.object(harvest, "fetch", return_value=page):
            chart = harvest.dice_game_of_year("https://example.com/{year}", 2026, "now")
        self.assertEqual(chart["source"]["kind"], "awardWinner")
        self.assertEqual(chart["items"][0]["title"], "Clair Obscur: Expedition 33")

    def test_cinetel_chart_is_national_theatrical_gross(self):
        page = b'''<div>Settimana Dal 14/09/2026 Al 20/09/2026</div><table><tr>
        <td>1</td><td>RESIDENT EVIL</td>
        <td><span style="display:none;">'2026-09-17'</span>17/09/2026</td>
        <td>USA</td><td>EAGLE PICTURES</td><td style="display:none;">1445189.36</td>
        <td style="display:none;">170472</td><td style="display:none;">1586144.78</td>
        <td style="display:none;">208788</td></tr></table>'''
        with mock.patch.object(harvest, "fetch", return_value=page):
            chart = harvest.cinetel_box_office_chart("https://example.com", "2026-09-23", 10)
        self.assertEqual(chart["source"]["kind"], "theatricalBoxOffice")
        self.assertEqual(chart["source"]["territory"], "IT")
        self.assertEqual(chart["items"][0]["weekendGrossEUR"], 1445189.36)

    def test_box_office_mojo_selects_completed_country_weekend_and_reads_rankings(self):
        index = b'''<table>
        <tr><td><a href="/weekend/2026W39/?area=AU">Sep 24-27</a></td>
        <td class="mojo-field-type-release mojo-cell-wide"><a href="/release/future">Future</a></td></tr>
        <tr><td><a href="/weekend/2026W38/?area=AU">Sep 17-20</a></td>
        <td class="mojo-field-type-release mojo-cell-wide"><a href="/release/current">Film One</a></td></tr>
        </table>'''
        weekend = b'''<h4>September 17-20, 2026</h4><table><tr>
        <td>1</td><td>-</td><td><a href="/release/current">Film One</a></td>
        <td>$1,234</td><td>-</td><td>200</td><td>-</td><td>$6,170</td>
        <td>$1,234</td><td>1</td><td><a href="/studio">Distributor</a></td></tr></table>'''
        with mock.patch.object(harvest, "fetch", side_effect=[index, weekend]):
            chart = harvest.box_office_mojo_chart(
                "https://www.boxofficemojo.com/weekend/by-year/?area=AU",
                "AU",
                "2026-09-23",
                10,
            )
        self.assertEqual(chart["source"]["territory"], "AU")
        self.assertEqual(chart["source"]["kind"], "theatricalBoxOffice")
        self.assertEqual(chart["items"][0]["title"], "Film One")
        self.assertIn("2026W38", chart["reportURL"])

    def test_media_control_chart_is_official_admissions_ranking(self):
        page = b'''<div>Erhebungszeitraum: 27.08. - 30.08.2026</div>
        <table><tbody class="chart-table"><tr><td><div class="pos-span">1</span></td>
        <td></td><td><img src="https://example.com/poster.jpg"></td>
        <td><span class="info1-span">Film Eins</span><br><span class="info2-span">Drama</span></td>
        </tr></tbody></table>'''
        with mock.patch.object(harvest, "fetch", return_value=page):
            chart = harvest.media_control_germany_box_office_chart(
                "https://example.com/charts", "2026-09-23", 5
            )
        self.assertEqual(chart["source"]["territory"], "DE")
        self.assertIn("admissions", chart["source"]["metric"])
        self.assertEqual(chart["items"][0]["title"], "Film Eins")

    def test_publishers_weekly_public_chart_keeps_its_non_national_kind(self):
        page = b'''<table><tr><td class="nielsen-rank">1</td><td></td><td></td>
        <td class="nielsen-bookinfo"><div class="nielsen-booktitle"><a href="/book">Book One</a></div>
        <div>Writer One, Author</div><div class="nielsen-isbn">978-1-234-56789-0</div>
        <div>Highest Rank Date 09/21/2026</div></td></tr></table>'''
        with mock.patch.object(harvest, "fetch", return_value=page):
            chart = harvest.publishers_weekly_book_chart(
                "https://example.com/books", "US", "Top 10 Overall", "adult", "2026-09-23", 10
            )
        self.assertEqual(chart["source"]["kind"], "publicBookChart")
        self.assertEqual(chart["source"]["observedAt"], "2026-09-21")
        self.assertEqual(chart["items"][0]["creator"], "Writer One")

    def test_aba_public_chart_retains_buying_group_scope(self):
        page = b'''<p>NielsenIQ BookScan Australia | Week Ending 12 September, 2026 | ABA Buying Group Stores</p>
        <span class="badge">1</span><p class="card-title"><strong>
        <a href="/book-one">Book One</a></strong></br>Writer One</p>'''
        with mock.patch.object(harvest, "fetch", return_value=page):
            chart = harvest.aba_australia_book_chart("https://example.com/top", "2026-09-23", 10)
        self.assertEqual(chart["source"]["kind"], "publicBookChart")
        self.assertIn("ABA Buying Group", chart["source"]["metric"])
        self.assertEqual(chart["items"][0]["creator"], "Writer One")

    def test_booksellers_nz_children_category_is_not_relabelled_overall(self):
        page = b'''<meta property="article:modified_time" content="2026-09-17T11:12:20+12:00">
        <h2>NZ Children and Teens</h2><table><tr><td>1</td><td>9781234567890</td>
        <td>Child Book</td><td>Writer</td><td>Publisher</td><td>$20</td></tr></table>'''
        with mock.patch.object(harvest, "fetch", return_value=page):
            chart = harvest.booksellers_nz_category_chart(
                "https://example.com/nz", "NZ Children and Teens", "NZ Children and Teens",
                "children", "2026-09-23", 10
            )
        self.assertEqual(chart["title"], "NZ Children and Teens")
        self.assertEqual(chart["source"]["kind"], "publicBookChart")

    def test_name_meaning_shards_fold_accents(self):
        self.assertEqual(harvest.name_meaning_shard("Élodie"), "e")
        self.assertEqual(harvest.name_meaning_shard("李"), "other")

    def test_annual_country_data_overrides_world_data(self):
        def chart(title):
            return {
                "id": title,
                "title": title,
                "source": {
                    "provider": "Provider",
                    "url": "https://example.com",
                    "territory": "GB",
                    "metric": "winner",
                    "observedAt": "2026-01-01",
                    "kind": "awardWinner",
                },
                "items": [{"rank": 1, "title": title}],
            }

        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "annual.json"
            path.write_text(
                json.dumps({"2026": {"WORLD": {"games": chart("World")}, "GB": {"games": chart("GB")}}}),
                encoding="utf-8",
            )
            self.assertEqual(harvest.annual_charts(path, 2026, "GB")["games"]["title"], "GB")
            self.assertEqual(harvest.annual_charts(path, 2026, "US")["games"]["title"], "World")


if __name__ == "__main__":
    unittest.main()
