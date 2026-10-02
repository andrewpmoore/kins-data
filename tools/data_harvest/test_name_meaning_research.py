import json
from pathlib import Path
import tempfile
import unittest

from tools.data_harvest.import_name_lexical_roots import extract, lexical_gloss
from tools.data_harvest.import_wiktionary_names import prose_etymology
from tools.data_harvest.name_meaning_research import enrich_entries, load_interpretations, load_research


class MeaningResearchTests(unittest.TestCase):
    def test_native_spelling_must_match_an_explicit_single_borrowing(self):
        entries = [dict(names=[name], meaning=source, sourceURL='https://en.wiktionary.org/wiki/'+name) for name, source in [
            ('Naveen', 'Borrowed from Sanskrit नवीन (navīna).'),
            ('Guessed', 'Possibly from Sanskrit नवीन (navīna).'),
            ('Compound', 'From Sanskrit नवीन + अन्य.'),
            ('Accent', 'From Yoruba Adéjùgbé.'),
            ('Similar', 'From Yoruba Adejugbe.'),
        ]]
        records = {'Sanskrit': [dict(word='नवीन', pos='adj', senses=[dict(glosses=['new, fresh, young'])])],
                   'Yoruba': [dict(word='Adejugbe', pos='name', forms=[dict(form='Adéjùgbé',tags=['canonical'])], senses=[dict(glosses=['a male given name meaning “Royalty does not perish”'])])]}
        found = extract(entries, records)
        self.assertEqual(set(found), {'Naveen', 'Accent', 'Similar'})
        self.assertIn('new, fresh, young', found['Naveen']['meaning'])
        self.assertTrue(found['Accent']['sourceURL'].endswith('#Yoruba'))

    def test_person_names_and_inflections_are_not_literal_glosses(self):
        self.assertIsNone(lexical_gloss(dict(pos='name',senses=[dict(glosses=['a name of Surya'])])))
        self.assertIsNone(lexical_gloss(dict(pos='noun',senses=[dict(glosses=['Rukmini, first queen of Krishna'])])))
        self.assertIsNone(lexical_gloss(dict(pos='noun',senses=[dict(glosses=['new'],form_of=[{'word':'root'}])])))

    def test_original_gloss_and_ambiguous_lexemes_are_held_for_review(self):
        entries = [dict(names=['Kiran'],meaning='From Sanskrit किरण (“ray of light”).',sourceURL='https://en.wiktionary.org/wiki/Kiran'),
                   dict(names=['Ambiguous'],meaning='From Sanskrit राम.',sourceURL='https://en.wiktionary.org/wiki/Ambiguous')]
        records = {'Sanskrit': [dict(word='किरण',pos='noun',senses=[dict(glosses=['dust'])]),
                                dict(word='राम',pos='noun',senses=[dict(glosses=['darkness'])]),
                                dict(word='राम',pos='adj',senses=[dict(glosses=['pleasant'])])]}
        self.assertEqual(extract(entries,records),{})

    def test_tree_prose_survives_but_machine_labels_do_not(self):
        self.assertEqual(prose_etymology('Etymology tree\nLatin Testbor.\nEnglish Test\nFrom Latin test (“example”).'), 'From Latin test (“example”).')
        self.assertEqual(prose_etymology('Etymology tree\nLatin Testbor.\nEnglish Test'), '')

    def test_reviewed_entries_keep_priority_and_original_provenance_is_retained(self):
        entry = dict(names=['Naveen'], meaning='Borrowed from Sanskrit नवीन.',sourceURL='https://en.wiktionary.org/wiki/Naveen')
        record = dict(meaning='Linked to a Sanskrit word meaning “new”.',hasMeaning=True,sourceURL='https://en.wiktionary.org/wiki/नवीन#Sanskrit',evidence='new',license='CC BY-SA 4.0')
        enriched = enrich_entries([entry], {'Naveen':record})[0]
        self.assertEqual(enriched['sourceEtymology'],entry['meaning'])
        self.assertEqual(enriched['nameSourceURL'],entry['sourceURL'])
        reviewed = dict(entry,sourceURL='https://www.behindthename.com/name/naveen')
        self.assertEqual(enrich_entries([reviewed],{'Naveen':record})[0],reviewed)

    def test_research_rejects_unsupported_source_and_keeps_individual_priority(self):
        record = dict(meaning='Means “new”.',hasMeaning=True,sourceURL='https://en.wiktionary.org/wiki/नवीन',evidence='new',license='CC BY-SA 4.0')
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            (path/'baby-name-meaning-lexical-roots.json').write_text(json.dumps({'Naveen':record}))
            (path/'baby-name-meaning-research-n-z.json').write_text(json.dumps({'Naveen':dict(record,meaning='Means “fresh”.')}))
            self.assertEqual(load_research(path)['Naveen']['meaning'],'Means “fresh”.')
            record['sourceURL']='https://example.com/name'
            (path/'baby-name-meaning-research-n-z.json').write_text(json.dumps({'Naveen':record}))
            with self.assertRaises(ValueError):load_research(path)

    def test_ai_interpretations_fill_only_gaps_and_retain_the_original_etymology(self):
        interpretation = dict(meaning='Traditionally linked to “bear”; the origin is uncertain.',hasMeaning=True,confidence='medium',meaningStatus='ai-assisted',evidence='Conventional association, qualified because the root is disputed.')
        entries = [dict(names=[name],meaning=meaning,sourceURL='https://en.wiktionary.org/wiki/'+name) for name, meaning in [('Unknown', 'Of unknown meaning.'),('Clear','From a word meaning “light”.')]]
        result = enrich_entries(entries,{},dict(Unknown=interpretation,Clear=interpretation))
        self.assertEqual(result[0]['meaningStatus'],'ai-assisted')
        self.assertEqual(result[0]['meaningConfidence'],'medium')
        self.assertEqual(result[0]['sourceEtymology'],'Of unknown meaning.')
        self.assertEqual(result[1],entries[1])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'baby-name-meaning-ai-a-h.json'
            path.write_text(json.dumps({'Unknown':interpretation}))
            self.assertEqual(load_interpretations(path.parent)['Unknown'],interpretation)
            interpretation['meaningStatus']='source-verified'
            path.write_text(json.dumps({'Unknown':interpretation}))
            with self.assertRaises(ValueError):load_interpretations(path.parent)
