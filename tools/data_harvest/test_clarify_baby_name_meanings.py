import json
import unittest
from pathlib import Path

from tools.data_harvest.clarify_baby_name_meanings import rewrite_meaning
SOURCE = Path(__file__).resolve().parents[2] / 'hosted-data/v1/editorial/name-meanings.json'


class MeaningResolutionTests(unittest.TestCase):
    def rewrite(self, name, source, sources=None):
        return rewrite_meaning(name, source, False, sources or {})

    def test_variants_can_inherit_through_several_sourced_links(self):
        sources = {
            "abigail": ("My father is joy", True),
            "abigale": ("A rare non-standard spelling of Abigail.", False),
            "abigayle": ("Modern spelling variant of Abigale.", False),
        }
        result = self.rewrite("Abbygail", "A variant of Abigayle.", sources)
        self.assertTrue(result.usable_in_quiz)
        self.assertIn("my father is joy", result.text)
        self.assertLessEqual(len(result.text), 120)

    def test_compound_root_meanings_survive_relation_chains(self):
        sources = {
            "adolf": ('From Old High German adal ("noble") + wulf ("wolf").', False),
            "adolph": ("Variant spelling of Adolf.", False),
        }
        result = self.rewrite("Adolphe", "A French variant of Adolph.", sources)
        self.assertTrue(result.usable_in_quiz)
        self.assertIn('its roots mean “noble” and “wolf”', result.text)

    def test_qualified_relation_does_not_turn_into_a_certain_root_claim(self):
        sources = {
            "hadley": ('From words for "heath" + "woodland clearing".', False),
            "adley": ("Possibly a variant of Hadley.", False),
        }
        direct = self.rewrite("Adley", sources["adley"][0], sources)
        self.assertTrue(direct.text.startswith("Possibly a variant"))
        inherited = self.rewrite("Adlee", "Variant of Adley.", sources)
        self.assertTrue(inherited.usable_in_quiz)
        self.assertIn("roots may mean", inherited.text)

    def test_uncertain_literal_root_keeps_its_caveat(self):
        sources = {"root": ('Possibly means "companion".', False)}
        result = self.rewrite("Alias", "Variant of Root.", sources)
        self.assertTrue(result.usable_in_quiz)
        self.assertIn("may mean", result.text)

    def test_traditional_association_is_not_reworded_as_a_literal_meaning(self):
        sources = {"claude": ('Traditionally said to derive from "lame".', False)}
        result = self.rewrite("Claud", "Variant of Claude.", sources)
        self.assertTrue(result.usable_in_quiz)
        self.assertIn("Traditionally linked to “lame”; the origin is uncertain", result.text)

    def test_semicolons_in_a_gloss_are_not_treated_as_relation_separators(self):
        sources = {"root": ('Means "son of the furrows; son of Ptolemy".', False)}
        result = self.rewrite("Alias", "Variant of Root.", sources)
        self.assertTrue(result.usable_in_quiz)
        self.assertIn("son of the furrows; son of Ptolemy", result.text)

    def test_languages_are_skipped_in_favour_of_the_actual_name(self):
        sources = {"theophilus": ("Love of God", True), "latin": ("Wrong answer", True)}
        result = self.rewrite("Theophila", "A feminine form of Latin Theophilus.", sources)
        self.assertTrue(result.usable_in_quiz)
        self.assertIn("Theophilus", result.text)
        self.assertIn("love of God", result.text)
        self.assertNotIn("Wrong answer", result.text)
        unknown = self.rewrite("Christophora", "A feminine form of Latin Chrīstophorus.", sources)
        self.assertFalse(unknown.usable_in_quiz)
        self.assertIn("Chrīstophorus", unknown.text)
        self.assertNotIn("form of Latin;", unknown.text)

    def test_cycle_and_self_reference_do_not_supply_a_meaning(self):
        sources = {"alpha": ("Variant of Beta.", False), "beta": ("Variant of Alpha.", False)}
        self.assertFalse(self.rewrite("Alpha", sources["alpha"][0], sources).usable_in_quiz)
        self.assertFalse(self.rewrite("Root", "Variant of Root.", {"root": ("Variant of Root.", False)}).usable_in_quiz)

    def test_multi_origin_and_blended_names_do_not_inherit_only_one_root(self):
        sources = {"agnes": ("Pure", True), "agatha": ("Good", True)}
        for source in ("Diminutive of Agnes or Agatha.", "Diminutive of Agnes and Agatha.", "From Agnes + Agatha."):
            with self.subTest(source=source):
                self.assertFalse(self.rewrite("Aggie", source, sources).usable_in_quiz)

    def test_alternative_roots_require_independently_matching_meanings(self):
        sources = {
            "william": ("Will and protection", True),
            "will": ("A short form of William.", False),
            "willow": ("The willow tree", True),
        }
        same = self.rewrite("Alias", "Diminutive of William or Will.", sources)
        self.assertTrue(same.usable_in_quiz)
        self.assertIn("will and protection", same.text)
        self.assertFalse(self.rewrite("Alias", "Diminutive of William or Willow.", sources).usable_in_quiz)
        self.assertFalse(self.rewrite("Alias", "Diminutive of William or Missing.", sources).usable_in_quiz)
        self.assertFalse(self.rewrite("Alias", "Diminutive of William or Will, also a short form of Willow.", sources).usable_in_quiz)

    def test_alternative_roots_preserve_any_uncertain_interpretation(self):
        sources = {"one": ('Means "bright".', False), "two": ('Possibly means "bright".', False)}
        result = self.rewrite("Alias", "Diminutive of One or Two.", sources)
        self.assertTrue(result.usable_in_quiz)
        self.assertIn("may mean", result.text)
        self.assertFalse(result.text.startswith("Possibly"))

    def test_equivalent_alternative_roots_can_share_sourced_components(self):
        sources = {
            "archibald": ('From words for "pure" + "bold".', False),
            "archie": ("Clipping of Archibald + -ie.", False),
        }
        result = self.rewrite("Arch", "Clipping of Archibald or Archie.", sources)
        self.assertTrue(result.usable_in_quiz)
        self.assertIn("“pure” and “bold”", result.text)
        self.assertLessEqual(len(result.text), 120)

    def test_see_references_require_a_sourced_root(self):
        sources = {"root": ('Means "bright".', False)}
        self.assertTrue(self.rewrite("Alias", "See Root.", sources).usable_in_quiz)
        self.assertFalse(self.rewrite("Alias", "See Missing.", sources).usable_in_quiz)

    def test_main_translation_is_not_confused_with_its_component_glosses(self):
        result = self.rewrite("Abner", 'From Hebrew Abner ("father of light"). From av ("father") + nur ("light").')
        self.assertEqual(result.text, "Means “father of light”.")
        result = self.rewrite("Abbas", 'From Arabic Abbas ("untamed lion"), from abasa ("to frown").')
        self.assertEqual(result.text, "Means “untamed lion”.")

    def test_local_gloss_remains_available_when_the_related_name_is_missing(self):
        result = self.rewrite("Renata", 'Feminine form of Latin Renatus, from renatus ("reborn").')
        self.assertTrue(result.usable_in_quiz)
        self.assertEqual(result.text, "Means “reborn”.")

    def test_suffix_forms_can_inherit_but_blended_names_cannot(self):
        sources = {"george": ("Worker of the earth", True)}
        for source in ("Diminutive of George + -ie.", "From George + -etta."):
            with self.subTest(source=source):
                result = self.rewrite("Georgetta", source, sources)
                self.assertTrue(result.usable_in_quiz)
                self.assertIn("worker of the earth", result.text)
        self.assertFalse(self.rewrite("Georgean", "From George + Ann.", sources).usable_in_quiz)

    def test_primary_translation_does_not_hide_alternatives_or_composite_elements(self):
        for source in (
            'Either from name A ("happy"), or from name B ("traveller").',
            'From either word A ("black") or from word B ("white").',
            'Various origins: from word A ("level"), from word B ("shorn").',
        ):
            with self.subTest(source=source):
                self.assertFalse(self.rewrite("Alias", source).usable_in_quiz)
        composite = self.rewrite("Muriel", 'From muir ("sea"), from older muir; + geal ("white, bright").')
        self.assertEqual(composite.text, "Built from words for “sea” and “white, bright”.")

    def test_dictionary_name_labels_do_not_become_literal_meanings(self):
        result = self.rewrite("Quintus", 'Borrowed from Latin Quintus ("masculine praenomen"), from quintus ("fifth").')
        self.assertEqual(result.text, "Means “fifth”.")
        self.assertFalse(self.rewrite("Alias", 'From Root ("name of a Roman gens").').usable_in_quiz)

    def test_occupational_and_place_associations_are_sourced_explanations(self):
        result = self.rewrite("Abbott", "An occupational surname for someone employed by an abbot. Later variants also exist.")
        self.assertEqual(result.text, "Originally a surname for someone employed by an abbot.")
        self.assertTrue(result.usable_in_quiz)
        self.assertTrue(self.rewrite("Alias", "Variant of Abbott.", {"abbott": (
            "An occupational surname for someone employed by an abbot.", False
        )}).usable_in_quiz)
        self.assertEqual(self.rewrite("Brooke", "A topographic surname for someone who lived near a brook.").text,
                         "Originally a surname for someone who lived near a brook.")
        self.assertEqual(self.rewrite("Alby", "From place names in England. Variant: Albee.").text,
                         "Taken from place names in England.")

    def test_scraped_meaning_sentences_can_seed_other_variants(self):
        sources = {"root": ('From Arabic, meaning "generous".', False)}
        result = self.rewrite("Alias", "Rare modern spelling of Root.", sources)
        self.assertTrue(result.usable_in_quiz)
        self.assertIn("generous", result.text)

    def test_existing_corpus_preserves_ambiguous_names_and_card_limit(self):
        entries = json.loads(SOURCE.read_text())["entries"]
        entries = [{**entry, "meaning": entry.get("sourceEtymology", entry["meaning"])}
                   if entry.get("meaningStatus") == "ai-assisted" else entry for entry in entries]
        sources = {name.casefold(): (entry["meaning"], "behindthename.com" in entry["sourceURL"])
                   for entry in entries for name in entry["names"]}
        results = {entry["names"][0]: rewrite_meaning(
            entry["names"][0], entry["meaning"], "behindthename.com" in entry["sourceURL"], sources
        ) for entry in entries}
        for name in ("Catherine", "Chad", "Day"):
            self.assertFalse(results[name].usable_in_quiz, name)
        self.assertGreater(sum(result.usable_in_quiz for result in results.values()), 2_000)
        for name, result in results.items():
            self.assertLessEqual(len(result.text), 120, name)


if __name__ == "__main__":
    unittest.main()
