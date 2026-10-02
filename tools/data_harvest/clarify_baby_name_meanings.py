"""Turn source etymology notes into concise, cautious copy for name cards.

This never guesses a literal meaning from a spelling or a language alone. The
original wording stays in the hosted source, alongside its attribution URL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ClearMeaning:
    text: str
    usable_in_quiz: bool


RELATIONS = (
    (r"(?:spelling )?variant(?: form| spelling)? of", "A variant of"),
    (r"(?:altered |phonetic |pronunciation )?(?:spelling|re-spelling|respelling|spelling alteration) of", "A spelling of"),
    (r"feminine (?:diminutive )?form of", "A feminine form of"),
    (r"masculine form of", "A masculine form of"),
    (r"(?:diminutive(?: form)?|pet[- ]form|hypocorism) of", "A pet form of"),
    (r"(?:clipping|contraction|abbreviation|shortening) (?:of|from)", "Short for"),
    (r"short(?:ened)? form of", "Short for"),
    (r"short(?:ened)? (?:from|for)", "Short for"),
    (r"(?:latinate|latini[sz]ed) (?:form|variant) of|latini[sz]ation of", "A Latin form of"),
    (r"anglici[sz](?:ed|ation) (?:(?:form|spelling) )?(?:of|from)", "An English form of"),
    (r"equivalent of|alternative form of|form of", "A form of"),
    (r"see", "A form of"),
)

# Only these grammatical prefixes may intervene before a referenced name.
# In particular, a language or a descriptive word must never become the root
# of a relation, as in "feminine form of Latin Chrīstophorus".
LANGUAGES = (
    "Ancient Greek", "Church Latin", "Medieval Latin", "Middle English",
    "Old English", "Old French", "Old High German", "Old Norse",
    "Scottish Gaelic", "Irish Gaelic", "Late Latin", "Roman",
    "English", "French", "German", "Irish", "Scottish", "Welsh", "Celtic",
    "Latin", "Italian", "Portuguese", "Spanish", "Hebrew", "Arabic",
    "Russian", "Ukrainian", "Greek", "Dutch", "Danish", "Swedish",
    "Norwegian", "Polish", "Breton", "Albanian", "Occitan", "Bulgarian",
)
LANGUAGE_PATTERN = "(?:" + "|".join(map(re.escape, LANGUAGES)) + ")"
RELATION_MODIFIERS = (
    r"(?:(?:a|an|the|very|rather|rare|less common|modern|medieval|fanciful|"
    r"non-standard|nonstandard|standard|biblical|vernacular|phonetic|"
    r"reduced|metathesized|informal|traditional|\d+(?:st|nd|rd|th)-century|"
    + LANGUAGE_PATTERN + r")\s+)*"
)

QUOTED_GLOSS = re.compile(r"[“\"]([^”\"]{2,85})[”\"]")
NAME = r"([A-ZÀ-ÖØ-Þ][\wÀ-ÖØ-öø-ÿ'-]*)"
NO_GLOSS = ClearMeaning("No clear literal meaning is recorded in the source.", False)

REVIEWED_COPY = {
    "Charlotte": "From a name element meaning “man”.",
    "Eden": "Possibly means “delight”.",
    "Isla": "Named after Islay, a Scottish island.",
    "Jacob": "Traditionally interpreted as “holder of the heel” or “supplanter”.",
    "James": "Traditionally interpreted as “holder of the heel” or “supplanter”.",
    "Kai": "Can mean “sea” in Hawaiian.",
    "Lucas": "Probably comes from Lucania, a region in Italy.",
    "Mia": "Means “mine” in Italian; also a short form of Maria.",
    "Oliver": "Possibly associated with the olive tree.",
    "Olivia": "Associated with the olive tree.",
    "Oscar": "Possibly means “friend of deer”.",
    "Rose": "Associated with the rose flower.",
    "Rowan": "Associated with the colour red.",
    "Sage": "Can refer to a wise person or the herb.",
    "Sebastian": "From Sebaste, an ancient city name.",
    "Theo": "Short for Theodore, traditionally meaning “God's gift”.",
}

EDITORIAL_COPY = {
    "Athanasios": ClearMeaning("Means “immortal”.", True),
    "Demetrius": ClearMeaning("Named for Demeter, the Greek earth goddess.", True),
    "Lachlan": ClearMeaning("A Scottish Gaelic name meaning “land of the lochs”.", True),
    "Rebecca": ClearMeaning("The source gives several senses, including “captivating”.", True),
    "Zinaida": ClearMeaning("Related to Zeus, the Greek god.", True),
}


def _sentence(text: str) -> str:
    text = " ".join(text.strip().split()).strip(" .;:")
    return text + ("" if text.endswith(("?", "!")) else ".") if text else ""


def _reviewed_meaning(name: str, source: str) -> ClearMeaning:
    if name in REVIEWED_COPY:
        return ClearMeaning(REVIEWED_COPY[name], True)
    gloss = source if source.startswith("God") else source[0].lower() + source[1:]
    gloss = gloss.rstrip(".")
    return ClearMeaning(f"Means “{gloss}”{'' if gloss.endswith(('?', '!')) else '.'}", True)


def _valid_gloss(gloss: str) -> bool:
    return bool(
        2 <= len(gloss) <= 72
        and gloss[0].isalnum()
        and re.search(r"[A-Za-z]", gloss)
        and not re.search(r"[\u0370-\u052f\u0900-\u0fff]", gloss)
        and not gloss.casefold().startswith(("or ", "and "))
        and not re.search(r"\b(?:surname|praenomen|cognomen|given name|name of a|diminutive suffix|patronymic suffix)\b", gloss, re.IGNORECASE)
    )


def _glosses(source: str, known_names: set[str]) -> list[str]:
    results = []
    for match in QUOTED_GLOSS.finditer(source):
        gloss = match.group(1).strip(" .;:,")
        folded = gloss.casefold()
        if (
            (folded in known_names and gloss[0].isupper())
            or all(part.strip().casefold() in known_names and part.strip()[0].isupper() for part in gloss.split(","))
            or "“" in gloss
            or "(" in gloss
            or gloss.startswith("-")
            or any(word in folded for word in (
                "suffix", "prefix", "forming place names", "given name",
                "the region of", "roman cognomen", "site in the parish",
            ))
            or not _valid_gloss(gloss)
        ):
            continue
        if gloss not in results:
            results.append(gloss)
    return results


def _plain_gloss(gloss: str, name: str) -> str:
    """Drop a repeated name from a dictionary gloss such as 'Dror, freedom'."""
    parts = [part.strip() for part in gloss.split(",")]
    if len(parts) > 1 and parts[0].casefold() == name.casefold():
        parts = parts[1:]
    return ", ".join(parts).replace("lightsource", "light source")


def _relation(source: str) -> tuple[str, tuple[str, ...], bool] | None:
    stripped = source.lstrip("* ")
    uncertainty = re.match(r"^(?:possibly|probably|perhaps)\s+", stripped, re.IGNORECASE)
    if uncertainty:
        stripped = stripped[uncertainty.end():]
    for pattern, wording in RELATIONS:
        match = re.match(rf"^{RELATION_MODIFIERS}(?:{pattern})\s+", stripped, re.IGNORECASE)
        if match:
            remainder = stripped[match.end():]
            remainder = re.sub(
                rf"^(?:(?:the |modern )?{LANGUAGE_PATTERN}(?: (?:or|and) {LANGUAGE_PATTERN})*\s+)?"
                r"(?:(?:the |a )?(?:male |female )?(?:given |personal |saint's |family )?name\s+)?",
                "", remainder, count=1, flags=re.IGNORECASE,
            )
            root_match = re.match(NAME + r"\b", remainder)
            if not root_match:
                continue
            root = root_match.group(1)
            if root in LANGUAGES:
                continue
            tail = remainder[root_match.end():]
            roots = [root]
            # A list is useful only if every named alternative independently
            # supplies the same meaning; the caller verifies that below.
            while separator := re.match(r"\s*(?:,\s*)?(?:or|and)\s+", tail, re.IGNORECASE):
                tail = tail[separator.end():]
                next_root = re.match(NAME + r"\b", tail)
                if not next_root or next_root.group(1) in LANGUAGES:
                    return None
                roots.append(next_root.group(1))
                tail = tail[next_root.end():]
            if len(roots) > 1 and re.search(r"\b(?:also|other|less often|in some cases)\b", tail, re.IGNORECASE):
                return None
            if re.match(r"\s*\+(?!\s*-)", tail):
                return None
            return wording, tuple(roots), bool(uncertainty)
    match = re.match(rf"^From\s+{NAME}\s*(?:\+\s*-[a-z]+\b|\.(?:\s|$)|$)", stripped)
    if match:
        return "A form of", (match.group(1),), bool(uncertainty)
    return None


def _root_text(result: ClearMeaning) -> str:
    text = result.text
    relation_prefix = re.compile(
        rf"^(?:Possibly )?(?:A (?:variant|spelling|form|feminine form|masculine form|pet form|Latin form) of|"
        rf"An English form of|Short for) {NAME}(?:; |\. )", re.IGNORECASE,
    )
    while match := relation_prefix.match(text):
        text = text[match.end():]
    return text


def _shared_meaning(wording: str, roots: tuple[str, ...], results: list[ClearMeaning], uncertain: bool) -> ClearMeaning | None:
    identities = []
    for result in results:
        text = _root_text(result)
        literal = re.search(r"\b(?:means|mean|meaning) “([^”]+)”\.?$", text, re.IGNORECASE)
        components = re.search(r"(?:from words for|roots (?:may )?mean) (.+)\.$", text)
        if literal:
            identities.append(("literal", literal.group(1)))
        elif components:
            identities.append(("components", components.group(1)))
        else:
            return None
    if len(set(identities)) != 1:
        return None
    meaning_uncertain = any(re.search(r"\b(?:possibly|may)\b", result.text.split("“")[0], re.IGNORECASE) for result in results)
    traditional = any("traditionally" in result.text.lower().split("“")[0] for result in results)
    kind, gloss = identities[0]
    if kind == "literal":
        qualifier = "Possibly means" if meaning_uncertain else "Means"
        if traditional:
            qualifier = "Traditionally means" if not meaning_uncertain else "Possibly traditionally means"
        shared = ClearMeaning(f"{qualifier} “{gloss}”.", True)
    else:
        shared = ClearMeaning(f"{'Possibly built' if meaning_uncertain else 'Built'} from words for {gloss}.", True)
    return _inherited_meaning(wording, " or ".join(roots), shared, uncertain)


def _inherited_meaning(wording: str, root: str, result: ClearMeaning, uncertain: bool) -> ClearMeaning | None:
    """Retain the root's claims and caveats without expanding every chain link."""
    text = _root_text(result)
    prefix = f"{wording} {root}"
    if uncertain:
        prefix = "Possibly " + prefix[0].lower() + prefix[1:]
    literal = re.search(r"\b(?:means|mean|meaning) “([^”]+)”\.?$", text, re.IGNORECASE)
    if literal:
        qualifier = "may mean" if re.search(r"\b(?:possibly|may)\b", result.text.split("“")[0], re.IGNORECASE) else "means"
        if re.search(r"\btraditionally\b", result.text, re.IGNORECASE):
            qualifier = "may traditionally mean" if qualifier == "may mean" else "traditionally means"
        subject = "both" if " or " in root else root
        if subject == "both":
            qualifier = qualifier.removesuffix("s")
        summary = f"{prefix}; {subject} {qualifier} “{literal.group(1)}”."
    elif text.startswith(("Built from words for ", "Possibly built from words for ", "its roots mean ")):
        components = text.split("words for ")[-1].removeprefix("its roots mean ")
        qualifier = "may mean" if "Possibly" in result.text or "may mean" in result.text else "mean"
        subject = "their roots" if " or " in root else "its roots"
        summary = f"{prefix}; {subject} {qualifier} {components}"
    else:
        # A flower association or a traditional interpretation is useful too,
        # but must keep its wording instead of becoming a literal translation.
        text = re.sub(r"^[A-ZÀ-ÖØ-Þ][\wÀ-ÖØ-öø-ÿ'-]* (?:means|may mean) ", "Means ", text)
        summary = f"{prefix}. {text}"
    if len(summary) <= 120:
        return ClearMeaning(summary, True)
    # Preserve the meaning and qualify how it applies when the spelled-out
    # relationship would exceed the card's existing copy limit.
    summary = ("Possibly shares this meaning: " if uncertain else "Shares this meaning: ") + text
    return ClearMeaning(summary, True) if len(summary) <= 120 else None


def _surname_description(source: str) -> ClearMeaning | None:
    """Keep an explicit occupation or place association as an explanation."""
    occupational = re.search(r"\b(?:occupational|topographic) surname for ([^.]+)(?:\.|$)", source, re.IGNORECASE)
    if occupational:
        text = _sentence(f"Originally a surname for {occupational.group(1)}")
        if len(text) <= 120:
            return ClearMeaning(text, True)

    start = re.match(
        rf"^(?:An? )?(?:{LANGUAGE_PATTERN} )?(?:occupational|habitational|topographic) surname,? (.+?)(?:\.(?:\s|$)|$)",
        source, re.IGNORECASE,
    )
    if start:
        description = re.split(r", (?:both |all )?from\b", start.group(1), maxsplit=1)[0]
        if description.lower().startswith(("for ", "from ")):
            text = _sentence(f"Originally a surname {description}")
            if len(text) <= 120:
                return ClearMeaning(text, True)

    place = re.match(r"^From ((?:a |an )?(?:(?:Norman|English|Scottish|Welsh|Irish) )?place names?[^.]+)(?:\.(?:\s|$)|$)", source)
    if place:
        text = _sentence(f"Taken from {place.group(1)}")
        if len(text) <= 120:
            return ClearMeaning(text, True)
    return None


def rewrite_meaning(
    name: str,
    source: str,
    reviewed: bool,
    source_by_name: dict[str, tuple[str, bool]],
    *,
    seen: frozenset[str] = frozenset(),
) -> ClearMeaning:
    """Summarize one record; resolve a variant only through another sourced record."""
    source = " ".join(source.split())
    if not source or "[Term?]" in source:
        return NO_GLOSS
    if reviewed:
        return _reviewed_meaning(name, source)
    if name in EDITORIAL_COPY:
        return EDITORIAL_COPY[name]

    if re.search(r"\b(?:proper noun senses|noun senses|folk etymology)\b", source, re.IGNORECASE):
        return ClearMeaning("The source gives competing interpretations, so no single meaning is certain.", False)

    if name == "Lydia" and "originally indicated ancestry or residence" in source:
        return ClearMeaning("Originally described someone from Lydia, an ancient region.", True)

    modern_word = re.search(r"the modern given name is associated with English ([a-z]+)", source, re.IGNORECASE)
    if modern_word:
        return ClearMeaning(f"Associated with the English word “{modern_word.group(1)}”.", True)

    if source.lstrip().startswith("*") and " * " in source:
        return ClearMeaning("The source lists several different origins for this name.", False)

    if re.search(r"\b(?:possibly either|multiple origins|(?:several|various|two) (?:possible |main )?origins|from various sources|origin is debated|origin is uncertain|(?:the )?meaning is (?:obscure|uncertain|unknown|debated)|of (?:uncertain|obscure|debated|unknown) meaning)\b", source, re.IGNORECASE):
        return ClearMeaning("The name's origin is debated, so no single meaning is certain.", False)

    # Later comparisons and possible influences are not the name's main root.
    source = re.split(
        r"\b(?:Possibly influenced by|Compare |Doublet of |Equivalent to |By surface analysis)",
        source, maxsplit=1,
    )[0].strip()

    known_names = set(source_by_name)
    relation = _relation(source)
    unresolved_relation = None
    if relation:
        wording, roots, uncertain = relation
        results = []
        for root in roots:
            root_key = root.casefold()
            if root_key not in source_by_name or root_key in seen or root_key == name.casefold() or len(seen) >= 24:
                break
            root_source, root_reviewed = source_by_name[root_key]
            root_result = rewrite_meaning(
                root, root_source, root_reviewed, source_by_name,
                seen=seen | {name.casefold()},
            )
            if not root_result.usable_in_quiz:
                break
            results.append(root_result)
        if len(results) == len(roots):
            inherited = (_shared_meaning(wording, roots, results, uncertain) if len(roots) > 1
                         else _inherited_meaning(wording, roots[0], results[0], uncertain))
            if inherited:
                return inherited
        if len(roots) > 1:
            return ClearMeaning("The source lists multiple root names; no single shared meaning is established.", False)
        root = roots[0]
        prefix = "Possibly " + wording[0].lower() + wording[1:] if uncertain else wording
        unresolved_relation = ClearMeaning(f"{prefix} {root}; no separate literal meaning is recorded.", False)

    literal = re.search(r"\b(?:literally|meaning|means|interpreted as)\s+[“\"']([^”\"']{2,85})[”\"']", source, re.IGNORECASE)
    if literal:
        gloss = literal.group(1).strip(" .;:,")
        if not (gloss.casefold() in known_names and gloss[0].isupper()) and _valid_gloss(gloss):
            qualifier = "Possibly means" if re.search(r"\b(?:possibly|perhaps|uncertain|debated)\b", source[:literal.start()], re.IGNORECASE) else "Means"
            return ClearMeaning(f"{qualifier} “{gloss}”.", True)

    glosses = _glosses(source, known_names)
    if glosses:
        if "[of]" in source and " + " in source:
            return ClearMeaning("This is part of a longer name; the source gives no complete meaning on its own.", False)
        first_quote = re.search(r"[“\"]" + re.escape(glosses[0]) + r"[”\"]", source)
        first_plus = source.find(" + ")
        second_quote = re.search(r"[“\"]" + re.escape(glosses[1]) + r"[”\"]", source) if len(glosses) > 1 else None
        # A complete gloss followed by the derivation of that word is the
        # primary translation, not a list of independent possible meanings.
        # For example, Abner "father of light", followed by "father" + "light".
        if len(glosses) > 1 and first_quote and second_quote and re.search(
            r"\b(?:from|ultimately from)\s+", source[first_quote.end():second_quote.start()], re.IGNORECASE
        ) and (first_plus == -1 or first_plus > second_quote.start()) and not re.search(
            r"\b(?:either|compound|component|element)\b", source[:first_quote.start()], re.IGNORECASE
        ) and not re.search(
            r"\b(?:or|alternatively|also)\b", source[first_quote.end():second_quote.start()], re.IGNORECASE
        ) and not (glosses[0][0].isupper() and " " not in glosses[0]):
            gloss = _plain_gloss(glosses[0], name)
            if len(gloss) <= 52:
                qualifier = "Possibly means" if re.search(r"\b(?:possibly|probably|perhaps)\b", source[:first_quote.start()], re.IGNORECASE) else "Means"
                return ClearMeaning(f"{qualifier} “{gloss}”.", True)
        # Where a source lists several components or origins, presenting only
        # one as the definitive meaning would change what the source says.
        if len(glosses) > 1:
            if re.search(r"[”\"]\s+or\s+[“\"]", source):
                return ClearMeaning(f"Can mean “{glosses[0]}” or “{glosses[1]}”.", True)
            if " + " in source and len(glosses) == 2 and max(map(len, glosses)) <= 45:
                qualifier = "Possibly built" if re.search(r"\b(?:possibly|probably|perhaps)\b", source, re.IGNORECASE) else "Built"
                return ClearMeaning(f"{qualifier} from words for “{glosses[0]}” and “{glosses[1]}”.", True)
            return ClearMeaning(
                "The source lists several possible origins; no single meaning is established.", False
            )
        if " + " in source:
            return ClearMeaning("The source describes several name elements but gives no complete meaning.", False)
        gloss = _plain_gloss(glosses[0], name)
        if gloss.casefold() == name.casefold():
            return NO_GLOSS
        if "traditionally said to" in source.lower():
            return ClearMeaning(f"Traditionally linked to “{gloss}”; the origin is uncertain.", True)
        qualifier = "Possibly means" if re.search(r"\b(?:possibly|perhaps|uncertain|debated)\b", source, re.IGNORECASE) else "Means"
        return ClearMeaning(f"{qualifier} “{gloss}”.", True)

    direct_meaning = re.search(r"\bmeaning\s+(?:of\s+)?([a-z][a-z -]{2,60})(?:[.;]|$)", source, re.IGNORECASE)
    if direct_meaning:
        gloss = direct_meaning.group(1).strip(" .;:")
        if not re.search(r"\b(?:name|form|variant|origin)\b", gloss, re.IGNORECASE):
            return ClearMeaning(f"Means “{gloss}”.", True)

    word = re.match(r"From ([a-z][a-z -]{2,35})(?:\.(?:\s|$)|$)", source)
    if word and word.group(1).casefold() == name.casefold():
        return ClearMeaning(f"Taken from the word “{word.group(1)}”.", True)

    blend = re.match(r"^(?:Modern coinage,? (?:probably )?|A )?blend of\s+(.+?)(?:\.|$)", source, re.IGNORECASE)
    if blend and len(blend.group(1)) < 75:
        return ClearMeaning(_sentence(f"A name formed by blending {blend.group(1)}"), True)

    return _surname_description(source) or unresolved_relation or NO_GLOSS
