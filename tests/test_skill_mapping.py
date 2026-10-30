"""Offline tests for transform/skill_mapping.py: normalisation and the exact/alias/stem/fuzzy ladder."""
from transform.skill_mapping import (SkillMatcher, SkillRef, normalise, stem_key, tokens_align)


def ref(sid, title, stype="tsc", emerging=False, casl=False):
    return SkillRef(sid, title, stype, emerging, casl)


SKILLS = {
    "s1": ref("s1", "Project Management"),
    "s2": ref("s2", "Collaboration", "ccs"),
    "s3": ref("s3", "Cargo Warehousing Operations"),
    "s4": ref("s4", "Contract Management"),
    "s5": ref("s5", "Lighting Operations"),
    "s6": ref("s6", "Customer Relationship Management", casl=True),
    "s7": ref("s7", "Tools Development"),
    "s8": ref("s8", "Development Tools"),
    "s9": ref("s9", "Hazards Identification"),
}
ALIASES = {"team player": ("s2", 0.8), "teamwork": ("s2", 0.9)}


def matcher():
    return SkillMatcher(SKILLS, ALIASES)


def test_normalise_rules():
    assert normalise("  Customer Relationship Management (CRM) ") == "customer relationship management"
    assert normalise("R&D / Planning") == "r and d planning"
    assert normalise("Problem-Solving") == "problem solving"
    assert normalise(None) == ""
    assert stem_key("hazards identification of the sites") == "hazard identification site"
    assert stem_key("business analysis") == "business analysis"     # 'ss' and 'us' endings untouched


def test_exact_match_has_confidence_one():
    m = matcher().match("project management")
    assert m.method == "exact" and m.confidence == 1.0 and m.skill_id == "s1"
    m = matcher().match("Customer Relationship Management (CRM)")
    assert m.method == "exact" and m.is_casl is True


def test_alias_keeps_curated_confidence():
    m = matcher().match("Team Player")
    assert m.method == "alias" and m.skill_id == "s2" and m.confidence == 0.8 and m.skill_type == "ccs"


def test_stem_match_is_plural_insensitive_but_keeps_word_order():
    assert matcher().match("Hazard Identification").method == "stem"
    # 'Development Tools' and 'Tools Development' both exist: each must hit its own exact title,
    # and a reordered unknown tag must not be guessed.
    assert matcher().match("Tools Development").skill_id == "s7"
    assert matcher().match("Development Tools").skill_id == "s8"
    m2 = SkillMatcher({"s7": SKILLS["s7"]}, {})
    assert m2.match("Development Tools").method == "unmatched"


def test_fuzzy_accepts_small_spelling_differences_only():
    m = matcher().match("Cargo Warehouse Operations")
    assert m.method == "fuzzy" and m.skill_id == "s3" and m.confidence >= 0.90
    assert matcher().match("Hazard Identifications").method in ("stem", "fuzzy")
    # A different word, even with a high character ratio, is rejected by the per-word guard.
    assert matcher().match("Flight Operations").method == "unmatched"       # not 'Lighting Operations'
    assert matcher().match("Case Management").method == "unmatched"         # not 'Asset Management'
    m2 = SkillMatcher({"p": ref("p", "Product Management")}, {})
    assert m2.match("Production Management").method == "unmatched"          # same whole-string ratio as the good case


def test_tokens_align_guard():
    assert tokens_align("cargo warehouse operation", "cargo warehousing operation")   # shared prefix
    assert tokens_align("organizational design", "organisational design")             # one letter
    assert not tokens_align("production management", "product management")
    assert not tokens_align("flight operation", "lighting operation")
    assert not tokens_align("a b", "a b c")


def test_unmatched_tag_is_kept_not_dropped():
    m = matcher().match("Microsoft Excel")
    assert m.method == "unmatched" and m.skill_id is None and m.confidence is None
    assert matcher().match("").method == "unmatched"
    assert matcher().match("ERP").method == "unmatched"       # shorter than FUZZY_MIN_LEN: never fuzzy
