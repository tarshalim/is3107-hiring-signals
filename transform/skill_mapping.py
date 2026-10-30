"""Skill mapping v1: MyCareersFuture skill tags -> SkillsFuture Unique Skills List (USL).

MCF tags are free text typed by employers (21,600 distinct tags a day, "Team Player", "Microsoft
Excel", "Able To Work Independently"). The USL has 2,388 official skills with sector wording
("Apply Teamwork in the Workplace"). Only about 9% of tags match by name, so the mapping is a
ladder of methods, each recorded with a confidence so the dashboard can filter by it:

    1. exact  : normalised tag equals a normalised USL title                       confidence 1.00
    2. alias  : tag is in the alias table                                           confidence as set
                 - curated aliases in data/reference/skill_aliases.csv (hand-checked, each with its
                   own confidence, mostly generic tags mapped to the 16 Critical Core Skills)
                 - sector titles and previous titles from the TSC-to-USL mapping file (0.90)
    3. stem   : equal after light stemming (plural s/ies only) and stopword removal, word order kept
                                                                                    confidence 0.95
    4. fuzzy  : rapidfuzz ratio >= 90 on the stem key, AND the same number of words, AND each word
                 pairs with the word in the same position at ratio >= 91 or with a shared prefix of
                 8+ letters (warehouse/warehousing). This guard is what separates a spelling variant
                 from a different word: "Cargo Warehouse Operations" -> "Cargo Warehousing Operations"
                 (92) is accepted, "Production Management" -> "Product Management" (also 92),
                 "Flight Operations" -> "Lighting Operations" and "Case Management" -> "Asset
                 Management" are rejected. The word bar was raised from 85 to 91 after the first
                 full run let "Quantitative Analysis" land on "Qualitative Analysis" (word ratio 87),
                 "Attrition" on "Attribution" (90), "Curation" on "Creation" (87.5) and "Customs" on
                 "Customer" (85.7).                                                   confidence = ratio/100
    5. unmatched : kept in dim_skill with match_method 'unmatched' so nothing is lost and the
                 unmatched share is reported honestly.

Why these thresholds: tested on the 24 Oct snapshot (21,606 distinct tags). A plain whole-string
ratio cannot tell the good and bad pairs above apart (both score 92), so the word-level guard was
added and the bad pairs in a manual scan of fuzzy hits disappeared. Precision was preferred over
recall for v1 because a wrong skill label silently corrupts the "barrier skills" signal, while an
unmatched tag is only a smaller sample. Known limit: "Contractor Management" still lands on
"Contract Management" (word ratio 89); such cases carry confidence < 1 and can be filtered.

rapidfuzz is used when installed (it is in requirements.txt); difflib is the fallback so the
tests and the pipeline run anywhere.
"""
import csv
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

try:  # fast C implementation
    from rapidfuzz import fuzz as _rf_fuzz
    from rapidfuzz import process as _rf_process
except ImportError:  # pragma: no cover - exercised only where rapidfuzz is missing
    _rf_fuzz = None
    _rf_process = None
import difflib

STOPWORDS = {"and", "of", "the", "in", "for", "to", "a", "an", "with", "on", "at", "or"}
FUZZY_THRESHOLD = 90      # rapidfuzz ratio on the whole stem key, 0 to 100
TOKEN_THRESHOLD = 91      # per-word guard: ratio of each positional word pair
TOKEN_PREFIX = 8          # or the pair shares this many leading letters (warehouse / warehousing)
FUZZY_MIN_LEN = 6         # shorter keys are too easy to confuse


def normalise(name) -> str:
    """Lower case ASCII, parentheticals removed, '&' -> 'and', punctuation -> space, single spaces."""
    text = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode()
    text = re.sub(r"\([^)]*\)", " ", text)
    text = text.lower().replace("&", " and ").replace("/", " ").replace("-", " ")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _stem_token(token: str) -> str:
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 3 and token.endswith("s") and not token.endswith(("ss", "us", "is")):
        return token[:-1]
    return token


def stem_key(norm_name: str) -> str:
    """Plural-insensitive key with stopwords removed. Word order is kept on purpose:
    'Development Tools' and 'Tools Development' are different skills."""
    return " ".join(_stem_token(t) for t in norm_name.split() if t not in STOPWORDS)


def _ratio(a: str, b: str) -> float:
    if _rf_fuzz is not None:
        return float(_rf_fuzz.ratio(a, b))
    return 100.0 * difflib.SequenceMatcher(None, a, b).ratio()


def _shared_prefix(x: str, y: str) -> int:
    n = 0
    for cx, cy in zip(x, y):
        if cx != cy:
            break
        n += 1
    return n


def tokens_align(a_key: str, b_key: str, threshold: float = TOKEN_THRESHOLD, prefix: int = TOKEN_PREFIX) -> bool:
    """Same number of words, and each word pairs with the word in the same position: ratio >= threshold
    or a shared prefix of `prefix` letters (a suffix variant such as warehouse / warehousing)."""
    a_toks, b_toks = a_key.split(), b_key.split()
    if len(a_toks) != len(b_toks) or not a_toks:
        return False
    return all(_ratio(x, y) >= threshold or _shared_prefix(x, y) >= prefix for x, y in zip(a_toks, b_toks))


@dataclass(frozen=True)
class SkillRef:
    skill_id: str
    skill_title: str
    skill_type: str
    is_emerging: bool
    is_casl: bool


@dataclass(frozen=True)
class Match:
    skill_id: str | None
    skill_title: str | None
    skill_type: str | None
    is_emerging: bool | None
    is_casl: bool | None
    method: str          # 'exact', 'alias', 'stem', 'fuzzy', 'unmatched'
    confidence: float | None


UNMATCHED = Match(None, None, None, None, None, "unmatched", None)


class SkillMatcher:
    def __init__(self, skills: dict, aliases: dict | None = None,
                 fuzzy_threshold: float = FUZZY_THRESHOLD, min_len: int = FUZZY_MIN_LEN):
        """skills: skill_id -> SkillRef. aliases: normalised alias -> (skill_id, confidence)."""
        self.skills = skills
        self.fuzzy_threshold = fuzzy_threshold
        self.min_len = min_len
        self.by_norm = {}
        self.by_stem = {}
        ambiguous = set()
        for sid, ref in skills.items():
            n = normalise(ref.skill_title)
            self.by_norm.setdefault(n, sid)
            k = stem_key(n)
            if k in self.by_stem and self.by_stem[k] != sid:
                ambiguous.add(k)
            self.by_stem.setdefault(k, sid)
        for k in ambiguous:          # two different skills share a stem key: do not guess
            self.by_stem.pop(k, None)
        self.aliases = {}
        for alias, (sid, conf) in (aliases or {}).items():
            if sid in skills:
                self.aliases[normalise(alias)] = (sid, float(conf))
        self._stem_keys = list(self.by_stem.keys())

    def _result(self, sid: str, method: str, confidence: float) -> Match:
        ref = self.skills[sid]
        return Match(sid, ref.skill_title, ref.skill_type, ref.is_emerging, ref.is_casl, method, round(confidence, 3))

    def match(self, raw_name) -> Match:
        n = normalise(raw_name)
        if not n:
            return UNMATCHED
        if n in self.by_norm:
            return self._result(self.by_norm[n], "exact", 1.0)
        if n in self.aliases:
            sid, conf = self.aliases[n]
            return self._result(sid, "alias", conf)
        k = stem_key(n)
        if k in self.by_stem:
            return self._result(self.by_stem[k], "stem", 0.95)
        if k in self.aliases:
            sid, conf = self.aliases[k]
            return self._result(sid, "alias", conf)
        if len(k) >= self.min_len:
            hit = self._fuzzy(k)
            if hit is not None:
                key, score = hit
                return self._result(self.by_stem[key], "fuzzy", score / 100.0)
        return UNMATCHED

    def _fuzzy(self, key: str):
        if not self._stem_keys:
            return None
        if _rf_process is not None:
            candidates = _rf_process.extract(key, self._stem_keys, scorer=_rf_fuzz.ratio,
                                             score_cutoff=self.fuzzy_threshold, limit=5)
            candidates = [(c[0], c[1]) for c in candidates]
        else:
            close = difflib.get_close_matches(key, self._stem_keys, n=5, cutoff=self.fuzzy_threshold / 100.0)
            candidates = [(c, _ratio(key, c)) for c in close]
        for cand, score in candidates:
            if score >= self.fuzzy_threshold and tokens_align(key, cand):
                return cand, score
        return None

    @classmethod
    def from_db(cls, conn) -> "SkillMatcher":
        with conn.cursor() as cur:
            cur.execute("SELECT skill_id, skill_title, skill_type, is_emerging, is_casl FROM clean.ref_ssg_skill")
            skills = {r[0]: SkillRef(*r) for r in cur.fetchall()}
            cur.execute("SELECT norm_alias, skill_id, confidence FROM clean.ref_skill_alias")
            aliases = {r[0]: (r[1], float(r[2])) for r in cur.fetchall()}
        return cls(skills, aliases)


def match_rate_report(conn, snapshot_date) -> dict:
    """Share of skill mentions and of distinct tags mapped, for the postings live on snapshot_date.
    A mention = one (posting, skill) pair that day. Both views matter: mentions tell us how much of
    the data the dashboard can label, distinct tags tell us how much of the long tail is covered."""
    sql = """
        WITH live AS (
            SELECT b.skill_key, b.job_key
            FROM clean.fact_job_day f
            JOIN clean.bridge_job_skill b ON b.job_key = f.job_key
            WHERE f.snapshot_date = %(d)s
        )
        SELECT s.match_method,
               count(*)                      AS mentions,
               count(DISTINCT live.skill_key) AS distinct_tags
        FROM live JOIN clean.dim_skill s ON s.skill_key = live.skill_key
        GROUP BY s.match_method ORDER BY s.match_method
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"d": snapshot_date})
        rows = cur.fetchall()
    by_method = {m: {"mentions": int(a), "distinct_tags": int(b)} for m, a, b in rows}
    mentions = sum(v["mentions"] for v in by_method.values())
    tags = sum(v["distinct_tags"] for v in by_method.values())
    mapped_m = sum(v["mentions"] for m, v in by_method.items() if m != "unmatched")
    mapped_t = sum(v["distinct_tags"] for m, v in by_method.items() if m != "unmatched")
    return {
        "snapshot_date": str(snapshot_date),
        "mentions": mentions,
        "distinct_tags": tags,
        "mapped_share_by_mentions": round(mapped_m / mentions, 4) if mentions else None,
        "mapped_share_by_distinct_tags": round(mapped_t / tags, 4) if tags else None,
        "by_method": by_method,
    }


def remap_skills(conn, matcher: "SkillMatcher | None" = None) -> dict:
    """Re-match EVERY tag in clean.dim_skill with the current rules and reference tables.

    Mapping normally happens once, when a tag is first seen. When the alias file grows or a
    threshold changes, this brings the old rows up to date without reloading any snapshot (the
    bridge and fact tables only hold skill_key, so nothing else moves). Idempotent: running it
    twice changes nothing the second time.
    """
    matcher = matcher or SkillMatcher.from_db(conn)
    changed = 0
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT skill_key, mcf_skill_name, ssg_skill_id, match_method, match_confidence FROM clean.dim_skill")
            rows = cur.fetchall()
            updates = []
            for skill_key, name, old_id, old_method, old_conf in rows:
                m = matcher.match(name)
                old_conf_f = float(old_conf) if old_conf is not None else None
                if (m.skill_id, m.method, m.confidence) != (old_id, old_method, old_conf_f):
                    updates.append((m.skill_id, m.skill_title, m.skill_type, m.is_emerging, m.is_casl,
                                    m.method, m.confidence, skill_key))
            if updates:
                from psycopg2.extras import execute_batch
                execute_batch(cur, """
                    UPDATE clean.dim_skill SET ssg_skill_id = %s, ssg_skill_title = %s, ssg_skill_type = %s,
                        is_emerging = %s, is_casl = %s, match_method = %s, match_confidence = %s
                    WHERE skill_key = %s
                """, updates, page_size=1000)
            changed = len(updates)
    return {"tags": len(rows), "remapped": changed}


def read_alias_csv(path: Path) -> list[dict]:
    """data/reference/skill_aliases.csv: alias, ssg_skill_title, confidence, note."""
    with open(path, newline="", encoding="utf-8") as f:
        return [row for row in csv.DictReader(f) if row.get("alias")]
