"""Load the reference files in data/reference/ into clean.ref_* tables (idempotent upserts).

Files (public downloads, copies kept in the repo because they are small and change rarely):
    unique_skills_list.xlsx            SkillsFuture Unique Skills List, 2,388 skills (sheet 'Unique Skills List')
    tsc_to_unique_skills_mapping.xlsx  sector TSC titles -> unique skill (sheet 'data'), used as aliases
    ssoc2020_detailed_definitions.xlsx SingStat SSOC 2020 codes and titles (header on row 5)
    skill_aliases.csv                  hand-curated aliases for the generic MCF tags, with confidences

The loader is a separate, re-runnable step (L7: small atomic tasks). Running it twice changes
nothing except loaded_at. Aliases whose target title is not in the USL are reported and skipped,
never guessed.

CLI:  python -m transform.load_reference [--reference-dir data/reference]
"""
import argparse
import json
import re
import warnings
from pathlib import Path

import openpyxl
from psycopg2.extras import execute_values

from transform.skill_mapping import normalise, read_alias_csv

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DIR = REPO_ROOT / "data" / "reference"

MOM_GROUP = {1: "PMET", 2: "PMET", 3: "PMET", 4: "clerical_sales_services", 5: "clerical_sales_services",
             6: "production_transport", 7: "production_transport", 8: "production_transport", 9: "production_transport"}


def _truthy(v) -> bool:
    return str(v).strip().lower() in {"true", "1", "yes", "y"}


def read_usl(path: Path) -> list[tuple]:
    """(skill_id, skill_title, norm_title, skill_type, is_emerging, is_casl)"""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        wb = openpyxl.load_workbook(path, read_only=True)
    rows = list(wb["Unique Skills List"].iter_rows(values_only=True))
    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    out = {}
    for r in rows[1:]:
        d = dict(zip(header, r))
        sid, title = d.get("skill_id"), d.get("skill_title")
        if not sid or not title:
            continue
        out[str(sid).strip()] = (str(sid).strip(), str(title).strip(), normalise(title),
                                 str(d.get("skill_type") or "").strip().lower(),
                                 _truthy(d.get("Emerging Skills")), _truthy(d.get("CASL Skills")))
    return list(out.values())


def read_tsc_aliases(path: Path, usl_by_norm: dict) -> dict:
    """norm_alias -> (skill_id, confidence, source). Sector titles and previous titles that differ from
    the unique skill title. Ambiguous aliases (point at two skills) are dropped."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        wb = openpyxl.load_workbook(path, read_only=True)
    rows = list(wb["data"].iter_rows(values_only=True))
    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    aliases, conflicts = {}, set()
    for r in rows[1:]:
        d = dict(zip(header, r))
        target = d.get("Unique skill_updated_skill_title")
        if not target or str(target).strip() == "No mapping":
            continue
        sid = usl_by_norm.get(normalise(target))
        if not sid:
            continue
        for col, source in (("skills_framework_skill_title", "tsc_sector_title"),
                            ("Unique skill_previous_skill_title", "tsc_previous_title")):
            n = normalise(d.get(col) or "")
            if not n or n in usl_by_norm:
                continue
            if n in aliases and aliases[n][0] != sid:
                conflicts.add(n)
            aliases.setdefault(n, (sid, 0.90, source))
    for n in conflicts:
        aliases.pop(n, None)
    return aliases


def read_curated_aliases(path: Path, usl_by_norm: dict):
    """Returns (aliases dict, invalid list). Curated aliases win over TSC aliases."""
    aliases, invalid = {}, []
    if not path.exists():
        return aliases, invalid
    for row in read_alias_csv(path):
        n = normalise(row["alias"])
        sid = usl_by_norm.get(normalise(row["ssg_skill_title"]))
        if not n or not sid:
            invalid.append(row["alias"] + " -> " + row["ssg_skill_title"])
            continue
        try:
            conf = float(row.get("confidence") or 0.8)
        except ValueError:
            conf = 0.8
        aliases[n] = (sid, conf, "curated")
    return aliases, invalid


def read_ssoc(path: Path) -> list[tuple]:
    """(ssoc_code, ssoc_title, major_group, major_group_title, mom_group) for 5-digit codes."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.worksheets[0]
    majors, codes = {}, []
    for r in ws.iter_rows(values_only=True):
        code = str(r[0]).strip() if r and r[0] is not None else ""
        title = str(r[1]).strip() if len(r) > 1 and r[1] is not None else ""
        if not title:
            continue
        if re.fullmatch(r"\d", code):
            majors[int(code)] = title.title()
        elif re.fullmatch(r"\d{5}", code):
            codes.append((code, title))
    out = []
    for code, title in codes:
        major = int(code[0])
        out.append((code, title, major, majors.get(major), MOM_GROUP.get(major, "other")))
    return out


def load_reference(conn, reference_dir: Path = DEFAULT_DIR) -> dict:
    usl = read_usl(reference_dir / "unique_skills_list.xlsx")
    usl_by_norm = {norm: sid for sid, _t, norm, *_ in usl}
    tsc = read_tsc_aliases(reference_dir / "tsc_to_unique_skills_mapping.xlsx", usl_by_norm)
    curated, invalid = read_curated_aliases(reference_dir / "skill_aliases.csv", usl_by_norm)
    aliases = dict(tsc)
    aliases.update(curated)
    ssoc = read_ssoc(reference_dir / "ssoc2020_detailed_definitions.xlsx")

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO clean.ref_ssg_skill (skill_id, skill_title, norm_title, skill_type, is_emerging, is_casl)
                VALUES %s
                ON CONFLICT (skill_id) DO UPDATE SET
                    skill_title = EXCLUDED.skill_title, norm_title = EXCLUDED.norm_title,
                    skill_type = EXCLUDED.skill_type, is_emerging = EXCLUDED.is_emerging,
                    is_casl = EXCLUDED.is_casl, loaded_at = now()
            """, usl, page_size=1000)
            execute_values(cur, """
                INSERT INTO clean.ref_skill_alias (norm_alias, skill_id, source, confidence)
                VALUES %s
                ON CONFLICT (norm_alias) DO UPDATE SET
                    skill_id = EXCLUDED.skill_id, source = EXCLUDED.source,
                    confidence = EXCLUDED.confidence, loaded_at = now()
            """, [(n, sid, src, conf) for n, (sid, conf, src) in aliases.items()], page_size=1000)
            execute_values(cur, """
                INSERT INTO clean.ref_ssoc (ssoc_code, ssoc_title, major_group, major_group_title, mom_group)
                VALUES %s
                ON CONFLICT (ssoc_code) DO UPDATE SET
                    ssoc_title = EXCLUDED.ssoc_title, major_group = EXCLUDED.major_group,
                    major_group_title = EXCLUDED.major_group_title, mom_group = EXCLUDED.mom_group,
                    loaded_at = now()
            """, ssoc, page_size=1000)
    return {
        "ssg_skills": len(usl),
        "aliases_total": len(aliases),
        "aliases_curated": len(curated),
        "aliases_tsc": len(tsc),
        "aliases_invalid": invalid,
        "ssoc_codes": len(ssoc),
    }


def main():
    from transform.db import connect
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reference-dir", default=str(DEFAULT_DIR))
    ap.add_argument("--remap", action="store_true",
                    help="after loading, re-match every tag already in clean.dim_skill with the current rules")
    args = ap.parse_args()
    conn = connect()
    try:
        print(json.dumps(load_reference(conn, Path(args.reference_dir)), indent=2))
        if args.remap:
            from transform.skill_mapping import remap_skills
            print("remap:", json.dumps(remap_skills(conn)))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
