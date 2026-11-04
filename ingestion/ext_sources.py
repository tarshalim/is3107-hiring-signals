"""Where each external dataset comes from, under which licence, and the lineage row each load writes.

Every loader calls register_source() inside its own transaction, so ext.source_registry always describes
exactly the copy of the file that the ext tables were built from (download time and sha256 included).
"""
import hashlib
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EXT_DATA_DIR = REPO_ROOT / "data" / "ext"          # git-ignored (data/* in .gitignore)

SG_OPEN_DATA_LICENCE = "Singapore Open Data Licence v1.0 (https://data.gov.sg/open-data-licence)"


@dataclass(frozen=True)
class Source:
    source_id: str
    title: str
    publisher: str
    url: str
    licence: str
    notes: str = ""


KAGGLE_MCF_2019 = Source(
    source_id="kaggle_mcf_2019",
    title="mycareerfuture-job-postings (mycareersfuture.json, version 1)",
    publisher="Kaggle user asrsaiteja (Asrst), uploaded 11 Sep 2023; postings crawled from MyCareersFuture in mid 2019",
    url="https://www.kaggle.com/datasets/asrsaiteja/mycareerfuture-job-postings",
    licence="Unknown (none stated on Kaggle). Used for non-commercial coursework analysis only, never redistributed",
    notes="Download: curl -L -o data/ext/kaggle_mcf2019/archive.zip "
          "https://www.kaggle.com/api/v1/datasets/download/asrsaiteja/mycareerfuture-job-postings",
)

MOM_OWS_2024 = Source(
    source_id="mom_ows_2024",
    title="Resident Occupational Wages, June 2024",
    publisher="Ministry of Manpower, Manpower Research & Statistics Department (Occupational Wage Survey), via data.gov.sg",
    url="https://data.gov.sg/datasets/d_9917e751f7498502f70052a940a3f312/view",
    licence=SG_OPEN_DATA_LICENCE,
    notes="Private sector establishments with 25+ employees, residents only. Gross wage excludes employer CPF and bonuses.",
)

MOM_LFS_INCOME_OCCUPATION = Source(
    source_id="mom_lfs_income_occupation",
    title="Median Gross Monthly Income From Employment (Including Employer CPF) Of Full-Time Employed "
          "Residents By Occupations And Sex, End June, Annual",
    publisher="Ministry of Manpower (Comprehensive Labour Force Survey), via SingStat and data.gov.sg",
    url="https://data.gov.sg/datasets/d_8f024ddf2553d81ee00ede55b1d9b0ff/view",
    licence=SG_OPEN_DATA_LICENCE,
    notes="Includes employer CPF and 1/12 of annual bonuses. SSOC 2020 back-mapped. No data for 2005 and 2006.",
)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def register_source(cur, source: Source, *, retrieved_at, file_path: Path | None, rows_in_file: int | None,
                    rows_loaded: int, coverage_start=None, coverage_end=None, extra_notes: str = ""):
    """Upsert the lineage row. Call it inside the loader's transaction so it commits with the data."""
    notes = " ".join(x for x in (source.notes, extra_notes) if x)
    cur.execute("""
        INSERT INTO ext.source_registry (source_id, title, publisher, url, licence, retrieved_at, file_name,
            file_sha256, rows_in_file, rows_loaded, coverage_start, coverage_end, notes, loaded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (source_id) DO UPDATE SET
            title = EXCLUDED.title, publisher = EXCLUDED.publisher, url = EXCLUDED.url,
            licence = EXCLUDED.licence, retrieved_at = EXCLUDED.retrieved_at, file_name = EXCLUDED.file_name,
            file_sha256 = EXCLUDED.file_sha256, rows_in_file = EXCLUDED.rows_in_file,
            rows_loaded = EXCLUDED.rows_loaded, coverage_start = EXCLUDED.coverage_start,
            coverage_end = EXCLUDED.coverage_end, notes = EXCLUDED.notes, loaded_at = now()
    """, (source.source_id, source.title, source.publisher, source.url, source.licence, retrieved_at,
          file_path.name if file_path else None, sha256_of(file_path) if file_path else None,
          rows_in_file, rows_loaded, coverage_start, coverage_end, notes))
