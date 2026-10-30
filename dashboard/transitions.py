"""Career-transition logic on occupation skill profiles (pure pandas, no database).

Input frames come from queries.occupation_skill_profiles():
  occupations: ssoc_code, ssoc_title, mom_group, postings
  skills:      ssoc_code, skill_id, skill_label, skill_type, skill_postings
A skill's "share" in an occupation is the fraction of that occupation's postings that ask for it.
"""
import numpy as np
import pandas as pd


def skill_shares(occupations: pd.DataFrame, skills: pd.DataFrame) -> pd.DataFrame:
    merged = skills.merge(occupations[["ssoc_code", "postings"]], on="ssoc_code", how="inner")
    merged["share"] = (merged["skill_postings"] / merged["postings"]).clip(upper=1.0)
    return merged[["ssoc_code", "skill_id", "skill_label", "skill_type", "share"]]


def _matrix(shares: pd.DataFrame) -> pd.DataFrame:
    return shares.pivot_table(index="ssoc_code", columns="skill_id", values="share", fill_value=0.0)


def similar_occupations(source_code: str, occupations: pd.DataFrame, shares: pd.DataFrame, top: int = 10) -> pd.DataFrame:
    """Occupations whose skill profile is closest (cosine similarity) to the source occupation's."""
    matrix = _matrix(shares)
    if source_code not in matrix.index:
        return pd.DataFrame(columns=["ssoc_code", "ssoc_title", "mom_group", "postings", "similarity"])
    values = matrix.to_numpy()
    norms = np.linalg.norm(values, axis=1)
    source = values[matrix.index.get_loc(source_code)]
    denom = norms * np.linalg.norm(source)
    similarity = np.divide(values @ source, denom, out=np.zeros(len(matrix)), where=denom > 0)
    result = pd.DataFrame({"ssoc_code": matrix.index, "similarity": similarity})
    result = result[result["ssoc_code"] != source_code]
    result = result.merge(occupations, on="ssoc_code", how="left")
    return result.sort_values("similarity", ascending=False).head(top).reset_index(drop=True)


def skill_gap(source_code: str, target_code: str, shares: pd.DataFrame, min_target_share: float = 0.15) -> pd.DataFrame:
    """Skills the target occupation commonly asks for, with how common they are in the source occupation.
    `gap` = target share minus source share; a large positive gap is a skill to learn."""
    target = shares[(shares["ssoc_code"] == target_code) & (shares["share"] >= min_target_share)]
    source = shares[shares["ssoc_code"] == source_code][["skill_id", "share"]].rename(columns={"share": "source_share"})
    gap = target.rename(columns={"share": "target_share"}).merge(source, on="skill_id", how="left")
    gap["source_share"] = gap["source_share"].fillna(0.0)
    gap["gap"] = gap["target_share"] - gap["source_share"]
    cols = ["skill_label", "skill_type", "target_share", "source_share", "gap"]
    return gap.sort_values("gap", ascending=False)[cols].reset_index(drop=True)


def coverage_by_skills(owned_skill_ids: list, occupations: pd.DataFrame, shares: pd.DataFrame) -> pd.DataFrame:
    """Rank occupations by how much of their skill demand the owned skills cover.
    coverage = sum of shares of owned skills / sum of shares of all the occupation's skills."""
    cols = ["ssoc_code", "ssoc_title", "mom_group", "postings", "coverage", "matched_skills"]
    if not owned_skill_ids or shares.empty:
        return pd.DataFrame(columns=cols)
    owned = shares["skill_id"].isin(owned_skill_ids)
    total = shares.groupby("ssoc_code")["share"].sum()
    covered = shares[owned].groupby("ssoc_code")["share"].sum()
    matched = shares[owned].groupby("ssoc_code")["skill_id"].nunique()
    result = pd.DataFrame({"coverage": (covered / total).dropna(), "matched_skills": matched}).dropna()
    result["matched_skills"] = result["matched_skills"].astype(int)
    result = result.reset_index().merge(occupations, on="ssoc_code", how="left")
    return result.sort_values(["coverage", "postings"], ascending=False)[cols].reset_index(drop=True)


def missing_skills(occupation_code: str, owned_skill_ids: list, shares: pd.DataFrame, top: int = 10) -> pd.DataFrame:
    """Most requested skills of an occupation that the user does not have yet."""
    rows = shares[(shares["ssoc_code"] == occupation_code) & (~shares["skill_id"].isin(owned_skill_ids))]
    return rows.sort_values("share", ascending=False).head(top)[["skill_label", "skill_type", "share"]].reset_index(drop=True)
