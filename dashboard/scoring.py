"""Hiring-difficulty index (pure pandas).

Difficulty here is a relative signal, not a measurement: an occupation is harder to hire for when postings
attract few applications, are reposted often, and stay open longer. Each signal is converted to a
percentile rank among the occupations on screen and the three ranks are averaged (0 = easiest, 100 = hardest).
"""
import pandas as pd

SIGNALS = (
    ("applications_per_posting", False),   # (column, higher_means_harder)
    ("repost_rate", True),
    ("avg_days_open", True),
)


def difficulty_index(frame: pd.DataFrame) -> pd.Series:
    ranks = []
    for column, higher_is_harder in SIGNALS:
        if column not in frame or frame[column].notna().sum() < 2:
            continue
        pct = frame[column].rank(pct=True, method="average")
        ranks.append(pct if higher_is_harder else 1 - pct + 1 / frame[column].notna().sum())
    if not ranks:
        return pd.Series(float("nan"), index=frame.index)
    return (pd.concat(ranks, axis=1).mean(axis=1) * 100).clip(0, 100).round(1)
