"""Sidebar filter values as a hashable object, so the cached queries can use it as a cache key."""
from dataclasses import dataclass

WINDOW_OPTIONS = (7, 14, 30)
SKILL_TYPES = ("tsc", "ccs", "unmapped")
SKILL_TYPE_LABELS = {
    "tsc": "Technical (SSG TSC)",
    "ccs": "Generic (SSG CCS)",
    "unmapped": "Not mapped to SSG",
}
MOM_GROUP_LABELS = {
    "PMET": "PMET (professionals, managers, executives, technicians)",
    "clerical_sales_services": "Clerical, sales and services",
    "production_transport": "Production, transport, labourers",
    "unknown": "Unknown",
}


@dataclass(frozen=True)
class Filters:
    window_days: int = 30
    mom_groups: tuple = ()
    major_groups: tuple = ()
    categories: tuple = ()
    skill_types: tuple = ()
    emerging_only: bool = False

    def params(self) -> dict:
        """Query parameters. An empty list means "no restriction" (the SQL checks cardinality = 0)."""
        return {
            "window": int(self.window_days),
            "mom": list(self.mom_groups),
            "major": [int(m) for m in self.major_groups],
            "cat": list(self.categories),
            "stype": list(self.skill_types),
            "emerging": bool(self.emerging_only),
        }
