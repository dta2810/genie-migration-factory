"""Deterministic confidence scoring of converted output.

Principle (see docs/ARCHITECTURE.md): confidence must NOT be a number the LLM assigns
itself. We score the conversion output with reproducible, rule-based checks so routing
(auto-accept vs send to interactive review) is auditable.

Score is 0..1. Each failed check subtracts weight. The same output always scores the same.
The checks are engine-agnostic (apply to output from Genie Code or an FMAPI batch alike).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# (pattern, weight, category, severity, message) — patterns that indicate residual risk.
_RISK_CHECKS = [
    (r"\bTODO\b", 0.20, "manual_review", "warning", "unresolved TODO marker left in output"),
    (r"spark\.read\b", 0.15, "anti_pattern", "warning", "spark.read used (should be inline spark.sql)"),
    (r"\bPATINDEX\b", 0.15, "untranslated_fn", "blocker", "PATINDEX has no Spark equivalent"),
    (r"\b(EXEC|EXECUTE|sp_executesql)\b", 0.20, "stored_proc", "blocker", "stored-procedure call needs manual migration"),
    (r"\bMERGE\s+INTO\b", 0.10, "incremental_merge", "warning", "MERGE left in output (declarative pipelines full-refresh)"),
    (r"\bdbo\.", 0.05, "anti_pattern", "info", "dbo. prefix not stripped"),
    (r"@\[User::", 0.10, "untranslated_fn", "warning", "SSIS variable reference not translated"),
    (r"\bGETDATE\(\)", 0.05, "untranslated_fn", "info", "GETDATE() not converted to current_timestamp()"),
]


@dataclass
class ScoreResult:
    confidence: float
    findings: list[dict]  # each: category, severity, message, count

    def as_todos(self) -> list[dict]:
        """Findings shaped for Registry.add_todo()."""
        return [
            {"category": f["category"], "severity": f["severity"], "message": f["message"]}
            for f in self.findings
        ]


def score(converted_code: str) -> ScoreResult:
    """Score converted output deterministically. Returns confidence + findings."""
    conf = 1.0
    findings: list[dict] = []
    for pattern, weight, category, severity, message in _RISK_CHECKS:
        count = len(re.findall(pattern, converted_code, flags=re.IGNORECASE))
        if count:
            conf -= weight  # flat penalty per distinct risk type (not per occurrence)
            findings.append(
                {"category": category, "severity": severity, "message": message, "count": count}
            )
    # Structural sanity: empty or non-code output is zero confidence.
    if not converted_code.strip():
        return ScoreResult(0.0, [{"category": "anti_pattern", "severity": "blocker",
                                  "message": "empty conversion output", "count": 1}])
    return ScoreResult(round(max(conf, 0.0), 3), findings)
