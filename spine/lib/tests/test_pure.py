"""Unit tests for the DB-free pieces of the spine (no workspace needed).

Run: pytest spine/lib/tests/test_pure.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import confidence  # noqa: E402
import registry  # noqa: E402


# -- confidence.score ---------------------------------------------------------

def test_clean_output_scores_high():
    r = confidence.score("SELECT customer_id, sum(amount) FROM sales GROUP BY customer_id")
    assert r.confidence == 1.0
    assert r.findings == []


def test_empty_output_is_zero():
    r = confidence.score("   ")
    assert r.confidence == 0.0
    assert r.findings[0]["severity"] == "blocker"


def test_todo_and_stored_proc_lower_confidence():
    r = confidence.score("SELECT * FROM dbo.foo -- TODO fix\nEXEC sp_thing")
    assert r.confidence < 1.0
    cats = {f["category"] for f in r.findings}
    assert "manual_review" in cats  # TODO
    assert "stored_proc" in cats  # EXEC
    assert "anti_pattern" in cats  # dbo.


def test_findings_shape_as_todos():
    r = confidence.score("x = spark.read.table('t')  # PATINDEX")
    todos = r.as_todos()
    assert all({"category", "severity", "message"} <= set(t) for t in todos)


def test_flat_penalty_per_risk_type_not_per_occurrence():
    # Two TODOs should not double-penalize (flat per distinct risk type).
    one = confidence.score("SELECT 1 -- TODO")
    two = confidence.score("SELECT 1 -- TODO\nSELECT 2 -- TODO")
    assert one.confidence == two.confidence


# -- registry._sql_str --------------------------------------------------------

def test_sql_str_none_is_null():
    assert registry._sql_str(None) == "NULL"


def test_sql_str_bool():
    assert registry._sql_str(True) == "TRUE"
    assert registry._sql_str(False) == "FALSE"


def test_sql_str_numbers_unquoted():
    assert registry._sql_str(42) == "42"
    assert registry._sql_str(0.8) == "0.8"


def test_sql_str_escapes_single_quotes():
    assert registry._sql_str("O'Brien") == "'O''Brien'"


def test_config_hash_stable_and_order_independent():
    a = registry.config_hash({"client": "acme", "target": "main"})
    b = registry.config_hash({"target": "main", "client": "acme"})
    assert a == b
    assert a != registry.config_hash({"client": "other"})


# -- registry MERGE / transition behavior (with a fake sql capturing statements) --

class FakeSql:
    """Records emitted SQL; returns canned rows for SELECT existence/status checks."""

    def __init__(self, exists=False, status="discovered"):
        self.statements = []
        self._exists = exists
        self._status = status

    def __call__(self, query):
        self.statements.append(query)
        q = query.strip().upper()
        if q.startswith("SELECT 1 FROM") and "OBJECTS" in q:
            return [{"1": 1}] if self._exists else []
        if q.startswith("SELECT STATUS FROM"):
            return [{"status": self._status}] if self._exists else []
        return []


def _reg(fake):
    return registry.Registry("cat", "sch", sql=fake, actor="test", config={"c": 1})


def test_register_new_object_audits_as_register():
    fake = FakeSql(exists=False)
    _reg(fake).register_object("o1", source_type="alteryx", object_kind="workflow",
                               volume_path="/v/o1.yxmd", complexity="low")
    joined = " ".join(fake.statements).upper()
    assert "MERGE INTO" in joined
    assert "'REGISTER'" in joined  # audit action
    assert "'REREGISTER'" not in joined


def test_register_existing_object_audits_as_reregister_not_discovered():
    fake = FakeSql(exists=True)
    _reg(fake).register_object("o1", source_type="alteryx", object_kind="workflow",
                               volume_path="/v/o1.yxmd", complexity="high")
    joined = " ".join(fake.statements)
    assert "'reregister'" in joined
    # must NOT claim a transition back to discovered on re-scan
    assert "'register'" not in joined


def test_transition_unknown_object_raises():
    fake = FakeSql(exists=False)
    import pytest
    with pytest.raises(ValueError, match="unknown object"):
        _reg(fake).transition("ghost", "converted")


def test_transition_rejects_unknown_status():
    fake = FakeSql(exists=True)
    import pytest
    with pytest.raises(ValueError, match="unknown status"):
        _reg(fake).transition("o1", "bogus")


def test_transition_known_object_updates_and_audits():
    fake = FakeSql(exists=True, status="discovered")
    _reg(fake).transition("o1", "converted", confidence=0.9)
    joined = " ".join(fake.statements).upper()
    assert "UPDATE" in joined
    assert "'TRANSITION:CONVERTED'" in joined
