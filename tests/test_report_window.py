"""Independent reported-window arithmetic and per-day field coverage; offline."""
import base64
import hashlib
import json
import os
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts/calculations/fec/report-window/v1"
AUDIT = os.environ.get("LT_REPORT_WINDOW_AUDIT")
METADATA = os.environ.get("LT_REPORT_WINDOW_METADATA")
REPORTS = os.environ.get("LT_REPORT_WINDOW_REPORTS")
REQUIRES_AUDIT = pytest.mark.skipif(not all((AUDIT, METADATA, REPORTS)), reason="requires retained offline window gate")


def read(path):
    return json.loads(path.read_bytes(), parse_float=Decimal)


def days(start, end):
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    return [a + timedelta(days=i) for i in range((b - a).days + 1)]


def test_window_contract_guards_and_cash_roles():
    policy = read(CONTRACT / "policy.json")
    assert policy["version"] == "legal-tender.fec.report-window.v1"
    assert not any(policy["guards"].values())
    assert len(policy["sum_fields"]) == 5
    assert policy["boundary_fields"] == {"cash_on_hand_beginning_period": "opening_boundary", "cash_on_hand_end_period": "closing_boundary"}
    schema = read(CONTRACT / "documents.schema.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate({"version": policy["document_set_version"], "documents": []})


@REQUIRES_AUDIT
@pytest.mark.parametrize("name,gap,ready", [("sid-window", 0, True), ("sid-cycle", 335, False), ("sid-missing-cover", 92, False), ("nrcc-unresolved", 30, False)])
def test_every_member_sum_and_day(name, gap, ready):
    r = read(Path(AUDIT) / f"{name}.json")
    descriptor_path = Path(AUDIT) / f"{name}-documents.json"
    descriptor = read(descriptor_path)
    Draft202012Validator(read(CONTRACT / "documents.schema.json")).validate(descriptor)
    assert r["document_set"]["sha256"] == hashlib.sha256(descriptor_path.read_bytes()).hexdigest()
    m = r["membership"]
    stem = "nrcc-reports" if name == "nrcc-unresolved" else "sid-reports"
    capture_path = Path(METADATA) / f"{stem}-capture.json"
    capture = read(capture_path)
    assert m["evidence"]["capture_sha256"] == hashlib.sha256(capture_path.read_bytes()).hexdigest()
    source = []
    for page in capture["pages"]:
        for kind in ("body", "headers"):
            a = page[kind]
            raw = (Path(METADATA) / a["path"]).read_bytes()
            assert hashlib.sha256(raw).hexdigest() == a["sha256"] and len(raw) == a["bytes"]
        source.extend(read(Path(METADATA) / page["body"]["path"])["results"])
    assert source == [row["raw"] for page in m["evidence"]["pages"] for row in page["records"]]
    by_observation = {}
    for j, (b, d) in enumerate(zip(r["bindings"], descriptor["documents"], strict=True)):
        e = b["document"]
        for kind in ("body", "headers"):
            path = Path(REPORTS) / Path(d[kind]["path"]).name
            raw = path.read_bytes()
            assert hashlib.sha256(raw).hexdigest() == d[kind]["sha256"] == e[kind]["sha256"]
            assert len(raw) == d[kind]["bytes"] == e[kind]["bytes"]
        body = (Path(REPORTS) / Path(d["body"]["path"]).name).read_bytes()
        assert b"".join(base64.b64decode(x["raw_base64"], validate=True) for x in e["records"]) == body
        index = b["observation_index"]
        assert index is not None and str(source[index]["file_number"]) == e["file_number"]
        assert index not in by_observation
        by_observation[index] = (j, b)
        for f in b["fields"]:
            assert f["metadata"]["raw"] == source[index][f["name"]]
            if f["reported_value_bound"]:
                assert index in m["chain_candidate_indexes"] and source[index]["is_amended"] is False
                assert b["scope_bound"] and not f["blockers"]
                assert int(f["metadata"]["minor_units"]) == Decimal(source[index][f["name"]]) * 100
                assert all(v["minor_units"] == f["metadata"]["minor_units"] for v in f["cover"])
    window = set(days(m["window"]["start"], m["window"]["end"]))
    for f in r["fields"]:
        members, missing = [], []
        for i in m["chain_candidate_indexes"]:
            if m["observations"][i]["window_relation"] == "outside":
                continue
            if i in by_observation:
                j, b = by_observation[i]
                field = next((v for v in b["fields"] if v["name"] == f["name"]), None)
                if field and field["reported_value_bound"]:
                    members.append(j)
                    continue
            missing.append(i)
        assert f["member_binding_indexes"] == members
        assert [v["observation_index"] for v in f["missing"]] == missing
        counter, values = Counter(), []
        for j in members:
            i = r["bindings"][j]["observation_index"]
            p = m["observations"][i]["period"]
            report_days = set(days(p["start"], p["end"]))
            assert report_days <= window
            counter.update(report_days)
            values.append(int(Decimal(source[i][f["name"]]) * 100))
        cov = f["coverage"]
        actual = {}
        for s in cov["segments"]:
            for d in days(s["start"], s["end"]):
                assert d not in actual
                actual[d] = s["membership_count"]
        assert set(actual) == window and all(actual[d] == counter[d] for d in window)
        assert cov["gap_days"] == sum(counter[d] == 0 for d in window) == gap
        assert cov["overlap_days"] == sum(counter[d] > 1 for d in window) == 0
        assert cov["covered_days"] + cov["gap_days"] == cov["window_days"] == len(window)
        assert f["reported_window_ready"] is ready
        if f["operation"] == "sum_periods":
            expected = str(sum(values)) if values else None
            assert f["observed_sum_minor_units"] == expected
            assert f["window_value_minor_units"] == (expected if ready else None)
        else:
            assert f["observed_sum_minor_units"] is None
            if ready:
                which = "start" if f["operation"] == "opening_boundary" else "end"
                i = next(r["bindings"][j]["observation_index"] for j in members if m["observations"][r["bindings"][j]["observation_index"]]["period"][which] == m["window"][which])
                assert f["window_value_minor_units"] == str(int(Decimal(source[i][f["name"]]) * 100))
            else:
                assert f["window_value_minor_units"] is None
    assert not any(r[k] for k in ("financial_cycle_total_ready", "cash_basis_ready", "terminal_attribution_eligible"))


@REQUIRES_AUDIT
@pytest.mark.parametrize("name", ["sid-window", "sid-cycle", "sid-missing-cover", "nrcc-unresolved"])
def test_equations_keep_exact_operands_and_unavailable_states(name):
    r = read(Path(AUDIT) / f"{name}.json")
    fields = {f["name"]: f for f in r["fields"]}
    bindings = {b["observation_index"]: {f["name"]: f for f in b["fields"]} for b in r["bindings"]}
    for e in r["equations"]:
        values = []
        for o in e["operands"]:
            i = o["observation_index"]
            if i is None:
                value = fields[o["field"]]["window_value_minor_units"]
            else:
                f = bindings.get(i, {}).get(o["field"])
                value = f["metadata"]["minor_units"] if f and f["reported_value_bound"] else None
            values.append(None if value is None else int(value) * o["coefficient"])
        if any(v is None for v in values):
            assert e["state"] == "unavailable" and e["residual_minor_units"] is None and e["blockers"]
        else:
            residual = sum(values)
            assert e["residual_minor_units"] == str(residual)
            assert e["state"] == ("balanced" if residual == 0 else "mismatch")
        if e["kind"] == "adjacent_cash_carry_forward":
            right, left = (r["membership"]["observations"][o["observation_index"]] for o in e["operands"])
            assert date.fromisoformat(left["period"]["end"]) + timedelta(days=1) == date.fromisoformat(right["period"]["start"])
    if name == "sid-window":
        carry = [e for e in r["equations"] if e["kind"] == "adjacent_cash_carry_forward"]
        assert len(carry) == 4
        assert [e["residual_minor_units"] for e in carry] == ["0", "0", "0", "-150000000"]
        cash = next(e for e in r["equations"] if e["kind"] == "window_cash")
        assert cash["state"] == "mismatch" and cash["residual_minor_units"] == "-150000000"
    if name == "sid-missing-cover":
        assert sum(e["state"] == "unavailable" for e in r["equations"] if e["kind"] == "adjacent_cash_carry_forward") == 2
