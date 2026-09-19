"""Independent raw-CSV grouping/diagnostic evidence gate, not runtime policy."""

import copy
import csv
import hashlib
import io
import json
import os
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from orchestration.go_process import _local_registry
from tests.test_committee_summary_publication import identity

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"
SCHEMA = (
    CONTRACTS / "calculations/fec/committee-summary-assertions/v1/result.schema.json"
)
POLICY = json.loads(SCHEMA.with_name("policy.json").read_text())


def validator():
    return Draft202012Validator(
        json.loads(SCHEMA.read_text()),
        registry=_local_registry(CONTRACTS),
        format_checker=FormatChecker(),
    )


def go_json(value):
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
        .encode()
    )


def test_summary_assertion_schema():
    Draft202012Validator.check_schema(json.loads(SCHEMA.read_text()))


def test_go_assertion_fixture():
    path = os.environ.get("LT_SUMMARY_ASSERTION_FIXTURE")
    if not path:
        pytest.skip("requires synthetic Go output")
    result = json.loads(Path(path).read_text())
    validator().validate(result)
    for field in ("financial_use_eligible", "terminal_attribution_eligible"):
        bad = copy.deepcopy(result)
        bad[field] = True
        assert list(validator().iter_errors(bad))
    bad = copy.deepcopy(result)
    bad["committees"][0]["state"] = "conflicting_assertions"
    assert list(validator().iter_errors(bad))


def test_complete_published_assertions_against_raw_csv():
    output, root, audit = (
        os.environ.get(name)
        for name in (
            "LT_SUMMARY_ASSERTION_OUTPUT",
            "LT_SUMMARY_STORAGE_ROOT",
            "LT_SUMMARY_RELEASE_AUDIT",
        )
    )
    if not all((output, root, audit)):
        pytest.skip("requires read-only source release and real Go calculation output")
    release = json.loads((Path(audit) / "release.json").read_text())
    all_profiles = {}
    for cycle in release["periods"]:
        manifest_bytes = (
            Path(audit) / "summaries" / cycle / "manifest.json"
        ).read_bytes()
        manifest = json.loads(manifest_bytes)
        source = (Path(root) / manifest["source_artifact"]["storage_key"]).read_bytes()
        assert (
            hashlib.sha256(source).hexdigest() == manifest["source_artifact"]["sha256"]
        )
        result = json.loads((Path(output) / f"{cycle}.json").read_text())
        validator().validate(result)
        assert result["input"]["fact_set_id"] == manifest["fact_set_id"]
        assert (
            result["input"]["manifest_sha256"]
            == hashlib.sha256(manifest_bytes).hexdigest()
        )
        assert result["input"]["source_release_id"] == release["release_id"]
        assert (
            result["input"]["source_release_manifest_sha256"]
            == manifest["source_release_manifest_sha256"]
        )
        assert (
            result["input"]["source_artifact_sha256"]
            == hashlib.sha256(source).hexdigest()
        )
        unsigned = copy.deepcopy(result)
        unsigned["calculation_id"] = ""
        assert hashlib.sha256(go_json(unsigned)).hexdigest() == result["calculation_id"]

        reader = csv.DictReader(io.StringIO(source.decode(), newline=""), strict=True)
        rows = list(reader)
        fields = [name for name in reader.fieldnames if name != "CAND_ID"]
        grouped = defaultdict(lambda: defaultdict(list))
        for ordinal, row in enumerate(rows, 1):
            grouped[row["CMTE_ID"]][tuple(row[name] for name in fields)].append(ordinal)
        # These released snapshots have valid committee/cycle IDs. Invalid candidate
        # references are tested below and do not erase financial evidence.
        assert result["unindexed"] == []
        assert len(result["committees"]) == len(grouped)
        seen = set()
        profile = {
            "counts": result["counts"],
            "equations": {},
            "by_type": {},
            "cash_alternative": Counter(),
            "largest_differences": {},
        }
        eq_counts, by_type, differences = (
            defaultdict(Counter),
            defaultdict(Counter),
            defaultdict(list),
        )
        for committee in result["committees"]:
            variants = grouped[committee["committee_id"]]
            assert len(committee["assertions"]) == len(variants)
            assert committee["state"] == (
                "single_assertion" if len(variants) == 1 else "conflicting_assertions"
            )
            observed_variants = set()
            for assertion in committee["assertions"]:
                ordinals = [m["ordinal"] for m in assertion["members"]]
                assert ordinals == sorted(ordinals)
                raw = rows[ordinals[0] - 1]
                key = tuple(raw[name] for name in fields)
                assert key not in observed_variants
                observed_variants.add(key)
                assert variants[key] == ordinals
                assert (
                    assertion["representative_fact_id"]
                    == assertion["members"][0]["fact_id"]
                )
                assert (
                    assertion["non_candidate_fields_sha256"]
                    == hashlib.sha256(go_json(key)).hexdigest()
                )
                identity_parts = [
                    result["policy"],
                    manifest["fact_set_id"],
                    go_json(key).decode(),
                ]
                assert (
                    assertion["assertion_id"]
                    == hashlib.sha256(go_json(identity_parts)).hexdigest()
                )
                for member in assertion["members"]:
                    assert member["ordinal"] not in seen
                    seen.add(member["ordinal"])
                    row = rows[member["ordinal"] - 1]
                    assert member["candidate_raw"] == row["CAND_ID"]
                    exact = source[
                        member["offset"] : member["offset"] + member["length"]
                    ]
                    assert hashlib.sha256(exact).hexdigest() == member["raw_sha256"]
                    parsed = csv.reader(io.StringIO(exact.decode(), newline=""))
                    assert next(parsed) == [row[n] for n in reader.fieldnames]
                    assert next(parsed, None) is None
                    occurrence = identity(
                        "fec.committee-summary.occurrence.v1",
                        manifest["source_artifact"]["sha256"],
                        "whole_csv",
                        cycle,
                        str(member["ordinal"]),
                    )
                    version = identity(
                        "fec.committee-summary.record-version.v1",
                        cycle,
                        "unkeyed:" + occurrence,
                        member["raw_sha256"],
                    )
                    assert member["occurrence_id"] == occurrence
                    assert member["fact_id"] == identity(
                        manifest["fact_type"], version, manifest["parser_version"]
                    )
                for name, eq in assertion["diagnostic_equations"].items():
                    assert [
                        [op["field"], op["coefficient"]] for op in eq["operands"]
                    ] == POLICY["equations"][name]
                    # The reviewed real inputs have no invalid money. Never fill blanks.
                    assert all(op["raw"] == raw[op["field"]] for op in eq["operands"])
                    if any(raw[op["field"]] == "" for op in eq["operands"]):
                        state, delta = "missing", None
                    else:
                        delta = sum(
                            int(Decimal(raw[op["field"]]) * 100) * op["coefficient"]
                            for op in eq["operands"]
                        )
                        state = "equal" if delta == 0 else "different"
                    assert eq["state"] == state
                    assert eq["delta_minor_units"] == (
                        str(delta) if delta is not None else None
                    )
                    eq_counts[name][state] += 1
                    by_type[f"{raw['CMTE_TP']}:{raw['CMTE_DSGN']}:{name}"][state] += 1
                    if state == "different":
                        differences[name].append(
                            {
                                "committee": committee["committee_id"],
                                "name": raw["CMTE_NM"],
                                "type": raw["CMTE_TP"],
                                "designation": raw["CMTE_DSGN"],
                                "delta": str(delta),
                                "ordinal": ordinals[0],
                                "coverage_start": raw["CVG_START_DT"],
                                "coverage_end": raw["CVG_END_DT"],
                                "operands": {
                                    op["field"]: op["raw"] for op in eq["operands"]
                                },
                            }
                        )
                cash, federal = (
                    assertion["diagnostic_equations"][n]["state"]
                    for n in ("cash", "cash_federal_columns")
                )
                profile["cash_alternative"][f"{cash}:{federal}"] += 1
        counts = result["counts"]
        assert seen == set(range(1, len(rows) + 1))
        assert counts["source_rows"] == counts["indexed_rows"] == len(rows)
        assert counts["assertions"] == sum(len(v) for v in grouped.values())
        assert counts["repeated_evidence_rows"] == len(rows) - counts["assertions"]
        assert counts["conflicting_committees"] == sum(
            len(v) > 1 for v in grouped.values()
        )
        profile["equations"] = dict(eq_counts)
        profile["by_type"] = dict(by_type)
        profile["largest_differences"] = {
            k: sorted(v, key=lambda r: (-abs(int(r["delta"])), r["committee"]))[:10]
            for k, v in differences.items()
        }
        all_profiles[cycle] = profile
    (Path(output) / "independent-profile.json").write_text(
        json.dumps(all_profiles, indent=2) + "\n"
    )
