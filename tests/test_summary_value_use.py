"""Verify accepted usage boundaries against retained outputs, not new source data."""

import hashlib
import json
import os
from pathlib import Path

import pytest

from tests.test_summary_receipt_review import validator

AUDIT = os.environ.get("LT_SUMMARY_VALUE_USE_AUDIT")
# Pinned previously verified review results; audit IDs never drive runtime policy.
PINS = {
    "C00000935": "607357faac0b22f410bf03ec6159a15b0db460a1fc6cc234c09b0abd39ed3f3e",
    "C00075820": "e49e146c1ad174cf4e4bb699cd48a9066ad8c0a7026ef87bbd886eddf7fe798c",
    "C00249581": "49cf5918023622f4dd638b84bf77556835ce5271179cb5699b04c3c1494968df",
    "C00843367": "600f4824f62477e3b922cfec723129142767f62fe6767f6427b358d2a13a7f4a",
    "C99999999": "a93d30212fb6ed71b7e90342ed4580ea03089910ca60ff512001bf45bc6d806c",
}


@pytest.mark.skipif(not AUDIT, reason="requires retained five-case summary readiness audit")
@pytest.mark.parametrize("committee", PINS)
def test_retained_summary_use_keeps_observations_and_local_blockers(committee):
    raw = (Path(AUDIT) / f"{committee}.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == PINS[committee]
    result = json.loads(raw)
    validator().validate(result)
    assert not result["comparison_ready"]
    assert not result["complete_committee_funding_basis"]
    assert not result["terminal_attribution_eligible"]
    if committee == "C99999999":
        assert result["summary_state"] == "no_indexed_summary_in_snapshot"
        assert result["receipts"]["state"] == "no_rows_in_snapshot"
        assert result["assertions"] == []
        return
    assert result["assertions"] and result["receipts"]["total"]["rows"] > 0
    for variant in result["assertions"]:
        fields = {f["field"]: f for f in variant["fields"]}
        assert len(fields) == 9 and variant["members"]
        for field in fields.values():
            assert "raw" in field and "value" in field and field["delta_minor_units"] is None
        diagnostics = variant["summary_diagnostics"]
        # These reproduce established source observations, not accepted repairs.
        if committee == "C00075820":
            assert diagnostics["cash"]["delta_minor_units"] == "2187612494"
        elif committee == "C00843367":
            assert diagnostics["cash"]["delta_minor_units"] == "150000000"
        elif committee == "C00249581":
            assert diagnostics["individual"]["delta_minor_units"] == "69833400"
            assert fields["INDV_ITEM_CONTB"]["value"]["minor_units"] == "69903300"
        elif committee == "C00000935":
            assert diagnostics["cash"]["state"] == "equal"
            assert diagnostics["individual"]["state"] == "equal"
        for name in ("INDV_ITEM_CONTB", "INDV_UNITEM_CONTB", "INDV_CONTB"):
            assert not any(b.startswith("summary_cash_") for b in fields[name]["field_blockers"])
        for name in ("COH_BOP", "COH_COP", "TTL_RECEIPTS", "TTL_DISB"):
            assert not any(b.startswith("summary_individual_") for b in fields[name]["field_blockers"])
