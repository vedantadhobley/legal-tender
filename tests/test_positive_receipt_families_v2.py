"""Real source shapes for every newly mapped receipt family; no runtime policy."""
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_positive_receipt_families import read, sha, verify_original_family_case
from tests.test_report_field_binding import minor, workbook_rows

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'docs/audit/fixtures/positive-receipt-families-v2-2026-09-11.json'
AUDIT = os.environ.get('LT_POSITIVE_FAMILY_V2_AUDIT')
STORAGE = os.environ.get('LT_POSITIVE_FAMILY_V2_STORAGE')
REQUIRES = pytest.mark.skipif(not AUDIT or not STORAGE, reason='requires retained v2 positive source witnesses')


def test_positive_v2_shapes_cover_exact_policy_extension():
    fixture = read(FIXTURE)
    prior = read(ROOT / 'contracts/calculations/fec/receipt-family-comparison/v1/policy.json')
    current = read(ROOT / 'contracts/calculations/fec/receipt-family-comparison/v2/policy.json')
    expected = {(form, family) for form, families in current['families'].items() for family in families if family not in prior['families'][form]}
    observed = {(c['form'], family) for c in fixture['cases'] for family in c['targets']}
    assert observed == expected
    for c in fixture['cases']:
        assert set(c['targets']) == set(c['detail_targets'])
        assert all(int(v) > 0 for v in c['targets'].values())
        assert all(int(v) > 0 for v in c['detail_targets'].values())
        assert c['file'] not in json.dumps(current)


@pytest.fixture(scope='module')
def sources():
    fixture = read(FIXTURE)
    raw = (Path(STORAGE) / fixture['profile_storage_key']).read_bytes()
    assert sha(raw) == fixture['profile_sha256']
    profile = json.loads(raw)
    layout = read(ROOT / 'contracts/sources/fec/efile-format/v1/schedule-a-fields.json')
    index = {f['name']: f['sequence']-1 for f in layout['fields']}
    path = Path(STORAGE) / 'dumps/audits/fec/report-field-binding/2026-09-10/attempt-01/FEC_Format_v8.4.xlsx'
    assert sha(path.read_bytes()) == '9d3775d73e9398144b0e0267415ba53e1b5c6a326110b327ce2cd58d233bf3d6'
    sheets = workbook_rows(path, ('Sch A', 'F3', 'F3X'))
    for f in layout['fields']:
        if f['name'] in ('form_type', 'filer_committee_id_number', 'transaction_id', 'contribution_date', 'contribution_amount', 'memo_code'):
            assert sheets['Sch A'][f['sequence']]['B'].strip() == f['publisher_name']
    return fixture, profile, index, sheets


@REQUIRES
@pytest.mark.parametrize('file', [c['file'] for c in read(FIXTURE)['cases']])
def test_positive_v2_original_groups_and_separate_cover_detail(file, sources):
    fixture, profile, index, sheets = sources
    c = next(c for c in fixture['cases'] if c['file'] == file)
    result = verify_original_family_case(c, fixture, profile, index, sheets, AUDIT, STORAGE, os.environ.get('LT_POSITIVE_FAMILY_V2_RECORD_RESULTS') == '1', v2=True)
    assert not result['financial_use_eligible']
    for f in result['families']:
        if f['detail_relation'] == 'thresholded_component_of_total':
            assert f['cover_detail_difference_minor_units'] is None
        else:
            assert f['cover_detail_difference_minor_units'] == '0'


@REQUIRES
@pytest.mark.parametrize('file', [c['file'] for c in read(FIXTURE)['cases']])
def test_real_metadata_binding_preserves_source_relationship(file, sources):
    fixture, _, _, _ = sources
    case = next(c for c in fixture['cases'] if c['file'] == file)
    r = read(Path(AUDIT) / f'{file}-report.json')
    source = read(Path(AUDIT) / f'{file}-source.json')
    assert r['version'] == 'legal-tender.fec.receipt-family-comparison.v2'
    assert r['profile_id'] == source['profile_id']
    assert r['schedule_a_source'] == source['schedule_a_source']
    assert r['reported']['summary_input'] == source['summary_input']
    assert not any(r[k] for k in ('source_body_rescanned', 'family_window_comparison_ready', 'unique_transaction_membership_proven', 'financial_use_eligible', 'terminal_attribution_eligible'))
    review = r['family_reports']
    evidence = review['membership']['evidence']
    capture_path = Path(STORAGE if case.get('metadata_retained') else AUDIT) / case['metadata_capture']
    assert sha(capture_path.read_bytes()) == evidence['capture_sha256']
    capture = read(capture_path)
    assert capture['query']['committee_id'] == case['committee']
    assert len(capture['pages']) == len(evidence['pages'])
    for page, observed in zip(capture['pages'], evidence['pages'], strict=True):
        raw = (capture_path.parent / page['body']['path']).read_bytes()
        assert sha(raw) == page['body']['sha256'] and len(raw) == page['body']['bytes']
        head = (capture_path.parent / page['headers']['path']).read_bytes()
        assert sha(head) == page['headers']['sha256']
        rows = json.loads(raw, parse_float=Decimal)['results']
        assert rows == [row['raw'] for row in observed['records']]
    assert len(r['reports']) == len(review['bindings']) == 1
    report = r['reports'][0]
    binding = review['bindings'][report['binding_index']]
    assert report['groups'] == source['profile_groups']
    assert binding['document']['body'] == source['assessment']['body']
    assert binding['document']['headers'] == source['assessment']['headers']
    raw_metadata = next(row['raw'] for p in evidence['pages'] for row in p['records'] if row['file_number'] == file)
    targets = [f for f in report['families'] if f['field']['id'] in case['targets']]
    assert len(targets) == len(case['targets'])
    blockers = case.get('expected_scope_blockers', [])
    assert binding['scope_blockers'] == blockers
    assert binding['scope_bound'] == (not blockers)
    assert binding['observation_index'] in review['membership']['chain_candidate_indexes']
    if blockers:
        # A reviewed source-shape counterexample, not a runtime exemption.
        # The current binder demands blank amendment numbering on originals.
        assert blockers == ['header_chain_identity_mismatch']
        assert binding['document']['header_fields'][5:7] == ['', '0']
        assert binding['document']['cover']['form'] in ('F3N', 'F3XN')
        assert raw_metadata['amendment_indicator'] == 'N'
        assert raw_metadata['amendment_chain'] == [file] and not raw_metadata['is_amended']
    for f in targets:
        spec, field = f['field'], f['binding']
        assert field['metadata']['raw'] == raw_metadata[spec['metadata_field']]
        assert minor(raw_metadata[spec['metadata_field']]) == case['targets'][spec['id']]
        assert all(v['minor_units'] == case['targets'][spec['id']] for v in field['cover'])
        if blockers:
            assert not field['reported_value_bound'] and field['blockers'] == blockers
            assert field['delta_minor_units'] == '0'  # arithmetic retained, not a binding override
            assert f['state'] == 'blocked' and f['reported_minor_units'] is None and f['detail_minor_units'] is None and f['delta_minor_units'] is None
            assert f['nonmemo_occurrences']['signed_minor_units'] == case['detail_targets'][spec['id']]
            continue
        assert field['reported_value_bound'] and not field['blockers']
        assert case['targets'][spec['id']] == f['reported_minor_units']
        assert case['detail_targets'][spec['id']] == f['detail_minor_units']
        assert not f['blockers']
        if spec['detail_relation'] == 'thresholded_component_of_total':
            assert f['state'] == 'component_not_comparable' and f['delta_minor_units'] is None
        else:
            assert f['state'] == 'equal' and f['delta_minor_units'] == '0'


@REQUIRES
def test_header_counterexamples_remain_pinned_source_evidence(sources):
    _, _, _, sheets = sources
    path = Path(STORAGE) / 'dumps/audits/fec/report-field-binding/2026-09-10/attempt-01/FEC_Format_v8.4.xlsx'
    header = workbook_rows(path, ('HDR',))['HDR']
    assert header[6]['G'] == 'FEC report ID of original report (Amendment only)'
    assert header[7]['C'] == 'N-3' and header[7]['G'] == 'Sequential number of amendments'
    assert header[7]['F'] == '1,2,3,4…'
    assert sheets['F3'][28]['B'] == '(7b) Total Offset to Operating Expenditures'
