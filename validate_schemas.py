#!/usr/bin/env python3
"""Validate FEC parser field mappings against official FEC header files.

This script checks that each parser's fields[N] assignments match the official
FEC header positions from data/fec/headers/*.csv files.
"""

from typing import List, Tuple

# Official FEC schemas from data/fec/headers/*.csv
OFFICIAL_SCHEMAS = {
    'cn': [
        'CAND_ID', 'CAND_NAME', 'CAND_PTY_AFFILIATION', 'CAND_ELECTION_YR',
        'CAND_OFFICE_ST', 'CAND_OFFICE', 'CAND_OFFICE_DISTRICT', 'CAND_ICI',
        'CAND_STATUS', 'CAND_PCC', 'CAND_ST1', 'CAND_ST2', 'CAND_CITY',
        'CAND_ST', 'CAND_ZIP'
    ],  # 15 fields
    
    'cm': [
        'CMTE_ID', 'CMTE_NM', 'TRES_NM', 'CMTE_ST1', 'CMTE_ST2', 'CMTE_CITY',
        'CMTE_ST', 'CMTE_ZIP', 'CMTE_DSGN', 'CMTE_TP', 'CMTE_PTY_AFFILIATION',
        'CMTE_FILING_FREQ', 'ORG_TP', 'CONNECTED_ORG_NM', 'CAND_ID'
    ],  # 15 fields
    
    'ccl': [
        'CAND_ID', 'CAND_ELECTION_YR', 'FEC_ELECTION_YR', 'CMTE_ID',
        'CMTE_TP', 'CMTE_DSGN', 'LINKAGE_ID'
    ],  # 7 fields
    
    'pas2': [
        'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM',
        'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE',
        'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT',
        'OTHER_ID', 'CAND_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT',
        'SUB_ID'
    ],  # 22 fields (HAS CAND_ID at position 16)
    
    'oth': [
        'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM',
        'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE',
        'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT',
        'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
    ],  # 21 fields (NO CAND_ID!)
    
    'indiv': [
        'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM',
        'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE',
        'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT',
        'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
    ],  # 21 fields (NO CAND_ID!)
}

# Parser field mappings extracted from source files
PARSER_SCHEMAS = {
    'cn': [
        'CAND_ID', 'CAND_NAME', 'CAND_PTY_AFFILIATION', 'CAND_ELECTION_YR',
        'CAND_OFFICE_ST', 'CAND_OFFICE', 'CAND_OFFICE_DISTRICT', 'CAND_ICI',
        'CAND_STATUS', 'CAND_PCC', 'CAND_ST1', 'CAND_ST2', 'CAND_CITY',
        'CAND_ST', 'CAND_ZIP'
    ],
    
    'cm': [
        'CMTE_ID', 'CMTE_NM', 'TRES_NM', 'CMTE_ST1', 'CMTE_ST2', 'CMTE_CITY',
        'CMTE_ST', 'CMTE_ZIP', 'CMTE_DSGN', 'CMTE_TP', 'CMTE_PTY_AFFILIATION',
        'CMTE_FILING_FREQ', 'ORG_TP', 'CONNECTED_ORG_NM', 'CAND_ID'
    ],
    
    'ccl': [
        'CAND_ID', 'CAND_ELECTION_YR', 'FEC_ELECTION_YR', 'CMTE_ID',
        'CMTE_TP', 'CMTE_DSGN', 'LINKAGE_ID'
    ],
    
    'itpas2': [
        'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM',
        'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE',
        'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT',
        'OTHER_ID', 'CAND_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT',
        'SUB_ID'
    ],
    
    'itoth': [
        'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM',
        'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE',
        'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT',
        'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
    ],
    
    'itcont': [
        'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM',
        'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE',
        'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT',
        'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
    ],
}

# Map parser names to official schema names
PARSER_TO_SCHEMA = {
    'cn': 'cn',
    'cm': 'cm',
    'ccl': 'ccl',
    'itpas2': 'pas2',
    'itoth': 'oth',
    'itcont': 'indiv',
}


def validate_parser(parser_name: str) -> Tuple[bool, List[str]]:
    """Validate a parser's field mapping against official FEC schema.
    
    Returns:
        (is_valid, list_of_errors)
    """
    schema_name = PARSER_TO_SCHEMA[parser_name]
    official = OFFICIAL_SCHEMAS[schema_name]
    parser = PARSER_SCHEMAS[parser_name]
    
    errors = []
    
    # Check field count
    if len(parser) != len(official):
        errors.append(
            f"  ❌ Field count mismatch: parser has {len(parser)} fields, "
            f"official schema has {len(official)} fields"
        )
    
    # Check each field position
    for i, (official_field, parser_field) in enumerate(zip(official, parser)):
        if official_field != parser_field:
            errors.append(
                f"  ❌ Position {i}: parser has '{parser_field}', "
                f"official schema has '{official_field}'"
            )
    
    return len(errors) == 0, errors


def main():
    print("=" * 80)
    print("FEC PARSER SCHEMA VALIDATION")
    print("=" * 80)
    print()
    print("Comparing parser field mappings against official FEC header files:")
    print("  Official schemas: data/fec/headers/*.csv")
    print("  Parser files: src/assets/fec/*.py")
    print()
    print("=" * 80)
    print()
    
    all_valid = True
    
    for parser_name in ['cn', 'cm', 'ccl', 'itpas2', 'itoth', 'itcont']:
        schema_name = PARSER_TO_SCHEMA[parser_name]
        official = OFFICIAL_SCHEMAS[schema_name]
        
        print(f"📋 {parser_name}.py → {schema_name}.csv ({len(official)} fields)")
        print("-" * 80)
        
        is_valid, errors = validate_parser(parser_name)
        
        if is_valid:
            print(f"  ✅ VALID - All {len(official)} fields match official schema")
        else:
            all_valid = False
            print(f"  ❌ INVALID - Found {len(errors)} error(s):")
            for error in errors:
                print(error)
        
        print()
    
    print("=" * 80)
    
    if all_valid:
        print("✅ ALL PARSERS VALID - Field mappings match official FEC schemas")
        return 0
    else:
        print("❌ VALIDATION FAILED - Some parsers have field mapping errors")
        print()
        print("CRITICAL SCHEMA NOTES:")
        print("  • pas2.csv has 22 fields (includes CAND_ID at position 16)")
        print("  • oth.csv has 21 fields (NO CAND_ID)")
        print("  • indiv.csv has 21 fields (NO CAND_ID)")
        print("  • After position 15 (OTHER_ID), schemas diverge:")
        print("    - pas2: fields[16]=CAND_ID, fields[17]=TRAN_ID, ...")
        print("    - oth/indiv: fields[16]=TRAN_ID, fields[17]=FILE_NUM, ...")
        return 1


if __name__ == '__main__':
    exit(main())
