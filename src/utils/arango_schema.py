"""ArangoDB Schema Management - Add JSON Schema validation to collections.

ArangoDB is schema-less by default, but supports JSON Schema validation.
This module creates JSON schemas from FEC header files and applies them to collections.

Benefits:
1. Schema is visible in ArangoDB Web UI (Schema tab)
2. Documents are validated on insert (catches malformed data)
3. Self-documenting database structure
"""

from typing import Dict, Any, List, Optional
from arango.database import StandardDatabase

from src.utils.fec_schema import FECSchema, HEADER_FILE_MAP


# Field type hints based on FEC data types
# Most fields are strings, but some are numeric
NUMERIC_FIELDS = {
    'TRANSACTION_AMT',  # Dollar amounts
    'CAND_ELECTION_YR',  # Years
    'FEC_ELECTION_YR',
    'FILE_NUM',
    'SUB_ID',
    'PREV_FILE_NUM',
    'MEMOS_FILE_NUM',
}

DATE_FIELDS = {
    'TRANSACTION_DT',
    'CAND_ST1',  # Sometimes used for dates
}


def fec_fields_to_json_schema(fields: List[str], collection_name: str) -> Dict[str, Any]:
    """Convert FEC field list to JSON Schema.
    
    Args:
        fields: List of field names from FEC header
        collection_name: Name of the collection (for title)
    
    Returns:
        JSON Schema dict compatible with ArangoDB
    """
    properties = {}
    
    for field in fields:
        if field in NUMERIC_FIELDS:
            # Numeric fields can be number or string (FEC data is inconsistent)
            properties[field] = {
                "type": ["number", "string", "null"],
                "description": f"FEC field: {field}"
            }
        elif field in DATE_FIELDS:
            properties[field] = {
                "type": ["string", "null"],
                "description": f"FEC date field: {field} (format: MMDDYYYY)"
            }
        else:
            properties[field] = {
                "type": ["string", "null"],
                "description": f"FEC field: {field}"
            }
    
    # Add ArangoDB system fields
    properties["_key"] = {"type": "string", "description": "ArangoDB document key"}
    properties["_id"] = {"type": "string", "description": "ArangoDB document ID"}
    properties["_rev"] = {"type": "string", "description": "ArangoDB revision"}
    properties["updated_at"] = {"type": ["string", "null"], "description": "Last update timestamp"}
    
    return {
        "type": "object",
        "title": f"FEC {collection_name.upper()} Collection",
        "description": f"Official FEC bulk data schema for {collection_name}",
        "properties": properties,
        "additionalProperties": True,  # Allow extra fields
    }


def apply_schema_to_collection(
    db: StandardDatabase,
    collection_name: str,
    schema: Dict[str, Any],
    level: str = "none"
) -> bool:
    """Apply JSON Schema to an ArangoDB collection.
    
    Args:
        db: ArangoDB database object
        collection_name: Name of the collection
        schema: JSON Schema dict
        level: Validation level - "none", "new", "moderate", "strict"
               - none: Schema stored but not enforced (for display only)
               - new: Validate new documents only
               - moderate: Validate new and modified documents
               - strict: Validate all operations
    
    Returns:
        True if successful
    """
    if not db.has_collection(collection_name):
        print(f"Collection {collection_name} does not exist")
        return False
    
    collection = db.collection(collection_name)
    
    # Update collection properties with schema
    collection.configure(schema={
        "rule": schema,
        "level": level,
        "message": f"Document does not match {collection_name} schema"
    })
    
    print(f"✅ Applied schema to {collection_name} (level={level})")
    return True


def apply_fec_schemas(
    db: StandardDatabase,
    collections: Optional[List[str]] = None,
    level: str = "none"
) -> Dict[str, bool]:
    """Apply FEC schemas to all (or specified) collections.
    
    Args:
        db: ArangoDB database object  
        collections: List of collection names, or None for all FEC collections
        level: Validation level
    
    Returns:
        Dict mapping collection name to success status
    """
    schema_loader = FECSchema()
    results = {}
    
    if collections is None:
        collections = list(HEADER_FILE_MAP.keys())
    
    for coll_name in collections:
        if coll_name not in HEADER_FILE_MAP:
            print(f"⚠️ {coll_name} is not a known FEC collection type")
            results[coll_name] = False
            continue
        
        if not db.has_collection(coll_name):
            print(f"⚠️ Collection {coll_name} does not exist in database")
            results[coll_name] = False
            continue
        
        try:
            fields = schema_loader.get_fields(coll_name)
            json_schema = fec_fields_to_json_schema(fields, coll_name)
            results[coll_name] = apply_schema_to_collection(db, coll_name, json_schema, level)
        except Exception as e:
            print(f"❌ Error applying schema to {coll_name}: {e}")
            results[coll_name] = False
    
    return results


def get_collection_schema(db: StandardDatabase, collection_name: str) -> Optional[Dict[str, Any]]:
    """Get the JSON Schema from a collection.
    
    Args:
        db: ArangoDB database object
        collection_name: Name of the collection
    
    Returns:
        JSON Schema dict or None if not set
    """
    if not db.has_collection(collection_name):
        return None
    
    collection = db.collection(collection_name)
    props = collection.properties()
    
    return props.get('schema')


def print_schema_summary(db: StandardDatabase, collection_name: str):
    """Print a human-readable schema summary for a collection."""
    schema = get_collection_schema(db, collection_name)
    
    if not schema:
        print(f"{collection_name}: No schema defined")
        return
    
    rule = schema.get('rule', {})
    properties = rule.get('properties', {})
    
    print(f"\n📋 {collection_name} Schema ({len(properties)} fields)")
    print(f"   Level: {schema.get('level', 'unknown')}")
    print(f"   Fields:")
    
    for field, spec in properties.items():
        if field.startswith('_'):
            continue  # Skip system fields
        field_type = spec.get('type', 'unknown')
        if isinstance(field_type, list):
            field_type = '/'.join(field_type)
        print(f"     - {field}: {field_type}")
