"""
MongoDB Collection Caching Utilities

Manages BSON dumps for fast data loading with proper separation:
- FEC raw data: ~/workspace/.legal-tender/bson/fec/{cycle}/
- Enriched data: ~/workspace/.legal-tender/bson/enriched/{cycle}/
- Aggregation data: ~/workspace/.legal-tender/bson/aggregation/

Workflow:
1. Parse FEC file → store in MongoDB (35 min)
2. Create indexes
3. Create BSON dump (indexes included)
4. Next time: check if source file changed
   - Unchanged → restore from dump (~30 sec)
   - Changed → re-parse (35 min)
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Literal
import json
import subprocess

from src.utils.storage import get_fec_dump_dir, get_enriched_dump_dir, get_aggregation_dump_dir


DumpType = Literal["fec", "enriched", "aggregation"]


class MongoDumpManager:
    """Manages MongoDB dumps for fast data loading."""
    
    def __init__(self, dump_type: DumpType = "fec"):
        """
        Initialize dump manager.
        
        Args:
            dump_type: Type of dumps to manage ("fec", "enriched", "aggregation")
        """
        self.dump_type = dump_type
    
    def _get_base_dir(self, cycle: Optional[str] = None) -> Path:
        """Get base directory for dumps based on type and cycle."""
        if self.dump_type == "fec":
            if not cycle:
                raise ValueError("Cycle required for FEC dumps")
            return get_fec_dump_dir(cycle)
        elif self.dump_type == "enriched":
            if not cycle:
                raise ValueError("Cycle required for enriched dumps")
            return get_enriched_dump_dir(cycle)
        elif self.dump_type == "aggregation":
            return get_aggregation_dump_dir()
        else:
            raise ValueError(f"Unknown dump type: {self.dump_type}")
    
    def get_dump_path(self, collection: str, cycle: Optional[str] = None) -> Path:
        """
        Get path to dump directory for a collection.
        
        Args:
            collection: Collection name (e.g., "cn", "pas2")
            cycle: FEC cycle (required for fec/enriched, ignored for aggregation)
            
        Returns:
            Path to dump directory
        """
        return self._get_base_dir(cycle)
    
    def get_bson_path(self, collection: str, cycle: Optional[str] = None) -> Path:
        """Get path to BSON file for a collection."""
        return self._get_base_dir(cycle) / f"{collection}.bson"
    
    def get_metadata_json_path(self, collection: str, cycle: Optional[str] = None) -> Path:
        """Get path to MongoDB metadata JSON file for a collection."""
        return self._get_base_dir(cycle) / f"{collection}.metadata.json"
    
    def get_cycle_metadata_path(self, cycle: Optional[str] = None) -> Path:
        """Get path to cycle-level metadata.json (tracks all collections)."""
        return self._get_base_dir(cycle) / "metadata.json"
    
    def dump_exists(self, collection: str, cycle: Optional[str] = None) -> bool:
        """Check if a dump exists for a collection."""
        bson_file = self.get_bson_path(collection, cycle)
        metadata_file = self.get_metadata_json_path(collection, cycle)
        return bson_file.exists() and metadata_file.exists()
    
    def should_restore(
        self, 
        collection: str, 
        source_file: Path,
        cycle: Optional[str] = None
    ) -> bool:
        """
        Determine if we should restore from dump or re-parse.
        
        Args:
            collection: Collection name
            source_file: Path to source file (ZIP or upstream data)
            cycle: FEC cycle (required for fec/enriched)
            
        Returns:
            True if we should restore, False if we should re-parse
        """
        # No dump? Must parse
        if not self.dump_exists(collection, cycle):
            return False
        
        # Load cycle metadata
        metadata = self.load_metadata(cycle)
        if not metadata:
            return False
        
        # Check if source file has changed since dump
        collection_meta = metadata.get('collections', {}).get(collection)
        if not collection_meta:
            return False
        
        source_modified = datetime.fromtimestamp(source_file.stat().st_mtime)
        dump_created = datetime.fromisoformat(collection_meta['dump_created'])
        
        # If source is newer than dump, need to re-parse
        if source_modified > dump_created:
            return False
        
        return True
    
    def _get_database_name(self, cycle: Optional[str] = None) -> str:
        """Get MongoDB database name for dump type and cycle."""
        if self.dump_type == "fec":
            return f"fec_{cycle}"
        elif self.dump_type == "enriched":
            return f"enriched_{cycle}"
        elif self.dump_type == "aggregation":
            return "aggregation"
        else:
            raise ValueError(f"Unknown dump type: {self.dump_type}")
    
    def create_dump(
        self,
        collection: str,
        mongo_uri: str,
        source_file: Path,
        record_count: int,
        cycle: Optional[str] = None,
        context = None
    ) -> bool:
        """
        Create BSON dump of a MongoDB collection.
        
        Args:
            collection: Collection name
            mongo_uri: MongoDB connection URI
            source_file: Path to source file (for metadata)
            record_count: Number of records in collection
            cycle: FEC cycle (required for fec/enriched)
            context: Optional Dagster context for logging
            
        Returns:
            True if successful
        """
        def log(msg):
            if context:
                context.log.info(msg)
            else:
                print(msg)
        
        base_dir = self._get_base_dir(cycle)
        database = self._get_database_name(cycle)
        
        # Ensure directory exists
        base_dir.mkdir(parents=True, exist_ok=True)
        
        log(f"📦 Creating dump: {database}.{collection} → {base_dir}")
        
        try:
            # Run mongodump
            # Ensure URI has proper format for authSource parameter
            if '?' in mongo_uri:
                uri_with_auth = mongo_uri
            elif mongo_uri.endswith('/'):
                uri_with_auth = f'{mongo_uri}?authSource=admin'
            else:
                uri_with_auth = f'{mongo_uri}/?authSource=admin'
            
            # mongodump creates {out_dir}/{database}/{collection}.bson
            # We want files directly in base_dir, so use a temp approach
            temp_out = base_dir.parent / f".tmp_{database}"
            
            cmd = [
                'mongodump',
                f'--uri={uri_with_auth}',
                f'--db={database}',
                f'--collection={collection}',
                f'--out={temp_out}'
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Move files from temp/{database}/ to base_dir/
            temp_db_dir = temp_out / database
            if temp_db_dir.exists():
                for f in temp_db_dir.iterdir():
                    dest = base_dir / f.name
                    if dest.exists():
                        dest.unlink()
                    f.rename(dest)
                # Cleanup temp dir
                temp_db_dir.rmdir()
                if temp_out.exists():
                    try:
                        temp_out.rmdir()
                    except OSError:
                        pass  # May have other cycle dirs
            
            # Update metadata
            self._update_metadata(
                collection=collection,
                source_file=source_file,
                record_count=record_count,
                database=database,
                cycle=cycle
            )
            
            bson_path = self.get_bson_path(collection, cycle)
            size_mb = bson_path.stat().st_size / 1024 / 1024 if bson_path.exists() else 0
            
            log(f"✅ Dump created: {record_count:,} records ({size_mb:.1f} MB)")
            return True
            
        except subprocess.CalledProcessError as e:
            log(f"❌ mongodump failed: {e.stderr}")
            return False
        except Exception as e:
            log(f"❌ Dump creation failed: {e}")
            return False
    
    def restore_dump(
        self,
        collection: str,
        mongo_uri: str,
        cycle: Optional[str] = None,
        context = None
    ) -> bool:
        """
        Restore BSON dump to MongoDB.
        
        Args:
            collection: Collection name
            mongo_uri: MongoDB connection URI
            cycle: FEC cycle (required for fec/enriched)
            context: Optional Dagster context for logging
            
        Returns:
            True if successful
        """
        def log(msg):
            if context:
                context.log.info(msg)
            else:
                print(msg)
        
        if not self.dump_exists(collection, cycle):
            log(f"❌ No dump found for {collection}")
            return False
        
        database = self._get_database_name(cycle)
        bson_path = self.get_bson_path(collection, cycle)
        
        log(f"🔄 Restoring from dump: {database}.{collection}")
        
        try:
            # Ensure URI has proper format for authSource parameter
            if '?' in mongo_uri:
                uri_with_auth = mongo_uri
            elif mongo_uri.endswith('/'):
                uri_with_auth = f'{mongo_uri}?authSource=admin'
            else:
                uri_with_auth = f'{mongo_uri}/?authSource=admin'
            
            cmd = [
                'mongorestore',
                f'--uri={uri_with_auth}',
                f'--db={database}',
                f'--collection={collection}',
                str(bson_path),
                '--drop'  # Drop collection before restoring
            ]
            
            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Get record count from metadata
            metadata = self.load_metadata(cycle)
            record_count = metadata['collections'][collection]['record_count'] if metadata else "?"
            
            log(f"✅ Restored {record_count:,} records (indexes included)")
            return True
            
        except subprocess.CalledProcessError as e:
            log(f"❌ mongorestore failed: {e.stderr}")
            return False
        except Exception as e:
            log(f"❌ Restore failed: {e}")
            return False
    
    def _update_metadata(
        self,
        collection: str,
        source_file: Path,
        record_count: int,
        database: str,
        cycle: Optional[str] = None
    ):
        """Update metadata.json with dump information."""
        metadata_path = self.get_cycle_metadata_path(cycle)
        
        # Load existing metadata or create new
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
        else:
            metadata = {
                'dump_type': self.dump_type,
                'cycle': cycle,
                'database': database,
                'collections': {}
            }
        
        # Update collection metadata
        metadata['collections'][collection] = {
            'dump_created': datetime.now().isoformat(),
            'source_file': str(source_file),
            'source_modified': datetime.fromtimestamp(source_file.stat().st_mtime).isoformat(),
            'record_count': record_count
        }
        
        # Save metadata
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def load_metadata(self, cycle: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Load metadata for a cycle/type."""
        metadata_path = self.get_cycle_metadata_path(cycle)
        
        if not metadata_path.exists():
            return None
        
        try:
            with open(metadata_path, 'r') as f:
                return json.load(f)
        except Exception:
            return None
    
    def get_dump_stats(self, cycle: Optional[str] = None) -> Dict[str, Any]:
        """Get statistics about dumps."""
        metadata = self.load_metadata(cycle)
        
        if not metadata:
            return {
                'dump_type': self.dump_type,
                'cycle': cycle,
                'dumps_exist': False,
                'collections': []
            }
        
        collections = []
        for coll_name, coll_meta in metadata.get('collections', {}).items():
            bson_file = self.get_bson_path(coll_name, cycle)
            
            collections.append({
                'name': coll_name,
                'record_count': coll_meta['record_count'],
                'dump_created': coll_meta['dump_created'],
                'source_file': coll_meta['source_file'],
                'dump_size_mb': bson_file.stat().st_size / 1024 / 1024 if bson_file.exists() else 0
            })
        
        return {
            'dump_type': self.dump_type,
            'cycle': cycle,
            'dumps_exist': True,
            'database': metadata['database'],
            'collections': collections
        }
    
    def delete_dump(self, collection: str, cycle: Optional[str] = None) -> bool:
        """Delete dump for a collection."""
        bson_file = self.get_bson_path(collection, cycle)
        metadata_file = self.get_metadata_json_path(collection, cycle)
        
        try:
            if bson_file.exists():
                bson_file.unlink()
            if metadata_file.exists():
                metadata_file.unlink()
            
            # Update cycle metadata
            metadata = self.load_metadata(cycle)
            if metadata and collection in metadata.get('collections', {}):
                del metadata['collections'][collection]
                
                with open(self.get_cycle_metadata_path(cycle), 'w') as f:
                    json.dump(metadata, f, indent=2)
            
            return True
        except Exception as e:
            print(f"Failed to delete dump: {e}")
            return False


# =============================================================================
# Convenience functions for FEC assets (backward compatible)
# =============================================================================

def should_restore_from_dump(
    cycle: str,
    collection: str,
    source_file: Path,
    dump_type: DumpType = "fec"
) -> bool:
    """
    Check if we should restore from dump or re-parse.
    
    Args:
        cycle: FEC cycle
        collection: Collection name
        source_file: Path to source file
        dump_type: Type of dump ("fec", "enriched", "aggregation")
        
    Returns:
        True if should restore, False if should parse
    """
    manager = MongoDumpManager(dump_type)
    return manager.should_restore(collection, source_file, cycle)


def create_collection_dump(
    cycle: str,
    collection: str,
    mongo_uri: str,
    source_file: Path,
    record_count: int,
    dump_type: DumpType = "fec",
    context = None
) -> bool:
    """
    Create dump of a collection.
    
    Args:
        cycle: FEC cycle
        collection: Collection name
        mongo_uri: MongoDB connection URI
        source_file: Path to source file
        record_count: Number of records
        dump_type: Type of dump ("fec", "enriched", "aggregation")
        context: Optional Dagster context
        
    Returns:
        True if successful
    """
    manager = MongoDumpManager(dump_type)
    return manager.create_dump(collection, mongo_uri, source_file, record_count, cycle, context)


def restore_collection_from_dump(
    cycle: str,
    collection: str,
    mongo_uri: str,
    dump_type: DumpType = "fec",
    context = None
) -> bool:
    """
    Restore collection from dump.
    
    Args:
        cycle: FEC cycle
        collection: Collection name
        mongo_uri: MongoDB connection URI
        dump_type: Type of dump ("fec", "enriched", "aggregation")
        context: Optional Dagster context
        
    Returns:
        True if successful
    """
    manager = MongoDumpManager(dump_type)
    return manager.restore_dump(collection, mongo_uri, cycle, context)
