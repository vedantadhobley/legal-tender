"""
MongoDB Collection Caching Utilities

Simple check: if collection exists and source file unchanged, skip parsing.
No external tools needed - just MongoDB metadata tracking.

Workflow:
1. Parse FEC file → store in MongoDB (35 min)
2. Store metadata (source file timestamp, record count)
3. Next time: check if source file changed
   - Unchanged → skip parsing (instant)
   - Changed → re-parse (35 min)
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import json
import subprocess


class MongoDumpManager:
    """Manages MongoDB dumps for fast data loading."""
    
    def __init__(self, dumps_dir: Path | None = None):
        """
        Initialize dump manager.
        
        Args:
            dumps_dir: Directory for storing dumps. Defaults to data/mongo/
        """
        if dumps_dir is None:
            repo_root = Path(__file__).parent.parent.parent
            dumps_dir = repo_root / 'data' / 'mongo'
        
        self.dumps_dir = dumps_dir
        self.dumps_dir.mkdir(parents=True, exist_ok=True)
    
    def get_dump_path(self, cycle: str, collection: str) -> Path:
        """
        Get path to dump directory for a collection.
        
        Args:
            cycle: FEC cycle (e.g., "2024")
            collection: Collection name (e.g., "pas2")
            
        Returns:
            Path to dump directory (e.g., data/mongo/fec_2024/pas2)
        """
        return self.dumps_dir / f"fec_{cycle}" / collection
    
    def get_metadata_path(self, cycle: str) -> Path:
        """
        Get path to metadata file for a cycle.
        
        Args:
            cycle: FEC cycle (e.g., "2024")
            
        Returns:
            Path to metadata.json (e.g., data/mongo/fec_2024/metadata.json)
        """
        return self.dumps_dir / f"fec_{cycle}" / "metadata.json"
    
    def dump_exists(self, cycle: str, collection: str) -> bool:
        """
        Check if a dump exists for a collection.
        
        Args:
            cycle: FEC cycle
            collection: Collection name
            
        Returns:
            True if dump exists
        """
        dump_path = self.get_dump_path(cycle, collection)
        bson_file = dump_path.parent / f"{collection}.bson"
        metadata_file = dump_path.parent / f"{collection}.metadata.json"
        
        return bson_file.exists() and metadata_file.exists()
    
    def should_restore(
        self, 
        cycle: str, 
        collection: str, 
        source_file: Path
    ) -> bool:
        """
        Determine if we should restore from dump or re-parse.
        
        Args:
            cycle: FEC cycle
            collection: Collection name
            source_file: Path to source ZIP file
            
        Returns:
            True if we should restore, False if we should re-parse
        """
        # No dump? Must parse
        if not self.dump_exists(cycle, collection):
            return False
        
        # Load dump metadata
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
    
    def create_dump(
        self,
        cycle: str,
        collection: str,
        mongo_uri: str,
        source_file: Path,
        record_count: int,
        context = None
    ) -> bool:
        """
        Create BSON dump of a MongoDB collection.
        
        Args:
            cycle: FEC cycle
            collection: Collection name
            mongo_uri: MongoDB connection URI
            source_file: Path to source ZIP file (for metadata)
            record_count: Number of records in collection
            context: Optional Dagster context for logging
            
        Returns:
            True if successful
        """
        def log(msg):
            if context:
                context.log.info(msg)
            else:
                print(msg)
        
        dump_path = self.get_dump_path(cycle, collection)
        database = f"fec_{cycle}"
        
        # Create dump directory
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        
        log(f"📦 Creating dump: {database}.{collection}")
        
        try:
            # Run mongodump - outputs to data/mongo/fec_{cycle}/
            # Add authSource=admin to URI for authentication
            uri_with_auth = mongo_uri if '?' in mongo_uri else f'{mongo_uri}?authSource=admin'
            
            cmd = [
                'mongodump',
                f'--uri={uri_with_auth}',
                f'--db={database}',
                f'--collection={collection}',
                f'--out={self.dumps_dir}'
            ]
            
            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Update metadata
            self._update_metadata(
                cycle=cycle,
                collection=collection,
                source_file=source_file,
                record_count=record_count,
                database=database
            )
            
            log(f"✅ Dump created: {record_count:,} records")
            return True
            
        except subprocess.CalledProcessError as e:
            log(f"❌ mongodump failed: {e.stderr}")
            return False
        except Exception as e:
            log(f"❌ Dump creation failed: {e}")
            return False
    
    def restore_dump(
        self,
        cycle: str,
        collection: str,
        mongo_uri: str,
        context = None
    ) -> bool:
        """
        Restore BSON dump to MongoDB.
        
        Args:
            cycle: FEC cycle
            collection: Collection name
            mongo_uri: MongoDB connection URI
            context: Optional Dagster context for logging
            
        Returns:
            True if successful
        """
        def log(msg):
            if context:
                context.log.info(msg)
            else:
                print(msg)
        
        if not self.dump_exists(cycle, collection):
            log(f"❌ No dump found for {cycle}.{collection}")
            return False
        
        database = f"fec_{cycle}"
        dump_path = self.dumps_dir / database
        
        log(f"🔄 Restoring from dump: {database}.{collection}")
        
        try:
            # Run mongorestore
            # Add authSource=admin to URI for authentication
            uri_with_auth = mongo_uri if '?' in mongo_uri else f'{mongo_uri}?authSource=admin'
            
            cmd = [
                'mongorestore',
                f'--uri={uri_with_auth}',
                f'--db={database}',
                f'--collection={collection}',
                f'{dump_path}/{collection}.bson',
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
            if not metadata:
                log(f"⚠️ No metadata found for {cycle}")
                return False
            
            record_count = metadata['collections'][collection]['record_count']
            
            log(f"✅ Restored {record_count:,} records in ~30 seconds")
            return True
            
        except subprocess.CalledProcessError as e:
            log(f"❌ mongorestore failed: {e.stderr}")
            return False
        except Exception as e:
            log(f"❌ Restore failed: {e}")
            return False
    
    def _update_metadata(
        self,
        cycle: str,
        collection: str,
        source_file: Path,
        record_count: int,
        database: str
    ):
        """Update metadata.json with dump information."""
        metadata_path = self.get_metadata_path(cycle)
        
        # Load existing metadata or create new
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
        else:
            metadata = {
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
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def load_metadata(self, cycle: str) -> Optional[Dict[str, Any]]:
        """
        Load metadata for a cycle.
        
        Args:
            cycle: FEC cycle
            
        Returns:
            Metadata dict or None if not found
        """
        metadata_path = self.get_metadata_path(cycle)
        
        if not metadata_path.exists():
            return None
        
        try:
            with open(metadata_path, 'r') as f:
                return json.load(f)
        except Exception:
            return None
    
    def get_dump_stats(self, cycle: str) -> Dict[str, Any]:
        """
        Get statistics about dumps for a cycle.
        
        Args:
            cycle: FEC cycle
            
        Returns:
            Dict with dump statistics
        """
        metadata = self.load_metadata(cycle)
        
        if not metadata:
            return {
                'cycle': cycle,
                'dumps_exist': False,
                'collections': []
            }
        
        collections = []
        for coll_name, coll_meta in metadata.get('collections', {}).items():
            dump_path = self.get_dump_path(cycle, coll_name)
            bson_file = dump_path.parent / f"{coll_name}.bson"
            
            collections.append({
                'name': coll_name,
                'record_count': coll_meta['record_count'],
                'dump_created': coll_meta['dump_created'],
                'source_file': coll_meta['source_file'],
                'dump_size_mb': bson_file.stat().st_size / 1024 / 1024 if bson_file.exists() else 0
            })
        
        return {
            'cycle': cycle,
            'dumps_exist': True,
            'database': metadata['database'],
            'collections': collections
        }
    
    def delete_dump(self, cycle: str, collection: str) -> bool:
        """
        Delete dump for a collection.
        
        Args:
            cycle: FEC cycle
            collection: Collection name
            
        Returns:
            True if successful
        """
        dump_path = self.get_dump_path(cycle, collection)
        bson_file = dump_path.parent / f"{collection}.bson"
        metadata_file = dump_path.parent / f"{collection}.metadata.json"
        
        try:
            if bson_file.exists():
                bson_file.unlink()
            if metadata_file.exists():
                metadata_file.unlink()
            
            # Update cycle metadata
            metadata = self.load_metadata(cycle)
            if metadata and collection in metadata.get('collections', {}):
                del metadata['collections'][collection]
                
                with open(self.get_metadata_path(cycle), 'w') as f:
                    json.dump(metadata, f, indent=2)
            
            return True
        except Exception as e:
            print(f"Failed to delete dump: {e}")
            return False


# Convenience functions for use in assets
def should_restore_from_dump(
    cycle: str,
    collection: str,
    source_file: Path,
    dumps_dir: Path | None = None
) -> bool:
    """
    Check if we should restore from dump or re-parse.
    
    Args:
        cycle: FEC cycle
        collection: Collection name
        source_file: Path to source ZIP file
        dumps_dir: Optional dumps directory
        
    Returns:
        True if should restore, False if should parse
    """
    manager = MongoDumpManager(dumps_dir)
    return manager.should_restore(cycle, collection, source_file)


def create_collection_dump(
    cycle: str,
    collection: str,
    mongo_uri: str,
    source_file: Path,
    record_count: int,
    dumps_dir: Path | None = None,
    context = None
) -> bool:
    """
    Create dump of a collection.
    
    Args:
        cycle: FEC cycle
        collection: Collection name
        mongo_uri: MongoDB connection URI
        source_file: Path to source ZIP file
        record_count: Number of records
        dumps_dir: Optional dumps directory
        context: Optional Dagster context
        
    Returns:
        True if successful
    """
    manager = MongoDumpManager(dumps_dir)
    return manager.create_dump(cycle, collection, mongo_uri, source_file, record_count, context)


def restore_collection_from_dump(
    cycle: str,
    collection: str,
    mongo_uri: str,
    dumps_dir: Path | None = None,
    context = None
) -> bool:
    """
    Restore collection from dump.
    
    Args:
        cycle: FEC cycle
        collection: Collection name
        mongo_uri: MongoDB connection URI
        dumps_dir: Optional dumps directory
        context: Optional Dagster context
        
    Returns:
        True if successful
    """
    manager = MongoDumpManager(dumps_dir)
    return manager.restore_dump(cycle, collection, mongo_uri, context)
