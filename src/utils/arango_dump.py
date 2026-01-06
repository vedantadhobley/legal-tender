"""
ArangoDB Collection and Graph Caching Utilities

Manages JSONL dumps for fast data loading with proper separation:
- FEC raw data: ~/workspace/.legal-tender/arango/fec/{cycle}/
- Enriched data: ~/workspace/.legal-tender/arango/enriched/{cycle}/
- Aggregation data: ~/workspace/.legal-tender/arango/aggregation/
- Graphs: ~/workspace/.legal-tender/arango/graphs/

Workflow:
1. Parse FEC file → store in ArangoDB (varies by collection)
2. Create indexes
3. Create JSONL dump (fast streaming format)
4. Next time: check if source file changed
   - Unchanged → restore from dump (~30 sec)
   - Changed → re-parse

JSONL Format:
- One JSON object per line (streaming-friendly)
- First line is metadata header
- Remaining lines are documents
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Literal, List, Iterator
import json
import gzip
import hashlib

from arango.database import StandardDatabase
from arango.collection import StandardCollection

from src.utils.storage import (
    get_arango_dump_dir, 
    get_arango_fec_dump_dir, 
    get_arango_enriched_dump_dir,
    get_arango_aggregation_dump_dir,
    get_arango_graph_dump_dir
)


DumpType = Literal["fec", "enriched", "aggregation", "graph"]


class ArangoDumpManager:
    """Manages ArangoDB dumps for fast data loading."""
    
    def __init__(self, dump_type: DumpType = "fec"):
        """
        Initialize dump manager.
        
        Args:
            dump_type: Type of dumps to manage ("fec", "enriched", "aggregation", "graph")
        """
        self.dump_type = dump_type
    
    def _get_base_dir(self, cycle: Optional[str] = None) -> Path:
        """Get base directory for dumps based on type and cycle."""
        if self.dump_type == "fec":
            if not cycle:
                raise ValueError("Cycle required for FEC dumps")
            return get_arango_fec_dump_dir(cycle)
        elif self.dump_type == "enriched":
            if not cycle:
                raise ValueError("Cycle required for enriched dumps")
            return get_arango_enriched_dump_dir(cycle)
        elif self.dump_type == "aggregation":
            return get_arango_aggregation_dump_dir()
        elif self.dump_type == "graph":
            return get_arango_graph_dump_dir()
        else:
            raise ValueError(f"Unknown dump type: {self.dump_type}")
    
    def get_dump_path(self, collection: str, cycle: Optional[str] = None) -> Path:
        """
        Get path to JSONL dump file for a collection.
        
        Args:
            collection: Collection name (e.g., "cn", "pas2")
            cycle: FEC cycle (required for fec/enriched, ignored for aggregation)
            
        Returns:
            Path to JSONL dump file (gzipped)
        """
        return self._get_base_dir(cycle) / f"{collection}.jsonl.gz"
    
    def get_metadata_path(self, cycle: Optional[str] = None) -> Path:
        """Get path to metadata.json for tracking all collections."""
        return self._get_base_dir(cycle) / "metadata.json"
    
    def dump_exists(self, collection: str, cycle: Optional[str] = None) -> bool:
        """Check if a dump exists for a collection."""
        dump_file = self.get_dump_path(collection, cycle)
        metadata_file = self.get_metadata_path(cycle)
        return dump_file.exists() and metadata_file.exists()
    
    def load_metadata(self, cycle: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Load metadata for a cycle/type."""
        path = self.get_metadata_path(cycle)
        if not path.exists():
            return None
        with open(path) as f:
            return json.load(f)
    
    def _save_metadata(self, metadata: Dict[str, Any], cycle: Optional[str] = None):
        """Save metadata for a cycle/type."""
        path = self.get_metadata_path(cycle)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
    
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
        
        # Load metadata
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
    
    def _update_metadata(
        self,
        collection: str,
        source_file: Path,
        record_count: int,
        indexes: Optional[List[Dict[str, Any]]] = None,
        cycle: Optional[str] = None
    ):
        """Update metadata after creating a dump."""
        metadata = self.load_metadata(cycle) or {
            'dump_type': self.dump_type,
            'cycle': cycle,
            'collections': {}
        }
        
        dump_path = self.get_dump_path(collection, cycle)
        
        metadata['collections'][collection] = {
            'dump_created': datetime.now().isoformat(),
            'source_file': str(source_file),
            'source_modified': datetime.fromtimestamp(source_file.stat().st_mtime).isoformat(),
            'record_count': record_count,
            'dump_size_bytes': dump_path.stat().st_size if dump_path.exists() else 0,
            'indexes': indexes or []
        }
        
        self._save_metadata(metadata, cycle)
    
    def create_dump(
        self,
        db: StandardDatabase,
        collection_name: str,
        source_file: Path,
        cycle: Optional[str] = None,
        context = None,
        batch_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Create JSONL dump of an ArangoDB collection.
        
        Args:
            db: ArangoDB database object
            collection_name: Name of the collection to dump
            source_file: Path to source file (for metadata)
            cycle: FEC cycle (required for fec/enriched)
            context: Optional Dagster context for logging
            batch_size: Number of documents to fetch per batch
            
        Returns:
            Dict with stats: record_count, size_bytes, duration_seconds
        """
        def log(msg):
            if context:
                context.log.info(msg)
            else:
                print(msg)
        
        collection = db.collection(collection_name)
        dump_path = self.get_dump_path(collection_name, cycle)
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        
        log(f"📦 Creating dump: {collection_name} → {dump_path}")
        
        start_time = datetime.now()
        record_count = 0
        
        # Get index info for metadata
        indexes = list(collection.indexes())
        
        # Stream to gzipped JSONL
        with gzip.open(dump_path, 'wt', encoding='utf-8') as f:
            # Write header with collection info
            header = {
                '_header': True,
                'collection': collection_name,
                'dump_type': self.dump_type,
                'cycle': cycle,
                'created': datetime.now().isoformat(),
                'indexes': indexes
            }
            f.write(json.dumps(header) + '\n')
            
            # Stream documents using AQL with extended timeout for large collections
            # ttl=3600 keeps cursor alive for 1 hour (default is 30 seconds)
            # stream=True uses server-side streaming cursor (lower memory)
            cursor = db.aql.execute(
                f'FOR doc IN {collection_name} RETURN doc',
                batch_size=batch_size,
                ttl=3600,  # 1 hour cursor timeout
                stream=True  # Server-side streaming for large results
            )
            for doc in cursor:
                f.write(json.dumps(doc, default=str) + '\n')
                record_count += 1
                
                if record_count % 500000 == 0:
                    log(f"   Dumped {record_count:,} records...")
        
        duration = (datetime.now() - start_time).total_seconds()
        size_bytes = dump_path.stat().st_size
        
        # Update metadata
        self._update_metadata(
            collection=collection_name,
            source_file=source_file,
            record_count=record_count,
            indexes=indexes,
            cycle=cycle
        )
        
        log(f"✅ Dump created: {record_count:,} records ({size_bytes / 1024 / 1024:.1f} MB) in {duration:.1f}s")
        
        return {
            'record_count': record_count,
            'size_bytes': size_bytes,
            'duration_seconds': duration
        }
    
    def restore_dump(
        self,
        db: StandardDatabase,
        collection_name: str,
        cycle: Optional[str] = None,
        context = None,
        batch_size: int = 10000,
        drop_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Restore JSONL dump to ArangoDB.
        
        Args:
            db: ArangoDB database
            collection_name: Name of collection to restore
            cycle: FEC cycle (required for fec/enriched)
            context: Optional Dagster context for logging
            batch_size: Number of documents to import per batch
            drop_existing: Drop existing collection before restoring
            
        Returns:
            Dict with stats: record_count, duration_seconds, created, updated, errors
        """
        def log(msg):
            if context:
                context.log.info(msg)
            else:
                print(msg)
        
        dump_path = self.get_dump_path(collection_name, cycle)
        
        if not dump_path.exists():
            raise FileNotFoundError(f"No dump found at {dump_path}")
        
        log(f"🔄 Restoring from dump: {dump_path} → {collection_name}")
        
        start_time = datetime.now()
        
        # Read header first to get index info
        with gzip.open(dump_path, 'rt', encoding='utf-8') as f:
            header_line = f.readline()
            header = json.loads(header_line)
            
            if not header.get('_header'):
                raise ValueError("Invalid dump file: missing header")
        
        # Drop and recreate collection if requested
        if drop_existing and db.has_collection(collection_name):
            db.delete_collection(collection_name)
        
        # Determine if this is an edge collection from indexes
        is_edge = any(idx.get('type') == 'edge' for idx in header.get('indexes', []))
        
        if not db.has_collection(collection_name):
            db.create_collection(collection_name, edge=is_edge)
        
        collection = db.collection(collection_name)
        
        # Stream import with batching
        stats = {'created': 0, 'updated': 0, 'errors': 0}
        batch = []
        record_count = 0
        
        with gzip.open(dump_path, 'rt', encoding='utf-8') as f:
            # Skip header
            f.readline()
            
            for line in f:
                doc = json.loads(line)
                batch.append(doc)
                
                if len(batch) >= batch_size:
                    result = collection.import_bulk(batch, on_duplicate='replace')
                    stats['created'] += result.get('created', 0)
                    stats['updated'] += result.get('updated', 0)
                    stats['errors'] += result.get('errors', 0)
                    record_count += len(batch)
                    batch = []
                    
                    if record_count % 100000 == 0:
                        log(f"   Restored {record_count:,} records...")
            
            # Final batch
            if batch:
                result = collection.import_bulk(batch, on_duplicate='replace')
                stats['created'] += result.get('created', 0)
                stats['updated'] += result.get('updated', 0)
                stats['errors'] += result.get('errors', 0)
                record_count += len(batch)
        
        # Recreate indexes from header
        for idx in header.get('indexes', []):
            idx_type = idx.get('type')
            if idx_type in ('primary', 'edge'):
                continue  # These are automatic
            
            try:
                if idx_type == 'persistent':
                    collection.add_persistent_index(
                        fields=idx.get('fields', []),
                        unique=idx.get('unique', False),
                        sparse=idx.get('sparse', False)
                    )
                elif idx_type == 'hash':
                    collection.add_hash_index(
                        fields=idx.get('fields', []),
                        unique=idx.get('unique', False),
                        sparse=idx.get('sparse', False)
                    )
                elif idx_type == 'fulltext':
                    collection.add_fulltext_index(
                        fields=idx.get('fields', []),
                        min_length=idx.get('minLength', 3)
                    )
                elif idx_type == 'geo':
                    collection.add_geo_index(
                        fields=idx.get('fields', []),
                        geo_json=idx.get('geoJson', False)
                    )
            except Exception as e:
                log(f"   Warning: Could not recreate index {idx}: {e}")
        
        duration = (datetime.now() - start_time).total_seconds()
        
        log(f"✅ Restored {record_count:,} records in {duration:.1f}s")
        
        return {
            'record_count': record_count,
            'duration_seconds': duration,
            **stats
        }
    
    def iter_dump(
        self, 
        collection_name: str, 
        cycle: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Iterate over documents in a dump file without loading all into memory.
        
        Args:
            collection_name: Name of the collection
            cycle: FEC cycle
            
        Yields:
            Documents from the dump
        """
        dump_path = self.get_dump_path(collection_name, cycle)
        
        if not dump_path.exists():
            raise FileNotFoundError(f"No dump found at {dump_path}")
        
        with gzip.open(dump_path, 'rt', encoding='utf-8') as f:
            # Skip header
            f.readline()
            
            for line in f:
                yield json.loads(line)


class GraphDumpManager:
    """Manages ArangoDB graph structure dumps."""
    
    def __init__(self):
        self.dump_dir = get_arango_graph_dump_dir()
    
    def get_graph_dump_path(self, graph_name: str) -> Path:
        """Get path to graph definition file."""
        return self.dump_dir / f"{graph_name}.json"
    
    def dump_graph(
        self,
        db: StandardDatabase,
        graph_name: str,
        context = None
    ) -> Dict[str, Any]:
        """
        Dump graph definition (structure only, not data).
        
        Args:
            db: ArangoDB database
            graph_name: Name of the graph
            context: Optional Dagster context for logging
            
        Returns:
            Graph definition dict
        """
        def log(msg):
            if context:
                context.log.info(msg)
            else:
                print(msg)
        
        if not db.has_graph(graph_name):
            raise ValueError(f"Graph {graph_name} does not exist")
        
        graph = db.graph(graph_name)
        
        definition = {
            'name': graph_name,
            'edge_definitions': graph.edge_definitions(),
            'orphan_collections': graph.orphan_collections(),
            'dumped_at': datetime.now().isoformat()
        }
        
        self.dump_dir.mkdir(parents=True, exist_ok=True)
        dump_path = self.get_graph_dump_path(graph_name)
        
        with open(dump_path, 'w') as f:
            json.dump(definition, f, indent=2)
        
        log(f"✅ Graph definition saved: {graph_name} → {dump_path}")
        
        return definition
    
    def restore_graph(
        self,
        db: StandardDatabase,
        graph_name: str,
        context = None
    ) -> Dict[str, Any]:
        """
        Restore graph definition from dump.
        
        Args:
            db: ArangoDB database
            graph_name: Name of the graph
            context: Optional Dagster context for logging
            
        Returns:
            Graph definition dict
        """
        def log(msg):
            if context:
                context.log.info(msg)
            else:
                print(msg)
        
        dump_path = self.get_graph_dump_path(graph_name)
        
        if not dump_path.exists():
            raise FileNotFoundError(f"No graph dump found at {dump_path}")
        
        with open(dump_path) as f:
            definition = json.load(f)
        
        # Delete existing graph if present
        if db.has_graph(graph_name):
            db.delete_graph(graph_name, drop_collections=False)
        
        # Recreate graph
        db.create_graph(
            graph_name,
            edge_definitions=definition.get('edge_definitions', []),
            orphan_collections=definition.get('orphan_collections', [])
        )
        
        log(f"✅ Graph restored: {graph_name}")
        
        return definition


# Convenience functions for common operations
def should_restore_from_dump(
    collection: str,
    source_file: Path,
    dump_type: DumpType,
    cycle: Optional[str] = None
) -> bool:
    """Check if we should restore from dump instead of re-parsing."""
    manager = ArangoDumpManager(dump_type)
    return manager.should_restore(collection, source_file, cycle)


def create_collection_dump(
    db: StandardDatabase,
    collection_name: str,
    source_file: Path,
    dump_type: DumpType,
    cycle: Optional[str] = None,
    context = None
) -> Dict[str, Any]:
    """Create a dump of an ArangoDB collection."""
    manager = ArangoDumpManager(dump_type)
    return manager.create_dump(db, collection_name, source_file, cycle, context)


def restore_collection_from_dump(
    db: StandardDatabase,
    collection_name: str,
    dump_type: DumpType,
    cycle: Optional[str] = None,
    context = None
) -> Dict[str, Any]:
    """Restore a collection from dump."""
    manager = ArangoDumpManager(dump_type)
    return manager.restore_dump(db, collection_name, cycle, context)
