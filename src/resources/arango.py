"""ArangoDB Resource for Dagster.

ArangoDB is a multi-model database supporting documents, graphs, and key-value storage.
We use it primarily for its native graph capabilities to track money flows in political
contributions with unlimited depth traversal.
"""

import os
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from dagster import ConfigurableResource
from arango import ArangoClient
from arango.database import StandardDatabase
from arango.collection import StandardCollection
from arango.graph import Graph


# Default connection settings - overridden by env vars
DEFAULT_HOST = "legal-tender-dev-arango"
DEFAULT_PORT = "8529"
DEFAULT_USER = "root"
DEFAULT_PASSWORD = "ltpass"
DEFAULT_DB = "legal_tender"


class ArangoDBResource(ConfigurableResource):
    """
    Dagster resource for ArangoDB connections.
    
    Connection settings are read from environment variables:
    - ARANGO_HOST: ArangoDB host (default: legal-tender-dev-arango)
    - ARANGO_PORT: ArangoDB port (default: 8529)
    - ARANGO_USER: Username (default: root)
    - ARANGO_PASSWORD: Password (default: ltpass)
    - ARANGO_DB: Database name (default: legal_tender)
    
    Usage in asset:
        @asset
        def my_asset(arango: ArangoDBResource):
            with arango.get_client() as client:
                db = arango.get_database(client)
                collection = db.collection('my_collection')
                # ... do work
                
    Graph usage:
        @asset
        def graph_asset(arango: ArangoDBResource):
            with arango.get_client() as client:
                db = arango.get_database(client)
                graph = arango.ensure_graph(
                    db, 
                    'money_flow',
                    edge_definitions=[{
                        'edge_collection': 'flows',
                        'from_vertex_collections': ['committees', 'individuals'],
                        'to_vertex_collections': ['committees', 'candidates']
                    }]
                )
                # Now use AQL for graph traversal
    """
    
    host: str = os.environ.get("ARANGO_HOST", DEFAULT_HOST)
    """ArangoDB host"""
    
    port: str = os.environ.get("ARANGO_PORT", DEFAULT_PORT)
    """ArangoDB port"""
    
    username: str = os.environ.get("ARANGO_USER", DEFAULT_USER)
    """ArangoDB username"""
    
    password: str = os.environ.get("ARANGO_PASSWORD", DEFAULT_PASSWORD)
    """ArangoDB password"""
    
    database: str = os.environ.get("ARANGO_DB", DEFAULT_DB)
    """ArangoDB database name"""
    
    @contextmanager
    def get_client(self):
        """
        Get an ArangoDB client as a context manager.
        
        Yields:
            ArangoClient: Connected ArangoDB client
            
        Example:
            with arango.get_client() as client:
                db = arango.get_database(client)
                collection = db.collection('committees')
                collection.insert({...})
        """
        from arango.http import DefaultHTTPClient
        
        # Custom HTTP client with longer timeout for large queries
        http_client = DefaultHTTPClient(request_timeout=3600)  # 1 hour timeout
        
        client = ArangoClient(
            hosts=f"http://{self.host}:{self.port}",
            http_client=http_client
        )
        try:
            yield client
        finally:
            client.close()
    
    def get_database(
        self, 
        client: ArangoClient, 
        database_name: Optional[str] = None,
        create_if_missing: bool = True
    ) -> StandardDatabase:
        """
        Get a database from the client, creating it if it doesn't exist.
        
        Args:
            client: ArangoDB client
            database_name: Name of the database (default: from config)
            create_if_missing: Create the database if it doesn't exist
            
        Returns:
            StandardDatabase: ArangoDB database
        """
        db_name = database_name or self.database
        
        # Connect to system database first to check/create
        sys_db = client.db("_system", username=self.username, password=self.password)
        
        if create_if_missing and not sys_db.has_database(db_name):
            sys_db.create_database(db_name)
        
        return client.db(db_name, username=self.username, password=self.password)
    
    def get_collection(
        self,
        db: StandardDatabase,
        collection_name: str,
        create_if_missing: bool = True,
        edge: bool = False
    ) -> StandardCollection:
        """
        Get a collection from the database, creating it if it doesn't exist.
        
        Args:
            db: ArangoDB database
            collection_name: Name of the collection
            create_if_missing: Create the collection if it doesn't exist
            edge: Whether this is an edge collection
            
        Returns:
            StandardCollection: ArangoDB collection
        """
        if create_if_missing and not db.has_collection(collection_name):
            db.create_collection(collection_name, edge=edge)
        
        return db.collection(collection_name)
    
    def ensure_graph(
        self,
        db: StandardDatabase,
        graph_name: str,
        edge_definitions: List[Dict[str, Any]]
    ) -> Graph:
        """
        Ensure a graph exists with the specified edge definitions.
        
        Args:
            db: ArangoDB database
            graph_name: Name of the graph
            edge_definitions: List of edge definition dicts with:
                - edge_collection: Name of the edge collection
                - from_vertex_collections: List of source vertex collections
                - to_vertex_collections: List of target vertex collections
                
        Returns:
            Graph: ArangoDB graph
            
        Example:
            graph = arango.ensure_graph(db, 'money_flow', [
                {
                    'edge_collection': 'flows',
                    'from_vertex_collections': ['committees', 'individuals'],
                    'to_vertex_collections': ['committees', 'candidates']
                }
            ])
        """
        if db.has_graph(graph_name):
            return db.graph(graph_name)
        
        return db.create_graph(graph_name, edge_definitions=edge_definitions)
    
    def execute_aql(
        self,
        db: StandardDatabase,
        query: str,
        bind_vars: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000
    ):
        """
        Execute an AQL query and return results.
        
        Args:
            db: ArangoDB database
            query: AQL query string
            bind_vars: Variables to bind to the query
            batch_size: Number of results per batch
            
        Returns:
            Cursor with query results
            
        Example:
            # Find all money flowing to a candidate
            results = arango.execute_aql(db, '''
                FOR v, e, p IN 1..10 INBOUND @candidate flows
                    RETURN {
                        path: p,
                        total: SUM(p.edges[*].amount)
                    }
            ''', bind_vars={'candidate': 'candidates/H8TX22024'})
        """
        return db.aql.execute(query, bind_vars=bind_vars or {}, batch_size=batch_size)
    
    def bulk_import(
        self,
        collection: StandardCollection,
        documents: List[Dict[str, Any]],
        on_duplicate: str = "update",
        batch_size: int = 10000
    ) -> Dict[str, int]:
        """
        Bulk import documents into a collection with batching.
        
        Args:
            collection: Target collection
            documents: List of documents to import
            on_duplicate: Action on duplicate _key: 'error', 'update', 'replace', 'ignore'
            batch_size: Number of documents per batch
            
        Returns:
            Dict with counts: created, errors, updated, ignored
        """
        total_stats = {"created": 0, "errors": 0, "updated": 0, "ignored": 0}
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            result = collection.import_bulk(batch, on_duplicate=on_duplicate)
            
            total_stats["created"] += result.get("created", 0)
            total_stats["errors"] += result.get("errors", 0)
            total_stats["updated"] += result.get("updated", 0)
            total_stats["ignored"] += result.get("ignored", 0)
        
        return total_stats


# Default resource instance - uses env vars
arango_resource = ArangoDBResource(
    host=os.environ.get("ARANGO_HOST", DEFAULT_HOST),
    port=os.environ.get("ARANGO_PORT", DEFAULT_PORT),
    username=os.environ.get("ARANGO_USER", DEFAULT_USER),
    password=os.environ.get("ARANGO_PASSWORD", DEFAULT_PASSWORD),
    database=os.environ.get("ARANGO_DB", DEFAULT_DB),
)
