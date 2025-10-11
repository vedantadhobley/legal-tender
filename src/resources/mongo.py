"""MongoDB resource for Dagster."""
import os
from contextlib import contextmanager
from typing import Any

from dagster import ConfigurableResource
from pymongo import MongoClient
from pymongo.database import Database


class MongoDBResource(ConfigurableResource):
    """MongoDB resource for connecting to and interacting with MongoDB.
    
    This resource provides a configured MongoDB client and database connection
    that can be shared across multiple assets and ops.
    
    Configuration:
        uri: MongoDB connection string (default: from MONGO_URI env var)
        database: Database name (default: from MONGO_DB env var or 'legal_tender')
    """
    
    uri: str = os.getenv("MONGO_URI", "mongodb://ltuser:ltpass@mongo:27017/admin")
    database: str = os.getenv("MONGO_DB", "legal_tender")
    
    @contextmanager
    def get_client(self):
        """Get MongoDB client as a context manager.
        
        Yields:
            MongoClient: Configured MongoDB client
        """
        client = MongoClient(self.uri)
        try:
            yield client
        finally:
            client.close()
    
    def get_database(self, client: MongoClient) -> Database:
        """Get configured database from client.
        
        Args:
            client: MongoDB client instance
            
        Returns:
            Database: Configured MongoDB database
        """
        return client[self.database]
    
    def get_collection(self, client: MongoClient, collection_name: str) -> Any:
        """Get a specific collection from the database.
        
        Args:
            client: MongoDB client instance
            collection_name: Name of the collection to retrieve
            
        Returns:
            Collection: MongoDB collection
        """
        db = self.get_database(client)
        return db[collection_name]


# Singleton instance for easy import
mongo_resource = MongoDBResource()
