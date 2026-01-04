"""MongoDB Resource for Dagster."""

import os
from contextlib import contextmanager
from dagster import ConfigurableResource
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection


# Default connection string - overridden by MONGODB_URI env var
DEFAULT_MONGODB_URI = "mongodb://ltuser:ltpass@legal-tender-dev-mongo:27017/"


class MongoDBResource(ConfigurableResource):
    """
    Dagster resource for MongoDB connections.
    
    Connection string is read from MONGODB_URI environment variable,
    falling back to the default for development.
    
    Usage in asset:
        @asset
        def my_asset(mongo: MongoDBResource):
            with mongo.get_client() as client:
                db = client['my_database']
                collection = db['my_collection']
                # ... do work
    """
    
    connection_string: str = os.environ.get("MONGODB_URI", DEFAULT_MONGODB_URI)
    """MongoDB connection string (from MONGODB_URI env var or default)"""
    
    @contextmanager
    def get_client(self):
        """
        Get a MongoDB client as a context manager.
        
        Yields:
            MongoClient: Connected MongoDB client
            
        Example:
            with mongo.get_client() as client:
                db = client['legal_tender']
                collection = db['members']
                collection.insert_one({...})
        """
        client = MongoClient(self.connection_string)
        try:
            # Test connection
            client.admin.command('ping')
            yield client
        finally:
            client.close()
    
    def get_database(self, client: MongoClient, database_name: str) -> Database:
        """
        Get a database from the client.
        
        Args:
            client: MongoDB client
            database_name: Name of the database
            
        Returns:
            Database: MongoDB database
        """
        return client[database_name]
    
    def get_collection(
        self, 
        client: MongoClient, 
        collection_name: str, 
        database_name: str = "legal_tender"
    ) -> Collection:
        """
        Get a collection from the database.
        
        Args:
            client: MongoDB client
            collection_name: Name of the collection
            database_name: Name of the database (default: legal_tender)
            
        Returns:
            Collection: MongoDB collection
        """
        db = self.get_database(client, database_name)
        return db[collection_name]


# Default resource instance - uses env var or default
mongo_resource = MongoDBResource(
    connection_string=os.environ.get("MONGODB_URI", DEFAULT_MONGODB_URI)
)
