"""Dagster resources for Legal Tender."""
from src.resources.arango import arango_resource, ArangoDBResource
from src.resources.embedding import EmbeddingResource, get_embedding_client

__all__ = [
    "arango_resource",
    "ArangoDBResource",
    "EmbeddingResource",
    "get_embedding_client",
]
