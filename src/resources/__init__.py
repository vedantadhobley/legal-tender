"""Dagster resources for Legal Tender."""
from src.resources.arango import arango_resource, ArangoDBResource

__all__ = [
    "arango_resource",
    "ArangoDBResource",
]
