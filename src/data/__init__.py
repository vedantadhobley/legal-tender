"""
Data management package.

Handles data repository structure, downloads, and caching.
"""

from .repository import DataRepository, get_repository

__all__ = ['DataRepository', 'get_repository']
