"""Embedding Model Resource for Dagster.

Configurable embedding client that can point to any OpenAI-compatible
embedding API (llama.cpp, vLLM, etc.). Configuration via environment
variables makes it easy to switch models, hosts, or servers.

Environment Variables:
    EMBEDDING_HOST: Host for embedding server (default: llama-embed)
    EMBEDDING_PORT: Port for embedding server (default: 8080)
    EMBEDDING_MODEL: Model name to request (default: text-embedding)
    EMBEDDING_BATCH_SIZE: Max items per batch request (default: 32)
"""

import os
import logging
from typing import List, Optional, Dict, Any
from contextlib import contextmanager

import requests
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


# Default settings - override via environment variables
DEFAULT_HOST = "llama-embed"
DEFAULT_PORT = "8080"
DEFAULT_MODEL = "text-embedding"
DEFAULT_BATCH_SIZE = 32
DEFAULT_TIMEOUT = 60


class EmbeddingResource(ConfigurableResource):
    """
    Dagster resource for embedding model access.
    
    Connects to any OpenAI-compatible embedding endpoint (llama.cpp, vLLM, etc.)
    Configuration via environment variables for easy deployment changes.
    
    Usage in asset:
        @asset
        def my_asset(embedding: EmbeddingResource):
            vectors = embedding.embed_texts(["hello world", "foo bar"])
            similarity = embedding.compute_similarity("citadel", "citadell")
            
    Configuration:
        Set these environment variables to change the endpoint:
        - EMBEDDING_HOST: Server hostname (default: llama-embed)
        - EMBEDDING_PORT: Server port (default: 8080)
        - EMBEDDING_MODEL: Model name (default: text-embedding)
    """
    
    host: str = os.environ.get("EMBEDDING_HOST", DEFAULT_HOST)
    """Embedding server hostname"""
    
    port: str = os.environ.get("EMBEDDING_PORT", DEFAULT_PORT)
    """Embedding server port"""
    
    model: str = os.environ.get("EMBEDDING_MODEL", DEFAULT_MODEL)
    """Model name for API requests"""
    
    batch_size: int = int(os.environ.get("EMBEDDING_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
    """Maximum texts per batch request"""
    
    timeout: int = int(os.environ.get("EMBEDDING_TIMEOUT", str(DEFAULT_TIMEOUT)))
    """Request timeout in seconds"""
    
    @property
    def base_url(self) -> str:
        """Get the base URL for the embedding API."""
        return f"http://{self.host}:{self.port}"
    
    @property
    def embeddings_url(self) -> str:
        """Get the embeddings endpoint URL."""
        return f"{self.base_url}/v1/embeddings"
    
    def health_check(self) -> bool:
        """Check if the embedding server is available."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            # Some servers don't have /health, try /v1/models
            try:
                response = requests.get(f"{self.base_url}/v1/models", timeout=5)
                return response.status_code == 200
            except requests.RequestException:
                return False
    
    def embed_text(self, text: str) -> Optional[List[float]]:
        """
        Get embedding vector for a single text.
        
        Args:
            text: Text to embed
            
        Returns:
            Embedding vector as list of floats, or None if failed
        """
        result = self.embed_texts([text])
        return result[0] if result else None
    
    def embed_texts(self, texts: List[str]) -> Optional[List[List[float]]]:
        """
        Get embedding vectors for multiple texts.
        
        Handles batching automatically for large lists.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embedding vectors, or None if failed
        """
        if not texts:
            return []
        
        all_embeddings = []
        
        # Process in batches
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            
            try:
                response = requests.post(
                    self.embeddings_url,
                    json={
                        "model": self.model,
                        "input": batch
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout
                )
                response.raise_for_status()
                
                data = response.json()
                
                # Handle different response formats
                if "data" in data:
                    # OpenAI format
                    batch_embeddings = [item["embedding"] for item in data["data"]]
                elif "embeddings" in data:
                    # Alternative format
                    batch_embeddings = data["embeddings"]
                else:
                    logger.error(f"Unknown embedding response format: {data.keys()}")
                    return None
                
                all_embeddings.extend(batch_embeddings)
                
            except requests.RequestException as e:
                logger.error(f"Embedding request failed: {e}")
                return None
        
        return all_embeddings
    
    def compute_similarity(self, text1: str, text2: str) -> Optional[float]:
        """
        Compute cosine similarity between two texts.
        
        Args:
            text1: First text
            text2: Second text
            
        Returns:
            Cosine similarity (0-1), or None if failed
        """
        embeddings = self.embed_texts([text1, text2])
        if not embeddings or len(embeddings) != 2:
            return None
        
        return self._cosine_similarity(embeddings[0], embeddings[1])
    
    def compute_similarity_matrix(self, texts: List[str]) -> Optional[List[List[float]]]:
        """
        Compute pairwise cosine similarity for a list of texts.
        
        Args:
            texts: List of texts to compare
            
        Returns:
            NxN similarity matrix, or None if failed
        """
        embeddings = self.embed_texts(texts)
        if not embeddings:
            return None
        
        n = len(embeddings)
        matrix = [[0.0] * n for _ in range(n)]
        
        for i in range(n):
            matrix[i][i] = 1.0  # Self-similarity
            for j in range(i + 1, n):
                sim = self._cosine_similarity(embeddings[i], embeddings[j])
                matrix[i][j] = sim
                matrix[j][i] = sim
        
        return matrix
    
    def find_similar(
        self, 
        query: str, 
        candidates: List[str], 
        threshold: float = 0.85
    ) -> List[Dict[str, Any]]:
        """
        Find candidates similar to a query text.
        
        Args:
            query: Text to match against
            candidates: List of candidate texts
            threshold: Minimum similarity (default: 0.85 for typo detection)
            
        Returns:
            List of {text, similarity} dicts above threshold, sorted by similarity
        """
        if not candidates:
            return []
        
        # Embed query and candidates together
        all_texts = [query] + candidates
        embeddings = self.embed_texts(all_texts)
        
        if not embeddings:
            return []
        
        query_embedding = embeddings[0]
        candidate_embeddings = embeddings[1:]
        
        results = []
        for i, cand_emb in enumerate(candidate_embeddings):
            sim = self._cosine_similarity(query_embedding, cand_emb)
            if sim >= threshold:
                results.append({
                    "text": candidates[i],
                    "similarity": sim,
                    "index": i
                })
        
        return sorted(results, key=lambda x: x["similarity"], reverse=True)
    
    @staticmethod
    def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        import math
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = math.sqrt(sum(a * a for a in vec1))
        norm2 = math.sqrt(sum(b * b for b in vec2))
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)
    
    def cluster_by_similarity(
        self,
        texts: List[str],
        threshold: float = 0.90
    ) -> List[List[int]]:
        """
        Cluster texts by embedding similarity.
        
        Uses single-linkage clustering: if A is similar to B and B is similar to C,
        then A, B, C are in the same cluster.
        
        Args:
            texts: List of texts to cluster
            threshold: Minimum similarity to consider same cluster (default: 0.90)
            
        Returns:
            List of clusters, where each cluster is a list of indices into texts
        """
        if not texts:
            return []
        
        embeddings = self.embed_texts(texts)
        if not embeddings:
            return [[i] for i in range(len(texts))]
        
        n = len(texts)
        
        # Union-find for clustering
        parent = list(range(n))
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
        
        # Compare all pairs
        for i in range(n):
            for j in range(i + 1, n):
                sim = self._cosine_similarity(embeddings[i], embeddings[j])
                if sim >= threshold:
                    union(i, j)
        
        # Group by cluster
        clusters: Dict[int, List[int]] = {}
        for i in range(n):
            root = find(i)
            clusters.setdefault(root, []).append(i)
        
        return list(clusters.values())


# Convenience function for non-Dagster usage
def get_embedding_client() -> EmbeddingResource:
    """
    Get an EmbeddingResource instance configured from environment.
    
    Useful for scripts and testing outside Dagster context.
    """
    return EmbeddingResource()
