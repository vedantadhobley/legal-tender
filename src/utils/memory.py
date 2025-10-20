"""Memory utilities for dynamic batch sizing based on available system memory."""

import psutil
from typing import Tuple


def get_available_memory_gb() -> float:
    """Get available system memory in GB.
    
    Returns:
        Available memory in gigabytes (float)
    """
    memory = psutil.virtual_memory()
    return memory.available / (1024 ** 3)


def calculate_optimal_batch_size(
    min_batch: int = 1000,
    max_batch: int = 100000,
    memory_per_1k_records_mb: float = 80.0,  # ~80MB per 1K FEC records (realistic)
    target_memory_usage_pct: float = 0.5,  # Use 50% of available memory (go big!)
) -> Tuple[int, float, str]:
    """Calculate optimal batch size based on available memory.
    
    Strategy:
    - Check available memory
    - Allocate a target percentage for processing (default 25%)
    - Calculate how many records fit in that allocation
    - Clamp to reasonable min/max bounds
    
    Args:
        min_batch: Minimum batch size (safety floor)
        max_batch: Maximum batch size (diminishing returns above this)
        memory_per_1k_records_mb: Estimated MB per 1000 records in memory
        target_memory_usage_pct: Percentage of available memory to use (0.0-1.0)
        
    Returns:
        Tuple of (batch_size, available_gb, reasoning)
    """
    # Get available memory
    available_gb = get_available_memory_gb()
    available_mb = available_gb * 1024
    
    # Calculate target allocation
    target_mb = available_mb * target_memory_usage_pct
    
    # Calculate batch size (how many 1K batches fit in target allocation)
    records_per_mb = 1000 / memory_per_1k_records_mb
    target_batch = int(target_mb * records_per_mb)
    
    # Clamp to reasonable bounds
    batch_size = max(min_batch, min(target_batch, max_batch))
    
    # Round to nearest 1000 for cleaner numbers
    batch_size = round(batch_size / 1000) * 1000
    
    # Generate reasoning
    if available_gb < 2:
        reasoning = f"Low memory ({available_gb:.1f}GB free) - using conservative batch size"
    elif available_gb < 4:
        reasoning = f"Moderate memory ({available_gb:.1f}GB free) - using small batches"
    elif available_gb < 8:
        reasoning = f"Good memory ({available_gb:.1f}GB free) - using medium batches"
    elif available_gb < 16:
        reasoning = f"Great memory ({available_gb:.1f}GB free) - using large batches"
    else:
        reasoning = f"Excellent memory ({available_gb:.1f}GB free) - using maximum batches"
    
    return batch_size, available_gb, reasoning


def get_recommended_batch_size() -> Tuple[int, str]:
    """Get recommended batch size with explanation.
    
    Simple wrapper around calculate_optimal_batch_size with sensible defaults
    for FEC data processing.
    
    Returns:
        Tuple of (batch_size, explanation)
    """
    batch_size, available_gb, reasoning = calculate_optimal_batch_size(
        min_batch=1000,           # Don't go below 1K (too slow)
        max_batch=100000,         # Don't go above 100K (diminishing returns)
        memory_per_1k_records_mb=80.0,  # ~80MB per 1000 FEC records (realistic estimate)
        target_memory_usage_pct=0.5,    # Use 50% of available memory (go fast!)
    )
    
    explanation = f"{reasoning} → batch_size={batch_size:,}"
    
    return batch_size, explanation
