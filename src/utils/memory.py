"""Memory utilities for dynamic batch sizing based on available system memory.

Updated December 2025 for high-memory systems (128GB+).
Supports aggressive batch sizing when memory is abundant.
"""

import psutil
from typing import Tuple

# Memory tiers for adaptive processing
MEMORY_TIER_LOW = 8          # < 8GB: Conservative streaming
MEMORY_TIER_MEDIUM = 32      # 8-32GB: Standard batching
MEMORY_TIER_HIGH = 64        # 32-64GB: Large batches
MEMORY_TIER_BEAST = 128      # 64GB+: Load entire files, max batches

# Reserved memory for OS + models (in GB)
# Assume ~30GB for local LLM + embedding model
RESERVED_MEMORY_GB = 35


def get_available_memory_gb() -> float:
    """Get available system memory in GB.
    
    Returns:
        Available memory in gigabytes (float)
    """
    memory = psutil.virtual_memory()
    return memory.available / (1024 ** 3)


def get_total_memory_gb() -> float:
    """Get total system memory in GB.
    
    Returns:
        Total memory in gigabytes (float)
    """
    memory = psutil.virtual_memory()
    return memory.total / (1024 ** 3)


def get_memory_tier() -> str:
    """Determine memory tier for processing strategy.
    
    Returns:
        Memory tier: 'low', 'medium', 'high', or 'beast'
    """
    total_gb = get_total_memory_gb()
    
    if total_gb >= MEMORY_TIER_BEAST:
        return 'beast'
    elif total_gb >= MEMORY_TIER_HIGH:
        return 'high'
    elif total_gb >= MEMORY_TIER_MEDIUM:
        return 'medium'
    else:
        return 'low'


def calculate_optimal_batch_size(
    min_batch: int = 1000,
    max_batch: int = 1000000,  # Increased from 100K to 1M for high-memory systems
    memory_per_1k_records_mb: float = 80.0,  # ~80MB per 1K FEC records (realistic)
    target_memory_usage_pct: float = 0.6,  # Use 60% of available memory
    respect_reserved: bool = True,  # Reserve memory for models
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
    
    # Generate reasoning based on memory tier
    tier = get_memory_tier()
    total_gb = get_total_memory_gb()
    
    if tier == 'beast':
        reasoning = f"🔥 BEAST MODE ({total_gb:.0f}GB total, {available_gb:.1f}GB free) - max batch size, in-memory processing"
    elif tier == 'high':
        reasoning = f"💪 High memory ({total_gb:.0f}GB total, {available_gb:.1f}GB free) - large batches"
    elif tier == 'medium':
        reasoning = f"👍 Good memory ({total_gb:.0f}GB total, {available_gb:.1f}GB free) - medium batches"
    else:
        reasoning = f"⚠️ Low memory ({total_gb:.0f}GB total, {available_gb:.1f}GB free) - conservative batches"
    
    return batch_size, available_gb, reasoning


def get_recommended_batch_size(for_bulk_load: bool = False) -> Tuple[int, str]:
    """Get recommended batch size with explanation.
    
    Args:
        for_bulk_load: If True, use more aggressive settings for bulk data loads
    
    Returns:
        Tuple of (batch_size, explanation)
    """
    tier = get_memory_tier()
    
    # Tier-based batch sizes
    if tier == 'beast':
        # 128GB+ system: Go aggressive
        max_batch = 500000 if for_bulk_load else 250000
        target_pct = 0.6
    elif tier == 'high':
        # 64GB system: Still good
        max_batch = 250000 if for_bulk_load else 100000
        target_pct = 0.5
    elif tier == 'medium':
        # 32GB system: Standard
        max_batch = 100000 if for_bulk_load else 50000
        target_pct = 0.4
    else:
        # < 8GB: Conservative
        max_batch = 25000 if for_bulk_load else 10000
        target_pct = 0.25
    
    batch_size, available_gb, reasoning = calculate_optimal_batch_size(
        min_batch=1000,
        max_batch=max_batch,
        memory_per_1k_records_mb=80.0,
        target_memory_usage_pct=target_pct,
    )
    
    explanation = f"{reasoning} → batch_size={batch_size:,}"
    
    return batch_size, explanation


def should_use_in_memory_processing() -> Tuple[bool, str]:
    """Determine if we should load entire files into memory.
    
    For high-memory systems, loading entire FEC files into memory
    can be much faster than streaming line-by-line.
    
    Returns:
        Tuple of (should_use_in_memory, explanation)
    """
    tier = get_memory_tier()
    available_gb = get_available_memory_gb()
    
    if tier == 'beast' and available_gb > 50:
        return True, f"🔥 In-memory processing enabled ({available_gb:.0f}GB available)"
    elif tier == 'high' and available_gb > 30:
        return True, f"💪 In-memory processing enabled ({available_gb:.0f}GB available)"
    else:
        return False, f"📂 Streaming mode ({available_gb:.0f}GB available - preserving memory for models)"


def get_processing_config() -> dict:
    """Get complete processing configuration based on system memory.
    
    Returns:
        Dictionary with processing settings:
        - tier: Memory tier (beast/high/medium/low)
        - batch_size: Recommended batch size
        - in_memory: Whether to use in-memory processing
        - parallel_cycles: How many cycles to process in parallel
        - mongo_bulk_write: Whether to use unordered bulk writes
    """
    tier = get_memory_tier()
    available_gb = get_available_memory_gb()
    total_gb = get_total_memory_gb()
    batch_size, _ = get_recommended_batch_size(for_bulk_load=True)
    in_memory, _ = should_use_in_memory_processing()
    
    # Parallel processing based on memory
    if tier == 'beast':
        parallel_cycles = 4  # Process all 4 cycles in parallel
    elif tier == 'high':
        parallel_cycles = 2
    else:
        parallel_cycles = 1  # Sequential
    
    return {
        'tier': tier,
        'total_memory_gb': total_gb,
        'available_memory_gb': available_gb,
        'batch_size': batch_size,
        'in_memory': in_memory,
        'parallel_cycles': parallel_cycles,
        'mongo_bulk_write': True,  # Always use unordered for speed
    }
