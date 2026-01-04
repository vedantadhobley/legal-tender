#!/usr/bin/env python3
"""Quick test to verify memory detection on the new hardware.

Standalone script - doesn't require dagster to be installed.
"""

import psutil

# Memory tiers for adaptive processing
MEMORY_TIER_LOW = 8          # < 8GB: Conservative streaming
MEMORY_TIER_MEDIUM = 32      # 8-32GB: Standard batching
MEMORY_TIER_HIGH = 64        # 32-64GB: Large batches
MEMORY_TIER_BEAST = 128      # 64GB+: Load entire files, max batches


def get_available_memory_gb() -> float:
    memory = psutil.virtual_memory()
    return memory.available / (1024 ** 3)


def get_total_memory_gb() -> float:
    memory = psutil.virtual_memory()
    return memory.total / (1024 ** 3)


def get_memory_tier() -> str:
    total_gb = get_total_memory_gb()
    if total_gb >= MEMORY_TIER_BEAST:
        return 'beast'
    elif total_gb >= MEMORY_TIER_HIGH:
        return 'high'
    elif total_gb >= MEMORY_TIER_MEDIUM:
        return 'medium'
    else:
        return 'low'


def main():
    print("=" * 70)
    print("🧠 LEGAL TENDER - MEMORY CONFIGURATION TEST")
    print("=" * 70)
    
    # Basic memory info
    total_gb = get_total_memory_gb()
    available_gb = get_available_memory_gb()
    tier = get_memory_tier()
    
    print(f"\n📊 System Memory:")
    print(f"   Total:     {total_gb:.1f} GB")
    print(f"   Available: {available_gb:.1f} GB")
    print(f"   Used:      {total_gb - available_gb:.1f} GB")
    print(f"   Tier:      {tier.upper()}")
    
    # Calculate batch size based on tier
    if tier == 'beast':
        batch_size = 500000
        target_pct = 0.6
    elif tier == 'high':
        batch_size = 250000
        target_pct = 0.5
    elif tier == 'medium':
        batch_size = 100000
        target_pct = 0.4
    else:
        batch_size = 25000
        target_pct = 0.25
    
    print(f"\n⚡ Batch Configuration:")
    print(f"   Recommended batch size: {batch_size:,} records")
    print(f"   Target memory usage: {target_pct * 100:.0f}%")
    
    # In-memory processing
    use_inmem = tier in ['beast', 'high'] and available_gb > 30
    print(f"\n💾 Processing Mode:")
    if use_inmem:
        print(f"   🔥 In-memory processing enabled ({available_gb:.0f}GB available)")
    else:
        print(f"   📂 Streaming mode ({available_gb:.0f}GB available)")
    
    # Parallel cycles
    if tier == 'beast':
        parallel = 4
    elif tier == 'high':
        parallel = 2
    else:
        parallel = 1
    
    print(f"\n🔧 Processing Config:")
    print(f"   tier: {tier}")
    print(f"   total_memory_gb: {total_gb:.1f}")
    print(f"   available_memory_gb: {available_gb:.1f}")
    print(f"   batch_size: {batch_size:,}")
    print(f"   in_memory: {use_inmem}")
    print(f"   parallel_cycles: {parallel}")
    
    # Comparison
    print(f"\n📈 Performance Comparison (vs 16GB system):")
    print(f"   Old batch size:  10,000 records")
    print(f"   New batch size:  {batch_size:,} records")
    print(f"   Speedup factor:  ~{batch_size // 10000}x fewer MongoDB round-trips")
    
    if tier == 'beast':
        print(f"\n🔥 BEAST MODE UNLOCKED!")
        print(f"   With {total_gb:.0f}GB RAM, you can:")
        print(f"   • Load entire FEC files into memory")
        print(f"   • Process all 4 cycles in parallel")
        print(f"   • Keep LLM + embedding models loaded while processing")
        print(f"   • Use batch sizes of 500K+ records")


if __name__ == "__main__":
    main()
