"""Donor Classification - Add donor_type and whale_tier fields to donors.

Classifies donors as individual or organization based on name patterns and employer/occupation data.
Also adds whale_tier for targeting mega-donors with RAG enrichment.

Classification logic:
- INDIVIDUAL: Names with clear personal indicators (first/last names, MR/MS/MRS/DR titles)
- ORGANIZATION: Names with corporate/org indicators (INC, LLC, PAC, FUND, COMMITTEE)
- CONDUIT: Known conduit PACs (WinRed, ActBlue) that appear in indiv data as earmarks
- LIKELY_INDIVIDUAL: Probable individuals but not certain
- UNCLEAR: Ambiguous cases that need manual review or LLM enrichment

Whale tiers (for RAG enrichment targeting):
- notable: $50K+ (top 2-3%, ~5K people)
- whale: $100K+ (top 1%, ~2K people)
- mega: $500K+ (top 0.1%, ~300 people)
- ultra: $1M+ (billionaires, ~100 people)

This enrichment was previously done manually via AQL UPDATE. Moving into pipeline
ensures it's tracked, reproducible, and can be refined over time.

Source: aggregation.donors
Target: aggregation.donors (adds donor_type and whale_tier fields)
"""

from typing import Dict, Any
import re

from dagster import asset, AssetExecutionContext, MetadataValue, Output

from src.resources.arango import ArangoDBResource


def classify_donor(name: str, employer: str, occupation: str) -> str:
    """Classify a donor as individual or organization.
    
    Args:
        name: Donor name (may be None)
        employer: Employer name (may be None)
        occupation: Occupation (may be None)
        
    Returns:
        One of: "individual", "organization", "likely_individual", "unclear"
    """
    # Normalize inputs
    name = (name or "").upper().strip()
    employer = (employer or "").upper().strip()
    occupation = (occupation or "").upper().strip()
    
    if not name:
        return "unclear"
    
    # Strong organization indicators in name
    org_patterns = [
        r'\b(INC|LLC|LLP|CORP|CORPORATION|CO\.?|COMPANY|LIMITED|LTD)\b',
        r'\b(PAC|COMMITTEE|FUND|FOUNDATION|TRUST|ASSOCIATION|SOCIETY)\b',
        r'\b(PARTNERS|GROUP|HOLDINGS|CAPITAL|VENTURES|MANAGEMENT)\b',
        r'\b(UNION|COUNCIL|FEDERATION|COALITION|ALLIANCE)\b',
    ]
    for pattern in org_patterns:
        if re.search(pattern, name):
            return "organization"
    
    # Strong individual indicators
    individual_patterns = [
        r'\b(MR|MRS|MS|DR|MD|PHD|JR|SR|III|IV)\b',  # Titles and suffixes
        r'^[A-Z]+\s+[A-Z]+$',  # FIRSTNAME LASTNAME pattern
        r',\s*[A-Z]+$',  # LASTNAME, FIRSTNAME pattern
    ]
    for pattern in individual_patterns:
        if re.search(pattern, name):
            return "individual"
    
    # Check occupation field - strong individual indicator
    individual_occupations = [
        r'\b(RETIRED|HOMEMAKER|ATTORNEY|LAWYER|PHYSICIAN|DOCTOR|PROFESSOR)\b',
        r'\b(ENGINEER|CONSULTANT|EXECUTIVE|PRESIDENT|CEO|CFO|VP)\b',
        r'\b(TEACHER|EDUCATOR|NURSE|ACCOUNTANT|DENTIST|PHARMACIST)\b',
        r'\b(NOT EMPLOYED|UNEMPLOYED|STUDENT|SELF[\s-]EMPLOYED)\b',
    ]
    for pattern in individual_occupations:
        if re.search(pattern, occupation):
            return "individual"
    
    # If employer is "SELF-EMPLOYED", "RETIRED", or "NOT EMPLOYED" → individual
    self_employer_patterns = [
        r'\b(SELF[\s-]EMPLOYED|RETIRED|NOT EMPLOYED|UNEMPLOYED|N/A|NONE)\b'
    ]
    for pattern in self_employer_patterns:
        if re.search(pattern, employer):
            return "individual"
    
    # Has comma in name (likely LASTNAME, FIRSTNAME) → probably individual
    if ',' in name:
        return "likely_individual"
    
    # Has multiple words but no org indicators → likely individual
    if len(name.split()) >= 2:
        return "likely_individual"
    
    return "unclear"


@asset(
    name="donor_classification",
    description="Enriches donors with donor_type and whale_tier fields for filtering and targeting.",
    group_name="enrichment",
    compute_kind="enrichment",
    deps=["donors"],
)
def donor_classification_asset(
    context: AssetExecutionContext,
    arango: ArangoDBResource,
) -> Output[Dict[str, Any]]:
    """Add donor_type and whale_tier fields to all donors.
    
    Uses server-side AQL UPDATE for efficiency with pattern matching.
    """
    
    with arango.get_client() as client:
        agg_db = client.db("aggregation", username=arango.username, password=arango.password)
        
        if not agg_db.has_collection("donors"):
            raise RuntimeError("donors collection missing - run donors asset first")
        
        # Update all donors with donor_type and whale_tier using server-side AQL
        # This is much faster than fetching and classifying in Python
        # NOTE: Donor documents use canonical_name and canonical_employer fields
        update_aql = """
        FOR d IN donors
            LET name = UPPER(d.canonical_name || "")
            LET employer = UPPER(d.canonical_employer || "")
            LET occupation = UPPER(d.occupation || "")
            LET total = d.total_amount || 0
            
            // Known conduit PAC patterns (WinRed, ActBlue, etc.)
            // These appear in indiv data as earmarked contributions but should be
            // marked as conduits, not individuals. They have proper committee records.
            LET is_conduit = (
                REGEX_TEST(name, "^WINRED") OR
                REGEX_TEST(name, "^ACTBLUE") OR
                REGEX_TEST(name, "EARMARK|CONDUIT") OR
                (name == "WINRED" OR name == "ACTBLUE")
            )
            
            // Strong organization indicators
            LET has_org_suffix = (
                REGEX_TEST(name, "\\\\b(INC|LLC|LLP|CORP|CORPORATION|CO\\\\.?|COMPANY|LIMITED|LTD)\\\\b") OR
                REGEX_TEST(name, "\\\\b(PAC|COMMITTEE|FUND|FOUNDATION|TRUST|ASSOCIATION|SOCIETY)\\\\b") OR
                REGEX_TEST(name, "\\\\b(PARTNERS|GROUP|HOLDINGS|CAPITAL|VENTURES|MANAGEMENT)\\\\b") OR
                REGEX_TEST(name, "\\\\b(UNION|COUNCIL|FEDERATION|COALITION|ALLIANCE)\\\\b")
            )
            
            // Strong individual indicators
            LET has_title = REGEX_TEST(name, "\\\\b(MR|MRS|MS|DR|MD|PHD|JR|SR|III|IV)\\\\b")
            LET has_name_pattern = (
                REGEX_TEST(name, "^[A-Z]+\\\\s+[A-Z]+$") OR
                REGEX_TEST(name, ",\\\\s*[A-Z]+$")
            )
            
            // Occupation-based individual indicators
            LET has_individual_occupation = (
                REGEX_TEST(occupation, "\\\\b(RETIRED|HOMEMAKER|ATTORNEY|LAWYER|PHYSICIAN|DOCTOR|PROFESSOR)\\\\b") OR
                REGEX_TEST(occupation, "\\\\b(ENGINEER|CONSULTANT|EXECUTIVE|PRESIDENT|CEO|CFO|VP)\\\\b") OR
                REGEX_TEST(occupation, "\\\\b(TEACHER|EDUCATOR|NURSE|ACCOUNTANT|DENTIST|PHARMACIST)\\\\b") OR
                REGEX_TEST(occupation, "\\\\b(NOT EMPLOYED|UNEMPLOYED|STUDENT|SELF[\\\\s-]EMPLOYED)\\\\b")
            )
            
            // Employer-based individual indicators
            LET has_self_employer = REGEX_TEST(employer, "\\\\b(SELF[\\\\s-]EMPLOYED|RETIRED|NOT EMPLOYED|UNEMPLOYED|N/A|NONE)\\\\b")
            
            // Has comma (likely LASTNAME, FIRSTNAME)
            LET has_comma = CONTAINS(name, ",")
            
            // Multiple words (likely first + last name)
            LET word_count = LENGTH(SPLIT(name, " "))
            
            // Classification logic
            LET donor_type = (
                // Empty name
                name == "" ? "unclear" :
                // Conduit PACs (WinRed, ActBlue) - special handling
                is_conduit ? "conduit" :
                // Organization patterns
                has_org_suffix ? "organization" :
                // Individual patterns
                has_title OR has_name_pattern OR has_individual_occupation OR has_self_employer ? "individual" :
                // Likely individual (comma or multiple words)
                has_comma OR word_count >= 2 ? "likely_individual" :
                // Default
                "unclear"
            )
            
            // Whale tier classification
            LET whale_tier = (
                total >= 1000000 ? "ultra" :
                total >= 500000 ? "mega" :
                total >= 100000 ? "whale" :
                total >= 50000 ? "notable" :
                null
            )
            
            UPDATE d WITH { 
                donor_type: donor_type,
                whale_tier: whale_tier
            } IN donors
            COLLECT 
                type = donor_type,
                tier = whale_tier
            WITH COUNT INTO cnt
            RETURN {type, tier, cnt}
        """
        
        context.log.info("🔧 Classifying donors with donor_type and whale_tier...")
        result = list(agg_db.aql.execute(update_aql))
        
        # Log donor_type results
        type_stats = {}
        whale_stats = {}
        for r in result:
            dtype = r['type']
            tier = r['tier']
            cnt = r['cnt']
            
            type_stats[dtype] = type_stats.get(dtype, 0) + cnt
            if tier:
                whale_stats[tier] = whale_stats.get(tier, 0) + cnt
        
        total = sum(type_stats.values())
        
        context.log.info(f"✅ Classified {total:,} donors by type:")
        for donor_type, count in sorted(type_stats.items(), key=lambda x: -x[1]):
            pct = count / total * 100
            context.log.info(f"  {donor_type}: {count:,} ({pct:.1f}%)")
        
        total_whales = sum(whale_stats.values())
        context.log.info(f"\n🐋 Identified {total_whales:,} high-value donors:")
        for tier in ["ultra", "mega", "whale", "notable"]:
            count = whale_stats.get(tier, 0)
            if count > 0:
                pct = count / total * 100
                context.log.info(f"  {tier}: {count:,} ({pct:.2f}%)")
        
        # Create indices on donor_type and whale_tier for fast filtering
        collection = agg_db.collection("donors")
        collection.add_persistent_index(fields=["donor_type"], unique=False, sparse=False)
        collection.add_persistent_index(fields=["whale_tier"], unique=False, sparse=True)
        context.log.info("📇 Created indices on donor_type and whale_tier")
        
        return Output(
            value={
                "total_classified": total,
                "by_type": type_stats,
                "by_whale_tier": whale_stats,
                "total_whales": total_whales,
            },
            metadata={
                "total_donors": MetadataValue.int(total),
                "donor_types": MetadataValue.json(type_stats),
                "whale_tiers": MetadataValue.json(whale_stats),
                "individual_count": MetadataValue.int(type_stats.get("individual", 0) + type_stats.get("likely_individual", 0)),
                "organization_count": MetadataValue.int(type_stats.get("organization", 0)),
                "unclear_count": MetadataValue.int(type_stats.get("unclear", 0)),
                "mega_ultra_whales": MetadataValue.int(whale_stats.get("mega", 0) + whale_stats.get("ultra", 0)),
            }
        )
