#!/bin/bash
#
# Legal Tender CLI Query Tool
# Query candidate funding data from the ArangoDB graph database
#
# Usage:
#   ./query.sh --candidate "Ted Cruz" --pies
#   ./query.sh --bioguide C001098 --pies
#   ./query.sh --fec S2TX00312 --pies
#   ./query.sh --candidate "Donald Trump" --summary
#   ./query.sh --list-members
#

set -e

# ArangoDB connection settings
ARANGO_HOST="${ARANGO_HOST:-localhost}"
ARANGO_PORT="${ARANGO_PORT:-4201}"
ARANGO_USER="${ARANGO_USER:-root}"
ARANGO_PASS="${ARANGO_PASS:-ltpass}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Parse arguments
CANDIDATE=""
BIOGUIDE=""
FEC_ID=""
ACTION="pies"  # default action
LIST_MEMBERS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --candidate|-c)
            CANDIDATE="$2"
            shift 2
            ;;
        --bioguide|-b)
            BIOGUIDE="$2"
            shift 2
            ;;
        --fec|-f)
            FEC_ID="$2"
            shift 2
            ;;
        --pies)
            ACTION="pies"
            shift
            ;;
        --summary)
            ACTION="summary"
            shift
            ;;
        --list-members)
            LIST_MEMBERS=true
            shift
            ;;
        --help|-h)
            echo "Legal Tender CLI Query Tool"
            echo ""
            echo "Usage:"
            echo "  ./query.sh --candidate \"Ted Cruz\" --pies"
            echo "  ./query.sh --bioguide C001098 --pies"
            echo "  ./query.sh --fec S2TX00312 --summary"
            echo "  ./query.sh --list-members"
            echo ""
            echo "Options:"
            echo "  --candidate, -c    Search by candidate name (uses member_fec_mapping)"
            echo "  --bioguide, -b     Search by bioguide ID (e.g., C001098)"
            echo "  --fec, -f          Search by FEC candidate ID (e.g., S2TX00312)"
            echo "  --pies             Show 5 pies analysis (default)"
            echo "  --summary          Show funding summary"
            echo "  --list-members     List all Congress members in database"
            echo ""
            echo "Environment variables:"
            echo "  ARANGO_HOST        ArangoDB host (default: localhost)"
            echo "  ARANGO_PORT        ArangoDB port (default: 4201)"
            echo "  ARANGO_USER        ArangoDB user (default: root)"
            echo "  ARANGO_PASS        ArangoDB password (default: ltpass)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Function to run ArangoDB query
run_aql() {
    local script="$1"
    echo "$script" | docker exec -i legal-tender-dev-arango arangosh \
        --server.username "$ARANGO_USER" \
        --server.password "$ARANGO_PASS" \
        --console.colors false \
        --quiet 2>/dev/null | grep -v "^$" | grep -v "^true$" | grep -v "wiederluege" | grep -v "再见" | grep -v "להתראות" | grep -v "Arrivederci"
}

# List all Congress members
if [ "$LIST_MEMBERS" = true ]; then
    echo -e "${CYAN}=== Congress Members in Database ===${NC}"
    run_aql '
var db = require("@arangodb").db;
db._useDatabase("aggregation");
var members = db._query(`
  FOR m IN member_fec_mapping
  SORT m.name.last, m.name.first
  RETURN {
    bioguide: m._key,
    name: CONCAT(m.name.first, " ", m.name.last),
    state: m.terms[-1].state,
    party: m.terms[-1].party,
    fec_ids: m.fec.candidate_ids
  }
`).toArray();
print("Found " + members.length + " Congress members\n");
members.forEach(function(m) {
  print(m.bioguide + " | " + m.name + " (" + m.party + "-" + m.state + ") | FEC: " + m.fec_ids.join(", "));
});
'
    exit 0
fi

# Validate input
if [ -z "$CANDIDATE" ] && [ -z "$BIOGUIDE" ] && [ -z "$FEC_ID" ]; then
    echo -e "${RED}Error: Must specify --candidate, --bioguide, or --fec${NC}"
    echo "Use --help for usage information"
    exit 1
fi

# Build the query script
QUERY_SCRIPT='
var db = require("@arangodb").db;
db._useDatabase("aggregation");

function formatMoney(amount) {
  if (amount >= 1000000000) return "$" + (amount/1000000000).toFixed(2) + "B";
  if (amount >= 1000000) return "$" + (amount/1000000).toFixed(2) + "M";
  if (amount >= 1000) return "$" + (amount/1000).toFixed(1) + "K";
  return "$" + amount.toFixed(0);
}
'

# Add candidate resolution logic
if [ -n "$CANDIDATE" ]; then
    QUERY_SCRIPT+="
var searchName = \"$CANDIDATE\".toUpperCase();

// First try member_fec_mapping (Congress members)
var member = db._query(\`
  FOR m IN member_fec_mapping
  LET fullName = CONCAT(UPPER(m.name.first), \" \", UPPER(m.name.last))
  FILTER CONTAINS(fullName, @name) OR CONTAINS(@name, UPPER(m.name.last))
  RETURN m
\`, {name: searchName}).toArray()[0];

var candIds = [];
var cmteIds = [];
var candName = \"\";

if (member) {
  candIds = member.fec.candidate_ids;
  cmteIds = member.fec.committee_ids;
  candName = member.name.first + \" \" + member.name.last;
  print(\"Found Congress member: \" + candName + \" (\" + member._key + \")\");
} else {
  // Fall back to candidates collection
  var cand = db._query(\`
    FOR c IN candidates
    FILTER CONTAINS(UPPER(c.CAND_NAME), @name)
    RETURN c
  \`, {name: searchName}).toArray()[0];
  
  if (cand) {
    candIds = [cand.CAND_ID];
    candName = cand.CAND_NAME;
    print(\"Found candidate: \" + candName);
  } else {
    print(\"ERROR: Candidate not found: $CANDIDATE\");
    throw new Error(\"Candidate not found\");
  }
}
"
elif [ -n "$BIOGUIDE" ]; then
    QUERY_SCRIPT+="
var member = db.member_fec_mapping.document(\"$BIOGUIDE\");
if (!member) {
  print(\"ERROR: Bioguide ID not found: $BIOGUIDE\");
  throw new Error(\"Bioguide not found\");
}
var candIds = member.fec.candidate_ids;
var cmteIds = member.fec.committee_ids;
var candName = member.name.first + \" \" + member.name.last;
print(\"Found: \" + candName + \" (\" + member._key + \")\");
"
else
    QUERY_SCRIPT+="
var candIds = [\"$FEC_ID\"];
var cmteIds = [];
var cand = db._query(\"FOR c IN candidates FILTER c.CAND_ID == @id RETURN c\", {id: \"$FEC_ID\"}).toArray()[0];
var candName = cand ? cand.CAND_NAME : \"$FEC_ID\";
print(\"Found: \" + candName);
"
fi

# Add the analysis logic based on action
if [ "$ACTION" = "pies" ]; then
    QUERY_SCRIPT+='

// Get ALL affiliated committees
var affiliatedCmtes = db._query(`
  FOR cand_id IN @cands 
  FOR a IN affiliated_with 
  FILTER a._to == CONCAT("candidates/", cand_id) 
  RETURN DISTINCT PARSE_IDENTIFIER(a._from).key
`, {cands: candIds}).toArray();

// Merge with known committee IDs
cmteIds.forEach(function(c) { if (affiliatedCmtes.indexOf(c) < 0) affiliatedCmtes.push(c); });

print("\nCandidate IDs: " + candIds.join(", "));
print("Affiliated committees: " + affiliatedCmtes.length);

print("\n======================================================================");
print("                    FIVE PIES ANALYSIS: " + candName);
print("======================================================================\n");

// PIE 1: Direct Donations
print("PIE 1: DIRECT DONATIONS (donor → campaign committee)");
print("----------------------------------------------------------------------");
var directDonations = db._query(`
  FOR cmte_id IN @cmtes
    FOR c IN contributed_to
      FILTER c._to == CONCAT("committees/", cmte_id)
      FOR d IN donors FILTER d._id == c._from
      COLLECT donor_name = d.canonical_name, employer = d.canonical_employer
      AGGREGATE total = SUM(c.total_amount)
      SORT total DESC
      RETURN {name: donor_name, employer: employer, amount: total}
`, {cmtes: affiliatedCmtes}).toArray();

var directTotal = directDonations.reduce(function(s, d) { return s + d.amount; }, 0);
print("Total: " + formatMoney(directTotal) + " from " + directDonations.length + " donors\n");
print("Top 15 donors:");
directDonations.slice(0, 15).forEach(function(d, i) {
  print("  " + (i+1) + ". " + d.name + " (" + (d.employer || "N/A") + "): " + formatMoney(d.amount));
});

// PIE 2: PAC Transfers
print("\n\nPIE 2: PAC TRANSFERS (committee → campaign)");
print("----------------------------------------------------------------------");
var pacTransfers = db._query(`
  FOR cmte_id IN @cmtes
    FOR t IN transferred_to
      FILTER t._to == CONCAT("committees/", cmte_id)
      FOR src IN committees FILTER src._id == t._from
      COLLECT source_name = src.CMTE_NM, source_id = src.CMTE_ID
      AGGREGATE total = SUM(t.total_amount)
      SORT total DESC
      RETURN {name: source_name, id: source_id, amount: total}
`, {cmtes: affiliatedCmtes}).toArray();

var pacTotal = pacTransfers.reduce(function(s, t) { return s + t.amount; }, 0);
print("Total: " + formatMoney(pacTotal) + " from " + pacTransfers.length + " committees\n");
print("Top 15 PAC sources:");
pacTransfers.slice(0, 15).forEach(function(t, i) {
  print("  " + (i+1) + ". " + t.name + ": " + formatMoney(t.amount));
});

// PIE 3: IE Support
print("\n\nPIE 3: INDEPENDENT EXPENDITURE SUPPORT (Super PACs FOR candidate)");
print("----------------------------------------------------------------------");
var ieSupport = db._query(`
  FOR cand_id IN @cands
    FOR e IN spent_on
      FILTER e._to == CONCAT("candidates/", cand_id)
      FILTER e.support_oppose == "S"
      FOR src IN committees FILTER src._id == e._from
      COLLECT source_name = src.CMTE_NM
      AGGREGATE total = SUM(e.total_amount)
      SORT total DESC
      RETURN {name: source_name, amount: total}
`, {cands: candIds}).toArray();

var ieSupportTotal = ieSupport.reduce(function(s, e) { return s + e.amount; }, 0);
print("Total: " + formatMoney(ieSupportTotal) + " from " + ieSupport.length + " committees\n");
print("Top 15 IE supporters:");
ieSupport.slice(0, 15).forEach(function(e, i) {
  print("  " + (i+1) + ". " + e.name + ": " + formatMoney(e.amount));
});

// PIE 4: IE Opposition
print("\n\nPIE 4: INDEPENDENT EXPENDITURE OPPOSITION (Super PACs AGAINST candidate)");
print("----------------------------------------------------------------------");
var ieOppose = db._query(`
  FOR cand_id IN @cands
    FOR e IN spent_on
      FILTER e._to == CONCAT("candidates/", cand_id)
      FILTER e.support_oppose == "O"
      FOR src IN committees FILTER src._id == e._from
      COLLECT source_name = src.CMTE_NM
      AGGREGATE total = SUM(e.total_amount)
      SORT total DESC
      RETURN {name: source_name, amount: total}
`, {cands: candIds}).toArray();

var ieOpposeTotal = ieOppose.reduce(function(s, e) { return s + e.amount; }, 0);
print("Total: " + formatMoney(ieOpposeTotal) + " from " + ieOppose.length + " committees\n");
print("Top 15 IE opposition:");
ieOppose.slice(0, 15).forEach(function(e, i) {
  print("  " + (i+1) + ". " + e.name + ": " + formatMoney(e.amount));
});

// Summary
print("\n\n======================================================================");
print("                           SUMMARY");
print("======================================================================\n");
var proTotal = directTotal + pacTotal + ieSupportTotal;
print("PRO-" + candName.split(",")[0].split(" ").pop() + " Total: " + formatMoney(proTotal));
print("  ├── Pie 1 (Direct):     " + formatMoney(directTotal) + " (" + (directTotal/proTotal*100).toFixed(1) + "%)");
print("  ├── Pie 2 (PAC):        " + formatMoney(pacTotal) + " (" + (pacTotal/proTotal*100).toFixed(1) + "%)");
print("  └── Pie 3 (IE Support): " + formatMoney(ieSupportTotal) + " (" + (ieSupportTotal/proTotal*100).toFixed(1) + "%)\n");
print("ANTI-" + candName.split(",")[0].split(" ").pop() + " Total: " + formatMoney(ieOpposeTotal));
print("  └── Pie 4 (IE Oppose):  " + formatMoney(ieOpposeTotal));
'
elif [ "$ACTION" = "summary" ]; then
    QUERY_SCRIPT+='

// Get affiliated committees
var affiliatedCmtes = db._query(`
  FOR cand_id IN @cands 
  FOR a IN affiliated_with 
  FILTER a._to == CONCAT("candidates/", cand_id) 
  RETURN DISTINCT PARSE_IDENTIFIER(a._from).key
`, {cands: candIds}).toArray();

cmteIds.forEach(function(c) { if (affiliatedCmtes.indexOf(c) < 0) affiliatedCmtes.push(c); });

print("\n======================================================================");
print("                    FUNDING SUMMARY: " + candName);
print("======================================================================\n");

// Quick totals
var directTotal = db._query(`
  FOR cmte_id IN @cmtes
    FOR c IN contributed_to
      FILTER c._to == CONCAT("committees/", cmte_id)
      RETURN c.total_amount
`, {cmtes: affiliatedCmtes}).toArray().reduce(function(s, a) { return s + a; }, 0);

var pacTotal = db._query(`
  FOR cmte_id IN @cmtes
    FOR t IN transferred_to
      FILTER t._to == CONCAT("committees/", cmte_id)
      RETURN t.total_amount
`, {cmtes: affiliatedCmtes}).toArray().reduce(function(s, a) { return s + a; }, 0);

var ieSupport = db._query(`
  FOR cand_id IN @cands
    FOR e IN spent_on
      FILTER e._to == CONCAT("candidates/", cand_id) AND e.support_oppose == "S"
      RETURN e.total_amount
`, {cands: candIds}).toArray().reduce(function(s, a) { return s + a; }, 0);

var ieOppose = db._query(`
  FOR cand_id IN @cands
    FOR e IN spent_on
      FILTER e._to == CONCAT("candidates/", cand_id) AND e.support_oppose == "O"
      RETURN e.total_amount
`, {cands: candIds}).toArray().reduce(function(s, a) { return s + a; }, 0);

var proTotal = directTotal + pacTotal + ieSupport;

print("Candidate IDs: " + candIds.join(", "));
print("Affiliated Committees: " + affiliatedCmtes.length + "\n");

print("PRO Funding:  " + formatMoney(proTotal));
print("  Direct:     " + formatMoney(directTotal));
print("  PAC:        " + formatMoney(pacTotal));
print("  IE Support: " + formatMoney(ieSupport) + "\n");

print("ANTI Funding: " + formatMoney(ieOppose));
print("  IE Oppose:  " + formatMoney(ieOppose));
'
fi

# Run the query
echo ""
run_aql "$QUERY_SCRIPT"
echo ""
