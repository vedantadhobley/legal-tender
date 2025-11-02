# Understanding Upstream Money Tracing - Data Flow Guide

## The Problem You Asked About

> "where are they going to be stored and how are we going to be able to query to the root money"

Let me show you EXACTLY how the money flows and where it's stored:

---

## 🔄 THE COMPLETE MONEY CHAIN

```
┌─────────────────────────────────────────────────────────────────────┐
│ LEVEL 0: ULTIMATE SOURCE (Root Money)                              │
│ ═══════════════════════════════════════════════════════════════════ │
│                                                                     │
│  ┌──────────────────┐   ┌──────────────────┐   ┌───────────────┐  │
│  │ Koch Industries  │   │ Elon Musk        │   │ Citadel LLC   │  │
│  │ (Corporation)    │   │ (Billionaire)    │   │ (Hedge Fund)  │  │
│  └────────┬─────────┘   └────────┬─────────┘   └───────┬───────┘  │
│           │                      │                      │           │
│           │ $2M                  │ $75M                 │ $1.5M     │
│           ▼                      ▼                      ▼           │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ LEVEL 1: INTERMEDIARIES (Super PACs, Party Committees)             │
│ ═══════════════════════════════════════════════════════════════════ │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐ │
│  │ SENATE LEADERSHIP FUND (Super PAC)                            │ │
│  │ Committee ID: C00571703                                        │ │
│  │                                                                 │ │
│  │ ⬆️ UPSTREAM (WHO FUNDS THEM):                                  │ │
│  │   • Koch Industries PAC → $2M                                  │ │
│  │   • Elon Musk → $75M                                           │ │
│  │   • Citadel PAC → $1.5M                                        │ │
│  │   Total: $78.5M                                                │ │
│  │                                                                 │ │
│  │ ⬇️ DOWNSTREAM (WHERE IT GOES):                                 │ │
│  │   • Ted Cruz → $5M                                             │ │
│  │   • Marco Rubio → $4M                                          │ │
│  │   • Josh Hawley → $3M                                          │ │
│  └───────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ LEVEL 2: FINAL DESTINATION (Politicians)                           │
│ ═══════════════════════════════════════════════════════════════════ │
│                                                                     │
│  ┌───────────────────┐   ┌───────────────────┐   ┌──────────────┐ │
│  │ Ted Cruz          │   │ Marco Rubio       │   │ Josh Hawley  │ │
│  │ bioguide: C001118 │   │ bioguide: R000595 │   │ bioguide: H.. │ │
│  │                   │   │                   │   │              │ │
│  │ Received $5M from │   │ Received $4M from │   │ Received $3M │ │
│  │ Senate Leadership │   │ Senate Leadership │   │ from SLF     │ │
│  │ Fund              │   │ Fund              │   │              │ │
│  └───────────────────┘   └───────────────────┘   └──────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📊 WHERE THE DATA IS STORED

### 1️⃣ **Upstream Data** (NEW - what we just built!)

**Collection**: `enriched_{cycle}.committee_funding_sources`

**Example Document** (Senate Leadership Fund):
```javascript
{
  _id: "C00571703",
  committee_id: "C00571703",
  committee_name: "SENATE LEADERSHIP FUND",
  committee_type: "O",  // Super PAC
  cycle: "2024",
  
  // ⬆️ WHO FUNDS THIS COMMITTEE (UPSTREAM)
  funding_sources: {
    // Corporate/Political PACs funding them
    from_committees: [
      {
        committee_id: "C00123456",
        committee_name: "Koch Industries PAC",
        committee_type: "Q",  // Traditional PAC
        total_amount: 2000000,
        transaction_count: 4,
        transactions: [
          {amount: 500000, date: "2024-01-15", transaction_id: "..."},
          {amount: 500000, date: "2024-03-20", transaction_id: "..."},
          // ... top 10 transactions
        ]
      },
      {
        committee_id: "C00789012",
        committee_name: "Citadel PAC",
        total_amount: 1500000,
        // ...
      }
    ],
    
    // Billionaires/Major donors funding them
    from_individuals: [
      {
        contributor_name: "MUSK, ELON",
        employer: "TESLA INC",
        occupation: "CEO",
        total_amount: 75000000,
        contribution_count: 3,
        contributions: [
          {amount: 25000000, date: "2024-02-01"},
          {amount: 25000000, date: "2024-06-01"},
          {amount: 25000000, date: "2024-09-01"}
        ]
      }
    ],
    
    // Corporations funding them directly
    from_organizations: [
      {
        organization_name: "AMAZON.COM",
        total_amount: 500000,
        contribution_count: 2
      }
    ]
  },
  
  transparency: {
    total_disclosed: 78500000,
    from_committees: 3500000,
    from_individuals: 75000000,
    from_organizations: 0,
    transparency_score: 0.98,
    
    red_flags: [
      {
        type: "shell_game",
        description: "Receives from 5 other Super PACs (layering)",
        severity: "medium"
      }
    ]
  }
}
```

---

### 2️⃣ **Downstream Data** (Already exists)

**Collection**: `aggregation.donor_financials`

**Example Document** (Senate Leadership Fund):
```javascript
{
  _id: "C00571703",
  committee_id: "C00571703",
  committee_name: "SENATE LEADERSHIP FUND",
  committee_type: "O",
  
  // ⬇️ WHERE THEY SPEND MONEY (DOWNSTREAM)
  total_spent: 85000000,
  total_to_candidates: 45000000,
  
  by_cycle: {
    "2024": {
      independent_expenditures: {
        support: {
          candidates: [
            {
              candidate_id: "S2TX00312",
              candidate_name: "CRUZ, TED",
              total_amount: 5000000,
              transaction_count: 150
            },
            {
              candidate_id: "S6FL00158",
              candidate_name: "RUBIO, MARCO",
              total_amount: 4000000
            }
          ]
        }
      }
    }
  }
}
```

---

### 3️⃣ **Politician Data** (Already exists)

**Collection**: `aggregation.candidate_financials`

**Example Document** (Ted Cruz):
```javascript
{
  _id: "C001118",  // bioguide_id
  bioguide_id: "C001118",
  candidate_ids: ["S2TX00312", "S8TX00567"],  // All his campaign IDs
  candidate_name: "CRUZ, TED",
  
  // ⬇️ MONEY HE RECEIVED (DOWNSTREAM - from his perspective)
  total_independent_support: 15000000,
  
  totals: {
    independent_expenditures: {
      support: {
        committees: [
          {
            committee_id: "C00571703",
            committee_name: "SENATE LEADERSHIP FUND",
            total_amount: 5000000
          },
          // ... other Super PACs
        ]
      }
    }
  }
}
```

---

## 🔍 HOW TO QUERY THE ROOT MONEY

### Query 1: Find WHO funds Senate Leadership Fund (UPSTREAM)

```javascript
// Get the funding sources
db.enriched_2024.committee_funding_sources.findOne({
  "committee_name": /SENATE LEADERSHIP/i
})

// Result shows:
// - from_committees: [Koch Industries PAC, Citadel PAC, ...]
// - from_individuals: [Elon Musk, ...]
// - from_organizations: [Amazon, ...]
```

**This is the ROOT MONEY - the ultimate source!** 🎯

---

### Query 2: Trace money from Koch Industries → Politicians

```javascript
// Step 1: Find committees Koch Industries funds
db.enriched_2024.committee_funding_sources.find({
  "funding_sources.from_committees.committee_name": /KOCH INDUSTRIES/i
})

// Result: [Senate Leadership Fund, Congressional Leadership Fund, ...]

// Step 2: For each committee, see which politicians they support
db.aggregation.donor_financials.find({
  "_id": {$in: ["C00571703", "C00..."]}  // Committee IDs from Step 1
})

// Result: Shows all politicians these committees funded
```

**This traces Koch → Super PAC → Politicians!**

---

### Query 3: Complete chain for ONE politician (e.g., Ted Cruz)

```javascript
// Full aggregation pipeline
db.candidate_financials.aggregate([
  // Find Ted Cruz
  {$match: {bioguide_id: "C001118"}},
  
  // Get committees that supported him
  {$project: {
    name: "$candidate_name",
    supporting_committees: "$totals.independent_expenditures.support.committees"
  }},
  
  // Unwind to get one doc per committee
  {$unwind: "$supporting_committees"},
  
  // Join to committee funding sources (UPSTREAM)
  {$lookup: {
    from: "committee_funding_sources",
    localField: "supporting_committees.committee_id",
    foreignField: "_id",
    as: "committee_funding"
  }},
  
  // Now we have:
  // Cruz ← Senate Leadership Fund ← [Koch Industries, Elon Musk, ...]
  
  {$unwind: "$committee_funding"},
  
  // Show the full chain
  {$project: {
    politician: "$name",
    amount_received: "$supporting_committees.total_amount",
    intermediary: "$committee_funding.committee_name",
    ultimate_sources: {
      committees: "$committee_funding.funding_sources.from_committees",
      individuals: "$committee_funding.funding_sources.from_individuals"
    }
  }}
])
```

**Result**:
```javascript
{
  politician: "CRUZ, TED",
  amount_received: 5000000,
  intermediary: "SENATE LEADERSHIP FUND",
  ultimate_sources: {
    committees: [
      {committee_name: "Koch Industries PAC", total_amount: 2000000},
      {committee_name: "Citadel PAC", total_amount: 1500000}
    ],
    individuals: [
      {contributor_name: "MUSK, ELON", total_amount: 75000000}
    ]
  }
}
```

**THIS IS THE COMPLETE CHAIN!** 🎉

---

## 🎯 SIMPLE SUMMARY

**3 Collections Working Together:**

1. **`committee_funding_sources`** (NEW!)
   - Shows WHO funds each committee (UPSTREAM)
   - Koch Industries → Senate Leadership Fund
   - Elon Musk → America PAC
   - **This is the ROOT MONEY source**

2. **`donor_financials`** (Already exists)
   - Shows WHERE each committee spends (DOWNSTREAM)
   - Senate Leadership Fund → Ted Cruz
   - America PAC → Trump

3. **`candidate_financials`** (Already exists)
   - Shows WHAT each politician receives
   - Ted Cruz ← $5M from Senate Leadership Fund

**Combined Query**:
- Start at politician (candidate_financials)
- JOIN to committees that funded them (donor_financials)
- JOIN to who funded those committees (committee_funding_sources) ← **ROOT MONEY HERE!**

---

## 🚀 NEXT STEP

In **Step 2** (next ~1 hour of work), we'll **merge** `committee_funding_sources` INTO `donor_financials` so you can query it all from ONE place:

```javascript
db.donor_financials.findOne({"_id": "C00571703"})

// Will return:
{
  committee_name: "SENATE LEADERSHIP FUND",
  
  // ⬆️ UPSTREAM (NEW - from committee_funding_sources)
  funding: {
    from_corporate_pacs: 70000000,
    from_billionaires: 75000000,
    ultimate_sources: [
      {organization: "KOCH INDUSTRIES", amount: 2000000},
      {name: "MUSK, ELON", amount: 75000000}
    ]
  },
  
  // ⬇️ DOWNSTREAM (already exists)
  total_to_candidates: 45000000,
  spending: {
    to_ted_cruz: 5000000,
    to_marco_rubio: 4000000
  }
}
```

**Then you can query ROOT MONEY → FINAL DESTINATION in ONE query!** 🎯

---

## Does this make sense now?

The data is stored in **3 layers**:
1. Root sources (`committee_funding_sources`) ← **NEW**
2. Intermediaries (`donor_financials`) ← Already exists
3. Final recipients (`candidate_financials`) ← Already exists

You **JOIN** them together to trace the complete money chain! 🔗
