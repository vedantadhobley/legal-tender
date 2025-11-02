# pas2.zip Data Model - CRITICAL Understanding

## The Fundamental Mistake We Made

We initially assumed pas2 contains transactions from the **RECIPIENT's** perspective (money IN). 

**THIS IS WRONG.**

pas2 contains transactions from the **FILER's** perspective (money OUT).

## Correct Data Model

### Field Meanings:
- **CMTE_ID** = Committee FILING the report (the DONOR/GIVER)
- **NAME** = Recipient's name (or attributed individual name)
- **OTHER_ID** = Recipient's FEC ID (when recipient is a committee)
- **TRANSACTION_TP = '24K'** = "Contribution to nonaffiliated committee" (money OUT)
- **ENTITY_TP** = Type of the transaction/recipient

### Example Transaction:
```javascript
{
  CMTE_ID: 'C00039578',                    // Council of Insurance Agents PAC (DONOR)
  NAME: 'JASON SMITH FOR CONGRESS',         // Recipient name
  OTHER_ID: 'C00541862',                    // Recipient's FEC ID
  ENTITY_TP: 'CCM',                         // Candidate Committee
  TRANSACTION_TP: '24K',                    // Contribution OUT
  TRANSACTION_AMT: 5000
}
```

**Meaning**: Council of Insurance Agents PAC **GAVE** $5,000 **TO** Jason Smith For Congress

## Tracing UPSTREAM Funding (Who Gave TO a Committee)

To find who gave money TO a committee, you must query:

```javascript
db.itpas2.find({
  OTHER_ID: "C00541862",  // The committee RECEIVING money
  TRANSACTION_TP: "24K",
  ENTITY_TP: {$in: ["COM", "PAC", "PTY", "CCM"]}
})
```

Then:
- **CMTE_ID** = The committee that GAVE money (upstream donor)
- **OTHER_ID** = The committee that RECEIVED money (your target)

## Entity Type Behaviors

### ENTITY_TP = "COM", "PAC", "PTY", "CCM" (Committees)
- **CMTE_ID** = Committee making contribution (DONOR)
- **OTHER_ID** = Committee receiving contribution (RECIPIENT)
- **NAME** = Recipient's name
- These are straightforward committee→committee transfers

### ENTITY_TP = "IND" (Individual - **MISLEADING NAME!**)
- **CMTE_ID** = PAC making contribution (DONOR)
- **OTHER_ID** = Candidate receiving contribution (RECIPIENT)
- **NAME** = Individual the PAC attributes the money to
- **NOT** direct individual→committee donations!
- These are PAC→Candidate transfers attributed to individual PAC members
- Example: "Steel Manufacturers PAC gave $1,000 to Terri Sewell, attributed to Philip Bell"

### ENTITY_TP = "ORG" (Organization - **ALSO MISLEADING!**)
- **CMTE_ID** = Committee making contribution (DONOR)
- **OTHER_ID** = Committee receiving contribution (RECIPIENT)
- **NAME** = Organization name or recipient name
- These are committee→committee transfers with organizational attribution
- Often earmarked contributions (e.g., "IAO PROPERTY HOLDINGS LLC" via PAC to candidate)
- **NOT** direct organization→committee contributions!

## What pas2 DOES NOT Contain

1. **Direct Individual → Committee Donations**
   - Need `itcont.zip` for true individual contributions TO committees
   - pas2 only has PAC-attributed individual names

2. **Direct Organization → Committee Contributions**
   - Need different data source for true corporate contributions
   - pas2 only has committee→committee transfers with org attribution

3. **Money Received By Committees**
   - pas2 is from FILER perspective (money OUT)
   - To trace upstream, query WHERE recipient matches target committee

## Impact on Our Code

### WRONG (Original Code):
```python
recipient_id = txn.get('CMTE_ID')  # ❌ This is the DONOR
donor_id = txn.get('OTHER_ID')     # ❌ This is the RECIPIENT
```

### CORRECT (Fixed Code):
```python
donor_id = txn.get('CMTE_ID')      # ✅ Committee GIVING money
recipient_id = txn.get('OTHER_ID') # ✅ Committee RECEIVING money
```

## Verification Queries

### Find who gave TO a committee:
```javascript
// Who gave money TO Jason Smith For Congress?
db.itpas2.find({
  OTHER_ID: "C00541862",
  TRANSACTION_TP: "24K"
}).forEach(doc => {
  print(`${doc.CMTE_ID} gave $${doc.TRANSACTION_AMT} to ${doc.NAME}`);
});
```

### Find who a committee gave TO:
```javascript
// Who did Congressional Leadership Fund give TO?
db.itpas2.find({
  CMTE_ID: "C00504530",
  TRANSACTION_TP: "24K"
}).forEach(doc => {
  print(`CLF gave $${doc.TRANSACTION_AMT} to ${doc.NAME} (${doc.OTHER_ID})`);
});
```

## Lessons Learned

1. **READ FEC DOCUMENTATION CAREFULLY** - "recipient" can mean different things in different contexts
2. **VERIFY WITH SAMPLE DATA** - Always query actual data to understand structure
3. **FEC ENTITY_TP IS MISLEADING** - "IND" doesn't mean individual donors, "ORG" doesn't mean org donors
4. **pas2 = FILER PERSPECTIVE** - Always remember this is money OUT, not money IN
5. **24K = Contributions TO committees** - This is the key filter for money flow

## Transaction Type Reference

| Code | Description | Direction | Use For |
|------|-------------|-----------|---------|
| **24K** | Contribution to nonaffiliated committee | OUT | Committee transfers |
| **24A** | Independent expenditure opposing | OUT | Anti-candidate spending |
| **24E** | Independent expenditure advocating | OUT | Pro-candidate spending |

## References

- FEC pas2.zip documentation: [FEC Bulk Data](https://www.fec.gov/data/browse-data/?tab=bulk-data)
- Transaction type codes: [FEC Transaction Types](https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/)
- Our analysis: `docs/DATA_QUALITY_NOTES.md`
