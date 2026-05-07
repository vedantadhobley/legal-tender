
### Links

Downloads: https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/index.html

Documentation: https://www.fec.gov/data/browse-data/?tab=bulk-data

https://www.fec.gov/files/bulk-downloads/data_dictionaries/cn_header_file.csv

### Files

File|Purpose|Notes
|---|---|---|
cn.zip|Candidate Master File|The candidate master file contains one record for each candidate who has either registered with the Federal Election Commission or appeared on a ballot list prepared by a state elections office.
ccl.zip|Candidate-Committee Linkages|This file contains one record for each candidate to committee linkage.
cm.zip|Committee Master File|The committee master file contains one record for each committee registered with the Federal Election Commission. This includes federal political action committees and party committees, campaign committees for presidential, house and senate candidates, as well as groups or organizations who are spending money for or against candidates for federal office.
webl.zip|Committee Summary|These files contain one record for each campaign. This record contains summary financial information.
weball.zip|Candidate Summary|The all candidate summary file contains one record including summary financial information for all candidates who raised or spent money during the period no matter when they are up for election.
webk.zip|PAC Summary|This file gives overall receipts and disbursements for each PAC and party committee registered with the commission, along with a breakdown of overall receipts by source and totals for contributions to other committees, independent expenditures made and other information.
pas2.zip|Committee Transfers|The itemized committee contributions file contains each contribution or independent expenditure made by a PAC, party committee, candidate committee, or other federal committee to a candidate during the two-year election cycle.
independent_expenditure.csv|Super PAC spending (for/against candidates)|Only use if you want to track independent expenditures. Could be skipped if your focus is corporate/PAC totals.
oppexp.zip|Historical oppexp data|This file contains disbursements reported on FEC Form 3 Line 17, FEC Form 3P Line 23and FEC Form 3X Lines 21(a)(i), 21(a)(ii) and 21(b).

---
---
---

### Structure

```
legal-tender/
└── data/
    ├── fec/
    │   ├── 20xx/
    │   │   ├── cn.zip
    │   │   ├── ccl.zip
    │   │   ├── cm.zip
    │   │   ├── webl.zip
    │   │   ├── weball.zip
    │   │   ├── webk.zip
    │   │   ├── pas2.zip
    │   │   ├── independent_expenditure.csv
    │   │   └── oppexp.zip
    │   └── ...
    └── ...
```

---
---
---

### cn.zip
<!-- 
Column name|Field name|Position|Null|Data type|Description|Example data
|---|---|---|---|---|---|---|
CAND_ID|Candidate identification|1|N|VARCHAR2 (9)||H8VA01233
CAND_NAME|Candidate name|2|Y|VARCHAR2(200)||Martha Washington
CAND_ICI|Incumbent challenger status|3|Y|VARCHAR2(1)||I
PTY_CD|Party code|4|Y|VARCHAR2(1)||NON
CAND_PTY_AFFILIATION|Party affiliation|5|Y|VARCHAR2(3)||NON
TTL_RECEIPTS|Total receipts|6|Y|Number(14,2)||345,456.34
TRANS_FROM_AUTH|Transfers from authorized committees|7|Y|Number(14,2)||4000.00
TTL_DISB|Total disbursements|8|Y|Number(14,2)||175645.21
TRANS_TO_AUTH|Transfers to authorized committees|9|Y|Number(14,2)||0.00
COH_BOP|Beginning cash|10|Y|Number(14,2)||845901.23
COH_COP|Ending cash|11|Y|Number(14,2)||915671.43
CAND_CONTRIB|Contributions from candidate|12|Y|Number(14,2)||500.00
CAND_LOANS|Loans from candidate|13|Y|Number(14,2)||250000.00
OTHER_LOANS|Other loans|14|Y|Number(14,2)||0.00
CAND_LOAN_REPAY|Candidate loan repayments|15|Y|Number(14,2)||100000.00
OTHER_LOAN_REPAY|Other loan repayments|16|Y|Number(14,2)||0.00
DEBTS_OWED_BY|Debts owed by|17|Y|Number(14,2)||250.00
TTL_INDIV_CONTRIB|Total individual contributions|18|Y|Number(14,2)||450000.00
CAND_OFFICE_ST|Candidate state|19|Y|VARCHAR2(2||VA
CAND_OFFICE_DISTRICT|Candidate district|20|Y|VARCHAR2(2)||01
SPEC_ELECTION|Special election status|21|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|W
PRIM_ELECTION|Primary election status|22|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|L
RUN_ELECTION|Runoff election status|23|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|L
GEN_ELECTION|General election status|24|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|W
GEN_ELECTION_PRECENT|General election percentage|25|Y|Number(7,4)|Election result data included in 1996-2006 files only.|63.2
OTHER_POL_CMTE_CONTRIB|Contributions from other political committees|26|Y|Number(14,2)||200000.00
POL_PTY_CONTRIB|Contributions from party committees|27|Y|Number(14,2)||200000.00
CVG_END_DT|Coverage end date|28|Y|DATE(MM/DD/YYYY)|Through date|10/19/2018
INDIV_REFUNDS|Refunds to individuals|29|Y|Number(14,2)||4000.00
CMTE_REFUNDS|Refunds to committees|30|Y|Number(14,2)||100.00
 -->

Column name|Field name|Position|Null|Data type|Description|Example data
|---|---|---|---|---|---|---|
CAND_ID|Candidate identification|1|N|VARCHAR2(9)|A 9-character alpha-numeric code assigned to a candidate by the Federal Election Commission. The candidate ID for a specific candidate remains the same across election cycles as long as the candidate is running for the same office.|H8VA01233
CAND_NAME|Candidate name|2|Y|VARCHAR2(200)||Martha Washington
CAND_PTY_AFFILIATION|Party affiliation|3|Y|VARCHAR2(3)|The political party affiliation reported by the candidate. For more information about political party affiliation codes see this list of political party codes|NON
CAND_ELECTION_YR|Year of election|4|Y|Number(4)|Candidate's election year from a Statement of Candidacy or state ballot list|2018
CAND_OFFICE_ST|Candidate state|5|Y|VARCHAR2(2)|House = state of race, President = US, Senate = state of race|VA
CAND_OFFICE|Candidate office|6|Y|VARCHAR2(1)|H = House, P = President, S = Senate|H
CAND_OFFICE_DISTRICT|Candidate district|7|Y|VARCHAR2(2)|Congressional district number: Congressional at-large 00, Senate 00, Presidential 00|01
CAND_ICI|Incumbent challenger status|8|Y|VARCHAR2(1)|C = Challenger, I = Incumbent, O = Open Seat is used to indicate an open seat; Open seats are defined as seats where the incumbent never sought re-election.|I
CAND_STATUS|Candidate status|9|Y|VARCHAR2(1)|C = Statutory candidate, F = Statutory candidate for future election, N = Not yet a statutory candidate, P = Statutory candidate in prior cycle|C
CAND_PCC|Principal campaign committee|10|Y|VARCHAR2(9)|The ID assigned by the Federal Election Commission to the candidate's principal campaign committee for a given election cycle.|C00100005
CAND_ST1|Mailing address - street|11|Y|VARCHAR2(34)|Mailing address - street|1001 George Washington Hwy
CAND_ST2|Mailing address - street2|12|Y|VARCHAR2(34)|Mailing address - street2|Suite 100
CAND_CITY|Mailing address - city|13|Y|VARCHAR2(30)|Mailing address - city|Alexandria
CAND_ST|Mailing address - state|14|Y|VARCHAR2(2)|Mailing address - state|VA
CAND_ZIP|Mailing address - ZIP code|15|Y|VARCHAR2(9)|Mailing address - ZIP code|22201

---
---
---

### ccl.zip

Column name|Field name|Position|Null|Data type|Description|Example data
|---|---|---|---|---|---|---|
CAND_ID|Candidate identification|1|N|VARCHAR2 (9)|A 9-character alpha-numeric code assigned to a candidate by the Federal Election Commission. The candidate ID for a specific candidate remains the same across election cycles as long as the candidate is running for the same office.|H8VA01233
CAND_ELECTION_YR|Candidate election year|2|N|NUMBER (4)|Candidate's election year|2018
FEC_ELECTION_YR|FEC election year|3|N|NUMBER (4)|Active 2-year period|2018
CMTE_ID|Committee identification|4|Y|VARCHAR2 (9)|A 9-character alpha-numeric code assigned to a committee by the Federal Election Commission. The committee ID for a specific committee always remains the same.|C00100005
CMTE_TP|Committee type|5|Y|VARCHAR2 (1)|List of committee type codes|H
CMTE_DSGN|Committee designation|6|Y|VARCHAR2 (1)|A = Authorized by a candidate, B = Lobbyist/Registrant PAC, D = Leadership PAC, J = Joint fundraiser, P = Principal campaign committee of a candidate, U = Unauthorized|A
LINKAGE_ID|Linkage ID|7|N|NUMBER (12)|Unique link ID|123456789012

---
---
---

### cm.zip

Column name|Field name|Position|Null|Data type|Description|Example data
|---|---|---|---|---|---|---|
CMTE_ID|Committee identification|1|N|VARCHAR2(9)|A 9-character alpha-numeric code assigned to a committee by the Federal Election Commission. Committee IDs are unique and an ID for a specific committee always remains the same.|C00100005
CMTE_NM|Committee name|2|Y|VARCHAR2(200)||Martha Washington for Congress
TRES_NM|Treasurer's name|3|Y|VARCHAR2(90)|The officially registered treasurer for the committee.|Alexander Hamilton
CMTE_ST1|Street one|4|Y|VARCHAR2(34)||1001 George Washington Hwy
CMTE_ST2|Street two|5|Y|VARCHAR2(34)||Suite 203
CMTE_CITY|City or town|6|Y|VARCHAR2(30)||Alexandria
CMTE_ST|State|7|Y|VARCHAR2(2)||VA
CMTE_ZIP|ZIP code|8|Y|VARCHAR2(9)||22201
CMTE_DSGN|Committee designation|9|Y|VARCHAR2(1)|A = Authorized by a candidate, B = Lobbyist/Registrant PAC, D = Leadership PAC, J = Joint fundraiser, P = Principal campaign committee of a candidate, U = Unauthorized|A
CMTE_TP|Committee type|10|Y|VARCHAR2(1)|List of committee type codes|H
CMTE_PTY_AFFILIATION|Committee party|11|Y|VARCHAR2(3)|List of party codes|NON
CMTE_FILING_FREQ|Filing frequency|12|Y|VARCHAR2(1)|A = Administratively terminated, D = Debt, M = Monthly filer, Q = Quarterly filer, T = Terminated, W = Waived|Q
ORG_TP|Interest group category|13|Y|VARCHAR2(1)|C = Corporation, L = Labor organization, M = Membership organization, T = Trade association, V = Cooperative, W = Corporation without capital stock|C
CONNECTED_ORG_NM|Connected organization's name|14|Y|VARCHAR2(200)||Widgets, Incorporated
CAND_ID|Candidate identification|15|Y|VARCHAR2(9)|When a committee has a committee type designation of H, S, or P, the candidate's identification number will be entered in this field.|H1VA01225

---
---
---

### webl.zip

Column name|Field name|Position|Null|Data type|Description|Example data
|---|---|---|---|---|---|---|
CAND_ID|Candidate identification|1|N|VARCHAR2 (9)||H8VA01233
CAND_NAME|Candidate name|2|Y|VARCHAR2(200)||Martha Washington
CAND_ICI|Incumbent challenger status|3|Y|VARCHAR2(1)||I
PTY_CD|Party code|4|Y|VARCHAR2(1)||NON
CAND_PTY_AFFILIATION|Party affiliation|5|Y|VARCHAR2(3)||NON
TTL_RECEIPTS|Total receipts|6|Y|Number(14,2)||345,456.34
TRANS_FROM_AUTH|Transfers from authorized committees|7|Y|Number(14,2)||4000.00
TTL_DISB|Total disbursements|8|Y|Number(14,2)||175645.21
TRANS_TO_AUTH|Transfers to authorized committees|9|Y|Number(14,2)||0.00
COH_BOP|Beginning cash|10|Y|Number(14,2)||845901.23
COH_COP|Ending cash|11|Y|Number(14,2)||915671.43
CAND_CONTRIB|Contributions from candidate|12|Y|Number(14,2)||500.00
CAND_LOANS|Loans from candidate|13|Y|Number(14,2)||250000.00
OTHER_LOANS|Other loans|14|Y|Number(14,2)||0.00
CAND_LOAN_REPAY|Candidate loan repayments|15|Y|Number(14,2)||100000.00
OTHER_LOAN_REPAY|Other loan repayments|16|Y|Number(14,2)||0.00
DEBTS_OWED_BY|Debts owed by|17|Y|Number(14,2)||250.00
TTL_INDIV_CONTRIB|Total individual contributions|18|Y|Number(14,2)||450000.00
CAND_OFFICE_ST|Candidate state|19|Y|VARCHAR2(2||VA
CAND_OFFICE_DISTRICT|Candidate district|20|Y|VARCHAR2(2)||01
SPEC_ELECTION|Special election status|21|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|W
PRIM_ELECTION|Primary election status|22|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|L
RUN_ELECTION|Runoff election status|23|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|W
GEN_ELECTION|General election status|24|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|L
GEN_ELECTION_PRECENT|General election percentage|25|Y|Number(7,4)|Election result data included in 1996-2006 files only.|63.2
OTHER_POL_CMTE_CONTRIB|Contributions from other political committees|26|Y|Number(14,2)||200000.00
POL_PTY_CONTRIB|Contributions from party committees|27|Y|Number(14,2)||200000.00
CVG_END_DT|Coverage end date|28|Y|DATE(MM/DD/YYYY)|Through date|10/19/2018
INDIV_REFUNDS|Refunds to individuals|29|Y|Number(14,2)||4000.00
CMTE_REFUNDS|Refunds to committees|30|Y|Number(14,2)||100

---
---
---

### weball.zip

Column name|Field name|Position|Null|Data type|Description|Example data
|---|---|---|---|---|---|---|
CAND_ID|Candidate identification|1|N|VARCHAR2 (9)||H8VA01233
CAND_NAME|Candidate name|2|Y|VARCHAR2(200)||Martha Washington
CAND_ICI|Incumbent challenger status|3|Y|VARCHAR2(1)||I
PTY_CD|Party code|4|Y|VARCHAR2(1)||NON
CAND_PTY_AFFILIATION|Party affiliation|5|Y|VARCHAR2(3)||NON
TTL_RECEIPTS|Total receipts|6|Y|Number(14,2)||345,456.34
TRANS_FROM_AUTH|Transfers from authorized committees|7|Y|Number(14,2)||4000.00
TTL_DISB|Total disbursements|8|Y|Number(14,2)||175645.21
TRANS_TO_AUTH|Transfers to authorized committees|9|Y|Number(14,2)||0.00
COH_BOP|Beginning cash|10|Y|Number(14,2)||845901.23
COH_COP|Ending cash|11|Y|Number(14,2)||915671.43
CAND_CONTRIB|Contributions from candidate|12|Y|Number(14,2)||500.00
CAND_LOANS|Loans from candidate|13|Y|Number(14,2)||250000.00
OTHER_LOANS|Other loans|14|Y|Number(14,2)||0.00
CAND_LOAN_REPAY|Candidate loan repayments|15|Y|Number(14,2)||100000.00
OTHER_LOAN_REPAY|Other loan repayments|16|Y|Number(14,2)||0.00
DEBTS_OWED_BY|Debts owed by|17|Y|Number(14,2)||250.00
TTL_INDIV_CONTRIB|Total individual contributions|18|Y|Number(14,2)||450000.00
CAND_OFFICE_ST|Candidate state|19|Y|VARCHAR2(2)||VA
CAND_OFFICE_DISTRICT|Candidate district|20|Y|VARCHAR2(2)||01
SPEC_ELECTION|Special election status|21|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|W
PRIM_ELECTION|Primary election status|22|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|L
RUN_ELECTION|Runoff election status|23|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|L
GEN_ELECTION|General election status|24|Y|VARCHAR2(1)|Election result data included in 1996-2006 files only.|W
GEN_ELECTION_PRECENT|General election percentage|25|Y|Number(7,4)|Election result data included in 1996-2006 files only.|63.2
OTHER_POL_CMTE_CONTRIB|Contributions from other political committees|26|Y|Number(14,2)||200000.00
POL_PTY_CONTRIB|Contributions from party committees|27|Y|Number(14,2)||200000.00
CVG_END_DT|Coverage end date|28|Y|DATE(MM/DD/YYYY)|Through date|10/19/2018
INDIV_REFUNDS|Refunds to individuals|29|Y|Number(14,2)||4000.00
CMTE_REFUNDS|Refunds to committees|30|Y|Number(14,2)||100.00

---
---
---

### webk.zip

Column name|Field name|Position|Null|Data type|Description|Example data
|---|---|---|---|---|---|---|
CMTE_ID|Committee identification|1|N|VARCHAR2 (9)||C00100005
CMTE_NM|Committee name|2|Y|VARCHAR2 (200)||National Finance Political Action Committee
CMTE_TP|Committee type|3|Y|VARCHAR2 (1)||Q
CMTE_DSGN|Committee designation|4|Y|VARCHAR2 (1)||U
CMTE_FILING_FREQ|Committee filing frequency|5|Y|VARCHAR2 (1)||M
TTL_RECEIPTS|Total receipts|6|Y|Number(14,2)||150000.00
TRANS_FROM_AFF|Transfers from affiliates|7|Y|Number(14,2)||0.00
INDV_CONTRIB|Contributions from individuals|8|Y|Number(14,2)||100000.00
OTHER_POL_CMTE_CONTRIB|Contributions from other political committees|9|Y|Number(14,2)||50000.00
CAND_CONTRIB|Contributions from candidate|10|Y|Number(14,2)|Not applicable|
CAND_LOANS|Candidate loans|11|Y|Number(14,2)|Not applicable|
TTL_LOANS_RECEIVED|Total loans received|12|Y|Number(14,2)||0.00
TTL_DISB|Total disbursements|13|Y|Number(14,2)||145000.00
TRANF_TO_AFF|Transfers to affiliates|14|Y|Number(14,2)||0.00
INDV_REFUNDS|Refunds to individuals|15|Y|Number(14,2)||0.00
OTHER_POL_CMTE_REFUNDS|Refunds to other political committees|16|Y|Number(14,2)||0.00
CAND_LOAN_REPAY|Candidate loan repayments|17|Y|Number(14,2)|Not applicable|
LOAN_REPAY|Loan repayments|18|Y|Number(14,2)||0.00
COH_BOP|Cash beginning of period|19|Y|Number(14,2)||304000.00
COH_COP|Cash close Of period|20|Y|Number(14,2)||315000.00
DEBTS_OWED_BY|Debts owed by|21|Y|Number(14,2)||0.00
NONFED_TRANS_RECEIVED|Nonfederal transfers received|22|Y|Number(14,2)||0.00
CONTRIB_TO_OTHER_CMTE|Contributions to other committees|23|Y|Number(14,2)||75000.00
IND_EXP|Independent expenditures|24|Y|Number(14,2)||10000.00
PTY_COORD_EXP|Party coordinated expenditures|25|Y|Number(14,2)||0.00
NONFED_SHARE_EXP|Nonfederal share expenditures|26|Y|Number(14,2)||0.00
CVG_END_DT|Coverage end date|27|Y|DATE(MM/DD/YYYY)|Through date|04/30/2018

---
---
---

### pas2.zip

Column name|Field name|Position|Null|Data type|Description|Example data
|---|---|---|---|---|---|---|
CMTE_ID|Filer identification number|1|N|VARCHAR2 (9)|A 9-character alpha-numeric code assigned to a committee by the Federal Election Commission|C00100005
AMNDT_IND|Amendment indicator|2|Y|VARCHAR2 (1)|Indicates if the report being filed is new (N), an amendment (A) to a previous report or a termination (T) report.|A
RPT_TP|Report type|3|Y|VARCHAR2 (3)|Indicates the type of report filed. List of report type codes|Q2
TRANSACTION_PGI|Primary-general indicator|4|Y|VARCHAR2 (5)|This code indicates the election for which the contribution was made. EYYYY (election Primary, General, Other plus election year)|P2018
IMAGE_NUM|Image number|5|Y|VARCHAR2(18)|18-digit image number normat YYYYMMDDSSPPPPPPPP: YYYY - scanning year, MM - scanning month, DD - scanning day, SS - source (02 - Senate, 03 - FEC Paper, 90-99 - FEC Electronic), PPPPPPPP - page (reset to zero every year on January 1)|201810170912341234
TRANSACTION_TP|Transaction type|6|Y|VARCHAR2 (3)|Transaction types 24A, 24C, 24E, 24F, 24H, 24K, 24N, 24P, 24R, 24Z are included in the PAS2 file. For more information about transaction type codes see this list of transaction type codes|24A
ENTITY_TP|Entity type|7|Y|VARCHAR2 (3)|ONLY VALID FOR ELECTRONIC FILINGS received after April 2002. CAN = Candidate, CCM = Candidate Committee, COM = Committee, IND = Individual (a person), ORG = Organization (not a committee and not a person), PAC = Political Action Committee, PTY = Party Organization|COM
NAME|Contributor/lender/transfer Name|8|Y|VARCHAR2 (200)||Martha Washington for Congress
CITY|City|9|Y|VARCHAR2 (30)||Alexandria
STATE|State|10|Y|VARCHAR2 (2)||VA
ZIP_CODE|ZIP code|11|Y|VARCHAR2 (9)||22201
EMPLOYER|Employer|12|Y|VARCHAR2 (38)||
OCCUPATION|Occupation|13|Y|VARCHAR2 (38)||
TRANSACTION_DT|Transaction date (MMDDYYYY)|14|Y|DATE||05112018
TRANSACTION_AMT|Transaction amount|15|Y|NUMBER (14,2)||5000.00
OTHER_ID|Other identification number|16|Y|VARCHAR2 (9)|For contributions from individuals this column is null. For contributions from candidates or other committees this column will contain the recipient's FEC ID.|C00100502
CAND_ID|Candidate ID|17|Y|VARCHAR2 (9)|A 9-character alpha-numeric code assigned to a candidate by the Federal Election Commission. The candidate ID for a specific candidate remains the same across election cycles as long as the candidate is running for the same office.|H8VA01233
TRAN_ID|Transaction ID|18|Y|VARCHAR2 (32)|ONLY VALID FOR ELECTRONIC FILINGS. A unique identifier associated with each itemization or transaction appearing in an FEC electronic file. A transaction ID is unique for a specific committee for a specific report. In other words, if committee, C1, files a Q3 New with transaction SA123 and then files 3 amendments to the Q3 transaction SA123 will be identified by transaction ID SA123 in all 4 filings.|SA11AI.8317
FILE_NUM|File number / Report ID|19|Y|NUMBER (22)|Unique report id|1197695
MEMO_CD|Memo code|20|Y|VARCHAR2 (1)|'X' indicates that the amount of the transaction is not incorporated into the total figure disclosed on the detailed summary page of the committee’s report. 'X' may also indicate that the amount was received as part of a joint fundraising transfer or other lump sum contribution required to be attributed to individual contributors. Memo items may be used to denote that a transaction was previously reported or in the case of an independent expenditure, that the amount represents activity that has occurred but has not yet been paid by the committee. When using the bulk data file these memo items should be included in your analysis.|X
MEMO_TEXT|Memo text|21|Y|VARCHAR2 (100)|A description of the activity. Memo text is available on itemized amounts on Schedules A and B. These transactions are included in the itemization total.|Contribution to federal committee
SUB_ID|FEC record number|22|N|NUMBER (19)|Unique row ID|1234567891234567891

---
---
---

### independent_expenditure.csv

CAND_ID|CAND_NAME|SPE_ID|SPE_NAM|ELE_TYPE|CAN_OFFICE_STATE|CAN_OFFICE_DIS|CAN_OFFICE|CAND_PTY_AFF|EXP_AMO|EXP_DATE|AGG_AMO|SUP_OPP|PUR|PAY|FILE_NUM|AMNDT_IND|TRAN_ID|IMAGE_NUM|RECEIPT_DAT|FEC_ELECTION_YR|PREV_FILE_NUM|DISSEM_DT
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
H4CO08034|Evans, Gabe|C00866517|Go America PAC|G|CO|08|H|REPUBLICAN PARTY|9000||9000|S|texts supporting Gabe Evans CO-8|TTHM.com|1845617|N|E2D2833410CAA40CB9A5|202410319719900778|31-OCT-24|2024||30-OCT-24

### oppexp.zip

Column name|Field name|Position|Null|Data type|Description|Example data
|---|---|---|---|---|---|---|
CMTE_ID|Filer identification number|1|N|VARCHAR2 (9)|Identification number of committee filing report. A 9-character alpha-numeric code assigned to a committee by the Federal Election Commission|C00100005
AMNDT_IND|Amendment indicator|2|Y|VARCHAR2 (1)|Indicates if the report being filed is new (N), an amendment (A) to a previous report, or a termination (T) report.|A
RPT_YR|Report year|3|Y|Number(4)||2018
RPT_TP|Report type|4|Y|VARCHAR2 (3)|Indicates the type of report filed. List of report type codes|12G
IMAGE_NUM|Image number|5|Y|VARCHAR2(18)|18-digit Image Number Format YYYYMMDDSSPPPPPPPP: YYYY - scanning year, MM - scanning month, DD - scanning day, SS - source (02 - Senate, 03 - FEC Paper, 90-99 - FEC Electronic), PPPPPPPP - page (reset to zero every year on January 1)|201810170912341234
LINE_NUM|Line number|6|Y||Indicates FEC form line number||17
FORM_TP_CD|Form type|7|Y|VARCHAR2 (8)|Indicates FEC Form|3
SCHED_TP_CD|Schedule type|8|Y|VARCHAR2 (8)|Schedule B - Itemized disbursements|SB
NAME|Contributor/Lender/Transfer Name|9|Y|VARCHAR2 (200)| |XYZ Printing
CITY|City|10|Y|VARCHAR2 (30)| |Alexandria
STATE|State|11|Y|VARCHAR2 (2)| |VA
ZIP_CODE|ZIP code|12|Y|VARCHAR2 (9)| |22201
TRANSACTION_DT|Transaction date (MMDDYYYY)|13|Y|DATE| |05112018
TRANSACTION_AMT|Transaction amount|14|Y|NUMBER (14,2)| |512.34
TRANSACTION_PGI|Primary general indicator|15|Y|VARCHAR2 (5)| |P2018
PURPOSE|Purpose|16|Y|VARCHAR2 (100)| |Printing yard signs
CATEGORY|Disbursement category code|17|Y|VARCHAR2 (3)|001-012 and 101-107|006
CATEGORY_DESC|Disbursement Category Code Description|18|Y|VARCHAR2 (40)|List of Disbursement Category Codes and their meaning|Campaign materials
MEMO_CD|Memo code|19|Y|VARCHAR2 (1)|'X' indicates that the amount is NOT to be included in the itemization total.|X
MEMO_TEXT|Memo text|20|Y|VARCHAR2 (100)|A description of the activity. Memo Text is available on itemized amounts on Schedule B. These transactions are included in the itemization total.|Credit card payment – AmEx 5/1/18
ENTITY_TP|Entity type|21|Y|VARCHAR2 (3)|ONLY VALID FOR ELECTRONIC FILINGS received after April 2002. CAN = Candidate, CCM = Candidate committee, COM = Committee, IND = Individual (a person), ORG = Organization (not a committee and not a person), PAC = Political action committee, PTY = Party organization|ORG
SUB_ID|FEC record number|22|N|NUMBER (19)|Unique row ID|1234567891234567891
FILE_NUM|File number/report ID|23|Y|NUMBER (7)|Unique report id|1197685
TRAN_ID|Transaction ID|24||VARCHAR2 (32)|ONLY VALID FOR ELECTRONIC FILINGS. A unique identifier associated with each itemization or transaction appearing in an FEC electronic file. A transaction ID is unique for a specific committee for a specific report. In other words, if committee, C1, files a Q3 New with transaction SA123 and then files 3 amendments to the Q3 transaction SA123 will be identified by transaction ID SA123 in all 4 filings.|SB17.4326
BACK_REF_TRAN_ID|Back reference transaction ID|25|Y|VARCHAR2 (32)|ONLY VALID FOR ELECTRONIC FILINGS. Used to associate one transaction with another transaction in the same report (using file number, transaction ID and back reference transaction ID). For example, a credit card payment and the subitemization of specific purchases. The back reference transaction ID of the specific purchases will equal the transaction ID of the payment to the credit card company.|SB17.8690