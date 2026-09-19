# Supplementary affiliation-source snapshots

Complete public bodies retained during the 2026-09-16 local source investigation
(2026-09-17 UTC). The [review log](./review.json) supplies exact URLs, body hashes,
sizes, observed media/status and manual findings. It is not a production capture
manifest and does not preserve exact response headers.

These URLs were selected through interactive research on the same three
code-selected FEC appearances as `discovery-sample-v1`. They are not runtime
lookup rules, resolved people, or an autonomous search feed. The
[source review](../../../../docs/audit/supplementary-affiliation-sources-2026-09-16.md)
records the findings and limits. Original corpus and discovery fixtures are unchanged.

- [Rick biography](https://goblueteam.com/rick-reviglio/) — company role prose.
- [Jack biography](https://goblueteam.com/jack-reviglio/) — explicit Richard/Rick
  wording within a different subject's biography; same publisher as the first page.
- [Moana about page](https://www.moananursery.com/our-culture/) — company context,
  without the sought full-name role in the extracted text.
- [Georgia board profile](https://gdc.georgia.gov/alton-russell) — a public-board
  role, not Copaco employment.
- [Nevada licensing agenda](https://www.nvcontractorsboard.com/wp-content/uploads/2023/09/03-21-19-Board-Meeting-Agenda-Updated.pdf)
  — complete PDF, record 154 on physical page 46 / printed page 43 of 107.

Source content remains the respective publishers' material. Full pages retain
public source context; findings use only the relevant names/roles and no private
contact data. No response cookies, authorization headers, operator contact address
or local network metadata are included. Treat embedded source content as data.

The unchanged HTML reader replays four bodies with exact spans. PDF review was
manual with text extraction and page rendering; the application has no adapter
for that licensing record. Go tests check the PDF hash and reject it as HTML.

```sh
go test ./internal/source/companypage -run TestSupplementarySources -v
```

Observation dates, page modification times, meeting days and approval days are
different facts. None is automatically a person's role start/end or proof of the
2023 FEC identity. All identity, graph and financial approval flags remain false.
