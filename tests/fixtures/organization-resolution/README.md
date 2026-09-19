# Organization-resolution evaluation evidence

This directory holds bounded source captures and reviewed benchmark annotations.
It is test data, not a runtime organization-ID dictionary or an identity whitelist.

The source population is selected by the Go FEC query builder from immutable
committee facts. Capture directories retain complete Wikipedia/Wikidata API
responses, request metadata and source references. Do not edit captured bytes;
capture a new version instead. Wikipedia source metadata is retained with its
page and revision identity, not article text. Keep publisher attribution with
the source snapshots.

Annotations are review assertions supported by cited primary-source evidence.
They are not claims that a name match alone proves an organization identity,
employment, ownership or the origin of money. Unreviewed cases remain explicit.
The evaluation command does not use these labels to influence the resolver.

See the [evaluation contract](../../../docs/design/organization-evaluation.md).

`gleif-capture-v1/` adds an independently fetched exact-LEI record, using the
normal Go request selector over `capture-v1/`. Its manifest binds that Wikimedia
capture, the request-selection policy, build, request and body hashes. The
[corroboration contract](../../../docs/design/organization-corroboration.md)
records the digest and results. Do not edit these bytes. This fixture does not
add benchmark labels or approve any FEC identity connections.

`sec-company-tickers-v1.json` retains the exact public body from one successful
Go fetch of [SEC's company directory](https://www.sec.gov/files/company_tickers.json),
observed on 2026-09-15 at 17:55:26 UTC. It is 797,759 bytes with SHA-256
`82cd5fd9ccffda811b93ba76070460dd41429c02c00e726f83deb71f553c6cff`.
The full capture and CLI results remain private because they include the operator's
request contact. This file is not a redacted or substitute capture manifest.
The [live issuer gate](../../../docs/design/organization-issuer-discovery.md#retained-live-gate)
records the exact capture/query/result pins and the replay boundary. Regression
tests read the public body and original queries without inventing HTTP provenance.
The existing reviewed annotations and Wikimedia/GLEIF captures remain unchanged.

`sec-annual-report-v1.htm` retains the exact public
[2024 annual report](https://www.sec.gov/Archives/edgar/data/34782/000003478225000025/source-20241231.htm)
used by the [filed registrant check](../../../docs/design/organization-filed-identity.md).
Its 4,263,684 bytes have SHA-256
`d4ce073c085d7193a586565759038b28e08aca3c4a166566de437b6eca9ce1cd`.
The normal Go capture on 2026-09-15 matches these bytes exactly; its full request
metadata remains private. This is source evidence, not an approved FEC identity.

`sec-submissions-review-v1.json` retains the exact public
[SEC submissions response](https://data.sec.gov/submissions/CIK0000034782.json)
observed during the 2026-09-15 bounded research review. Its 169,528 bytes have
SHA-256 `2bbfbf0a6bf0504ac1995d4c4f03231de1f4dd60359067550fc81569683382a4`.
It supports review of filing references and mailing-address scope. It is not input
to the automated comparison or an implemented submissions-adapter fixture.
Do not mistake the committee's FEC address for the connected organization's own
address, or a submissions observation date for historical identity validity.
