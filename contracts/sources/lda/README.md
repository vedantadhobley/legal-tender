# LDA source contracts

These draft contracts map the current LDA.gov API and printable filing
documents into Legal Tender's source boundary:

- [`filings/v1/`](./filings/v1/) — LD-1 and LD-2 filing versions and children.
- [`contribution-reports/v1/`](./contribution-reports/v1/) — LD-203 reports and items.
- [`registrants/v1/`](./registrants/v1/) — registrant masters.
- [`clients/v1/`](./clients/v1/) — client masters and registrant relationships.
- [`lobbyists/v1/`](./lobbyists/v1/) — lobbyist masters and registrant relationships.
- [`constants/v1/`](./constants/v1/) — all current publisher code-list endpoints.
- [`printable-documents/v1/`](./printable-documents/v1/) — exact LD-1, LD-2,
  and LD-203 document bytes and form-level evidence.
- [`rules/v1/`](./rules/v1/) — period-aware registration coverage thresholds,
  LD-2 reporting bands, rounding precision, accounting-method treatment, and
  unresolved rule questions.

The official OpenAPI artifact retrieved on 2026-08-27 had upstream SHA-256
digest:

```text
6c5f13c470bf1b4b09071db32d10df72372210fd2c2be921214e3c94f47dde8a
```

The [checked-in artifact](./openapi/2026-08-27.yaml) redacts only the
publisher's example authorization token. The file header records that
transformation so the upstream artifact remains independently identifiable
without retaining credential-shaped data in the repository. Its local
SHA-256 digest is:

```text
55d6313e22292f4848cdb596502cb4da23e0d4e85088b5408dabf936740b8b7a
```

The reviewed record schemas intentionally differ from that artifact where
live fixtures prove it wrong. The known differences and their semantic impact
are recorded in the [LDA schema audit](../../../docs/design/lda-source-schema.md).
The selected [API/document comparison](../../../docs/audit/lda-api-printable-comparison-2026-08-27.md)
records where neither representation is lossless.
