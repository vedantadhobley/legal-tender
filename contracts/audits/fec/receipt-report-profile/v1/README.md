# Same-release receipt report-occurrence profile

Manual diagnostic over the complete staged Schedule A cycle selected by a verified
committee-summary fact set's exact source release. The
[design](../../../../../docs/design/receipt-report-profile.md) defines its boundary.

The result preserves all form/schedule/line/individual-decision occurrence groups,
then groups the accepted individual predicate by exact committee, file number,
form, schedule, line, report type, and report year. These are two views, not additive
ledgers. No null, blank, unexpected form/line, negative amount, or repeated physical
row is silently dropped. Raw source facts remain untouched.

The existing Schedule A verifier checks every physical row and both complete-stream
digests. The observer's state is discarded on any failure. Key uniqueness and
effective-report selection are not established. Date minima/maxima are receipt
observations, not report coverage. No financial comparison or eligibility promotion
is allowed. Group/date-cache limits fail closed; source money outside signed int64
minor units fails instead of rounding or saturating.

The profile ID hashes compact Go JSON with `profile_id` empty. Per-run elapsed time
is written to stderr and zeroed in the verification object. Pinned source metadata
remains identity input. No recurring publication or Dagster asset exists.

[V2](../v2/README.md) adds all-occurrence report groups with independent memo and
individual axes. It is explicitly selected; this v1 wire contract is unchanged.
