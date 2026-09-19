# Committee-flow HTTP response v1

[Response schema](./response.schema.json) covers the Go API's pinned projection
envelope, paged observations and identities, graph paths, component summaries,
full source rows, and redacted errors. Full source/master objects retain their
source-specific fields; the reader verifies them against their own contracts.

The [synthetic observation fixture](./fixtures/observation-page.json) is a wire
test, not a publisher filing or a valid content-addressed publication. Its
amount deliberately exceeds JavaScript's exact integer range. No genuine
person's data or record-specific production rule is encoded in it.

See the [HTTP design and routes](../../../../docs/design/committee-flow-api.md)
and [real execution gate](../../../../docs/audit/committee-flow-api-2026-09-08.md).
