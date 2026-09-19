# Committee-flow evidence readiness v1

The [manifest schema](./manifest.schema.json) freezes one published
[candidate reconciliation](../../../../calculations/fec/committee-flow-reconciliation/v1/),
its exact Schedule A/B facts and coordinated release, and one same-cycle
committee-master fact set. Go owns publication and verification.

`ready` means these inputs passed verification for the observation-only
consumer. It does not mean a graph exists, identities are complete, source
claims are correct, or economic payments and terminal sources are resolved.
V1 does not accept historical identity inputs. Missing current masters must
remain explicit in the future graph, never become terminal sources.

Identity is SHA-256 of Go's compact JSON encoding of the complete bundle with
`bundle_id` set to the empty string. Run time and worker count are not inputs.
The stored manifest is indented JSON with a trailing newline. Publication
uses create-only immutable files and an atomic per-cycle pointer; a consumer
compares pointer bytes with immutable bytes and revalidates all backing.

Storage is `bundles/fec/committee-flow-evidence/v1/` under the storage root:
`manifests/<bundle-id>.json` and `current/<cycle>.json`.

Verification checks are a fixed ordered set of successful invariants, not
user-provided flags that can bypass validation. The loader recomputes the
complete bundle from its immutable inputs and compares it exactly. Source
ancestry checks compare archive identity and selected compressed/uncompressed
member digests and sizes across the coordinated, fact, and occurrence releases.

See the [graph boundary](../../../../../docs/design/arango-committee-flow-evidence.md)
and [publication contract](../../../../../docs/design/committee-flow-publication.md).
