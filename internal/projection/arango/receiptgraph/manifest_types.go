package receiptgraph

// CycleManifest and SharedManifest expose the existing closed on-disk shapes
// to metadata-only inventory consumers. Decoding them does not verify a live
// graph, source membership or recovery readiness; use the readers for that.
type CycleManifest = completion
type SharedManifest = sharedCompletion
