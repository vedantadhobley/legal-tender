# Names-only model experiment

This bounded test isolates literal name selection from role interpretation. Four
known cases reuse exact excerpts from the [derived-reference trial](../reference-model-v1/README.md).
Four fresh cases contain one previously unused excerpt from retained AMD HTML and
three new synthetic controls. Fresh means unused in project trials, not absent
from model training or newly fetched. No additional source fetch was needed.

`inputs.json`, `expected.json` and the fixed prompt are authored before inference.
Expected labels never enter a request. Model input contains only source text and
its lexical token catalog. Person/organization kinds are proposed, not verified.

The task selects one occurrence per distinct literal name spelling, including
separately written surnames and abbreviations. It does not enumerate every repeated
occurrence or merge spellings into identities. Full source context is retained.
Honorifics/possessives/role titles are excluded; name punctuation and legal suffixes
are preserved. Named educational institutions are included; internal corporate
department labels are not. These are explicit experiment labels, not runtime policy.

Exact `(kind, source-selected text)` comparison reports matched, missing and
unexpected surfaces plus duplicate selections. No case folding, trimming, fuzzy
matching or role/identity acceptance is performed. Different source occurrences of
the same exact spelling can match; occurrence completeness is not measured. A wrong
span or kind counts as an unexpected selection and leaves the expected name missing.
Invalid/truncated answers are recorded separately and do not receive partial scores.

All eight attempts are retained and replayed offline: all pass coordinate checks;
exact comparison matches 22/22 regression spellings and 13/15 fresh spellings, with
two extra fresh selections. `review.json` explains the omitted surname, legal-suffix
punctuation and governing-body scope error. See the
[trial audit](../../../../docs/audit/prose-name-selection-model-2026-09-17.md).
Old trial captures and validators remain unchanged. No downstream relationship
producer is wired to this experiment, and no identity, graph or financial approval
follows from it.
