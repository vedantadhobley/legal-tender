# Candidate dossier v1

This contract presents one compact candidate view from an exact retained
candidate-evidence v2 report and one complete published candidate-interpretation
set from the same cycle. Each domain keeps its exact FEC source-release ID. The
dossier reports whether those IDs match; it does not require or imply that two
non-additive evidence domains were refreshed in one release.

The dossier keeps candidate-linked receipt evidence separate from
independent-expenditure evidence. Within independent expenditures, the
source-reported endpoint, corroborated safe default, inferred alternative and
conflicting alternative are separate, overlapping views. They must not be
added together. Each relevant interpretation appears once in `evidence` and
lists every candidate role it supplies.

This presentation does not define a terminal source, allocate committee money,
combine outside spending with receipts, resolve people or corporations, or
include lobbying, legislation, votes or official actions.

`result.schema.json` is the normative JSON contract.
