# Grounding-bound referent assessment trial

This test-only experiment follows the rejected combined grounding-and-assessment
producer. The request supplies exact caller-owned grounding that has already passed
the existing Go validator. The response can contain only referent and correction-
target assessments; Go joins it back to the unchanged grounding.

The comparison completed on 2026-09-19. `review.json` pins both model profiles,
captures, completion markers, aggregate usage and independent semantic findings.
Each model returned ten HTTP 200 responses, passed nine structural joins and produced
six usable unverified assessments, including one of three real-source cases.

`cases.json` fixes case order, grounding origin, grounding hash and semantic review
checks before inference. Review checks never enter requests. Three retained real
source excerpts, five retained synthetic controls and two additional synthetic
controls separate real-source behavior, ambiguity, anonymous endpoints, nearby
names, pronouns and selective corrections. These are known diagnostics, not a
held-out accuracy estimate.

Both models still confuse short mention occurrences with supported full-name
antecedents. This exposes the next representation boundary; no named-case prompt
patch was added. No model output from this directory approves an identity, relationship, graph
publication or financial attribution. Run only through the explicit
`LT_PROSE_REFERENT_ASSESSMENT_TRIAL=1` opt-in and always use a new output directory.
