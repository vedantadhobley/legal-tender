# Receiver-reported committee-flow projection bundle v2

This bundle composes the immutable v1 flow readiness bundle with one exact
receiver-flow committee identity-coverage calculation. It does not mutate or
replace the v1 bundle.

The publisher verifies that the identity calculation names the same v1 bundle,
flow calculation, selected committee master, cycle, and coordinated release.
Its counts conserve all referenced committees across current-cycle master,
historical registration, alternate-release registration, and unresolved
reported-ID states.
