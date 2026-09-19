# Resolved independent expenditures v1

This calculation turns the dense per-fact candidate-resolution decisions into
the compact spender-to-candidate groups required by graph projection. It does
not reread Schedule E or reinterpret source fields.

`confirmed`, `resolved`, and `unverified` decisions group by spender committee,
the separate resolved candidate ID, and support-or-oppose stance. Every result
retains exact signed totals, sign counts, and separate count and signed-amount
components for all three resolution states. Consumers can therefore filter or
label lower-confidence identity without reconstructing the dense decisions.

`ambiguous` and `unresolved` decisions never become candidate edges. Each
remains one sparse exception with its upstream decision ID, source fact ID,
reported candidate ID, spender, stance, method, and exact amount. The manifest
conserves every input decision and signed cent through one of these two routes.

The upstream dense decisions remain the identity evidence and are retained
indefinitely. The sparse exceptions are an operational and investigative
surface, not a replacement for that evidence.
