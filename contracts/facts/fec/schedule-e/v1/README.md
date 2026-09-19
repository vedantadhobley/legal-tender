# Processed Schedule E independent-expenditure facts

Each selected Schedule E occurrence produces one fact. `source_fields`
preserves all 80 decoded source lexemes with nulls intact. `typed_fields`
adds exact minor-unit money observations, local timestamps and dates, and
named spender, payee, candidate, election, expenditure, conduit,
certification, filer, and filing structures.

This layer remains policy-free. A later calculation must resolve amendment
chains and decide how memo rows or other reporting states contribute to an
effective independent-expenditure measure.
