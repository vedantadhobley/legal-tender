# Independent structural/census gate. Go verifies hashes and source bytes.
def whole: type == "number" and . >= 0 and . == floor;
def states($n):
  ([.null, .empty, .nonempty] | all(whole)) and
  (.null + .empty + .nonempty == $n);
def source:
  (.published_facts | whole) and (.source_occurrences | whole) and
  (.excluded_occurrences | whole) and
  .source_occurrences == (.published_facts + .excluded_occurrences);
def require($ok; $message): if $ok then . else error($message) end;
. as $r
| require(.schema_version == "legal-tender.fec.reported-identity-assertions.v1"
    and .policy == "fec/reported-identity-field-projection@1.0.0"
    and .state == "complete_published_fact_assertion_view"
    and .storage_mode == "references_immutable_source_facts"; "view boundary")
| require([.person_corporation_identity_resolved, .employment_verified,
    .ownership_verified, .terminal_policy_adopted, .financial_attribution]
    | all(. == false); "inference promotion")
| require((.receipt_source | source) and (.committee_source | source)
    and .receipt_source.excluded_occurrences == 0; "source populations")
| require((.receipt_columns | length) == 18 and (.committee_columns | length) == 4
    and (.receipt_field_counts | length) == 18; "field count")
| require(([.receipt_shards[].rows] | add) == .receipt_source.published_facts
    and .committee_proof.rows == .committee_source.published_facts
    and .committee_proof.rows == .committee_proof.source_artifact.record_count; "fact conservation")
| require(all(.receipt_shards[]; . as $p
    | .rows == .source_shard.facts and .rows == .source_shard.source_rows
    and .rows == .source_shard.valid_facts and .source_shard.invalid_facts == 0
    and .rows == (.source_shard.last_source_row_ordinal - .source_shard.first_source_row_ordinal + 1)
    and (.field_counts | length) == 18 and all(.field_counts[]; states($p.rows))); "shard field census")
| require(all(range(0; .receipt_shards | length); . as $i
    | $r.receipt_shards[$i].source_shard.index == $i
    and $r.receipt_shards[$i].source_shard.first_source_row_ordinal ==
      (if $i == 0 then 1 else $r.receipt_shards[$i-1].source_shard.last_source_row_ordinal + 1 end)); "dense scope")
| require(all(range(0; 18); . as $i
    | all(["null", "empty", "nonempty"][]; . as $state
      | ([$r.receipt_shards[].field_counts[$i][$state]] | add)
        == $r.receipt_field_counts[$i][$state])); "merged field census")
| require(all(.receipt_field_counts[]; states($r.receipt_source.published_facts))
    and (.committee_proof.field_counts | length) == 4
    and all(.committee_proof.field_counts[]; .null == 0 and states($r.committee_source.published_facts)); "complete field census")
| true
