# Opt-in bounded native cron runs

This source candidate adds a complete trusted `run_limits` field to existing native jobs. Jobs without that field retain their existing behavior. It does not create jobs or alter recurrence, credentials, account authorization, delivery, or the execution ledger.

## Trusted operator contract

Use the existing `cron.jobs.update_job(job_id, updates)` after the owner release and exact job readback are approved. The complete field includes daily_tokens (at most100000), deadline_seconds (at most3600), parent_max_iterations (at most12), child_max_iterations (at most3), max_concurrent_children (at most3), max_total_children (at most12), child_max_output_tokens (at most2000), child_context_max_chars (at most4000), estimated_child_tokens (at most10000), and audit_job_ids (exact finite job IDs sharing the UTC-day allowance). Every numeric limit must be a positive integer. Partial or invalid contracts fail before a job is saved.

## Enforcement and evidence

Before constructing an opted-in agent, the runner reads its existing native usage audit for the declared IDs and UTC day. Missing, malformed, oversized, or unknown usage fails closed. The parent and registered children share one monotonic deadline and stop latch. The native cancellation request reaches Codex interrupt and inline request-abort hooks. Uncooperative work can outlive cancellation; this is not process termination proof.

Admission limits twelve total leaf children, three concurrent, with no nested delegation. Children receive bounded context, output and iterations and no tools. This release therefore supports specialist analysis of supplied evidence, not autonomous department account writes. Routine business authorization remains separate from current review-mode tool scope.

The receipt counts each native session_total_tokens once. Missing or malformed response usage, failed or interrupted terminal results, and unsettled children produce unknown total usage, which prevents the next bounded wake from reusing an incomplete baseline. Complete receipts include the parent and child totals in the existing audit record. Estimated admission plus observed usage is not a hard provider token cap: in-flight requests can overrun the allowance before their usage arrives.

## Release and rollback

This candidate is source-only. Before activation, independently verify installed owner version, exact opted-in job IDs, effective prompts/tool scope, saved recurrence, current UTC audit completeness, and a bounded representative run. Do not infer installed behavior from source tests. Revert the exact reviewed source commit through the canonical owner release route; pause only affected jobs if usage is unknown or exceeds the allowance. Preserve job history and audit receipts. Removing the optional run_limits field restores legacy behavior and is not an appropriate response to a budget incident.
