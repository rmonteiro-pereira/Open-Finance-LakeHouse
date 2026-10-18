# OFL product direction and evaluation

Finance workers and data workers are primary. Assessores, consultores and lay
market/investment-portfolio users are explicitly considered. The engineering
portfolio project is secondary.

- [Shape definido e primeira fatia de implementação](SHAPE.md).
- [Repeatable evaluation workflow](WORKFLOW.md): process, evidence, baselines,
  adversarial cases, outcome gates and decision cadence.
- [First adversarial review](REVIEW.md): findings across the whole OFL, leading
  product hypotheses, weaknesses of the current agent and next experiments.
- [Twenty persona hypotheses](personas.json) and [rankings/sensitivity](evidence/persona-ranking.md).
- [Read-only live inventory](evidence/inventory-2026-10-18.json): collection counts,
  indexed availability and bounded qualifications.
- [Reproducible agent diagnostic](evidence/agent-diagnostic.json), generated with
  `cd agents && bun evals/product-audit.ts`: four structural gaps and two retained
  safeguards, using the actual edge with scripted models/synthetic sources.
- [Proposed decisions and reversal conditions](DECISIONS.md).
- [Outcome case bank](cases.json): planned paired user-task and adversarial trials.

This is an initial evidence-based assessment. It contains no invented interviews,
real-model benchmark results, adoption, willingness-to-pay or product-market-fit
claims. Subjective rankings are reproducible prioritization aids, not measurements.
