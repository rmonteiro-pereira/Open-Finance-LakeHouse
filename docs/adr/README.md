# Architecture Decision Records

One file per decision that would be expensive to reverse or surprising to inherit.

Each record states **the alternative that was rejected and why**, and **the condition that
would reverse the decision**. A decision without a stated reversal condition is a preference,
not a decision — if nothing could change your mind, there was no trade-off to record.

Records are for choices a reader could not reconstruct from the code. In particular, a
deliberate *absence* belongs here: unwritten, it reads as an oversight.

| # | Decision | Status |
|---|---|---|
| [0001](0001-no-dbt-sdp-or-sqlmesh-in-the-mart-layer.md) | Why the mart layer has no dbt, SDP or SQLMesh | Accepted |

## Format

```markdown
# NNNN — Title

- **Status:** Proposed | Accepted | Superseded by ADR-NNNN
- **Date:** YYYY-MM-DD

## Context
What forced a decision.

## Decision
What was chosen.

## Alternatives rejected
Each one, and the specific reason it lost.

## Consequences
What this costs, including what it makes harder.

## Reversal condition
The concrete change in circumstances that should make someone revisit this.
```
