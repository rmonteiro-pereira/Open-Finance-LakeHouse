# 0002 — The published release is the product

- **Status:** Accepted
- **Date:** 2026-08-06
- **Supersedes nothing. Amended by:** none yet
- **Full design:** `_specs/rfc-ofl-produto-de-dados.md` (v4) and `_specs/DECISOES-OFL.md`

## Context

This repository was measured in units of supply — 51 registered series, 10 handlers, 5
facts, 10 marts, 41 tests — and every one of those numbers is true. None of them moves
when somebody uses the thing.

Measured from the demand side, the position was different. The only human-facing surface
(a Next.js dashboard) was not in git at all; it existed in one working tree. Its cluster
deployment sat behind Cloudflare Access. The snapshot it served by default was
**synthetic**. Every programmatic path required MinIO credentials from a home Kubernetes
cluster. The one portable artefact, `tools/export_gold_duckdb.py`, is documented as living
"outside the repo and never committed".

The surface available to anyone who is not the author was a README.

## Decision

**The primary interface of this project is a published, versioned artefact — not a
screen.** The lakehouse is the factory; `ofl/release/` is the counter. Every other
surface (reader site, agent, status page) is a view of that artefact, and **none of them
may read MinIO**.

Consequences that follow, each of which is enforced by a gate rather than by intent:

1. **Grain is declared and checked.** Every published table states a primary key, and the
   `grain` gate verifies it. Its twin runs *before* deduplication, in the silver lane,
   because after a dedup `count == count distinct` holds by construction — precisely
   because the dedup deleted the rows that would have shown a collision.
2. **Provenance and authenticity are columns.** `provider` (whose data) and `data_class`
   (live / sandbox / synthetic) travel from the fetch through every mart. Both gates check
   the column's PRESENCE before its values: a predicate over an absent column is satisfied
   by its absence.
3. **Licence is a type, default deny.** `sources/providers.yml` is keyed by handler — the
   value the data actually carries — with `rights_holder` as a separate field.
4. **A quantity is a tuple**, not a symbol: `unit` + `basis` + `scale` + `day_count` +
   `horizon`. `unit: percent` alone covered the SELIC daily rate, monthly IPCA variation,
   the unemployment level and debt/GDP.
5. **Freshness is published as INPUTS and never as a verdict**, at the grain of the series.
   A manifest that declares itself "ok" lies on exactly the day the producer dies.
6. **Freshness never blocks publication.** An outage that stops the release pins every
   consumer to a stale artefact *with no signal*, which is worse than a visible old date.

## What this costs, stated plainly

The public release is **smaller than the lakehouse**, and that is the intended outcome
rather than a regression. Of 51 registered series:

| Licence state | Series | Handlers |
|---|---:|---|
| `open` — may be redistributed | **38** | `bacen_sgs` (30), `bacen_focus` (3), `ipea` (3), `ibge` (1), `tesouro_direto` (1) |
| `restricted` — written verdict says no | **9** | `anbima` (4), `b3_arquivos` (3), `b3` (1), `b3_cotahist` (1) |
| `unverified` — no written verdict, so denied by default | **4** | `yahoo` (4) |

B3 barred redistribution of derived values and indices absent written authorisation;
ANBIMA denied the Feed, and the series currently ingested from it run against a sandbox
whose values are **fictitious**. Yahoo has no audited term, and silence counts as red.

Restricted series stay **visible in the catalogue** with the reason attached. A catalogue
that declares what it cannot give is more trustworthy than one that pretends the series
does not exist.

## Reversal criteria

- A series moves between states when a **written verdict** citing the term of use exists.
  That is a field in `providers.yml`, not a refactor.
- If, six weeks after the first published release, the artefact has **no independent
  consumer** beyond this repository's own dashboard, the demand this decision presumes
  does not exist and the effort belongs back in substrate engineering.
- If maintaining two builds (public filtered / internal complete) costs more than the
  public one returns — that is, if nearly everything falls out on licence — the right
  product is narrower and more honest about it, not this one.

## Note on method

Two adversarial review rounds each found blocking defects **in the previous round's
corrections**. The first proposed keying `fact_treasury` by a `bond_type` bucket, which
merges NTN-B Principal and NTN-B com Juros Semestrais at the same maturity — it
reintroduced the exact defect it was written to fix. The second corrected a `grain` gate
that could not fail on the defect that motivated it.

The operational conclusion is written into the gates rather than left as advice: a
proposed correction deserves the same scepticism as the defect, and the only sceptic that
does not tire is a test that can fail.
