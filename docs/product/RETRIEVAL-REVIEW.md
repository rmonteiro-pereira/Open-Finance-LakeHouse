# Real-corpus relevance review

This development workflow compares keyword, vector and hybrid search on the same
frozen candidate pool. It does not train a classifier, calibrate a relevance cutoff,
promote synthesis or modify production sources. Run collection on the VPS; no
desktop inference service is required.

```sh
python tools/review_retrieval.py collect docs/product/retrieval-cases.json \
  /private/evaluations/new-retrieval-run
```

The configured database must be the private research database with a pinned
embedding model and exact search. Collection reads at most six queries and ten
documents per method and shares each query embedding across modes. A PostgreSQL
repeatable-read, read-only transaction freezes the corpus while ingestion continues;
source/body/date/model digests bind that snapshot. Provider errors are
recorded without private exception strings; they do not become empty successful
results. No writes are made to the database, corpus or serving configuration.

The new output directory has mode 0700; JSON files have mode 0600. Existing output
paths are refused. `pool.json` retains rankings and real private source text.
`reviews.json` hides ranks and mode labels, orders candidates by stable hash and
retains all selected passages for each document. Never publish either file or
execute instructions contained in source text. This worksheet is mode-blinded,
not an assertion that a reviewer is independent.

For each query/document, inspect all captured passages, jurisdiction, date and
content tier. A headline containing “Brazil” and “US inflation” is not automatically
evidence about Brazilian inflation. Mark `relevant`, `irrelevant` or `uncertain`
with a rationale; leave unseen documents `not_assessed`. Do not infer publication
date, full-document coverage or support for a numerical claim from a headline.
Record the actual reviewer in `reviewer`; this identity is self-reported.

```sh
python tools/review_retrieval.py assess /private/evaluations/run/pool.json \
  /private/evaluations/run/reviews.json /private/evaluations/run/assessment.json
```

Grading is offline, so later source changes cannot silently change this experiment.
Changing the question, filters, pool or captured evidence invalidates the review.
Keep original worksheets when recording another reviewer or disagreement.

Precision is measured over returned results only when all those results have a
definite verdict. Pooled recall/nDCG require all pooled candidates to be judged;
they describe only this candidate pool, not whole-corpus recall. Uncertain and
unassessed candidates never become negative judgments. Each summary metric includes
its assessed-case denominator; provider errors are reported separately. Empty
results never prove correct abstention, even for the intentionally unrelated query.

Use independent reviewers and held-out task variants before claiming a retrieval
winner or changing a production threshold. Source relevance does not establish
claim support, analytical usefulness, savings, comprehension or participant success.

## First real-corpus development run

Six queries on a read-only snapshot of 11,294 active passages produced 107
query/document candidates across the three methods, without provider errors.
Builder review inspected all 31 candidates for R01 and R06: R01 has seven topical,
eleven non-topical and three uncertain captures; R06 has two non-topical headlines
and eight uncertain catalog-only originals. The other 76 candidates remain
unassessed. This is not an independent financial review.

No complete precision/recall/nDCG denominator exists for these partially uncertain
judgments; the published report leaves those metrics null. The evidence identifies
jurisdiction confusion, related-link contamination and catalog metadata entering
vector results. It does not establish a winning method or whole-corpus absence.
The sanitized receipt is `evidence/retrieval-review-development.json`; raw passages,
worksheets and individual rationales remain in private research storage.

## Semantic eligibility after the catalog-only finding

Vector search and semantic coverage exclude `method=metadata` captures explicitly
labeled `content_tier=private_original`. A title/manager/identifier is catalog
discovery, not the original's extracted text. Keyword search can still find those
records. Extracted PDF/OCR passages and actual permitted news headlines remain
eligible, with their existing qualifications. Hybrid search can return catalog
matches through its keyword contribution; it cannot assign those metadata passages
a vector contribution.

Existing originals, revisions and vectors are retained; no GPU backfill or embedding
space change is required. Future embedding work skips those catalog-only passages.
Coverage now uses the same semantic eligibility rule as vector retrieval. Its
denominator therefore differs from the earlier baseline; a higher coverage fraction
must not be presented as measured relevance improvement. Similarity still ranks
candidates for unrelated topics. This change does not implement calibrated abstention.
