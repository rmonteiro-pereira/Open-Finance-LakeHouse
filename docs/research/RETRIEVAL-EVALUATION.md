# Retrieval coverage, measured relevance and worker capacity

## What the interfaces report

`GET /api/v1/workbench` and every search response expose `index_coverage` for the
currently pinned embedding space. Search counts use the same active-passage,
active-asset, source, collection and publication-date filters as retrieval. Query
terms do not filter the coverage denominator. Unknown publication dates are
excluded when dates are required. Empty, disabled, partial and complete are
separate states; zero eligible passages is not 100% coverage.

The dashboard explains these counts and carries the executed coverage into search
exports. Agent semantic-search tables retain the scope even with zero hits. MCP
receives the API response. Complete means all eligible local passages have vectors;
it says nothing about the original archive's completeness or relevance. Keyword
checks retain the coverage observation but exclude index progress from evidence
change detection. A wholly unindexed semantic scope does not call the model.

Passages are never truncated before embedding. When the remote model refuses a
batch because an input exceeds its context length, the batch is bisected: the
other passages are stored and each oversize passage is recorded in
`embedding_rejections` for that model, model revision, passage revision and text
digest. Rejected passages leave the queue, so later passages are indexed. They stay
in `eligible_passages`, are reported as `rejected_passages` and are not counted as
indexed or pending; a changed passage text or revision is eligible again. Any other
provider error still fails the embedding job.

## Worker pools

`ofl research worker --lane default` processes imports, library synchronization,
PDF/OCR, exports and publication serially. `--lane embedding` processes only
embedding jobs. `--lane checks` schedules and processes saved research checks.
The default CLI lane, `all`, preserves existing single-worker behavior.

Named lanes can run concurrently, with one execution per lane. Their shared
advisory lock is incompatible with the old/global worker lock, including during
an upgrade or rollback. Claims still use leases and row locks; the lock-holder
connection is kept alive by the job heartbeat. A long embedding job cannot claim
source work or checks. Multiple default workers do not publish concurrently.

The private rollout increases recurring embedding increments from eight to 64
passages on the dedicated pool. `ofl research embed --limit 64 --if-idle` reuses
an existing queued/running embedding job atomically, avoiding a growing recurring
queue when inference is slower than the timer. Each complete remote passage is
still persisted separately. This improves scheduling/capacity utilization; it is
not a promise to keep up with any ingestion rate or a model throughput benchmark.

## Relevance evaluation on a frozen corpus

The storage-engine pilot remains `ofl research pilot`. The separate
`ofl research evaluate /private/judged.json --limit 100` evaluates actual keyword,
exact vector and reciprocal-rank-fused hybrid retrieval at ten documents. This
matches the API's one-result-per-document grouping.

Obtain the snapshot descriptor without writing data:

```python
from ofl.research.config import ResearchSettings
from ofl.research.db import Database
from ofl.research.retrieval_eval import corpus_manifest
receipt = corpus_manifest(Database(ResearchSettings()))
```

Keep the judgments in private storage for private sources:

```json
{
  "version": 1,
  "split": "held_out",
  "judge": "Actual relevance reviewer and judgment protocol",
  "model": "COPY FROM RECEIPT",
  "model_revision": "COPY FROM RECEIPT",
  "corpus_sha256": "COPY FROM RECEIPT",
  "cases": [
    {"id": "finance-01", "query": "The actual research question", "relevant_item_ids": ["ACTUALLY JUDGED DOCUMENT ID"], "filters": {"source": "manager_letter"}},
    {"id": "absent-01", "query": "An actually judged absent topic", "relevant_item_ids": [], "expected_empty": true}
  ]
}
```

The receipt covers active text, document revisions/date/source metadata and the
actual vectors in this embedding space. The manifest and receipt hashes, model
revision and judgment provenance accompany results. Any corpus change during the
run invalidates it. Use an isolated, frozen copy when live ingestion is running.

All planned cases remain in the report. Judgments outside the filtered active
corpus are explicitly ineligible; query/embedding errors remain in eligible
metric denominators with zero retrieval credit. Positive and expected-empty
cases have separate metrics. Report recall@10, nDCG@10, reciprocal rank and
expected-empty correctness together with counts and failures. Query embeddings
are computed once and reused for vector/hybrid; their cost/latency is reported
separately from retrieval. No raw source text or backend exception is emitted.
A `held_out` label alone is not evidence of independent judgments or users.

## Jurisdiction evidence and rules

Each passage stores lexical jurisdiction evidence in `passages.jurisdiction`
(codes `BR`, `US`, `EA`, `CN`), `jurisdiction_evidence` (every match with its term,
offset, tier and location, counts per tier and location, and the decision per
jurisdiction) and `jurisdiction_tagger` (`jurisdiction-v4`). NULL means not yet
tagged; an empty array means tagged with no tag (unknown). A passage can carry
several codes.

The tagger (`ofl/research/jurisdiction.py`) is deterministic:

- Strong markers: a country or currency-area name (`Brasil`, `EUA`, `Estados Unidos`,
  `zona do euro`, `China`), a national institution or indicator (`IPCA`, `Selic`,
  `Copom`, `Fed`, `FOMC`, `BCE`, `PBoC`) or a national market (`Ibovespa`, `Wall
  Street`, `Nasdaq`).
- Weak markers: demonyms (`brasileiro`, `americana`, `chinês`, `europeu`), cities
  (`Brasília`, `Washington`, `Xangai`), currencies and symbols (`real`, `R$`, `US$`,
  `euro`, `yuan`), state banks (`Banco do Brasil`, `Bank of China`, `China
  Construction Bank`) and generic terms (`Banco Central`, `Tesouro`, `CPI`,
  `Treasury`, `payroll`). They are recorded but never create or support a tag. A
  state bank is national evidence, but its news is usually company news rather than a
  statement about the country, so it is weak, and its country word does not also
  count as a strong mention.
- A reviewed list of multiword entities whose country word does not refer to the
  country (roads, companies, brands, media, sports: `Rodovia Washington Luís`,
  `Lojas Americanas`, `CNN Brasil`, `Air China`, `futebol americano`, `moeda
  americana`) masks those words; they count as nothing. Entities and state banks win
  overlaps, then strong markers, then longer matches.
- A jurisdiction is tagged with one strong title mention or at least two strong body
  mentions. If another jurisdiction has at least three times its strong evidence, it
  needs at least two strong mentions. The decision (`title`, `body`, `single_body`,
  `dominated`, `no_strong_evidence`) is stored per jurisdiction.
- Language is not a marker: Portuguese text does not imply Brazil.
- Location: `title` (the item title, inherited by every passage of the item and
  counted once), `body`, `related` or `unclassified`; only title and body count.
  Related text: a related-link or navigation marker (`Leia também`, `Veja mais`,
  `Saiba mais`, `Mais notícias`, `Tags:` and similar) up to the end of its sentence, at
  most 30 words, and a feed footer (`The post ... appeared first on ...`).
  Abbreviations (`U.S.`, `E.U.A.`, initials, `Sr.`, `Dr.`) do not end a sentence.
  Unmarked text, including a closing fragment without punctuation, stays `body`:
  stored passages have no line structure that would show a link list, and masking
  without a cue would hide legitimate evidence.
- Related regions are found on the whole document before it is split into passages
  (350-word windows overlapping by 40), so a region opened in one passage stays
  `related` in the next. The evidence records `context: document`.

New and re-ingested passages are tagged when indexed. Existing passages are tagged
without re-embedding by:

```sh
ofl research tag-jurisdiction --dry-run      # pending count only
ofl research tag-jurisdiction --limit 1000   # committed batches; resumable
```

A passage is pending when its tagger version differs from the current one or its
item title changed since tagging. Original document text is not stored, so the
backfill rebuilds each document from all passages of the same item revision; their
overlaps must chain exactly, and the result then equals ingestion. When a chain is
incomplete or does not chain, each passage is tagged on its own text, its first 30
words (where a region from an earlier passage could reach) are `unclassified` and do
not count, and the evidence records `context: passage`. The backfill reports how many
passages were tagged that way (`partial_context`).

Schema migration runs in one transaction with `lock_timeout = 5s`; if another session
holds a research table (for example a running worker), it fails with a clear error and
changes nothing instead of queueing behind that session and blocking readers.

Search takes `jurisdiction=off|boost|exclude`, default `off`. Both rules are
experimental opt-ins: `exclude` is offered by the API, MCP `search_archive` and the
dashboard; `boost` only by the API. On the frozen held-out cases, `exclude` raised
P@10 by 0.1 on two cases and lowered none; `boost` lowered one case by 0.2.

Rules act only on precision-gated jurisdictions (`GATED_JURISDICTIONS` in
`ofl/research/jurisdiction.py`): those whose `jurisdiction-v4` tags reached at least
0.90 precision in the hand-check audit, currently `BR` (40/40), `US` (38-39/40,
depending on the judge) and `EA` (3/3). `CN` (17/21 = 0.81) is below the gate: China
passages are still tagged and their evidence stored, but a query naming China is
searched exactly as `off`. When a query mentions any ungated jurisdiction, the whole
query is searched as `off`, even if it also names a gated one (`Brasil e China`); a
partial rule would exclude on the gated name while silently ignoring the other. The
disclosure lists the ungated codes in `ignored` and reports `applied: false`.

Ungated mentions are found on the query side only; passage tagging is unchanged.
They are:

- any tagger match of an ungated code, weak ones included (`chinês`, `yuan`, `Bank of
  China`);
- a mention from a query-only lexicon (`FOREIGN` in `ofl/research/jurisdiction.py`)
  of countries and blocs the tagger has no code for, in Portuguese and English. It
  covers the G20 and OECD members and other frequent markets (`Japão`, `Reino
  Unido`, `Alemanha`, `Argentina`, `México`, `Índia`, `Turquia`, `Hong Kong`,
  `Taiwan`...), their main demonyms (`japonesa`, `argentino`), regions and blocs
  (`América Latina`, `Ásia`, `mercados emergentes`, `Mercosul`, `BRICS`, `G20`)
  and a short list of upper-case ISO codes (`CN`, `CHN`, `JP`, `JPN`, `UK`, `GBR`,
  `DEU`, `FRA`, `ARG`, `MEX`...). Other upper-case words and tickers are not read
  as codes. Matching ignores accents and, except where noted, case, and works on
  whole words. `ignored` reports ISO 3166 alpha-2 codes, or a region name
  (`LATAM`, `ASIA`, `EUROPE`, `EMERGING`, `BLOC`).

Queries are normalised to Unicode NFC before any matching, negation or span
computation, so decomposed accents (`a` followed by a combining tilde) give the same
result as composed ones; combining marks that cannot be composed are dropped when
folding. Passage text is tagged as stored. On the current archive, NFC changes no
tag (every passage in documents with non-NFC text or titles was compared). It is
not applied to tagging yet, because evidence offsets and related-region spans
refer to the stored text, and title hashes must keep matching it. The same negation rules as for named jurisdictions apply: `Brasil -Japão` still
applies the rule to Brazil. Words that are also common lowercase words match only
when capitalised: `Peru` (not `peru`, turkey), `Índia` (not `índia`), `Turkey`,
`Irã`. A mention inside a strong tagger marker does not count, so `Banco Central
Europeu` still names only the euro area.

Europe is not the euro area. `Europa`, `europeu`/`europeia`, `União Europeia`,
`UE` and `EU` never activate `EA`, as before, and a query that contains them is
searched as `off` (ignored `EUROPE` or `EU`), because a rule on the other named
jurisdictions would drop euro-area passages the query asks about. Only an explicit
euro-area name or institution (`zona do euro`, `BCE`, `Eurostat`) names `EA`.
Currency words (`euro`, `real`) are not mentions. Language names that double as
demonyms (`inglês`, `espanhol`) turn the rule off. Missing the rule only costs the
rule, never results. `português` is not in the lexicon, because in this corpus it
almost always names the language.

A rule applies only when the query names a jurisdiction;
`inflação`, `taxa de juros`, `Banco Central` or `câmbio dólar real` never fire
it. Strong markers and national demonyms (`inflação brasileira`) name a
jurisdiction in a query; cities, currencies, state banks and listed entities do not.
European demonyms (`inflação europeia`) do not name the euro area, which needs an
explicit name or institution (`zona do euro`, `eurozona`, `BCE`, `Eurostat`). A marker that overlaps a term
PostgreSQL's `websearch_to_tsquery` negates is ignored: `-Brasil`, `- Brasil`,
`-"Estados Unidos"`, `-Estados Unidos` (only `Estados` is negated, so the name is not
named), including an unclosed quote; a hyphen inside a word (`Brasil-EUA`) or inside
quotes is not negation.

- `off`: results and response identical to the code without jurisdiction support.
- `exclude`: removes passages whose evidence names some other jurisdiction and none
  of the named ones. Untagged, unknown and mixed passages stay. The condition is part
  of the scope, so the keyword arm, the vector arm and `index_coverage` see the same
  passages.
- `boost` (API only): adds one list to the reciprocal-rank fusion with the same k=60: the
  candidates already retrieved, in fused order, that carry evidence of a named
  jurisdiction. A passage gains at most 1/61, the weight of a first place in one arm;
  no passage is added or removed. In single-arm modes the score becomes the fused
  score of the arm and this list, so the boost is relatively stronger there than in
  hybrid.

With a non-`off` rule, the response carries `jurisdiction`: rule, tagger, named codes,
whether it applied, ungated named codes (`ignored`), and, when it applied, counts of scope passages, passages with named evidence,
conflicting and excluded passages, untagged passages, passages tagged by an older
tagger and boosted candidates/results. Each result carries its codes and evidence;
a boosted result has a `jurisdiction_boost` entry in `ranking`. When a jurisdiction
is named and the rule applies, the counts and `index_coverage` come from one scan of the filtered scope,
replacing the coverage query.

`tools/jurisdiction_latency.py [--modes keyword,hybrid] [--repetitions 15]` times the
held-out cases that name a jurisdiction under each rule, interleaved, with read-only
sessions and query embeddings excluded. It reports median and p95 per mode and rule
and exits non-zero when a rule's median exceeds `off` by more than 20%.

`tools/review_retrieval.py collect SPEC OUT --jurisdiction off,boost,exclude` collects
every mode under each rule into one pool; rankings are keyed `hybrid`,
`hybrid+boost`, `hybrid+exclude` and so on, and the reviewer worksheet stays without
modes, rules or tags. The pool records the tag snapshot digest; a tag change during
collection invalidates it. Up to 16 cases are accepted, which covers
`docs/product/retrieval-cases-heldout.json`.

`tools/jurisdiction_audit.py sample OUT --n 100 [--source news] [--exclude-sample
EARLIER]` writes a seeded
random sample stratified by `BR`, `US`, both, none and other tags (current tagger
only) to a new mode-0600 file, with each passage's text, tags and evidence. Reviewers
mark each tag correct, incorrect or uncertain and list missed codes;
`tools/jurisdiction_audit.py assess OUT` prints per-stratum tag precision (correct
over correct plus incorrect) and missed-tag rate, without passage text. Strata are
equal-sized, not population-weighted. `--exclude-sample` (repeatable) keeps passages
of earlier samples out of a new one.

## Public synthetic development exercise

`ofl research evaluate-fixture > /private/synthetic-report.json` creates twenty
explicitly fictional Portuguese documents in a new temporary database schema,
indexes them with the configured pinned real model, and evaluates six authored
cases, including an absent topic. Progress goes to stderr; the final report goes
to stdout. The temporary schema is removed in `finally`; the live corpus, source
objects, pointers and user artifacts are untouched. The database role must be
allowed to create a schema. This is an integration/development exercise, not
held-out relevance, a financial task or participant evidence. Do not tune on these
cases and present their scores as general performance.

Remaining gates: independently judged finance/data questions, source/date/currency
ambiguities, relevant counterevidence and manager disagreement, unsupported-topic
abstention, and actual artifact completion/comprehension by the intended users.
Raw vector similarity is not a calibrated relevance threshold or confidence score.

## GPU indexing workers

Remote inference defaults to one passage per request, preserving the CPU path.
A measured GPU worker can set `OFL_RESEARCH_EMBEDDING_BATCH_SIZE=8` (bounded 1–32).
A batch is committed atomically with pipelined database inserts; a failed later
batch leaves previous commits intact and retries skip those vectors. Input
truncation remains disabled and the configured model digest must match exactly.
Do not replace the pinned digest merely because two model tags share a name.

A GPU worker can connect through a private database tunnel and a loopback inference
endpoint. Use the embedding lane's shared global lock and exclusive lane lock,
including for a one-time backfill; never bypass them with a second unguarded
indexer. Existing source and checks workers can continue. The VPS worker remains
a compatible CPU peer after the GPU releases its lock. Desktop availability is
not a requirement for serving existing vectors or keyword search.

The development exercise indexed twenty documents in 423.48 seconds on the shared
VPS CPU and 67.32 seconds with the local RTX 4090, including the GPU's first model
load and database access over SSH. These are observed integration timings across
different Ollama versions and load conditions, not a controlled hardware-only
benchmark. Both runs returned identical aggregate metrics: vector/hybrid found
all five positive targets first, but both returned hits for the absent-topic case;
keyword found one of five positive targets and correctly returned no hits for the
absent topic. This identifies an abstention/calibration gap; it does not establish
a general search winner. Keep the full reports under docs/product/evidence.
