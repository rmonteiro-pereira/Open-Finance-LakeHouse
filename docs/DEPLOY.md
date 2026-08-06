# Deploying the public release

Everything in this document is a **human step**. Nothing here has been executed — the
code paths it describes are tested offline, the artefact builds and verifies locally, and
what is missing is exactly the two things an agent cannot create: a repository and a
credential.

Stating that plainly is the point. A `health.json` that nobody publishes is worse than an
acknowledged gap, because the gap at least appears on the status page.

---

## 1. What already works, and how to see it

From a clean clone, with no credentials and no cluster:

```bash
uv sync --extra dev

ofl release build \
  --from tests/fixtures/release \
  --to out \
  --release-id 1970-01-01.1 \
  --release-class fixture            # exit 0

ofl release verify out --expect-class fixture   # exit 0
ofl release verify out                          # exit 3 — gate `class`, see out/verify_report.json
ofl release verify /does/not/exist               # exit 2 — usage, not a verdict
```

The exit codes are load-bearing: **2** means the question was malformed, **3** means the
answer was no. A test that only asserts "non-zero" cannot tell a wrong path from a gate
that bit.

---

## 2. Create the data repository

The release is published to a **separate public repository**, `ofl-public-data`, with no
code in it. Two reasons, both about blast radius:

- The token the cluster holds needs `contents:write`. On the code repository that is
  permission to push code, so a compromised homelab would reach the source.
- `releases/latest` is **global to a repository**. The day the OFL cuts a *code* release,
  the alias serving *data* would start pointing at it, silently.

```bash
gh repo create rmonteiro-pereira/ofl-public-data --public \
  --description "Published data releases of the Open-Finance LakeHouse"

# the watchdog, from the path that is inert in this repo on purpose
git clone https://github.com/rmonteiro-pereira/ofl-public-data /tmp/ofl-public-data
cp -r publish/ofl-public-data/.github /tmp/ofl-public-data/
cd /tmp/ofl-public-data && git add .github && git commit -m "watchdog" && git push
```

## 3. Mint the token the cluster will hold

A fine-grained personal access token, scoped to **`ofl-public-data` only**, with
`Contents: read and write` and nothing else. Then, in the cluster:

```bash
kubectl create secret generic ofl-publish-token \
  --from-literal=GITHUB_TOKEN=<token> -n airflow
```

The push goes **from inside out**: Airflow POSTs to the GitHub API. There is no
self-hosted runner and no tunnel — a GitHub-hosted runner will never reach
`minio.minio.svc.cluster.local`, which is the same problem R2 and MinIO present from two
sides.

## 4. Publish for real

```bash
ofl release build --from <gold export dir> --to out \
  --release-id "$(date -u +%F).1" --release-class production --supersedes <previous|none> \
  --base-url "https://github.com/rmonteiro-pereira/ofl-public-data/releases/download/data-$(date -u +%F).1/"

ofl release verify out            # must exit 0 against the PRODUCTION expectation
```

The `gh://` sink refuses `publishable: false` by construction, so a fixture release cannot
reach a public URL by mistake.

---

## 5. What is NOT done, and what it costs

| Gap | Consequence today |
|---|---|
| `ofl-public-data` does not exist | No release has ever been published; every URL in this document is prospective |
| No `GITHUB_TOKEN` in the cluster | The `gh://` sink can plan an upload (`--dry-run`) but has never performed one |
| `health.json` therefore has no author | **The product has one channel and one author, and the author is the producer** — precisely the party that cannot attest to its own death |
| Site DNS | The reader surface builds as a static export; pointing a hostname at it is a deploy-time step |

The third row is the one worth repeating out loud, because the design leans on two
independent authors and only one of them exists. Until the watchdog runs, a total failure
of the producer looks identical to a quiet week.
