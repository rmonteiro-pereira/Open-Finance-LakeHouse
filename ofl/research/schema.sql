-- Application database only. Never apply to the Airflow metadata database.
-- One transaction. Fail fast instead of queueing behind (and blocking) live readers.
SET LOCAL lock_timeout = '5s';
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS research_schema (version integer PRIMARY KEY);
INSERT INTO research_schema VALUES (1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS imports (
 id text PRIMARY KEY, source text NOT NULL, sha256 text NOT NULL, name text NOT NULL,
 size bigint NOT NULL, object_key text NOT NULL, parser_version text NOT NULL,
 state text NOT NULL DEFAULT 'registered', metadata jsonb NOT NULL DEFAULT '{}',
 counts jsonb NOT NULL DEFAULT '{}', error text, created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(), UNIQUE(source,sha256,parser_version)
);
CREATE TABLE IF NOT EXISTS jobs (
 id text PRIMARY KEY, kind text NOT NULL, payload jsonb NOT NULL,
 state text NOT NULL DEFAULT 'queued', progress jsonb NOT NULL DEFAULT '{}', error text,
 lease_owner text, lease_until timestamptz, attempts integer NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jobs_pending ON jobs(state,created_at);
CREATE TABLE IF NOT EXISTS requests (
 actor text NOT NULL, key text NOT NULL, digest text NOT NULL, response jsonb NOT NULL,
 PRIMARY KEY(actor,key)
);
CREATE TABLE IF NOT EXISTS audit (
 id bigserial PRIMARY KEY, actor text NOT NULL, action text NOT NULL,
 target text NOT NULL, detail jsonb NOT NULL DEFAULT '{}', at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS quote_files (
 import_id text PRIMARY KEY REFERENCES imports(id), frequency integer NOT NULL,
 generated date NOT NULL, parquet_key text NOT NULL, dates jsonb NOT NULL,
 supersedes text REFERENCES quote_files(import_id)
);
CREATE TABLE IF NOT EXISTS quote_dates (
 date date PRIMARY KEY, import_id text NOT NULL REFERENCES quote_files(import_id)
);
CREATE TABLE IF NOT EXISTS releases (
 id text PRIMARY KEY, kind text NOT NULL, delta_uri text NOT NULL, delta_version bigint NOT NULL,
 metadata jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pointers (kind text PRIMARY KEY, release_id text NOT NULL REFERENCES releases(id));
CREATE TABLE IF NOT EXISTS items (
 id text PRIMARY KEY, source text NOT NULL, source_id text NOT NULL,
 title text NOT NULL, description text NOT NULL DEFAULT '', item_type text NOT NULL DEFAULT 'document',
 collection text NOT NULL DEFAULT '', date_start date, date_end date,
 date_text text NOT NULL DEFAULT '', date_precision text NOT NULL DEFAULT 'unknown',
 language text NOT NULL DEFAULT 'por', access text NOT NULL DEFAULT 'private',
 metadata jsonb NOT NULL DEFAULT '{}', revision text NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT now(), UNIQUE(source,source_id)
);
CREATE INDEX IF NOT EXISTS items_dates ON items(date_start,date_end);
CREATE TABLE IF NOT EXISTS assets (
 id text PRIMARY KEY, item_id text NOT NULL REFERENCES items(id), sha256 text NOT NULL,
 object_key text NOT NULL, name text NOT NULL, media_type text NOT NULL,
 state text NOT NULL DEFAULT 'registered', error text, UNIQUE(item_id,sha256)
);
-- Asset membership can be superseded without deleting the preserved original.
ALTER TABLE assets ADD COLUMN IF NOT EXISTS active boolean NOT NULL DEFAULT true;
CREATE TABLE IF NOT EXISTS passages (
 id text PRIMARY KEY, item_id text NOT NULL REFERENCES items(id),
 asset_id text REFERENCES assets(id), page integer, ordinal integer NOT NULL,
 text text NOT NULL, revision text NOT NULL, method text NOT NULL,
 evidence jsonb NOT NULL DEFAULT '{}', active boolean NOT NULL DEFAULT true,
 search tsvector NOT NULL, UNIQUE(item_id,revision,ordinal)
);
CREATE INDEX IF NOT EXISTS passages_search ON passages USING gin(search) WHERE active;
CREATE INDEX IF NOT EXISTS passages_item ON passages(item_id) WHERE active;
-- Lexical jurisdiction evidence (ofl/research/jurisdiction.py). NULL = not yet tagged;
-- an empty array = tagged, no evidence. Tagger version and title digest detect stale tags.
ALTER TABLE passages ADD COLUMN IF NOT EXISTS jurisdiction text[];
ALTER TABLE passages ADD COLUMN IF NOT EXISTS jurisdiction_evidence jsonb;
ALTER TABLE passages ADD COLUMN IF NOT EXISTS jurisdiction_tagger text;
-- Accent-insensitive, locale-independent lexical search (ofl/research/lexical.py). The
-- vectors are rebuilt by `ofl research rebuild-search`; search_version marks rebuilt rows.
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE OR REPLACE FUNCTION research_search_text(value text) RETURNS text
 LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
 AS $fn$ SELECT translate(value, '“”„‟«»‘’‚‛‹›—–‐‑‒―…•·', '""""""               ') $fn$;
DO $ts$ DECLARE dictionary text;
BEGIN
 -- The unaccent dictionary lives in the extension's schema, wherever it was installed.
 SELECT quote_ident(n.nspname) || '.unaccent' INTO dictionary FROM pg_ts_dict d
  JOIN pg_namespace n ON n.oid=d.dictnamespace JOIN pg_extension e ON e.extnamespace=n.oid
  WHERE d.dictname='unaccent' AND e.extname='unaccent';
 IF NOT EXISTS (SELECT 1 FROM pg_ts_config c JOIN pg_namespace n ON n.oid=c.cfgnamespace
   WHERE c.cfgname='portuguese_unaccent' AND n.nspname=current_schema()) THEN
  CREATE TEXT SEARCH CONFIGURATION portuguese_unaccent (COPY = pg_catalog.portuguese);
  EXECUTE format('ALTER TEXT SEARCH CONFIGURATION portuguese_unaccent
   ALTER MAPPING FOR hword, hword_part, word WITH %s, portuguese_stem', dictionary);
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_ts_config c JOIN pg_namespace n ON n.oid=c.cfgnamespace
   WHERE c.cfgname='simple_unaccent' AND n.nspname=current_schema()) THEN
  CREATE TEXT SEARCH CONFIGURATION simple_unaccent (COPY = pg_catalog.simple);
  EXECUTE format('ALTER TEXT SEARCH CONFIGURATION simple_unaccent
   ALTER MAPPING FOR hword, hword_part, word WITH %s, simple', dictionary);
 END IF;
END $ts$;
ALTER TABLE passages ADD COLUMN IF NOT EXISTS search_version text;
-- Item title vectors for keyword search, so a query does not rebuild one per matching
-- passage. The trigger keeps them in step with items.title; every migration refreshes any
-- that differ (a changed configuration or research_search_text), and search computes a
-- missing one on the fly. The expression must stay equal to search.HEADING.
CREATE TABLE IF NOT EXISTS item_headings (
 item_id text PRIMARY KEY REFERENCES items(id) ON DELETE CASCADE, heading tsvector NOT NULL
);
CREATE OR REPLACE FUNCTION research_item_heading() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
 INSERT INTO item_headings(item_id,heading)
  VALUES (NEW.id,setweight(to_tsvector('portuguese_unaccent',
   research_search_text(COALESCE(NEW.title,''))),'A'))
  ON CONFLICT (item_id) DO UPDATE SET heading=EXCLUDED.heading;
 RETURN NULL;
END $fn$;
CREATE OR REPLACE TRIGGER items_heading AFTER INSERT OR UPDATE OF title ON items
 FOR EACH ROW EXECUTE FUNCTION research_item_heading();
INSERT INTO item_headings(item_id,heading)
 SELECT id,setweight(to_tsvector('portuguese_unaccent',research_search_text(COALESCE(title,''))),'A')
 FROM items
 ON CONFLICT (item_id) DO UPDATE SET heading=EXCLUDED.heading
 WHERE item_headings.heading IS DISTINCT FROM EXCLUDED.heading;
-- Different model versions coexist; vector dimensions validated by the application.
CREATE TABLE IF NOT EXISTS embeddings (
 passage_id text NOT NULL REFERENCES passages(id), model text NOT NULL,
 revision text NOT NULL, embedding vector NOT NULL,
 PRIMARY KEY(passage_id,model,revision)
);
-- A passage the model cannot embed whole is rejected for that exact text and
-- model space, never truncated. Changed passage content becomes eligible again.
CREATE TABLE IF NOT EXISTS embedding_rejections (
 passage_id text NOT NULL REFERENCES passages(id), model text NOT NULL, revision text NOT NULL,
 passage_revision text NOT NULL, text_sha256 text NOT NULL, reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(passage_id,model,revision)
);
CREATE TABLE IF NOT EXISTS candidates (
 id text PRIMARY KEY, asset_id text NOT NULL REFERENCES assets(id), page integer NOT NULL,
 bbox jsonb NOT NULL, cells jsonb NOT NULL, version text NOT NULL,
 evidence jsonb NOT NULL DEFAULT '{}',
 state text NOT NULL DEFAULT 'needs_review', current_revision integer NOT NULL DEFAULT 0,
 UNIQUE(asset_id,page,version,bbox)
);
ALTER TABLE candidates ADD COLUMN IF NOT EXISTS evidence jsonb NOT NULL DEFAULT '{}';
CREATE TABLE IF NOT EXISTS reviews (
 candidate_id text NOT NULL REFERENCES candidates(id), revision integer NOT NULL,
 actor text NOT NULL, state text NOT NULL, cells jsonb NOT NULL, mapping jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(candidate_id,revision)
);
CREATE TABLE IF NOT EXISTS observations (
 id text PRIMARY KEY, candidate_id text NOT NULL REFERENCES candidates(id),
 revision integer NOT NULL, date date NOT NULL, subject text NOT NULL, market text NOT NULL,
 metric text NOT NULL, value numeric(38,12) NOT NULL, unit text NOT NULL, currency text,
 evidence jsonb NOT NULL, active boolean NOT NULL DEFAULT true
);
CREATE INDEX IF NOT EXISTS observations_lookup ON observations(subject,date) WHERE active;
ALTER TABLE observations ADD COLUMN IF NOT EXISTS date_end date;
ALTER TABLE observations ADD COLUMN IF NOT EXISTS date_precision text NOT NULL DEFAULT 'day';
ALTER TABLE observations ADD COLUMN IF NOT EXISTS date_text text NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS uploads (
 id text PRIMARY KEY, actor text NOT NULL, name text NOT NULL, source text NOT NULL,
 expected_size bigint NOT NULL, expected_sha256 text NOT NULL, received bigint NOT NULL DEFAULT 0,
 state text NOT NULL DEFAULT 'open', metadata jsonb NOT NULL DEFAULT '{}'
);

-- Existing collector/snapshot bridge. Originals and derived facts remain separate.
CREATE TABLE IF NOT EXISTS library_entries (
 kind text NOT NULL, source_key text NOT NULL, signature text NOT NULL,
 detail jsonb NOT NULL DEFAULT '{}', updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(kind,source_key)
);
CREATE TABLE IF NOT EXISTS macro_snapshots (
 id text PRIMARY KEY, generated_at timestamptz NOT NULL, fingerprint text NOT NULL,
 observations_key text NOT NULL, sha256 text NOT NULL,
 manifest jsonb NOT NULL, catalog jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS macro_pointer (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 snapshot_id text NOT NULL REFERENCES macro_snapshots(id)
);

-- User work is separate from collector content and immutable source versions.
CREATE TABLE IF NOT EXISTS workspaces (
 id text PRIMARY KEY, owner text NOT NULL, kind text NOT NULL,
 title text NOT NULL, current_revision integer NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS workspaces_owner ON workspaces(owner,kind);
CREATE TABLE IF NOT EXISTS workspace_revisions (
 workspace_id text NOT NULL REFERENCES workspaces(id), revision integer NOT NULL,
 body jsonb NOT NULL, sha256 text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(workspace_id,revision)
);
CREATE TABLE IF NOT EXISTS research_feedback (
 id text PRIMARY KEY, owner text NOT NULL, category text NOT NULL, detail text NOT NULL,
 artifact_id text, state text NOT NULL DEFAULT 'new', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS feedback_reviews (
 feedback_id text NOT NULL REFERENCES research_feedback(id), revision integer NOT NULL,
 actor text NOT NULL, state text NOT NULL, note text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(feedback_id,revision)
);
CREATE TABLE IF NOT EXISTS check_schedules (
 workspace_id text PRIMARY KEY REFERENCES workspaces(id), owner text NOT NULL,
 interval_minutes integer NOT NULL CHECK(interval_minutes BETWEEN 15 AND 10080),
 enabled boolean NOT NULL DEFAULT true, next_run_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS job_events (
 id bigserial PRIMARY KEY, job_id text NOT NULL REFERENCES jobs(id),
 state text NOT NULL, detail jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS job_events_job ON job_events(job_id,id);
