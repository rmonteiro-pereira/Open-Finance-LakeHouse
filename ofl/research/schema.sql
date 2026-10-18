-- Application database only. Never apply to the Airflow metadata database.
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
-- Different model versions coexist; vector dimensions validated by the application.
CREATE TABLE IF NOT EXISTS embeddings (
 passage_id text NOT NULL REFERENCES passages(id), model text NOT NULL,
 revision text NOT NULL, embedding vector NOT NULL,
 PRIMARY KEY(passage_id,model,revision)
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
