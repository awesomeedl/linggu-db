-- ============================================================
-- Chinese Poetry → Supabase: Schema
-- ============================================================
-- Run order:
--   1. Enable PGroonga:
--      Supabase dashboard → Extensions → search "pgroonga" → Enable
--      (or: CREATE EXTENSION IF NOT EXISTS pgroonga;)
--   2. Run this entire file in the SQL editor.
-- ============================================================


-- ── Extension ────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS pgroonga;


-- ── Authors table ─────────────────────────────────────────────────────────────
-- Seeded from the repo's authors.tang.json, authors.song.json, author.song.json.
-- Fields from the repo: name, short_desc, long_desc.
--
-- bio_short and bio_long are left NULL after import — you curate them manually.
-- source_desc preserves whatever raw description came from the repo so you have
-- something to reference while writing bios.

CREATE TABLE IF NOT EXISTS authors (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,   -- the join key used by poems.author
    dynasty     TEXT,                   -- inferred from which file they came from
    -- Repo-sourced description fields (raw, for your reference while curating)
    source_short_desc   TEXT,           -- "short_description" from repo JSON
    source_long_desc    TEXT,           -- "description" / "long_desc" from repo JSON
    -- Your curated fields (fill these in manually later)
    bio_short   TEXT,                   -- short biography (1–2 sentences)
    bio_long    TEXT,                   -- full biography
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- PGroonga index for searching author names and bios
CREATE INDEX IF NOT EXISTS idx_authors_pgroonga
    ON authors
    USING pgroonga (name, bio_short, bio_long, source_long_desc)
    WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false)');

-- Fast lookup by dynasty (for browsing)
CREATE INDEX IF NOT EXISTS idx_authors_dynasty
    ON authors (dynasty);


-- ── Poems table ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS poems (
    id          BIGSERIAL PRIMARY KEY,
    title       TEXT,
    author      TEXT,                   -- denormalised name, matches authors.name
    author_id   BIGINT REFERENCES authors (id) ON DELETE SET NULL,
    dynasty     TEXT,
    collection  TEXT NOT NULL,
    text        TEXT,                   -- newline-joined poem body (PGroonga target)
    tags        TEXT[],
    extra       JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- PGroonga full-text index across all searchable text columns
CREATE INDEX IF NOT EXISTS idx_poems_pgroonga
    ON poems
    USING pgroonga (title, author, dynasty, collection, text)
    WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false)');

-- Covering index for dynasty/author browsing (mirrors your SQLite schema)
CREATE INDEX IF NOT EXISTS idx_poems_dynasty_author
    ON poems (dynasty, author);

CREATE INDEX IF NOT EXISTS idx_poems_collection
    ON poems (collection);

CREATE INDEX IF NOT EXISTS idx_poems_author_id
    ON poems (author_id);


-- ── Views ─────────────────────────────────────────────────────────────────────

-- Direct port of your SQLite view_dynasty_author_counts
CREATE OR REPLACE VIEW view_dynasty_author_counts AS
SELECT
    dynasty,
    author,
    COUNT(*) AS poem_count
FROM poems
WHERE dynasty IS NOT NULL
  AND author IS NOT NULL
GROUP BY dynasty, author
ORDER BY dynasty, author;

-- Collection summary (poem counts + distinct authors per collection)
CREATE OR REPLACE VIEW view_collection_summary AS
SELECT
    collection,
    dynasty,
    COUNT(*)                AS poem_count,
    COUNT(DISTINCT author)  AS author_count
FROM poems
GROUP BY collection, dynasty
ORDER BY poem_count DESC;

-- Authors with their poem counts, joined from poems table
-- Useful for building author browse pages
CREATE OR REPLACE VIEW view_authors_with_counts AS
SELECT
    a.id,
    a.name,
    a.dynasty,
    a.bio_short,
    a.bio_long,
    a.source_short_desc,
    a.source_long_desc,
    COALESCE(p.poem_count, 0) AS poem_count,
    -- Curation progress flag: true once you've written a bio
    (a.bio_short IS NOT NULL) AS bio_curated
FROM authors a
LEFT JOIN (
    SELECT author, COUNT(*) AS poem_count
    FROM poems
    GROUP BY author
) p ON p.author = a.name
ORDER BY poem_count DESC;


-- ── Helper functions (called by the import script) ───────────────────────────

-- Returns author names that appear in poems but have no authors row yet.
-- Used by the import script to create stub rows for manual curation.
CREATE OR REPLACE FUNCTION find_unlinked_authors()
RETURNS TABLE (author TEXT, dynasty TEXT) AS $$
    SELECT DISTINCT p.author, p.dynasty
    FROM poems p
    WHERE p.author IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM authors a WHERE a.name = p.author
      );
$$ LANGUAGE sql;

-- Sets poems.author_id for every row where the name matches authors.name
-- but author_id is still NULL. Safe to call multiple times.
CREATE OR REPLACE FUNCTION drop_poem_search_index()
RETURNS void AS $$
BEGIN
    SET LOCAL statement_timeout = 0;
    DROP INDEX IF EXISTS idx_poems_pgroonga;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION rebuild_poem_search_index()
RETURNS void AS $$
BEGIN
    SET LOCAL statement_timeout = 0;
    CREATE INDEX IF NOT EXISTS idx_poems_pgroonga
        ON poems
        USING pgroonga (title, author, dynasty, collection, text)
        WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false)');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION link_poems_to_authors()
RETURNS void AS $$
BEGIN
    SET LOCAL statement_timeout = 0;
    UPDATE poems p
    SET author_id = a.id
    FROM authors a
    WHERE p.author = a.name
      AND p.author_id IS NULL;
END;
$$ LANGUAGE plpgsql;


-- ── Example queries ───────────────────────────────────────────────────────────

-- Full-text search poems:
-- SELECT id, title, author, dynasty FROM poems WHERE text &@~ '春风' LIMIT 20;

-- Search by author name (FTS):
-- SELECT * FROM poems WHERE author &@~ '李白';

-- Get an author with their poem count:
-- SELECT * FROM view_authors_with_counts WHERE name = '李白';

-- Authors you still need to curate bios for:
-- SELECT name, dynasty, poem_count FROM view_authors_with_counts
-- WHERE bio_curated = false ORDER BY poem_count DESC;

-- Link poems to authors after import (run once after both tables are loaded):
-- UPDATE poems p
-- SET author_id = a.id
-- FROM authors a
-- WHERE p.author = a.name;
