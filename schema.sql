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
-- bio_short and bio_long are left NULL after import — curate them manually.

CREATE TABLE IF NOT EXISTS authors (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    dynasty     TEXT,
    bio_short   TEXT,                   -- short biography (1–2 sentences)
    bio_long    TEXT,                   -- full biography
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- PGroonga index for searching author names and bios
CREATE INDEX IF NOT EXISTS idx_authors_pgroonga
    ON authors
    USING pgroonga (name, bio_short, bio_long)
    WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false)');

-- Fast lookup by dynasty (for browsing)
CREATE INDEX IF NOT EXISTS idx_authors_dynasty
    ON authors (dynasty);

-- ── Poems table ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS poems (
    id          BIGSERIAL PRIMARY KEY,
    title       TEXT,
    author_id   BIGINT REFERENCES authors (id) ON DELETE SET NULL,
    dynasty     TEXT,
    collection  TEXT NOT NULL,
    text        TEXT,                   -- newline-joined poem body (PGroonga target)
    tags        TEXT[],
    extra       JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- PGroonga full-text index (text and title only; author/dynasty served by B-tree)
CREATE INDEX IF NOT EXISTS idx_poems_pgroonga
    ON poems
    USING pgroonga (text, title)
    WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false)');

CREATE INDEX IF NOT EXISTS idx_poems_dynasty_author_id
    ON poems (dynasty, author_id);

CREATE INDEX IF NOT EXISTS idx_poems_collection
    ON poems (collection);

CREATE INDEX IF NOT EXISTS idx_poems_author_id
    ON poems (author_id);

-- ── Views ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW view_dynasty_author_counts AS
SELECT
    p.dynasty,
    a.name     AS author,
    COUNT(*)   AS poem_count
FROM  poems   p
JOIN  authors a ON a.id = p.author_id
WHERE p.dynasty IS NOT NULL
GROUP BY p.dynasty, a.name
ORDER BY p.dynasty, a.name;

-- Collection summary (poem counts + distinct authors per collection)
CREATE OR REPLACE VIEW view_collection_summary AS
SELECT
    collection,
    dynasty,
    COUNT(*)                  AS poem_count,
    COUNT(DISTINCT author_id) AS author_count
FROM poems
GROUP BY collection, dynasty
ORDER BY poem_count DESC;

-- Authors with their poem counts; useful for building author browse pages
CREATE OR REPLACE VIEW view_authors_with_counts AS
SELECT
    a.id,
    a.name,
    a.dynasty,
    a.bio_short,
    a.bio_long,
    COALESCE(p.poem_count, 0) AS poem_count,
    (a.bio_short IS NOT NULL)  AS bio_curated
FROM authors a
LEFT JOIN (
    SELECT author_id, COUNT(*) AS poem_count
    FROM poems
    GROUP BY author_id
) p ON p.author_id = a.id
ORDER BY poem_count DESC;

-- ── Helper functions (called by the import script) ───────────────────────────

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
        USING pgroonga (text, title)
        WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false)');
END;
$$ LANGUAGE plpgsql;

-- No-op: author_id is set during import via prescan before poems are inserted.
-- Kept for interface compatibility.
CREATE OR REPLACE FUNCTION find_unlinked_authors()
RETURNS TABLE (author TEXT, dynasty TEXT) AS $$
    SELECT NULL::TEXT, NULL::TEXT WHERE FALSE;
$$ LANGUAGE sql;

-- No-op: author_id is set during import.
-- Kept for interface compatibility.
CREATE OR REPLACE FUNCTION link_poems_to_authors()
RETURNS void AS $$
BEGIN
    NULL;
END;
$$ LANGUAGE plpgsql;

-- ── Example queries ───────────────────────────────────────────────────────────

-- Full-text search poems:
-- SELECT id, title, dynasty FROM poems WHERE text &@~ '春风' LIMIT 20;

-- Get an author with their poem count:
-- SELECT * FROM view_authors_with_counts WHERE name = '李白';

-- Authors you still need to curate bios for:
-- SELECT name, dynasty, poem_count FROM view_authors_with_counts
-- WHERE bio_curated = false ORDER BY poem_count DESC;