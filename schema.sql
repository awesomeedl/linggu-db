-- ============================================================
-- Chinese Poetry → Postgres: Schema
-- ============================================================
-- 1. Enable PGroonga (Supabase: Extensions → "pgroonga" → Enable,
--    or run: CREATE EXTENSION IF NOT EXISTS pgroonga;)
-- 2. Run this file in the SQL editor.
-- ============================================================


-- ── Extension ────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS pgroonga;
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- trigram similarity for fuzzy dedup
-- ── Authors ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS authors (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    dynasty     TEXT,
    bio_short   TEXT,          -- 1–2 sentence biography; curate manually
    bio_long    TEXT,          -- full biography; curate manually
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- PGroonga index for searching author names and bios
CREATE INDEX IF NOT EXISTS idx_authors_pgroonga
    ON authors
    USING pgroonga (name, bio_short, bio_long)
    WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false)');

CREATE INDEX IF NOT EXISTS idx_authors_dynasty ON authors (dynasty);

-- ── Poems ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS poems (
    id            BIGSERIAL PRIMARY KEY,
    title         TEXT,
    author_id     BIGINT REFERENCES authors (id) ON DELETE SET NULL,
    dynasty       TEXT,
    collection    TEXT NOT NULL,
    text          TEXT,          -- newline-joined poem body
    tags          TEXT[],
    extra         JSONB,
    content_hash  TEXT UNIQUE,   -- MD5(author|title|text) dedup key
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Full-text search on poem body and title (author/dynasty served by B-tree)
CREATE INDEX IF NOT EXISTS idx_poems_pgroonga
    ON poems
    USING pgroonga (text, title)
    WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false)');

CREATE INDEX IF NOT EXISTS idx_poems_dynasty_author_id ON poems (dynasty, author_id);
CREATE INDEX IF NOT EXISTS idx_poems_collection        ON poems (collection);
CREATE INDEX IF NOT EXISTS idx_poems_author_id         ON poems (author_id);

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

-- ── Migration: add content_hash to an existing database ──────────────────────
-- Run once on a live DB that pre-dates content_hash. All statements are idempotent.

-- 1. Add column.
-- ALTER TABLE poems ADD COLUMN IF NOT EXISTS content_hash TEXT;

-- 2. Backfill (covers punctuation/whitespace normalization; re-import with opencc
--    is needed to deduplicate trad/simplified variants).
-- UPDATE poems p
-- SET content_hash = md5(
--     regexp_replace(coalesce(a.name,  ''), '[[:space:]\u3000\u3001-\u303F\uFF00-\uFFEF]', '', 'g') || '|' ||
--     regexp_replace(coalesce(p.title, ''), '[[:space:]\u3000\u3001-\u303F\uFF00-\uFFEF]', '', 'g') || '|' ||
--     regexp_replace(coalesce(p.text,  ''), '[[:space:]\u3000\u3001-\u303F\uFF00-\uFFEF]', '', 'g'))
-- FROM authors a WHERE a.id = p.author_id AND p.content_hash IS NULL;
--
-- UPDATE poems
-- SET content_hash = md5(
--     '' || '|' ||
--     regexp_replace(coalesce(title, ''), '[[:space:]\u3000\u3001-\u303F\uFF00-\uFFEF]', '', 'g') || '|' ||
--     regexp_replace(coalesce(text,  ''), '[[:space:]\u3000\u3001-\u303F\uFF00-\uFFEF]', '', 'g'))
-- WHERE author_id IS NULL AND content_hash IS NULL;

-- 3. Drop exact duplicates, keep lowest id.
-- DELETE FROM poems WHERE id IN (
--     SELECT id FROM (
--         SELECT id, ROW_NUMBER() OVER (PARTITION BY content_hash ORDER BY id) AS rn
--         FROM poems WHERE content_hash IS NOT NULL
--     ) t WHERE rn > 1
-- );

-- 4. Apply the unique constraint.
-- ALTER TABLE poems ADD CONSTRAINT poems_content_hash_key UNIQUE (content_hash);

-- 5. Find fuzzy near-duplicates (trad/simplified variants, punctuation survivors).
-- CREATE INDEX IF NOT EXISTS idx_poems_trgm ON poems USING gin (text gin_trgm_ops);
--
-- SELECT p1.id AS keep_id, p2.id AS dup_id,
--        round(similarity(p1.text, p2.text)::numeric, 2) AS sim,
--        p1.title, p1.collection, p2.collection AS dup_collection
-- FROM poems p1
-- JOIN poems p2 ON p1.author_id = p2.author_id AND p1.id < p2.id
-- WHERE similarity(p1.text, p2.text) >= 0.90
-- ORDER BY sim DESC LIMIT 200;
--
-- DELETE FROM poems WHERE id IN (
--     SELECT p2.id FROM poems p1
--     JOIN poems p2 ON p1.author_id = p2.author_id
--                  AND p1.id < p2.id
--                  AND similarity(p1.text, p2.text) >= 0.90
-- );

-- ── Example queries ───────────────────────────────────────────────────────────

-- SELECT id, title, dynasty FROM poems WHERE text &@~ '春风' LIMIT 20;
-- SELECT * FROM view_authors_with_counts WHERE name = '李白';
-- SELECT name, dynasty, poem_count FROM view_authors_with_counts
-- WHERE NOT bio_curated ORDER BY poem_count DESC;