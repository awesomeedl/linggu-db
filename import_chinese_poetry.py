"""
Chinese Poetry → Postgres Import Script
=========================================
Imports all collections from https://github.com/chinese-poetry/chinese-poetry
into the `poems` and `authors` tables (see schema.sql).

Run order:
  1. Start Postgres: docker compose up -d
  2. This script (imports authors first, then poems, then links them)

SETUP
------
  pip install "psycopg[binary]" gitpython tqdm python-dotenv

  export POSTGRES_DSN="postgresql://postgres:yourpassword@localhost:5432/linggu"
  python import_chinese_poetry.py
"""

import os
import json
import hashlib
import logging

from dotenv import load_dotenv

from pathlib import Path
from typing import Any, Generator

import psycopg
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────────────

load_dotenv()

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "")

REPO_URL   = "https://github.com/chinese-poetry/chinese-poetry.git"
REPO_DIR   = Path("/tmp/chinese-poetry")
BATCH_SIZE = 500
TRUNCATE   = True   # Wipes both tables before importing. Set False to append.

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Repo clone ────────────────────────────────────────────────────────────────

def ensure_repo():
    if REPO_DIR.exists():
        log.info("Repo already present at %s, skipping clone.", REPO_DIR)
        return
    log.info("Cloning repo (may take a minute for ~1 GB of JSON)...")
    import git
    git.Repo.clone_from(REPO_URL, REPO_DIR, depth=1)
    log.info("Clone complete.")

# ── Helpers ───────────────────────────────────────────────────────────────────

def _lines(v) -> list[str]:
    if not v:
        return []
    if isinstance(v, str):
        return [v]
    if isinstance(v, list):
        out = []
        for item in v:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, list):
                out.extend(str(x) for x in item)
        return out
    return [str(v)]


def _text(lines: list[str]) -> str | None:
    return "\n".join(lines) if lines else None


def load_json(path: Path) -> list[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    except Exception as e:
        log.warning("Skipping %s: %s", path, e)
        return []


def content_hash(author: str | None, title: str | None, text: str | None) -> str:
    first_line = (text or "").split("\n")[0].strip()
    key = f"{(author or '').strip()}|{(title or '').strip()}|{first_line}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def chunked(iterable, size):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch

# ── Author import ─────────────────────────────────────────────────────────────
#
# Author files in the repo:
#   全唐诗/authors.tang.json   — Tang poets
#   全唐诗/authors.song.json   — Song shi poets
#   宋词/author.song.json      — Song ci poets
#
# Each record shape (fields vary slightly by file):
#   { "name": "李白", "description": "...", "short_description": "..." }
#   { "name": "苏轼", "desc": "...", "short_desc": "..." }   (ci file uses these)
#
# We normalise everything into source_long_desc / source_short_desc and
# leave the curated bio_short / bio_long columns NULL for manual editing.

AUTHOR_FILES = [
    ("全唐诗/authors.tang.json",  "唐"),
    ("全唐诗/authors.song.json",  "宋"),
    ("宋词/author.song.json",     "宋"),
]


def parse_author(record: dict, dynasty: str) -> dict | None:
    name = (record.get("name") or "").strip()
    if not name:
        return None

    return {
        "name":     name,
        "dynasty":  dynasty,
        "bio_short": None,   # curate manually later
        "bio_long":  None,
    }


def import_authors(conn: psycopg.Connection, repo: Path) -> dict[str, int]:
    """
    Load all author files, deduplicate by name, upsert into authors table.
    Returns a dict of {name: id} for use when linking poems.
    """
    log.info("Importing authors...")
    seen: dict[str, dict] = {}   # name → record (first occurrence wins)

    for rel_path, dynasty in AUTHOR_FILES:
        path = repo / rel_path
        if not path.exists():
            log.warning("Author file not found, skipping: %s", path)
            continue
        for record in load_json(path):
            parsed = parse_author(record, dynasty)
            if parsed and parsed["name"] not in seen:
                seen[parsed["name"]] = parsed

    records = list(seen.values())
    log.info("Found %d unique authors across all files.", len(records))

    total = 0
    with conn.cursor() as cur:
        for batch in chunked(records, BATCH_SIZE):
            cur.executemany(
                """
                INSERT INTO authors (name, dynasty, bio_short, bio_long)
                VALUES (%(name)s, %(dynasty)s, %(bio_short)s, %(bio_long)s)
                ON CONFLICT (name) DO UPDATE SET
                    dynasty = EXCLUDED.dynasty
                """,
                batch,
            )
            total += len(batch)
    conn.commit()
    log.info("Authors imported: %d", total)

    # Fetch all author ids back so we can link poems
    log.info("Fetching author id map...")
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM authors")
        return {row[1]: row[0] for row in cur.fetchall()}

# ── Poem normalizers ──────────────────────────────────────────────────────────

def norm_shi(r: dict, collection: str, dynasty: str) -> dict:
    lines = _lines(r.get("paragraphs"))
    return {
        "collection": collection,
        "dynasty":    dynasty,
        "author":     r.get("author") or r.get("poet"),
        "title":      r.get("title"),
        "text":       _text(lines),
        "tags":       _lines(r.get("tags")),
        "extra":      None,
    }

def norm_ci(r: dict) -> dict:
    lines = _lines(r.get("paragraphs"))
    return {
        "collection": "宋词",
        "dynasty":    "宋",
        "author":     r.get("author"),
        "title":      r.get("rhythmic"),
        "text":       _text(lines),
        "tags":       [r["rhythmic"]] if r.get("rhythmic") else [],
        "extra":      None,
    }

def norm_shijing(r: dict) -> dict:
    lines = _lines(r.get("content"))
    used = {"title", "content", "section"}
    return {
        "collection": "诗经",
        "dynasty":    "先秦",
        "author":     None,
        "title":      r.get("title"),
        "text":       _text(lines),
        "tags":       [r["section"]] if r.get("section") else [],
        "extra":      {k: v for k, v in r.items() if k not in used} or None,
    }

def norm_lunyu(r: dict) -> dict:
    lines = _lines(r.get("paragraphs"))
    return {
        "collection": "论语",
        "dynasty":    "先秦",
        "author":     "孔子",
        "title":      r.get("chapter"),
        "text":       _text(lines),
        "tags":       [],
        "extra":      None,
    }

def norm_wudai(r: dict, sub: str) -> dict:
    lines = _lines(r.get("paragraphs"))
    return {
        "collection": f"五代·{sub}",
        "dynasty":    "五代",
        "author":     r.get("author"),
        "title":      r.get("rhythmic") or r.get("title"),
        "text":       _text(lines),
        "tags":       [r["rhythmic"]] if r.get("rhythmic") else [],
        "extra":      None,
    }

def norm_yuanqu(r: dict) -> dict:
    lines = _lines(r.get("paragraphs"))
    return {
        "collection": "元曲",
        "dynasty":    "元",
        "author":     r.get("author"),
        "title":      r.get("title"),
        "text":       _text(lines),
        "tags":       [r["rhythmic"]] if r.get("rhythmic") else [],
        "extra":      None,
    }

def norm_nalan(r: dict) -> dict:
    lines = _lines(r.get("para") or r.get("paragraphs"))
    return {
        "collection": "纳兰性德",
        "dynasty":    "清",
        "author":     "纳兰性德",
        "title":      r.get("rhythmic") or r.get("title"),
        "text":       _text(lines),
        "tags":       [r["rhythmic"]] if r.get("rhythmic") else [],
        "extra":      None,
    }

def norm_caocao(r: dict) -> dict:
    lines = _lines(r.get("paragraphs"))
    return {
        "collection": "曹操诗集",
        "dynasty":    "汉",
        "author":     "曹操",
        "title":      r.get("title"),
        "text":       _text(lines),
        "tags":       [],
        "extra":      None,
    }

def norm_chuci(r: dict) -> dict:
    lines = _lines(r.get("content") or r.get("paragraphs"))
    return {
        "collection": "楚辞",
        "dynasty":    "先秦",
        "author":     r.get("author"),
        "title":      r.get("title") or r.get("section"),
        "text":       _text(lines),
        "tags":       [],
        "extra":      None,
    }

def norm_mengxue(r: dict) -> dict:
    lines = _lines(r.get("paragraphs"))
    return {
        "collection": "蒙学",
        "dynasty":    None,
        "author":     r.get("author"),
        "title":      r.get("title"),
        "text":       _text(lines),
        "tags":       [],
        "extra":      None,
    }

def norm_sishuwujing(r: dict) -> dict:
    lines = _lines(r.get("paragraphs") or r.get("content"))
    used = {"title", "chapter", "paragraphs", "content"}
    return {
        "collection": "四书五经",
        "dynasty":    "先秦",
        "author":     None,
        "title":      r.get("title") or r.get("chapter"),
        "text":       _text(lines),
        "tags":       [],
        "extra":      {k: v for k, v in r.items() if k not in used} or None,
    }

def norm_youmeng(r: dict) -> dict:
    lines = _lines(r.get("paragraphs") or r.get("content"))
    return {
        "collection": "幽梦影",
        "dynasty":    "清",
        "author":     r.get("author"),
        "title":      r.get("title"),
        "text":       _text(lines),
        "tags":       [],
        "extra":      None,
    }

# ── Poem collection iterator ──────────────────────────────────────────────────

def iter_all_poems(repo: Path) -> Generator[dict, None, None]:
    """
    Yields normalised poem dicts for every collection.

    Intentionally SKIPPED (duplicates of primary files):
      全唐诗/唐诗三百首.json  — subset of poetry.tang.*.json
      全唐诗/水墨唐诗/        — curated subset of poetry.tang.*.json
      御定全唐詩/             — near-duplicate of 全唐诗, lower quality
    """
    tang_dir = repo / "全唐诗"

    for f in sorted(tang_dir.glob("poet.tang.*.json")):
        for r in load_json(f):
            yield norm_shi(r, "唐诗", "唐")

    for f in sorted(tang_dir.glob("poet.song.*.json")):
        for r in load_json(f):
            yield norm_shi(r, "宋诗", "宋")

    for f in sorted((repo / "宋词").glob("ci.song.*.json")):
        for r in load_json(f):
            yield norm_ci(r)

    for sub, label in [("huajianji", "花间集"), ("nantang", "南唐二主词")]:
        d = repo / "五代诗词" / sub
        if d.exists():
            for f in sorted(d.glob("*.json")):
                for r in load_json(f):
                    yield norm_wudai(r, label)

    for f in sorted((repo / "诗经").glob("*.json")):
        for r in load_json(f):
            yield norm_shijing(r)

    for f in sorted((repo / "论语").glob("*.json")):
        for r in load_json(f):
            yield norm_lunyu(r)

    for name, fn in [
        ("楚辞",   norm_chuci),
        ("蒙学",   norm_mengxue),
        ("幽梦影", norm_youmeng),
    ]:
        d = repo / name
        if d.exists():
            for f in sorted(d.glob("*.json")):
                for r in load_json(f):
                    yield fn(r)

    d = repo / "元曲"
    if d.exists():
        for f in sorted(d.glob("*.json")):
            for r in load_json(f):
                yield norm_yuanqu(r)

    d = repo / "纳兰性德"
    if d.exists():
        for f in sorted(d.glob("*.json")):
            for r in load_json(f):
                yield norm_nalan(r)

    d = repo / "曹操诗集"
    if d.exists():
        for f in sorted(d.glob("*.json")):
            for r in load_json(f):
                yield norm_caocao(r)

    d = repo / "四书五经"
    if d.exists():
        for f in sorted(d.glob("*.json")):
            for r in load_json(f):
                yield norm_sishuwujing(r)


def deduped(records: Generator[dict, None, None]) -> Generator[dict, None, None]:
    seen: set[str] = set()
    dupes = 0
    for rec in records:
        h = content_hash(rec["author"], rec["title"], rec["text"])
        if h in seen:
            dupes += 1
            if dupes % 1000 == 0:
                log.info("Skipped %d duplicates so far...", dupes)
            continue
        seen.add(h)
        yield rec
    if dupes:
        log.info("Total duplicates skipped: %d", dupes)

# ── Direct postgres DDL ───────────────────────────────────────────────────────

# ── Poem import ───────────────────────────────────────────────────────────────

def import_poems(conn: psycopg.Connection, repo: Path, author_id_map: dict[str, Any]):
    log.info("Importing poems...")

    # Drop the PGroonga full-text index before bulk loading — updating it on every
    # INSERT batch is slow. We rebuild it once at the end.
    log.info("Dropping PGroonga index for bulk load...")
    with conn.cursor() as cur:
        cur.execute("DROP INDEX IF EXISTS idx_poems_pgroonga")
    conn.commit()

    total = 0
    errors = 0

    with tqdm(unit=" rows", desc="Poems") as bar:
        for batch in chunked(deduped(iter_all_poems(repo)), BATCH_SIZE):
            rows = []
            for poem in batch:
                author_name = poem.pop("author", None)
                poem["author_id"] = author_id_map.get(author_name)
                rows.append(poem)
            try:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO poems
                            (title, author_id, dynasty, collection, text, tags, extra)
                        VALUES
                            (%(title)s, %(author_id)s, %(dynasty)s, %(collection)s,
                             %(text)s, %(tags)s, %(extra)s)
                        """,
                        rows,
                    )
                conn.commit()
                total += len(rows)
                bar.update(len(rows))
            except Exception as e:
                conn.rollback()
                errors += len(rows)
                log.error("Batch failed (%d rows): %s", len(rows), e)

    log.info("Poems imported: %d, errors: %d", total, errors)

    log.info("Rebuilding PGroonga index — this can take 10–30 min for ~1 M rows. The process is not frozen.")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE INDEX idx_poems_pgroonga ON poems
                USING pgroonga (title, dynasty, collection, text)
                WITH (tokenizer='TokenNgram("unify_alphabet", false, "unify_digit", false)')
                """
            )
        conn.commit()
        log.info("PGroonga index rebuilt.")
    except Exception as e:
        conn.rollback()
        log.error(
            "Index rebuild failed. Run this manually:\n"
            "  CREATE INDEX idx_poems_pgroonga ON poems\n"
            "  USING pgroonga (title, dynasty, collection, text)\n"
            "  WITH (tokenizer='TokenNgram(\"unify_alphabet\", false, \"unify_digit\", false)');\n"
            "Error: %s", e
        )

# ── Stub author prescan ───────────────────────────────────────────────────────

def prescan_stub_authors(conn: psycopg.Connection, repo: Path, author_id_map: dict[str, Any]) -> dict[str, Any]:
    """
    Walk all poem records once before import. For every author name that isn't
    already in author_id_map, insert a stub authors row so that import_poems
    can always resolve author_id.

    Returns an updated author_id_map that includes the newly created stubs.
    """
    log.info("Pre-scanning poems for author names missing from authors table...")
    stubs: dict[str, str | None] = {}  # name → dynasty (first occurrence)
    for rec in tqdm(iter_all_poems(repo), unit=" recs", desc="Pre-scan"):
        name = rec.get("author")
        if name and name not in author_id_map and name not in stubs:
            stubs[name] = rec.get("dynasty")

    if not stubs:
        log.info("No stub authors needed.")
        return author_id_map

    log.info("Inserting %d stub author rows...", len(stubs))
    stub_records = [{"name": n, "dynasty": d} for n, d in stubs.items()]
    with conn.cursor() as cur:
        for batch in chunked(stub_records, BATCH_SIZE):
            cur.executemany(
                """
                INSERT INTO authors (name, dynasty)
                VALUES (%(name)s, %(dynasty)s)
                ON CONFLICT (name) DO NOTHING
                """,
                batch,
            )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM authors")
        return {row[1]: row[0] for row in cur.fetchall()}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not POSTGRES_DSN:
        raise SystemExit(
            "Set POSTGRES_DSN before running.\n"
            "  export POSTGRES_DSN='postgresql://postgres:yourpassword@localhost:5432/linggu'"
        )

    ensure_repo()

    with psycopg.connect(POSTGRES_DSN, connect_timeout=10) as conn:
        if TRUNCATE:
            log.info("Truncating poems then authors...")
            with conn.cursor() as cur:
                cur.execute("TRUNCATE poems, authors RESTART IDENTITY CASCADE")
            conn.commit()

        # 1. Authors from the repo's dedicated author JSON files
        author_id_map = import_authors(conn, REPO_DIR)

        # 2. Stub rows for any author names that appear only in poem records
        #    (e.g. 孔子, anonymous poets, minor collections) — must run before
        #    import_poems so every poem.author_id can be resolved.
        author_id_map = prescan_stub_authors(conn, REPO_DIR, author_id_map)

        # 3. Poems (author_id resolved and author name key dropped before insert)
        import_poems(conn, REPO_DIR, author_id_map)

    log.info("All done.")


if __name__ == "__main__":
    main()
