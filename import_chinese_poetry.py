"""
Chinese Poetry → Postgres Import Script
=========================================
Imports all collections from https://github.com/chinese-poetry/chinese-poetry
into the `poems` and `authors` tables defined in schema.sql.

SETUP
-----
  pip install "psycopg[binary]" gitpython tqdm python-dotenv
  pip install opencc-python-reimplemented   # optional: catches trad/simplified variants

  export POSTGRES_DSN="postgresql://postgres:yourpassword@localhost:5432/linggu"
  python import_chinese_poetry.py
"""

import csv
import git
import hashlib
import json
import logging
import os
import re
import unicodedata
from pathlib import Path
from typing import Any, Generator

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb
from tqdm import tqdm

# ── Optional: traditional ↔ simplified Chinese conversion ─────────────────────
# Catches duplicate poems that exist in both scripts.
# Install: pip install opencc-python-reimplemented
_CC = None
try:
    import opencc as _opencc_mod
    _CC = _opencc_mod.OpenCC('t2s')   # traditional → simplified
except ImportError:
    pass

# Strips everything that is NOT a CJK ideograph or ASCII alphanumeric.
# This normalises away punctuation variants (。vs . vs ，vs ,) and whitespace.
_NON_HANZI_RE = re.compile(
    r'[^\u4E00-\u9FFF\u3400-\u4DBF\uF900-\uFAFFa-z0-9]',
    re.IGNORECASE,
)

# ── Configuration ─────────────────────────────────────────────────────────────

load_dotenv()

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "")

REPO_URL   = "https://github.com/chinese-poetry/chinese-poetry.git"
REPO_DIR   = Path("/tmp/chinese-poetry")
BATCH_SIZE = 500
TRUNCATE   = True   # Wipes both tables before importing. Set False to append.
REPORT_PATH = Path("discarded_poems.csv")  # Set to None to disable the report.

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


def _normalize(text: str | None) -> str:
    """
    Normalise a string for deduplication:
      1. NFKC — collapses full-width characters (Ａ→A, ０→0, etc.)
      2. Traditional → simplified Chinese (requires opencc; skipped if not installed)
      3. Strip everything except CJK ideographs and ASCII alphanumerics
         — removes all whitespace, punctuation, and symbol variants
    """
    if not text:
        return ''
    text = unicodedata.normalize('NFKC', text)
    if _CC is not None:
        text = _CC.convert(text)
    return _NON_HANZI_RE.sub('', text).lower()


def content_hash(author: str | None, title: str | None, text: str | None) -> str:
    key = f"{_normalize(author)}|{_normalize(title)}|{_normalize(text)}"
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
# Source files:
#   全唐诗/authors.tang.json   — Tang poets
#   全唐诗/authors.song.json   — Song 诗 poets
#   宋词/author.song.json      — Song 词 poets
#
# bio_short and bio_long are left NULL after import — curate them manually.

AUTHOR_FILES = [
    ("全唐诗/authors.tang.json",  "唐"),
    ("全唐诗/authors.song.json",  "宋"),
    ("宋词/author.song.json",     "宋"),
]


def import_authors(conn: psycopg.Connection, repo: Path) -> dict[str, int]:
    """Load all author files, deduplicate by name, upsert into authors table.
    Returns a dict of {name: id} for use when linking poems."""
    log.info("Importing authors...")
    seen: dict[str, dict] = {}   # name → record (first occurrence wins)

    for rel_path, dynasty in AUTHOR_FILES:
        path = repo / rel_path
        if not path.exists():
            log.warning("Author file not found, skipping: %s", path)
            continue
        for record in load_json(path):
            name = (record.get("name") or "").strip()
            if name and name not in seen:
                seen[name] = {"name": name, "dynasty": dynasty, "bio_short": None, "bio_long": None}

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

# ── Poem normalizer ───────────────────────────────────────────────────────────

_UNSET = object()  # sentinel: author not supplied → look it up from the record

def _norm(
    r: dict,
    *,
    collection: str,
    dynasty: str | None,
    author: Any = _UNSET,                        # fixed value, or _UNSET to read from record
    author_keys: tuple[str, ...] = ("author",),  # record keys tried in order when author is _UNSET
    body_keys: tuple[str, ...] = ("paragraphs",),
    title_keys: tuple[str, ...] = ("title",),
    tag_key: str | None = None,   # single tag: [r[tag_key]] or []
    tags_key: str | None = None,  # list of tags: _lines(r[tags_key])
    extra_exclude: set[str] | None = None,
) -> dict:
    body  = next((r.get(k) for k in body_keys  if r.get(k)), None)
    title = next((r.get(k) for k in title_keys if r.get(k)), None)
    if author is _UNSET:
        author = next((r.get(k) for k in author_keys if r.get(k)), None)
    if tags_key:
        tags = _lines(r.get(tags_key))
    elif tag_key:
        tags = [r[tag_key]] if r.get(tag_key) else []
    else:
        tags = []
    extra = ({k: v for k, v in r.items() if k not in extra_exclude} or None) if extra_exclude else None
    return {
        "collection": collection,
        "dynasty":    dynasty,
        "author":     author,
        "title":      title,
        "text":       _text(_lines(body)),
        "tags":       tags,
        "extra":      extra,
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
            yield _norm(r, collection="唐诗", dynasty="唐", author_keys=("author", "poet"), tags_key="tags")

    for f in sorted(tang_dir.glob("poet.song.*.json")):
        for r in load_json(f):
            yield _norm(r, collection="宋诗", dynasty="宋", author_keys=("author", "poet"), tags_key="tags")

    for f in sorted((repo / "宋词").glob("ci.song.*.json")):
        for r in load_json(f):
            yield _norm(r, collection="宋词", dynasty="宋", title_keys=("rhythmic",), tag_key="rhythmic")

    for sub, label in [("huajianji", "花间集"), ("nantang", "南唐二主词")]:
        d = repo / "五代诗词" / sub
        if d.exists():
            for f in sorted(d.glob("*.json")):
                for r in load_json(f):
                    yield _norm(r, collection=f"五代·{label}", dynasty="五代",
                                title_keys=("rhythmic", "title"), tag_key="rhythmic")

    for f in sorted((repo / "诗经").glob("*.json")):
        for r in load_json(f):
            yield _norm(r, collection="诗经", dynasty="先秦", author=None,
                        body_keys=("content",), tag_key="section",
                        extra_exclude={"title", "content", "section"})

    for f in sorted((repo / "论语").glob("*.json")):
        for r in load_json(f):
            yield _norm(r, collection="论语", dynasty="先秦", author="孔子", title_keys=("chapter",))

    for name, kwargs in [
        ("楚辞",   dict(collection="楚辞",   dynasty="先秦", body_keys=("content", "paragraphs"), title_keys=("title", "section"))),
        ("蒙学",   dict(collection="蒙学",   dynasty=None,   body_keys=("paragraphs", "content"))),
        ("幽梦影", dict(collection="幽梦影", dynasty="清",   body_keys=("paragraphs", "content"))),
    ]:
        d = repo / name
        if d.exists():
            for f in sorted(d.glob("*.json")):
                for r in load_json(f):
                    yield _norm(r, **kwargs)

    d = repo / "元曲"
    if d.exists():
        for f in sorted(d.glob("*.json")):
            for r in load_json(f):
                yield _norm(r, collection="元曲", dynasty="元", tag_key="rhythmic")

    d = repo / "纳兰性德"
    if d.exists():
        for f in sorted(d.glob("*.json")):
            for r in load_json(f):
                yield _norm(r, collection="纳兰性德", dynasty="清", author="纳兰性德",
                            body_keys=("para", "paragraphs"), title_keys=("rhythmic", "title"), tag_key="rhythmic")

    d = repo / "曹操诗集"
    if d.exists():
        for f in sorted(d.glob("*.json")):
            for r in load_json(f):
                yield _norm(r, collection="曹操诗集", dynasty="汉", author="曹操")

    d = repo / "四书五经"
    if d.exists():
        for f in sorted(d.glob("*.json")):
            for r in load_json(f):
                yield _norm(r, collection="四书五经", dynasty="先秦", author=None,
                            body_keys=("paragraphs", "content"), title_keys=("title", "chapter"),
                            extra_exclude={"title", "chapter", "paragraphs", "content"})


def deduped(
    records: Generator[dict, None, None],
    report: Any | None = None,
) -> Generator[dict, None, None]:
    # Maps hash → (title, collection) of the first-seen (canonical) poem.
    # Storing only two strings keeps peak memory low even for ~1 M records.
    seen: dict[str, tuple[str, str]] = {}
    dupes = 0
    for rec in records:
        h = content_hash(rec["author"], rec["title"], rec["text"])
        if h in seen:
            dupes += 1
            if dupes % 1000 == 0:
                log.info("Skipped %d duplicates so far...", dupes)
            if report is not None:
                canonical_title, canonical_collection = seen[h]
                report.writerow([
                    rec.get("author") or "",
                    rec.get("title") or "",
                    rec.get("dynasty") or "",
                    rec.get("collection") or "",
                    (rec.get("text") or "")[:120].replace("\n", " "),
                    h,
                    canonical_title,
                    canonical_collection,
                ])
            continue
        seen[h] = (rec.get("title") or "", rec.get("collection") or "")
        rec["content_hash"] = h
        yield rec
    if dupes:
        log.info("Total duplicates skipped: %d", dupes)


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

    report_fh = None
    report_writer = None
    if REPORT_PATH is not None:
        report_fh = open(REPORT_PATH, "w", newline="", encoding="utf-8")
        report_writer = csv.writer(report_fh)
        report_writer.writerow([
            "author", "title", "dynasty", "collection", "text_preview",
            "content_hash", "canonical_title", "canonical_collection",
        ])

    try:
        with tqdm(unit=" rows", desc="Poems") as bar:
            for batch in chunked(deduped(iter_all_poems(repo), report=report_writer), BATCH_SIZE):
                rows = []
                for poem in batch:
                    author_name = poem.pop("author", None)
                    poem["author_id"] = author_id_map.get(author_name)
                    if poem.get("extra") is not None:
                        poem["extra"] = Jsonb(poem["extra"])
                    rows.append(poem)
                try:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO poems
                                (title, author_id, dynasty, collection, text, tags, extra, content_hash)
                            VALUES
                                (%(title)s, %(author_id)s, %(dynasty)s, %(collection)s,
                                 %(text)s, %(tags)s, %(extra)s, %(content_hash)s)
                            ON CONFLICT (content_hash) DO NOTHING
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
    finally:
        if report_fh is not None:
            report_fh.close()
            log.info("Discard report written to %s", REPORT_PATH)

    log.info("Poems imported: %d, errors: %d", total, errors)

    log.info("Rebuilding PGroonga index...")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE INDEX idx_poems_pgroonga ON poems
                USING pgroonga (text, title)
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
            "  USING pgroonga (text, title)\n"
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

    if _CC is None:
        log.warning(
            "opencc not installed — traditional/simplified variants will NOT be "
            "caught by the content_hash dedup. "
            "Install with: pip install opencc-python-reimplemented"
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
