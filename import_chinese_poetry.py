"""
Chinese Poetry → Supabase Import Script
=========================================
Imports all collections from https://github.com/chinese-poetry/chinese-poetry
into the `poems` and `authors` tables (see schema.sql).

Run order:
  1. schema.sql in Supabase SQL editor
  2. This script (imports authors first, then poems, then links them)

SETUP
------
  pip install supabase gitpython tqdm

  export SUPABASE_URL="https://xxxx.supabase.co"
  export SUPABASE_KEY="your-service-role-key"
  python import_chinese_poetry.py
"""

import os
import json
import hashlib
import logging
import subprocess

from dotenv import load_dotenv

from pathlib import Path
from typing import Any, Generator, cast

from supabase import create_client, Client
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────────────

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://YOUR_PROJECT.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "YOUR_SERVICE_ROLE_KEY")

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

    # The repo uses inconsistent field names across the three files
    long_desc = (
        record.get("description")
        or record.get("long_description")
        or record.get("desc")
        or record.get("long_desc")
    )
    short_desc = (
        record.get("short_description")
        or record.get("short_desc")
    )

    return {
        "name":              name,
        "dynasty":           dynasty,
        "source_long_desc":  long_desc,
        "source_short_desc": short_desc,
        "bio_short":         None,   # curate manually later
        "bio_long":          None,
    }


def import_authors(supabase: Client, repo: Path):
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
    for batch in chunked(records, BATCH_SIZE):
        supabase.table("authors").upsert(batch, on_conflict="name").execute()
        total += len(batch)

    log.info("Authors imported: %d", total)

    # Fetch all author ids back so we can link poems
    log.info("Fetching author id map...")
    rows: list[dict] = cast(list[dict], supabase.table("authors").select("id, name").execute().data or [])
    return {row["name"]: row["id"] for row in rows}

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

# ── Direct postgres DDL (bypasses PostgREST for index management) ─────────────

POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://postgres:postgres@127.0.0.1:54322/postgres",
)

def _psql(sql: str) -> bool:
    """Run a SQL statement directly via psql. Returns True on success."""
    result = subprocess.run(
        ["psql", POSTGRES_DSN, "-c", sql],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log.warning("psql error: %s", result.stderr.strip())
        return False
    return True

# ── Poem import ───────────────────────────────────────────────────────────────

def import_poems(supabase: Client, repo: Path, author_id_map: dict[str, Any]):
    log.info("Importing poems...")

    # Drop the PGroonga full-text index before bulk loading — updating it on every
    # INSERT batch is what causes statement timeouts. We rebuild it once at the end.
    log.info("Dropping PGroonga index for bulk load...")
    if not _psql("DROP INDEX IF EXISTS idx_poems_pgroonga;"):
        log.warning("Could not drop index via psql (will proceed anyway).")

    total = 0
    errors = 0

    with tqdm(unit=" rows", desc="Poems") as bar:
        for batch in chunked(deduped(iter_all_poems(repo)), BATCH_SIZE):
            # Attach author_id where we have a match
            for poem in batch:
                poem["author_id"] = author_id_map.get(poem["author"])
            try:
                supabase.table("poems").insert(batch).execute()
                total += len(batch)
                bar.update(len(batch))
            except Exception as e:
                errors += len(batch)
                log.error("Batch failed (%d rows): %s", len(batch), e)

    log.info("Poems imported: %d, errors: %d", total, errors)

    log.info("Rebuilding PGroonga index (this may take ~30 seconds)...")
    ok = _psql(
        "CREATE INDEX idx_poems_pgroonga ON poems "
        "USING pgroonga (title, author, dynasty, collection, text) "
        "WITH (tokenizer='TokenNgram(\"unify_alphabet\", false, \"unify_digit\", false)');"
    )
    if ok:
        log.info("PGroonga index rebuilt.")
    else:
        log.error(
            "Index rebuild failed. Run this manually in the SQL editor:\n"
            "  CREATE INDEX idx_poems_pgroonga ON poems "
            "USING pgroonga (title, author, dynasty, collection, text) "
            "WITH (tokenizer='TokenNgram(\"unify_alphabet\", false, "
            "\"unify_digit\", false)');"
        )

# ── Link any remaining poems whose authors weren't in the author files ─────────

def link_remaining_authors(supabase: Client):
    """
    Some poems reference authors not present in the repo's author JSON files
    (e.g. poets from 楚辞, 诗经, minor collections). This step ensures every
    distinct author name in poems has a row in authors, even if bio fields are
    empty — so you can fill them in during curation.
    """
    log.info("Inserting stub author rows for any unlinked poem authors...")

    # Find authors in poems that have no matching authors row
    # Fallback if the RPC doesn't exist: do it in Python
    # (The RPC is optional — see schema.sql for the function definition)
    try:
        supabase.rpc("find_unlinked_authors").execute()
    except Exception:
        log.info("RPC not available, fetching unlinked authors in Python...")
        all_poem_authors: list[dict] = cast(
            list[dict],
            supabase.table("poems")
            .select("author, dynasty")
            .neq("author", "")
            .execute()
            .data or [],
        )
        existing: set[str] = {
            cast(str, row["name"])
            for row in cast(list[dict], supabase.table("authors").select("name").execute().data or [])
        }
        # Collect one dynasty per author name
        stubs: dict[str, str | None] = {}
        for row in all_poem_authors:
            name = cast(str | None, row.get("author"))
            if name and name not in existing and name not in stubs:
                stubs[name] = cast(str | None, row.get("dynasty"))

        if stubs:
            stub_records = [
                {"name": name, "dynasty": dynasty}
                for name, dynasty in stubs.items()
            ]
            log.info("Inserting %d stub author rows...", len(stub_records))
            for batch in chunked(stub_records, BATCH_SIZE):
                supabase.table("authors").upsert(
                    batch, on_conflict="name"
                ).execute()

    # Now link poems → authors for the newly inserted stubs too
    log.info("Linking poems.author_id → authors.id for any remaining gaps...")
    try:
        supabase.rpc("link_poems_to_authors").execute()
    except Exception as e:
        log.warning("RPC timed out (%s), falling back to batched Python linking...", e)
        # Re-fetch full author map (includes freshly inserted stubs)
        author_rows = cast(
            list[dict],
            supabase.table("authors").select("id, name").execute().data or [],
        )
        author_map = {row["name"]: row["id"] for row in author_rows}
        linked = 0
        PAGE = 1000
        offset = 0
        while True:
            page = cast(
                list[dict],
                supabase.table("poems")
                .select("id, author")
                .is_("author_id", "null")
                .neq("author", "null")
                .range(offset, offset + PAGE - 1)
                .execute().data or [],
            )
            if not page:
                break
            updates = [
                {"id": p["id"], "author_id": author_map[p["author"]]}
                for p in page
                if p.get("author") and p["author"] in author_map
            ]
            if updates:
                for batch in chunked(updates, BATCH_SIZE):
                    supabase.table("poems").upsert(batch).execute()
                linked += len(updates)
            offset += PAGE
        log.info("Linked %d previously unlinked poems via fallback.", linked)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if SUPABASE_URL.startswith("https://YOUR"):
        raise SystemExit(
            "Set SUPABASE_URL and SUPABASE_KEY before running.\n"
            "  export SUPABASE_URL='https://xxxx.supabase.co'\n"
            "  export SUPABASE_KEY='your-service-role-key'"
        )

    ensure_repo()

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    if TRUNCATE:
        log.info("Truncating poems then authors...")
        # poems first (FK dependency)
        supabase.table("poems").delete().neq("id", 0).execute()
        supabase.table("authors").delete().neq("id", 0).execute()

    # 1. Authors
    author_id_map = import_authors(supabase, REPO_DIR)

    # 2. Poems (author_id attached inline where author name matches)
    import_poems(supabase, REPO_DIR, author_id_map)

    # 3. Stub rows + final linking for authors not in the repo's author files
    link_remaining_authors(supabase)

    log.info("All done.")


if __name__ == "__main__":
    main()
