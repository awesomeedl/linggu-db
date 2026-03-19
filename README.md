# linggu-db

A Supabase database of classical Chinese poetry, seeded from the open-source [chinese-poetry](https://github.com/chinese-poetry/chinese-poetry) collection. Full-text search is powered by the [PGroonga](https://pgroonga.github.io/) extension with Ngram tokenisation, making it suitable for CJK queries.

## Tables

| Table | Description |
|-------|-------------|
| `authors` | Poets — Tang and Song dynasty. Includes raw repo descriptions (`source_short_desc`, `source_long_desc`) and curated biography fields (`bio_short`, `bio_long`) left empty for manual editing. |
| `poems` | Individual poems with title, author, dynasty, collection, full text, and optional tags/extra JSONB. |

## Views

| View | Description |
|------|-------------|
| `view_dynasty_author_counts` | Poem counts grouped by dynasty and author. |
| `view_collection_summary` | Poem and author counts per collection. |
| `view_authors_with_counts` | Authors joined with poem counts and a `bio_curated` flag. |

## Collections imported

- **全唐诗** — Complete Tang Poems (Tang dynasty shi poetry)
- **宋词** — Song Ci (Song dynasty ci poetry)

## Setup

### 1. Prerequisites

- [Supabase](https://supabase.com) project (or local CLI)
- Python 3.11+
- PGroonga extension enabled on your Supabase project

  ```sql
  CREATE EXTENSION IF NOT EXISTS pgroonga;
  ```

### 2. Apply the schema

Run [schema.sql](schema.sql) in the Supabase SQL editor, or push the migration:

```bash
supabase db push
```

### 3. Install Python dependencies

```bash
pip install supabase gitpython tqdm python-dotenv
```

### 4. Configure environment

Create a `.env` file in the project root:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key
```

### 5. Run the import

```bash
python import_chinese_poetry.py
```

The script will:
1. Clone the `chinese-poetry` repo to `/tmp/chinese-poetry` (≈ 1 GB, shallow clone).
2. Truncate both tables (set `TRUNCATE = False` in the script to append instead).
3. Import authors from the Tang and Song author JSON files.
4. Import poems in batches of 500.
5. Link `poems.author_id` to the corresponding `authors` row.

## Local development

```bash
supabase start   # starts local Postgres + API on port 54321
supabase db push # applies migrations
```

## Project structure

```
schema.sql                          Raw schema (create tables, indexes, views, functions)
import_chinese_poetry.py            Data import script
supabase/
  config.toml                       Supabase CLI config (project_id: linggu-db)
  migrations/
    20260319021148_initial_schema.sql
```

## License

Source poetry data: [chinese-poetry](https://github.com/chinese-poetry/chinese-poetry) — MIT / CC.  
This project's code and schema: MIT.
