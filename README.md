# linggu-db

A PostgreSQL database of classical Chinese poetry, seeded from the open-source [chinese-poetry](https://github.com/chinese-poetry/chinese-poetry) collection. Full-text search is powered by the [PGroonga](https://pgroonga.github.io/) extension with Ngram tokenisation, making it suitable for CJK queries.

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

- PostgreSQL server with the [PGroonga](https://pgroonga.github.io/install/) extension installed
- Python 3.11+

Enable the extension in your database:

```sql
CREATE EXTENSION IF NOT EXISTS pgroonga;
```

### 2. Apply the schema

```bash
psql -d your_database -f schema.sql
```

### 3. Install Python dependencies

```bash
pip install "psycopg[binary]" gitpython tqdm python-dotenv
```

### 4. Configure environment

Create a `.env` file in the project root:

```env
POSTGRES_DSN=postgresql://user:password@host:5432/your_database
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

## Project structure

```
schema.sql                  Raw schema (create tables, indexes, views, functions)
import_chinese_poetry.py    Data import script
```

## License

Source poetry data: [chinese-poetry](https://github.com/chinese-poetry/chinese-poetry) — MIT / CC.  
This project's code and schema: MIT.
