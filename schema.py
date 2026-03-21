import os, psycopg
from dotenv import load_dotenv
load_dotenv()
dsn = os.environ['POSTGRES_DSN']
with open('schema.sql') as f:
    sql = f.read()
with psycopg.connect(dsn) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql)
        print('Schema applied successfully.')