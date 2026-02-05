import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()  # loads .env from repo root (current working dir)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "rg_warehouse"),
        user=os.getenv("PGUSER", "rg_user"),
        password=os.getenv("PGPASSWORD", "rg_pass"),
        cursor_factory=RealDictCursor,
    )
