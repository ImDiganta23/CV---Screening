import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor
    )

# -------------------------------------------------
# INITIALIZE DATABASE
# -------------------------------------------------

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # -----------------------------
    # Candidates Table
    # -----------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS candidates(
        id SERIAL PRIMARY KEY,
        drive_file_id TEXT UNIQUE,
        name TEXT,
        primary_email TEXT UNIQUE,
        phone TEXT,
        location_text TEXT,
        linkedin_url TEXT,
        github_url TEXT,
        passout_year INTEGER,
        cv_file_name TEXT,
        file_hash TEXT UNIQUE,
        status TEXT DEFAULT 'processed',
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # -----------------------------
    # CV Extracts Table
    # -----------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cv_extracts(
        id SERIAL PRIMARY KEY,
        candidate_id INTEGER REFERENCES candidates(id) ON DELETE CASCADE,
        raw_text TEXT,
        extracted_json TEXT,
        version INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # -----------------------------
    # Evaluations Table
    # -----------------------------
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS evaluations(
        id SERIAL PRIMARY KEY,
        candidate_id INTEGER REFERENCES candidates(id) ON DELETE CASCADE,
        bucket TEXT,
        reasoning_3_bullets TEXT,
        confidence REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Indexes
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_candidate_hash
    ON candidates(file_hash)
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_candidate_active
    ON candidates(active)
    """)

    conn.commit()
    cursor.close()
    conn.close()

    print("PostgreSQL initialized successfully")

# Run initialization
init_db()