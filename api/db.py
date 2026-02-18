import sqlite3

conn = sqlite3.connect("app.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS candidates(
id INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT,
primary_email TEXT UNIQUE,
phone TEXT,
location_text TEXT,
linkedin_url TEXT,
github_url TEXT,
passout_year INTEGER,
cv_file_name TEXT UNIQUE,
file_hash TEXT UNIQUE,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS cv_extracts(
id INTEGER PRIMARY KEY AUTOINCREMENT,
candidate_id INTEGER UNIQUE,
raw_text TEXT,
extracted_json TEXT,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS evaluations(
id INTEGER PRIMARY KEY AUTOINCREMENT,
candidate_id INTEGER,
bucket TEXT,
reasoning_3_bullets TEXT,
confidence REAL,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

conn.commit()
print("Database initialized")
