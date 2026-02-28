import re
import json
import os
import hashlib
import fitz
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from api.bucketer import rule_score, bucket_from_score, rule_signals
from api.db import get_connection

load_dotenv()

MISTRAL_KEY = os.getenv("MISTRAL_API_KEY")
print("MISTRAL KEY LOADED:", bool(MISTRAL_KEY))

app = FastAPI()

app.mount("/static", StaticFiles(directory="Frontend"), name="static")

@app.get("/")
def serve_frontend():
    return FileResponse("Frontend/index.html")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# UTILITIES
# -----------------------------

def extract_text(path):
    doc = fitz.open(path)
    return "".join([p.get_text() for p in doc])

def call_mistral(text):

    schema = """
{
  "name": "",
  "email": "",
  "phone": "",
  "location": "",
  "github": "",
  "linkedin": "",
  "passout_year": null,
  "education": "",
  "projects": [],
  "skills": [],
  "evidence_hints": []
}
"""

    prompt = f"""
Return ONLY valid JSON.

Schema:
{schema}

CV TEXT:
{text}
"""

    r = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {MISTRAL_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": "mistral-small",
            "messages": [{"role": "user", "content": prompt}]
        }
    )

    if r.status_code != 200:
        raise Exception(f"Mistral API error: {r.text}")

    raw = r.json()["choices"][0]["message"]["content"]

    start = raw.find("{")
    end = raw.rfind("}") + 1

    if start == -1:
        raise Exception("No JSON returned")

    return raw[start:end]


# -----------------------------
# PARSE ENDPOINT
# -----------------------------

@app.post("/parse")
async def parse(file: UploadFile):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        path = f"temp_{hashlib.md5(file.filename.encode()).hexdigest()}.pdf"

        with open(path, "wb") as f:
            f.write(await file.read())

        with open(path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()

        raw_text = extract_text(path)
        structured = call_mistral(raw_text)

        data = json.loads(structured)

        name = data.get("name")
        email = data.get("email")
        phone = data.get("phone")

        EMAIL_REGEX = r"^[^@]+@[^@]+\.[^@]+$"
        PHONE_REGEX = r"\+?\d{7,15}"

        if email and not re.match(EMAIL_REGEX, email):
            email = None
        if phone and not re.match(PHONE_REGEX, phone):
            phone = None

        location = data.get("location")
        github = data.get("github")
        linkedin = data.get("linkedin")
        passout_year = data.get("passout_year")

        # --------------------
        # INSERT CANDIDATE
        # --------------------

        cursor.execute("""
        INSERT INTO candidates
        (name, primary_email, phone, location_text, github_url, linkedin_url, passout_year, cv_file_name, file_hash)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (file_hash) DO NOTHING
        """, (
            name, email, phone, location,
            github, linkedin, passout_year,
            path, file_hash
        ))

        cursor.execute(
            "SELECT id FROM candidates WHERE file_hash=%s",
            (file_hash,)
        )
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=500, detail="Candidate lookup failed")

        cid = row["id"]

        # --------------------
        # VERSIONING
        # --------------------

        cursor.execute("""
        SELECT MAX(version) FROM cv_extracts WHERE candidate_id=%s
        """, (cid,))
        row = cursor.fetchone()
        version = (row["max"] + 1) if row and row["max"] else 1

        cursor.execute("""
        INSERT INTO cv_extracts(candidate_id,raw_text,extracted_json,version)
        VALUES(%s,%s,%s,%s)
        """, (cid, raw_text, structured, version))

        # --------------------
        # BUCKETING
        # --------------------

        signals = rule_signals(data)
        score = rule_score(signals)
        bucket = bucket_from_score(score)
        confidence = round(min(1.0, score / 10), 2)

        cursor.execute(
            "DELETE FROM evaluations WHERE candidate_id=%s",
            (cid,)
        )

        cursor.execute("""
        INSERT INTO evaluations(candidate_id,bucket,reasoning_3_bullets,confidence)
        VALUES(%s,%s,%s,%s)
        """, (
            cid,
            bucket,
            f"Rule score: {score}\nML detected: {signals.get('has_ml')}\nDeployment detected: {signals.get('has_deployment')}",
            confidence
        ))

        cursor.execute(
            "UPDATE candidates SET status='processed', active=TRUE WHERE id=%s",
            (cid,)
        )

        conn.commit()
        os.remove(path)

        return {"candidate_id": cid}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()


# -----------------------------
# CHAT ENDPOINT
# -----------------------------

@app.get("/chat")
def chat(query: str):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        where_clauses = []

        query_lower = query.lower()

        if "selected" in query_lower:
            where_clauses.append("e.bucket='Selected'")
        if "review" in query_lower:
            where_clauses.append("e.bucket='Review'")
        if "rejected" in query_lower:
            where_clauses.append("e.bucket='Rejected'")
        if "2026" in query_lower:
            where_clauses.append("c.passout_year=2026")

        base_query = """
        SELECT c.name,c.primary_email,c.github_url,c.passout_year,
               e.bucket,e.reasoning_3_bullets,e.confidence
        FROM candidates c
        JOIN evaluations e ON c.id=e.candidate_id
        WHERE c.active = TRUE
        """

        if where_clauses:
            base_query += " AND " + " AND ".join(where_clauses)

        cursor.execute(base_query)
        rows = cursor.fetchall()

        payload = [dict(r) for r in rows]

        return payload

    finally:
        cursor.close()
        conn.close()


# -----------------------------
# SAFE DRIVE SYNC
# -----------------------------

class DriveSync(BaseModel):
    files: list[str]

@app.post("/sync_drive")
async def sync_drive(payload: DriveSync):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        files = payload.files

        if not files:
            return {"status": "no files received"}

        cursor.execute("UPDATE candidates SET active = FALSE")

        placeholders = ",".join(["%s"] * len(files))

        query = f"""
        UPDATE candidates
        SET active = TRUE
        WHERE cv_file_name IN ({placeholders})
        """

        cursor.execute(query, files)

        conn.commit()

        return {"status": "drive synced safely"}

    finally:
        cursor.close()
        conn.close()


# -----------------------------
# GET CANDIDATES
# -----------------------------

@app.get("/candidates")
def get_candidates():

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        SELECT c.id AS candidate_id,
               c.name,
               c.primary_email,
               c.github_url,
               c.passout_year,
               e.bucket,
               e.reasoning_3_bullets,
               e.confidence
        FROM candidates c
        JOIN evaluations e ON c.id = e.candidate_id
        WHERE c.active = TRUE
        """)

        rows = cursor.fetchall()
        return [dict(r) for r in rows]

    finally:
        cursor.close()
        conn.close()