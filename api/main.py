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

# -------------------------------------------------
# UTILITIES
# -------------------------------------------------

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

# -------------------------------------------------
# PARSE ENDPOINT
# -------------------------------------------------

@app.post("/parse")
async def parse(file: UploadFile):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        original_filename = file.filename

        # temp file only for processing
        temp_path = f"temp_{hashlib.md5(original_filename.encode()).hexdigest()}.pdf"

        with open(temp_path, "wb") as f:
            f.write(await file.read())

        with open(temp_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()

        raw_text = extract_text(temp_path)
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

        # -----------------------------
        # UPSERT CANDIDATE (EMAIL UNIQUE)
        # -----------------------------

        cursor.execute("""
        INSERT INTO candidates
        (name, primary_email, phone, location_text, github_url, linkedin_url,
         passout_year, cv_file_name, file_hash)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (primary_email)
        DO UPDATE SET
            name = EXCLUDED.name,
            phone = EXCLUDED.phone,
            location_text = EXCLUDED.location_text,
            github_url = EXCLUDED.github_url,
            linkedin_url = EXCLUDED.linkedin_url,
            passout_year = EXCLUDED.passout_year,
            cv_file_name = EXCLUDED.cv_file_name,
            file_hash = EXCLUDED.file_hash,
            active = TRUE
        RETURNING id
        """, (
            name, email, phone, location,
            github, linkedin, passout_year,
            original_filename, file_hash
        ))

        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="Candidate insert/update failed")

        cid = row["id"]

        # -----------------------------
        # VERSIONING
        # -----------------------------

        cursor.execute("""
        SELECT MAX(version) AS max_version
        FROM cv_extracts
        WHERE candidate_id=%s
        """, (cid,))

        row = cursor.fetchone()
        version = (row["max_version"] + 1) if row and row["max_version"] else 1

        cursor.execute("""
        INSERT INTO cv_extracts(candidate_id, raw_text, extracted_json, version)
        VALUES(%s,%s,%s,%s)
        """, (cid, raw_text, structured, version))

        # -----------------------------
        # BUCKETING
        # -----------------------------

        signals = rule_signals(data)
        score = rule_score(signals)
        bucket = bucket_from_score(score)
        confidence = round(min(1.0, score / 10), 2)

        cursor.execute(
            "DELETE FROM evaluations WHERE candidate_id=%s",
            (cid,)
        )

        cursor.execute("""
        INSERT INTO evaluations(candidate_id, bucket, reasoning_3_bullets, confidence)
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

        if os.path.exists(temp_path):
            os.remove(temp_path)

        return {"candidate_id": cid}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()

# -------------------------------------------------
# SAFE DRIVE SYNC
# -------------------------------------------------

class DriveSync(BaseModel):
    files: list[str]

@app.post("/sync_drive")
async def sync_drive(payload: DriveSync):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        drive_files = set(payload.files)

        if not drive_files:
            return {"status": "no files received"}

        cursor.execute("SELECT id, cv_file_name FROM candidates")
        db_rows = cursor.fetchall()

        db_files = {row["cv_file_name"]: row["id"] for row in db_rows}

        to_deactivate = [
            db_files[name]
            for name in db_files
            if name not in drive_files
        ]

        to_activate = [
            db_files[name]
            for name in drive_files
            if name in db_files
        ]

        if to_deactivate:
            cursor.execute(
                "UPDATE candidates SET active = FALSE WHERE id = ANY(%s)",
                (to_deactivate,)
            )

        if to_activate:
            cursor.execute(
                "UPDATE candidates SET active = TRUE WHERE id = ANY(%s)",
                (to_activate,)
            )

        conn.commit()

        return {
            "activated": len(to_activate),
            "deactivated": len(to_deactivate)
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()

# -------------------------------------------------
# GET CANDIDATES
# -------------------------------------------------

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