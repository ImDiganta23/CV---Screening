import re
import json
import os
import hashlib
import fitz
import requests
import uuid
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from api.bucketer import rule_score, bucket_from_score, rule_signals
from api.db import get_connection

load_dotenv()

MISTRAL_KEY = os.getenv("MISTRAL_API_KEY")

app = FastAPI()

# -------------------------------------------------
# FRONTEND
# -------------------------------------------------

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


# -----------------------------
# CV PARSER (LLM Extraction)
# -----------------------------

def call_mistral_parser(text):

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
Extract structured CV data.

Rules:
- Return ONLY valid JSON.
- Do NOT hallucinate email or phone.
- If email or phone is not explicitly present in text, return null.
- Do NOT invent links.
- Keep projects concise (title + 1–2 line summary).

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
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2
        }
    )

    if r.status_code != 200:
        raise Exception(f"Mistral API error: {r.text}")

    raw = r.json()["choices"][0]["message"]["content"]

    start = raw.find("{")
    end = raw.rfind("}") + 1

    if start == -1:
        raise Exception("No valid JSON returned from parser")

    return raw[start:end]


# -----------------------------
# CHAT FORMATTER (SAFE MODE)
# -----------------------------

def call_mistral_formatter(database_json, user_query):

    prompt = f"""
You are a formatting assistant.

STRICT RULES:
- Use ONLY the data provided in DATABASE_JSON.
- Do NOT add new candidates.
- Do NOT invent missing values.
- If information is missing, say "Not available".
- If query cannot be answered from data, reply:
  "No matching data found in database."

USER QUERY:
{user_query}

DATABASE_JSON:
{json.dumps(database_json, indent=2)}

Return a clean, well-formatted chat-style response.
"""

    r = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {MISTRAL_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": "mistral-small",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0
        }
    )

    if r.status_code != 200:
        raise Exception(f"Mistral API error: {r.text}")

    return r.json()["choices"][0]["message"]["content"]


# -------------------------------------------------
# PARSE ENDPOINT (Called by n8n)
# -------------------------------------------------

@app.post("/parse")
async def parse(file: UploadFile):

    conn = get_connection()
    cursor = conn.cursor()
    temp_path = None

    try:
        original_filename = file.filename
        temp_path = f"temp_{uuid.uuid4().hex}.pdf"

        with open(temp_path, "wb") as f:
            f.write(await file.read())

        # Generate file hash (duplicate protection)
        with open(temp_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()

        cursor.execute(
            "SELECT id FROM candidates WHERE file_hash=%s",
            (file_hash,)
        )
        existing = cursor.fetchone()

        if existing:
            return {
                "candidate_id": existing["id"],
                "status": "already_processed"
            }

        raw_text = extract_text(temp_path)
        structured = call_mistral_parser(raw_text)
        data = json.loads(structured)

        # Validate email & phone
        email = data.get("email")
        phone = data.get("phone")

        EMAIL_REGEX = r"^[^@]+@[^@]+\.[^@]+$"
        PHONE_REGEX = r"\+?\d{7,15}"

        if email and not re.match(EMAIL_REGEX, email):
            email = None
        if phone and not re.match(PHONE_REGEX, phone):
            phone = None

        cursor.execute("""
        INSERT INTO candidates
        (name, primary_email, phone, location_text, github_url,
         linkedin_url, passout_year, cv_file_name, file_hash)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
        """, (
            data.get("name"),
            email,
            phone,
            data.get("location"),
            data.get("github"),
            data.get("linkedin"),
            data.get("passout_year"),
            original_filename,
            file_hash
        ))

        cid = cursor.fetchone()["id"]

        # Store extract
        cursor.execute("""
        INSERT INTO cv_extracts(candidate_id, raw_text, extracted_json)
        VALUES(%s,%s,%s)
        """, (cid, raw_text, structured))

        # Run bucketer
        signals = rule_signals(raw_text)
        score = rule_score(signals)
        bucket = bucket_from_score(score)
        confidence = round(min(1.0, score / 10), 2)

        cursor.execute("""
        INSERT INTO evaluations(candidate_id, bucket, reasoning_3_bullets, confidence)
        VALUES(%s,%s,%s,%s)
        """, (
            cid,
            bucket,
            f"Rule score: {score}\nSignals: {signals}",
            confidence
        ))

        conn.commit()

        return {"candidate_id": cid, "status": "processed"}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        cursor.close()
        conn.close()


# -------------------------------------------------
# GET ALL CANDIDATES (Raw JSON)
# -------------------------------------------------

@app.get("/candidates")
def get_candidates():

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT c.id,
           c.name,
           c.primary_email,
           c.github_url,
           c.passout_year,
           e.bucket,
           e.reasoning_3_bullets,
           e.confidence
    FROM candidates c
    JOIN evaluations e ON c.id = e.candidate_id
    ORDER BY c.created_at DESC
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return rows


# -------------------------------------------------
# CHAT ENDPOINT (DB → LLM FORMAT ONLY)
# -------------------------------------------------

@app.get("/chat")
def chat(query: str):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        SELECT c.name,
               c.primary_email,
               c.github_url,
               c.passout_year,
               e.bucket,
               e.reasoning_3_bullets,
               e.confidence
        FROM candidates c
        JOIN evaluations e ON c.id = e.candidate_id
        ORDER BY c.created_at DESC
        """)

        rows = cursor.fetchall()

        if not rows:
            return "No candidates found in database."

        database_data = [dict(r) for r in rows]

        formatted_response = call_mistral_formatter(database_data, query)

        return formatted_response

    finally:
        cursor.close()
        conn.close()