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


# -------------------------------------------------
# ENHANCED LLM CV PARSER
# -------------------------------------------------

def call_mistral_parser(text):

    if not MISTRAL_KEY:
        raise Exception("MISTRAL_API_KEY not set")

    schema = """
{
  "candidate": {
    "name": "",
    "emails": [],
    "phones": [],
    "location": ""
  },
  "education": [
    {
      "degree": "",
      "institute": "",
      "passout_year": ""
    }
  ],
  "skills": {
    "programming": [],
    "ml_keywords": [],
    "genai_keywords": [],
    "tools": []
  },
  "projects": [
    {
      "title": "",
      "summary": ""
    }
  ],
  "links": {
    "github": "",
    "linkedin": "",
    "portfolio": ""
  },
  "evidence_hints": []
}
"""

    prompt = f"""
Extract structured CV data.

STRICT RULES:
- Return ONLY valid JSON.
- Do NOT hallucinate fields.
- If not present, return empty values.
- Extract max 4 strongest projects.

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
            "temperature": 0
        }
    )

    if r.status_code != 200:
        raise Exception(f"Mistral API error: {r.text}")

    raw = r.json()["choices"][0]["message"]["content"]

    start = raw.find("{")
    end = raw.rfind("}") + 1

    if start == -1:
        raise Exception("No valid JSON returned")

    return raw[start:end]


# -------------------------------------------------
# PARSE ENDPOINT
# -------------------------------------------------

@app.post("/parse")
async def parse(file: UploadFile):

    conn = get_connection()
    cursor = conn.cursor()
    temp_path = None

    try:
        temp_path = f"temp_{uuid.uuid4().hex}.pdf"

        with open(temp_path, "wb") as f:
            f.write(await file.read())

        with open(temp_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()

        raw_text = extract_text(temp_path)

        cursor.execute(
            "SELECT id FROM candidates WHERE file_hash=%s",
            (file_hash,)
        )
        existing = cursor.fetchone()

        structured = call_mistral_parser(raw_text)
        structured_json = json.loads(structured)

        candidate = structured_json.get("candidate", {})
        links = structured_json.get("links", {})

        name = candidate.get("name")
        email = (candidate.get("emails") or [None])[0]
        phone = (candidate.get("phones") or [None])[0]
        location = candidate.get("location")
        github = links.get("github")
        linkedin = links.get("linkedin")

        if existing:
            cid = existing["id"]
        else:
            cursor.execute("""
                INSERT INTO candidates
                (name, primary_email, phone, location_text, github_url,
                 linkedin_url, passout_year, cv_file_name, file_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                name,
                email,
                phone,
                location,
                github,
                linkedin,
                None,
                file.filename,
                file_hash
            ))
            cid = cursor.fetchone()["id"]

        # Store extract version
        cursor.execute("""
            INSERT INTO cv_extracts(candidate_id, raw_text, extracted_json)
            VALUES (%s,%s,%s)
        """, (cid, raw_text, json.dumps(structured_json)))

        # Deterministic scoring
        signals = rule_signals(raw_text)
        score = rule_score(signals)
        bucket = bucket_from_score(score)
        confidence = round(score / 10, 2)

        cursor.execute("DELETE FROM evaluations WHERE candidate_id=%s", (cid,))
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

        return {"candidate_id": cid, "bucket": bucket, "score": score}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        cursor.close()
        conn.close()


# -------------------------------------------------
# GET ALL CANDIDATES (NO DUPLICATES)
# -------------------------------------------------

@app.get("/candidates")
def get_candidates():

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT c.name,
               c.primary_email,
               c.phone,
               c.location_text,
               c.github_url,
               c.linkedin_url,
               e.bucket,
               e.confidence,
               x.extracted_json
        FROM candidates c
        JOIN evaluations e ON c.id = e.candidate_id
        JOIN LATERAL (
            SELECT *
            FROM cv_extracts x
            WHERE x.candidate_id = c.id
            ORDER BY x.created_at DESC
            LIMIT 1
        ) x ON TRUE
        ORDER BY c.created_at DESC
    """)

    rows = cursor.fetchall()

    result = []
    for r in rows:
        item = dict(r)
        if item["extracted_json"]:
            item["extracted_json"] = json.loads(item["extracted_json"])
        result.append(item)

    cursor.close()
    conn.close()

    return result


# -------------------------------------------------
# SAFE LLM REASONER
# -------------------------------------------------

def call_mistral_reasoner(database_json, user_query):

    if not MISTRAL_KEY:
        raise Exception("MISTRAL_API_KEY not set")

    prompt = f"""
You are a CV reasoning assistant.

STRICT RULES:
- Use ONLY DATABASE_JSON.
- Do NOT modify score or confidence.
- Do NOT invent signals.
- If answer cannot be derived, say:
  "Cannot determine from database."

DATABASE_JSON:
{json.dumps(database_json, indent=2)}

USER QUERY:
{user_query}

Provide a clear answer.
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
# CHAT ENDPOINT (NO DUPLICATES)
# -------------------------------------------------

@app.get("/chat")
def chat(query: str):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT c.name,
                   c.primary_email,
                   c.phone,
                   c.location_text,
                   c.github_url,
                   c.linkedin_url,
                   e.bucket,
                   e.confidence,
                   x.extracted_json
            FROM candidates c
            JOIN evaluations e ON c.id = e.candidate_id
            JOIN LATERAL (
                SELECT *
                FROM cv_extracts x
                WHERE x.candidate_id = c.id
                ORDER BY x.created_at DESC
                LIMIT 1
            ) x ON TRUE
            ORDER BY c.created_at DESC
        """)

        rows = cursor.fetchall()

        if not rows:
            return "No candidates found."

        data = []
        for r in rows:
            item = dict(r)
            if item["extracted_json"]:
                item["extracted_json"] = json.loads(item["extracted_json"])
            data.append(item)

        return call_mistral_reasoner(data, query)

    finally:
        cursor.close()
        conn.close()