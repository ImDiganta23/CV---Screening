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
# CV PARSER (LLM for structured info only)
# -------------------------------------------------

def call_mistral_parser(text):

    if not MISTRAL_KEY:
        raise Exception("MISTRAL_API_KEY not set")

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
  "skills": []
}
"""

    prompt = f"""
Extract structured CV data.

Rules:
- Return ONLY valid JSON.
- Do NOT hallucinate fields.
- If not explicitly present, return null.

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
        raise Exception("No valid JSON returned from parser")

    return raw[start:end]


# -------------------------------------------------
# PARSE ENDPOINT (Production Safe)
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

        with open(temp_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()

        raw_text = extract_text(temp_path)

        cursor.execute(
            "SELECT id FROM candidates WHERE file_hash=%s",
            (file_hash,)
        )
        existing = cursor.fetchone()

        if existing:
            cid = existing["id"]

            cursor.execute("""
                INSERT INTO cv_extracts(candidate_id, raw_text, extracted_json)
                VALUES (%s,%s,%s)
            """, (cid, raw_text, None))

        else:
            structured = call_mistral_parser(raw_text)
            data = json.loads(structured)

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

            cursor.execute("""
                INSERT INTO cv_extracts(candidate_id, raw_text, extracted_json)
                VALUES (%s,%s,%s)
            """, (cid, raw_text, structured))

        # -------- ALWAYS RE-SCORE --------

        signals = rule_signals(raw_text)
        score = rule_score(signals)
        bucket = bucket_from_score(score)
        confidence = round(score / 10, 2)

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
            f"Rule score: {score}\nSignals: {signals}",
            confidence
        ))

        conn.commit()

        return {
            "candidate_id": cid,
            "status": "processed_or_updated",
            "bucket": bucket,
            "score": score
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        cursor.close()
        conn.close()


# -------------------------------------------------
# GET ALL CANDIDATES
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
# CHAT ENDPOINT (Deterministic – NO LLM)
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

        response_lines = ["Here are all the candidates from the database:\n"]

        for idx, row in enumerate(rows, start=1):

            response_lines.append(f"{idx}. {row['name']}")
            response_lines.append(f"- Email: {row['primary_email'] or 'Not available'}")
            response_lines.append(f"- GitHub: {row['github_url'] or 'Not available'}")
            response_lines.append(f"- Passout Year: {row['passout_year'] or 'Not available'}")
            response_lines.append(f"- Bucket: {row['bucket']}")
            response_lines.append(f"- Confidence: {row['confidence']}")

            reasoning = row["reasoning_3_bullets"] or ""

            if reasoning:
                response_lines.append("- Details:")
                for line in reasoning.split("\n"):
                    if line.strip():
                        response_lines.append(f"- {line.strip()}")

            response_lines.append("")

        return "\n".join(response_lines)

    finally:
        cursor.close()
        conn.close()