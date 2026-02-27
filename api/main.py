import re
from api.bucketer import rule_score, bucket_from_score
from fastapi import FastAPI, HTTPException, UploadFile, Query, Body
import json, os
import fitz
import requests
from dotenv import load_dotenv
from api.db import conn, cursor
import hashlib
from pydantic import BaseModel
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from api.bucketer import rule_signals
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
            "model":"mistral-small",
            "messages":[{"role":"user","content":prompt}]
        }
    )
    if r.status_code != 200:
        raise Exception(f"Mistral API error: {r.text}")

    resp_json = r.json()

    if "choices" not in resp_json:
        raise Exception(f"Invalid Mistral response: {resp_json}")
    raw = resp_json["choices"][0]["message"]["content"]

    print("RAW RESPONSE:", raw)

    start = raw.find("{")
    end = raw.rfind("}") + 1

    if start == -1:
        raise Exception("No JSON returned")

    return raw[start:end]


def llm_judgement(data, signals):

    prompt = f"""
Candidate data:

{json.dumps(data, indent=2)}

Signals:
{signals}

Tasks:
1. Summarize best evidence of hands-on building
2. Identify missing critical info
3. Give 3 bullet reasons for bucket
4. Provide reasoning only (confidence calculated separately)

Return ONLY JSON:

{{
"bullets": ["","",""],
"confidence": 0.0,
"missing": ""
}}
"""

    r = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {MISTRAL_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model":"mistral-small",
            "messages":[{"role":"user","content":prompt}]
        }
    )
    if r.status_code != 200:
        raise Exception(f"Mistral API error: {r.text}")

    resp_json = r.json()
    if "choices" not in resp_json:
        raise Exception(f"Invalid Mistral response: {resp_json}")
    raw = resp_json["choices"][0]["message"]["content"]

    raw = raw[raw.find("{"):raw.rfind("}")+1]

    return json.loads(raw)

@app.post("/parse")
async def parse(file: UploadFile):
    try:
        path = f"temp_{hashlib.md5(file.filename.encode()).hexdigest()}.pdf"

        with open(path,"wb") as f:
            f.write(await file.read())

        with open(path,"rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()


        raw_text = extract_text(path)

        structured = call_mistral(raw_text)

        if not structured:
            raise HTTPException(status_code=500, detail="Empty response from Mistral")

        data = json.loads(structured)
    

        name = data.get("name")
        email = data.get("email")
        phone = data.get("phone")
        EMAIL_REGEX = r"^[^@]+@[^@]+\.[^@]+$"
        PHONE_REGEX = r"\+?\d{7,15}"

        if email and not re.match(EMAIL_REGEX, email):
            email = None
        if email and email not in raw_text:
            email = None
        if phone and not re.match(PHONE_REGEX, phone):
            phone = None
        location = data.get("location")
        github = data.get("github")
        linkedin = data.get("linkedin")
        passout_year = data.get("passout_year")

        projects = json.dumps(data.get("projects", []))
        skills = json.dumps(data.get("skills", []))
        evidence = json.dumps(data.get("evidence_hints", []))
        education = data.get("education")

        # INSERT CANDIDATE (ONLY ONCE)
        cursor.execute("""
        INSERT INTO candidates
        (name, primary_email, phone, location_text, github_url, linkedin_url, passout_year, cv_file_name, file_hash)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (file_hash) DO NOTHING
        """, (
        name,
        email,
        phone,
        location,
        github,
        linkedin,
        passout_year,
        path,
        file_hash
        ))


        conn.commit()

        # GET CID
        cursor.execute(
            "SELECT id FROM candidates WHERE file_hash=%s",
            (file_hash,)
     )
        row = cursor.fetchone()

        if row:
            cid = row["id"]
        else:
            cursor.execute(
                "SELECT id FROM candidates WHERE primary_email=%s",
                (email,)
            )
            fallback_row = cursor.fetchone()
            if fallback_row:
                cid = fallback_row["id"]
            else:
                raise HTTPException(status_code=500, detail="Candidate lookup failed")
    

        cursor.execute("""
        SELECT MAX(version) FROM cv_extracts WHERE candidate_id=%s
        """,(cid,))
        row = cursor.fetchone()
        if row and list(row.values())[0] is not None:
            version = list(row.values())[0] + 1
        else:
            version = 1

        # SAVE RAW EXTRACT
        cursor.execute("""
        INSERT INTO cv_extracts(candidate_id,raw_text,extracted_json,version)
        VALUES(%s,%s,%s, %s)
        """,(cid,raw_text,structured, version))

        conn.commit()

    
        # BUCKETING

        signals = rule_signals(data)
        score = rule_score(signals)
        bucket = bucket_from_score(score)

        max_score = 10
        confidence = round(min(1.0, score / max_score), 2)

        llm_eval = {
            "bullets": [
                f"Rule score: {score}",
                f"ML detected: {signals.get('has_ml')}",
                f"Deployment detected: {signals.get('has_deployment')}"
            ],
            "missing": ""
        }

        # Delete old evaluation for this candidate
        cursor.execute(
            "DELETE FROM evaluations WHERE candidate_id = %s",
            (cid,)
        )
        conn.commit()

        cursor.execute("""
        INSERT INTO evaluations(candidate_id,bucket,reasoning_3_bullets,confidence)
        VALUES(%s,%s,%s,%s)
        """,(
            cid,
            bucket,
            "\n".join(llm_eval["bullets"]),
            confidence
        ))
        cursor.execute(
            "UPDATE candidates SET status='processed' WHERE id=%s",
            (cid,)
        )
        conn.commit()
        os.remove(path)
        return {"candidate_id":cid}
    except Exception as e:
        print("PARSE ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/chat")
def chat(query: str):

    query_lower = query.lower()
    where_clauses = []

    if "selected" in query_lower:
        where_clauses.append("e.bucket='Selected'")
    if "review" in query_lower:
        where_clauses.append("e.bucket='Review'")
    if "rejected" in query_lower:
        where_clauses.append("e.bucket='Rejected'")
    if "2026" in query_lower:
        where_clauses.append("c.passout_year=2026")
    if "github" in query_lower:
        where_clauses.append("c.github_url IS NOT NULL")

    base_query = """
    SELECT c.name,c.primary_email,c.github_url,c.passout_year,
       e.bucket,e.reasoning_3_bullets,e.confidence
    FROM candidates c
    JOIN evaluations e ON c.id=e.candidate_id
    """

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)

    cursor.execute(base_query)
    rows = cursor.fetchall()

    payload = []

    for r in rows:
        payload.append({
        "name": r["name"],
        "email": r["primary_email"],
        "github": r["github_url"],
        "passout": r["passout_year"],
        "bucket": r["bucket"],
        "reason": r["reasoning_3_bullets"],
        "confidence": r["confidence"]
    })

    prompt = f"""
        You are an AI recruiter assistant.

        User question:
        {query}

        
        Candidate DB:
        {json.dumps(payload,indent=2)}

        Respond using this format strictly:
        Answer the user question clearly and professionally.

        Title line

            - Bullet point 1
            - Bullet point 2
            - Bullet point 3
        
        Rules:
        - Use clean plain text.
        - Do NOT use markdown symbols like ** or ###
        - Do NOT use \\n or \n in output
        - Format answers in clean bullet points
        - Keep structure clear
        - Separate candidates clearly
        - Use normal text and real line breaks.
        - Keep it structured like ChatGPT answers.

        Answer ONLY from the candidate database.
        
        Provide structured response only.
        """
        

    r = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {MISTRAL_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model":"mistral-small",
            "messages":[{"role":"user","content":prompt}]
        }
    )

    raw_output = r.json()["choices"][0]["message"]["content"]

        # Replace escaped newlines with actual newlines
    clean_output = raw_output.encode().decode("unicode_escape")
    print("FINAL OUTPUT:", clean_output)

    return clean_output


class DriveSync(BaseModel):
    files: list[str]

@app.post("/sync_drive")
async def sync_drive(payload: DriveSync):
    files = payload.files
    
    if not files:
        return {"status": "no files received"}
    placeholders = ",".join(["%s"] * len(files))

    query = f"""
    DELETE FROM candidates
    WHERE cv_file_name NOT IN ({placeholders})
    """

    cursor.execute(query, files)
    conn.commit()
    

    return {"status": "drive synced", "files": files}

@app.get("/candidates")
def get_candidates():
    cursor.execute("""
    SELECT 
        c.id AS candidate_id,
        c.name,
        c.primary_email,
        c.github_url,
        c.passout_year,
        e.bucket,
        e.reasoning_3_bullets,
        e.confidence 
    FROM candidates c
    JOIN evaluations e ON c.id = e.candidate_id
    """)
    rows = cursor.fetchall()

    return [
        {
            "id": r["candidate_id"],
            "name": r["name"],
            "email": r["primary_email"],
            "github": r["github_url"],
            "passout_year": r["passout_year"],
            "bucket": r["bucket"],
            "reasoning": r["reasoning_3_bullets"],
            "confidence": r["confidence"]
        }
        for r in rows
    ]

@app.get("/debug_evaluations")
def debug_evaluations():
    cursor.execute("""
    SELECT candidate_id, COUNT(*)
    FROM evaluations
    GROUP BY candidate_id
    """)
    rows = cursor.fetchall()
    return rows
@app.post("/cleanup_duplicates")
def cleanup_duplicates():
    cursor.execute("""
    DELETE FROM evaluations
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM evaluations
        GROUP BY candidate_id
    )
    """)
    conn.commit()
    return {"status": "duplicates removed"}





