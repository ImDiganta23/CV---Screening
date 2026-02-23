import re
import sqlite3
from api.bucketer import rule_score, bucket_from_score
from fastapi import FastAPI, HTTPException, UploadFile, Query, Body
import uuid, json, os
import fitz
import requests
from dotenv import load_dotenv
from api.db import conn, cursor
import hashlib
from pydantic import BaseModel
from typing import List
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

MISTRAL_KEY = os.getenv("MISTRAL_API_KEY")
print("MISTRAL KEY LOADED:", bool(MISTRAL_KEY))

app = FastAPI()

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

    raw = r.json()["choices"][0]["message"]["content"]

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
4. Return confidence 0–1

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

    raw = r.json()["choices"][0]["message"]["content"]

    raw = raw[raw.find("{"):raw.rfind("}")+1]

    return json.loads(raw)

@app.post("/parse")
async def parse(file: UploadFile):

    path = file.filename

    with open(path,"wb") as f:
        f.write(await file.read())

    with open(path,"rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()


    raw_text = extract_text(path)

    structured = call_mistral(raw_text)

    if not structured:
        return {"error": "Empty response from Mistral"}

    try:
        data = json.loads(structured)
    except Exception as e:
        print("JSON parse failed:", structured)
        raise HTTPException(status_code=500, detail="Bad LLM JSON")

    name = data.get("name")
    email = data.get("email")
    phone = data.get("phone")
    location = data.get("location")
    github = data.get("github")
    linkedin = data.get("linkedin")
    passout_year = data.get("passout_year")

    projects = json.dumps(data.get("projects", []))
    skills = json.dumps(data.get("skills", []))
    evidence = json.dumps(data.get("evidence_hints", []))
    education = data.get("education")

    os.remove(path)


    # INSERT CANDIDATE (ONLY ONCE)
    cursor.execute("""
    INSERT INTO candidates
    (name, primary_email, phone, location_text, github_url, linkedin_url, passout_year, cv_file_name, file_hash)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (primary_email) DO NOTHING
    """, (
    data.get("name"),
    data.get("email"),
    data.get("phone"),
    data.get("location"),
    data.get("github"),
    data.get("linkedin"),
    data.get("passout_year"),
    path,
    file_hash
    ))


    conn.commit()

    # GET CID
    cursor.execute(
    "SELECT id FROM candidates WHERE file_hash=%s",
    (file_hash,)
    )

    cid = cursor.fetchone()["id"]



    # SAVE RAW EXTRACT
    cursor.execute("""
    INSERT INTO cv_extracts(candidate_id,raw_text,extracted_json)
    VALUES(%s,%s,%s)
    """,(cid,raw_text,structured))

    conn.commit()

    # BUCKETING
    from api.bucketer import rule_signals

    signals = rule_signals(data)
    score = rule_score(signals)
    bucket = bucket_from_score(score)

    llm_eval = llm_judgement(data, signals)

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
    llm_eval["confidence"]
    ))

    conn.commit()

    return {"candidate_id":cid}
@app.get("/chat")
def chat(query: str):

        cursor.execute("""
        SELECT c.name,c.primary_email,c.github_url,c.passout_year,
           e.bucket,e.reasoning_3_bullets,e.confidence
        FROM candidates c
        JOIN evaluations e ON c.id=e.candidate_id
        """)
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
    WHERE cv_File_name NOT IN ({placeholders})
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
    rows = cursor.execute("""
    SELECT candidate_id, COUNT(*)
    FROM evaluations
    GROUP BY candidate_id
    """).fetchall()

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





