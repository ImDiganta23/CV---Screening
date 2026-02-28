# -------------------------------------------------
# KEYWORD LISTS
# -------------------------------------------------

ML_KEYWORDS = [
    "machine learning",
    "deep learning",
    "neural network",
    "cnn",
    "rnn",
    "transformer",
    "llm",
    "pytorch",
    "tensorflow",
    "xgboost",
    "lightgbm"
]

GENAI_KEYWORDS = [
    "genai",
    "rag",
    "langchain",
    "openai",
    "huggingface",
    "gpt"
]

DEPLOY_KEYWORDS = [
    "fastapi",
    "flask",
    "streamlit",
    "docker",
    "deployment",
    "rest api",
    "api endpoint"
]

AUTOMATION_KEYWORDS = [
    "automation",
    "rpa",
    "workflow",
    "n8n",
    "power automate"
]

# -------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------

def keyword_match(text, keywords):
    """
    Returns True if any keyword is found inside text.
    Deterministic lowercase match.
    """
    text = text.lower()
    return any(k in text for k in keywords)


def detect_ai_degree(text):
    """
    Detect REAL AI degrees (not workshops/certifications).
    More strict than simple keyword match.
    """
    text = text.lower()

    degree_patterns = [
        "m.sc artificial intelligence",
        "msc artificial intelligence",
        "b.tech artificial intelligence",
        "btech artificial intelligence",
        "artificial intelligence & machine learning",
        "artificial intelligence and machine learning"
    ]

    return any(p in text for p in degree_patterns)


def detect_github(text):
    """
    Detect real GitHub presence in raw CV text.
    """
    text = text.lower()
    return (
        "github.com" in text or
        "github/" in text or
        "github :" in text or
        "github -" in text
    )


# -------------------------------------------------
# MAIN RULE ENGINE (RAW TEXT BASED)
# -------------------------------------------------

def rule_signals_from_text(raw_text):
    """
    Deterministic scoring using ONLY raw CV text.
    No LLM dependency.
    """

    text = raw_text.lower()

    signals = {
        "has_github": detect_github(text),
        "has_python": "python" in text,
        "has_ml": keyword_match(text, ML_KEYWORDS),
        "has_genai": keyword_match(text, GENAI_KEYWORDS),
        "has_deployment": keyword_match(text, DEPLOY_KEYWORDS),
        "has_automation": keyword_match(text, AUTOMATION_KEYWORDS),
        "has_ai_degree": detect_ai_degree(text)
    }

    return signals


# -------------------------------------------------
# BACKWARD COMPATIBILITY (OPTIONAL)
# If older code calls rule_signals(data),
# it will still work.
# -------------------------------------------------

def rule_signals(data_or_text):
    """
    Accepts either:
    - raw_text (string)
    - extracted JSON (dict)

    Ensures compatibility with existing code.
    """

    if isinstance(data_or_text, str):
        return rule_signals_from_text(data_or_text)

    # Fallback for old JSON-based extraction
    text_blob = ""

    if isinstance(data_or_text, dict):
        skills = " ".join(data_or_text.get("skills", []))
        projects = data_or_text.get("projects", [])

        projects_text = ""
        for p in projects:
            if isinstance(p, dict):
                projects_text += " " + p.get("title", "")
                projects_text += " " + p.get("description", "")
            elif isinstance(p, str):
                projects_text += " " + p

        education = str(data_or_text.get("education", ""))

        text_blob = skills + " " + projects_text + " " + education

    return rule_signals_from_text(text_blob)


# -------------------------------------------------
# SCORING FUNCTION
# -------------------------------------------------

def rule_score(signals):

    weights = {
        "has_github": 1,
        "has_python": 1,
        "has_ml": 2,
        "has_genai": 2,
        "has_deployment": 1,
        "has_automation": 1,
        "has_ai_degree": 2
    }

    score = 0
    for key, value in signals.items():
        if value:
            score += weights.get(key, 0)

    return score


# -------------------------------------------------
# BUCKET CLASSIFICATION
# -------------------------------------------------

def bucket_from_score(score):

    if score >= 7:
        return "Selected"
    elif score >= 4:
        return "Review"
    else:
        return "Rejected"