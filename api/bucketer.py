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
    "huggingface"
]

DEPLOY_KEYWORDS = [
    "fastapi",
    "flask",
    "streamlit",
    "api",
    "deployment",
    "docker"
]

AUTOMATION_KEYWORDS = [
    "automation",
    "rpa",
    "workflow",
    "n8n"
]


def keyword_match(text, keywords):
    text = text.lower()
    return any(k in text for k in keywords)


def rule_signals(data):

    skills_text = " ".join(data.get("skills", [])).lower()
    projects_text = " ".join(data.get("projects", [])).lower()
    education_text = str(data.get("education", "")).lower()

    full_text = skills_text + " " + projects_text

    signals = {
        "has_github": bool(data.get("github")),
        "has_python": "python" in skills_text,
        "has_ml": keyword_match(full_text, ML_KEYWORDS),
        "has_genai": keyword_match(full_text, GENAI_KEYWORDS),
        "has_deployment": keyword_match(full_text, DEPLOY_KEYWORDS),
        "has_automation": keyword_match(full_text, AUTOMATION_KEYWORDS),
        "has_ai_degree": "artificial intelligence" in education_text
    }

    return signals


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


def bucket_from_score(score):

    if score >= 6:
        return "Selected"
    elif score >= 3:
        return "Review"
    else:
        return "Rejected"