def rule_signals(data):

    skills = " ".join(data.get("skills", [])).lower()

    return {
        "has_github": bool(data.get("github")),
        "has_python": "python" in skills,
        "has_ml": any(k in skills for k in ["ml","machine learning","deep learning","genai","llm"])
    }


def rule_score(signals):

    score = 0
    for v in signals.values():
        if v:
            score += 1

    return score


def bucket_from_score(score):

    if score >= 2:
        return "Selected"
    elif score == 1:
        return "Review"
    else:
        return "Rejected"
