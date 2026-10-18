"""Author judgments bound to a research question and captured source revision."""

import hashlib

from .db import digest

VERDICTS = {"relevant", "counterevidence", "irrelevant", "uncertain"}


def text_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()


def reference_reviews(question, captured, proposed, reviewer):
    if not isinstance(proposed, list) or len(proposed) > 50:
        raise ValueError("at most 50 reference reviews")
    sources = {r["id"]: r for r in captured}
    question_hash = text_hash(question)
    seen, reviews = set(), []
    for entry in proposed:
        if not isinstance(entry, dict):
            raise ValueError("invalid reference review")
        passage = entry.get("passage_id")
        if not isinstance(passage, str) or passage not in sources or passage in seen:
            raise ValueError("review must cite one distinct captured reference")
        seen.add(passage)
        verdict, rationale = entry.get("verdict"), entry.get("rationale")
        if not isinstance(verdict, str) or verdict not in VERDICTS or not isinstance(rationale, str) or not 1 <= len(rationale.strip()) <= 1000:
            raise ValueError("reference review needs a verdict and a bounded rationale")
        source_hash = digest(sources[passage])
        if entry.get("question_sha256", question_hash) != question_hash or entry.get("source_sha256", source_hash) != source_hash:
            raise ValueError("review is stale: question or source changed; review this reference again")
        reviews.append({
            "passage_id": passage, "verdict": verdict, "rationale": rationale.strip(),
            "reviewer": reviewer, "question_sha256": question_hash, "source_sha256": source_hash,
            "status": "author_judgment_not_independently_verified",
        })
    return reviews
