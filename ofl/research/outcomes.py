"""Manual task-outcome assessment; runtime completion is never task success."""
import hashlib
import json
from pathlib import Path

CONDITIONS = ("ofl_agent", "ofl_without_agent", "baseline")


def assess(protocol, trials, assessments, artifacts: Path):
    planned = protocol["cases"]
    ids = [r["id"] for r in planned]
    if not ids or len(ids) != len(set(ids)) or len(ids) > 100:
        raise ValueError("bounded distinct planned cases required")
    allowed = {(case, condition) for case in ids for condition in CONDITIONS}
    records, judgments = {}, {}
    for collection, target in ((trials, records), (assessments, judgments)):
        for row in collection:
            key = (row.get("case_id"), row.get("condition"))
            if key not in allowed or key in target:
                raise ValueError("unexpected or duplicate case/condition")
            target[key] = row
    rows = []
    for key in sorted(allowed):
        trial, judgment = records.get(key), judgments.get(key)
        result = {"case_id": key[0], "condition": key[1], "outcome": "unassessed"}
        rows.append(result)
        if not judgment:
            continue
        if not trial:
            raise ValueError("cannot assess an unrecorded trial")
        name = trial.get("artifact_file", "")
        path = artifacts / name
        if not name or Path(name).name != name or path.is_symlink() or not path.is_file():
            raise ValueError("artifact must be an existing direct file in the private trial directory")
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if trial.get("artifact_sha256") != actual or judgment.get("artifact_sha256") != actual:
            raise ValueError("judgment does not match the recorded artifact")
        if not isinstance(judgment.get("reviewer"), str) or not judgment["reviewer"].strip():
            raise ValueError("record the actual reviewer; identity is self-reported")
        eligibility = judgment.get("eligibility")
        outcome = judgment.get("outcome")
        if eligibility not in {"eligible", "unavailable"} or outcome not in {"completed", "failed", "correct_abstention"}:
            raise ValueError("explicit eligibility and outcome required")
        if outcome == "completed" and eligibility != "eligible":
            raise ValueError("an unavailable task cannot be completed")
        gates = (judgment.get("hard_gate"), judgment.get("claim_support"))
        if any(g not in {"pass", "fail", "not_assessed"} for g in gates):
            raise ValueError("explicit hard-gate and claim-support assessments required")
        if outcome == "completed" and gates != ("pass", "pass"):
            outcome = "failed_review_gate"
        if outcome == "correct_abstention" and gates != ("pass", "pass"):
            outcome = "failed_review_gate"
        artifact = json.loads(path.read_text())
        execution_failure = key[1] == "ofl_agent" and (artifact.get("http_status") != 200 or "transport_error" in artifact)
        if execution_failure and outcome in {"completed", "correct_abstention"}:
            outcome = "failed_execution"
        result.update(outcome=outcome, eligibility=eligibility, reviewer=judgment["reviewer"],
                      hard_gate=gates[0], claim_support=gates[1], execution_failure=execution_failure)
    summaries = {}
    for condition in CONDITIONS:
        selected = [r for r in rows if r["condition"] == condition]
        eligible = [r for r in selected if r.get("eligibility") == "eligible"]
        complete = sum(r["outcome"] == "completed" for r in eligible)
        summaries[condition] = {
            "planned": len(selected), "unassessed": sum(r["outcome"] == "unassessed" for r in selected),
            "assessed_eligible": len(eligible), "completed": complete,
            "completion_rate_assessed_eligible": complete / len(eligible) if eligible else None,
            "correct_abstentions": sum(r["outcome"] == "correct_abstention" for r in selected),
            "execution_failures": sum(r.get("execution_failure", False) for r in selected),
            "hard_gate_failures": sum(r.get("hard_gate") == "fail" for r in selected),
            "claim_support_failures": sum(r.get("claim_support") == "fail" for r in selected),
            "review_gate_failures": sum(r["outcome"] == "failed_review_gate" for r in selected),
        }
    return {"version": 1, "study_kind": protocol.get("study_kind", "unspecified_not_user_validation"), "summary": summaries, "cases": rows,
            "qualification": "Manual judgments on recorded artifacts. Reviewer identity is self-reported, not independently authenticated. Missing trials remain unassessed; runtime completion, synthetic runs and correct abstention do not establish requested artifact completion, user validation or population performance."}
