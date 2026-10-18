import hashlib
import pytest
from ofl.research.outcomes import assess


def test_runtime_success_is_not_task_completion_and_missing_trials_remain_visible(tmp_path):
    artifact = tmp_path / "T01.json"
    artifact.write_text('{"http_status":200,"runtime":"completed"}')
    sha = hashlib.sha256(artifact.read_bytes()).hexdigest()
    protocol = {"cases": [{"id": "T01"}, {"id": "T02"}]}
    trials = [{"case_id":"T01","condition":"ofl_agent","artifact_file":artifact.name,"artifact_sha256":sha}]
    pending = assess(protocol, trials, [], tmp_path)
    assert pending["summary"]["ofl_agent"]["unassessed"] == 2
    assert pending["summary"]["ofl_agent"]["completion_rate_assessed_eligible"] is None
    judgment = {"case_id":"T01","condition":"ofl_agent","artifact_sha256":sha,"reviewer":"actual reviewer", "eligibility":"eligible","outcome":"completed","hard_gate":"fail","claim_support":"pass"}
    failed = assess(protocol, trials, [judgment], tmp_path)
    assert failed["summary"]["ofl_agent"]["completed"] == 0
    assert failed["summary"]["ofl_agent"]["hard_gate_failures"] == 1
    assert failed["summary"]["ofl_agent"]["review_gate_failures"] == 1
    assert failed["summary"]["baseline"]["unassessed"] == 2
    artifact.write_text('{"different":"artifact"}')
    with pytest.raises(ValueError, match="match"):
        assess(protocol, trials, [judgment], tmp_path)


def test_completed_artifact_and_correct_abstention_are_separate(tmp_path):
    file = tmp_path / "artifact.json"
    file.write_text('{"http_status":200}')
    sha = hashlib.sha256(file.read_bytes()).hexdigest()
    trials = [{"case_id":case,"condition":"ofl_agent","artifact_file":file.name,"artifact_sha256":sha} for case in ("T01","T10")]
    judgments = [{"case_id":case,"condition":"ofl_agent","artifact_sha256":sha,"reviewer":"builder", "eligibility":eligibility,"outcome":outcome,"hard_gate":"pass","claim_support":"pass"} for case,eligibility,outcome in (("T01","eligible","completed"),("T10","unavailable","correct_abstention"))]
    report = assess({"study_kind":"builder_operated_development","cases":[{"id":"T01"},{"id":"T10"}]}, trials, judgments, tmp_path)
    assert report["study_kind"] == "builder_operated_development"
    summary = report["summary"]["ofl_agent"]
    assert (summary["completed"], summary["assessed_eligible"], summary["correct_abstentions"]) == (1,1,1)
    file.write_text('{"http_status":503}')
    new_sha = hashlib.sha256(file.read_bytes()).hexdigest()
    for trial in trials:
        trial["artifact_sha256"] = new_sha
    for judgment in judgments:
        judgment["artifact_sha256"] = new_sha
    error_report = assess({"cases":[{"id":"T01"},{"id":"T10"}]}, trials, judgments, tmp_path)
    assert error_report["summary"]["ofl_agent"]["completed"] == 0
    assert error_report["summary"]["ofl_agent"]["execution_failures"] == 2
    judgments[1]["outcome"] = "completed"
    with pytest.raises(ValueError, match="unavailable"):
        assess({"cases":[{"id":"T01"},{"id":"T10"}]}, trials, judgments, tmp_path)
