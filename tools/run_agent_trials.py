"""Bounded VPS-agent rehearsals; private artifacts, no automatic task-success grade."""
import argparse
import hashlib
import json
import os
import time
import uuid
from pathlib import Path

import requests


def write_private(path, value):
    raw = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
    return hashlib.sha256(raw).hexdigest()


def run(protocol, output, base, token, identity, origin):
    cases = protocol["cases"]
    if not 1 <= len(cases) <= 6:
        raise ValueError("run one to six cases; agree a larger budget separately")
    ids = [case["id"] for case in cases]
    if len(ids) != len(set(ids)) or any(not isinstance(v, str) or not v.isalnum() for v in ids):
        raise ValueError("distinct alphanumeric case IDs required")
    if any(case["agent"] not in {"archive", "market", "quality"} for case in cases):
        raise ValueError("unknown agent")
    output.mkdir(mode=0o700)
    headers = {"X-OFL-Gateway-Token": token, "X-OFL-Identity": identity, "Origin": origin}
    configuration = requests.get(base.rstrip("/") + "/api/agents", headers=headers, timeout=30, allow_redirects=False)
    configuration.raise_for_status()
    if configuration.status_code != 200:
        raise ValueError("agent configuration did not return HTTP 200")
    report = {"version": 1, "study_kind": "builder_operated_development", "configuration": configuration.json(), "trials": [], "qualification": "Private builder-operated integration rehearsals. HTTP/runtime completion is not requested artifact completion; all outcomes require manual assessment against the recorded artifact."}
    for case in cases:
        began = time.monotonic()
        artifact = {"case_id": case["id"], "prompt": case["prompt"], "required_artifact": case["required_artifact"], "hard_gate": case["hard_gate"]}
        try:
            response = requests.post(base.rstrip("/") + f"/api/agents/{case['agent']}/turn", headers=headers,
                                     json={"conversationId": uuid.uuid4().hex, "message": case["prompt"]}, timeout=125, allow_redirects=False)
            artifact.update(http_status=response.status_code, response=response.json())
        except Exception as error:
            artifact["transport_error"] = type(error).__name__
        name = case["id"] + "-ofl_agent.json"
        digest = write_private(output / name, artifact)
        trial = {"case_id": case["id"], "condition": "ofl_agent", "artifact_file": name, "artifact_sha256": digest,
                 "elapsed_seconds": round(time.monotonic() - began, 3), "http_status": artifact.get("http_status"), "outcome": "unassessed"}
        report["trials"].append(trial)
        print(json.dumps(trial), flush=True)
    write_private(output / "trials.json", report)
    write_private(output / "assessments.json", [])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("protocol", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--url", required=True)
    args = parser.parse_args()
    run(json.loads(args.protocol.read_text()), args.output, args.url, os.environ["OFL_RESEARCH_GATEWAY_TOKEN"], os.environ["OFL_EVAL_IDENTITY"], os.environ["OFL_RESEARCH_ORIGIN"])
