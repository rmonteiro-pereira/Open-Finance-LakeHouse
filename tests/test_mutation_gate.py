"""Self-tests for tools/mutation_gate.py: run failures must fail the gate, never pass it."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def gate(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("mutation_gate", ROOT / "tools/mutation_gate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    mutants = tmp_path / "mutants"
    monkeypatch.setattr(module, "MUTANTS", mutants)
    monkeypatch.setattr(module, "STATUS", mutants / "run.status.json")
    monkeypatch.setattr(module, "FRESH", mutants / "fresh-process.json")
    monkeypatch.setattr(module, "FRESH_LOGS", mutants / "fresh-logs")
    monkeypatch.setattr(module, "wait_for_capacity", lambda cores: None)
    monkeypatch.setattr(sys, "argv", ["mutation_gate.py"])
    return module


def fake_runs(gate, monkeypatch, fresh_outcome, mutmut_returncode=0):
    """mutmut writes one killed mutant per gated function; pytest answers per mutant."""
    fresh_calls = []

    def run(command, **kwargs):
        if "--max-children" in command:
            codes = {}
            for function in gate.gated_functions():
                codes.setdefault(gate.meta_path(function), {})[f"{function}__mutmut_1"] = 1
            for path, exit_codes in codes.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(
                    json.dumps({"exit_code_by_key": exit_codes, "hash_by_function_name": {}})
                )
            tests = {f: [f"tests/test_x.py::test_{i}"] for i, f in enumerate(gate.FRESH_PROCESS)}
            (gate.MUTANTS / "mutmut-stats.json").write_text(
                json.dumps({"tests_by_mangled_function_name": tests})
            )
            return subprocess.CompletedProcess(command, mutmut_returncode)
        mutant = kwargs["env"]["MUTANT_UNDER_TEST"]
        fresh_calls.append(mutant)
        outcome = 0 if not mutant else fresh_outcome(mutant, fresh_calls.count(mutant))
        if outcome == "timeout":
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return subprocess.CompletedProcess(command, outcome)

    monkeypatch.setattr(gate.subprocess, "run", run)
    return fresh_calls


def exit_code(gate):
    with pytest.raises(SystemExit) as stopped:
        gate.main()
    return stopped.value.code


def test_clean_run_with_every_mutant_killed_passes(gate, monkeypatch):
    fake_runs(gate, monkeypatch, lambda mutant, attempt: 1)
    assert exit_code(gate) == 0


def test_internal_error_in_a_fresh_process_mutant_fails_the_gate(gate, monkeypatch, capsys):
    target = gate.FRESH_PROCESS[0] + "__mutmut_1"
    fake_runs(gate, monkeypatch, lambda mutant, attempt: 3 if mutant == target else 1)
    assert exit_code(gate) == 2
    assert f"{target}: pytest exit 3 is an error, not a kill" in capsys.readouterr().out


@pytest.mark.parametrize("returncode", [2, 4, 5])
def test_usage_and_collection_errors_are_not_kills(gate, monkeypatch, returncode):
    fake_runs(gate, monkeypatch, lambda mutant, attempt: returncode)
    assert exit_code(gate) == 2


def test_a_timeout_is_retried_alone_once(gate, monkeypatch):
    calls = fake_runs(gate, monkeypatch, lambda mutant, attempt: "timeout" if attempt == 1 else 1)
    assert exit_code(gate) == 0
    target = gate.FRESH_PROCESS[0] + "__mutmut_1"
    assert calls.count(target) == 2


def test_a_second_timeout_fails_the_gate(gate, monkeypatch, capsys):
    fake_runs(gate, monkeypatch, lambda mutant, attempt: "timeout")
    assert exit_code(gate) == 2
    assert "timed out twice" in capsys.readouterr().out


def test_a_failed_mutmut_run_fails_the_gate(gate, monkeypatch, capsys):
    fake_runs(gate, monkeypatch, lambda mutant, attempt: 1, mutmut_returncode=1)
    assert exit_code(gate) == 2
    assert "mutmut exited with 1" in capsys.readouterr().out


def test_a_function_without_mutants_fails_the_gate(gate, monkeypatch, capsys):
    fake_runs(gate, monkeypatch, lambda mutant, attempt: 1)
    monkeypatch.setitem(gate.SCOPES, "extra", (0.85, ["ofl.research.query.x_missing"]))
    real = gate.subprocess.run

    def without_missing(command, **kwargs):
        result = real(command, **kwargs)
        if "--max-children" in command:
            meta = gate.meta_path("ofl.research.query.x_missing")
            data = json.loads(meta.read_text())
            data["exit_code_by_key"].pop("ofl.research.query.x_missing__mutmut_1")
            meta.write_text(json.dumps(data))
        return result

    monkeypatch.setattr(gate.subprocess, "run", without_missing)
    assert exit_code(gate) == 2
    assert "no mutants generated for ofl.research.query.x_missing" in capsys.readouterr().out
