"""Mutation gate for the research functions that decide access, money and evidence integrity.

Run from the repository root:

    uv run --extra dev --extra research --with mutmut==3.3.1 python tools/mutation_gate.py

The gate mutates only the functions listed in SCOPES (configuration in pyproject.toml,
[tool.mutmut]), runs the offline research tests against each mutant and exits non-zero
when a scope's kill rate is below its target. Equivalent mutants are excluded from the
denominator: those whose source is identical to the original, and those listed in
EQUIVALENT with the reason no input can tell them apart.

--idle-tests additionally lists tests that touch a gated function but kill no mutant.

CPU use: half of the available cores (override with MUTATION_WORKERS), pinned to that
half, at nice 10. The run waits while the load average exceeds the core count.
"""

import argparse
import ast
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MUTANTS = ROOT / "mutants"
STATUS = MUTANTS / "run.status.json"
FRESH = MUTANTS / "fresh-process.json"

SCOPES = {
    "access": (
        0.95,
        [
            "ofl.research.api.x_authenticate",
            "ofl.research.api.x_require_admin",
            "ofl.research.api.x_require_reviewer",
            "ofl.research.api.x_can_read_feedback",
            "ofl.research.workspaces.x_has_research_access",
            "ofl.research.workspaces.x_owned",
            "ofl.research.workspaces.x_schedule_due",
            "ofl.research.workspaces.x_load",
        ],
    ),
    "money": (
        0.95,
        [
            "ofl.research.query.x_per_unit",
            "ofl.research.query.x_quote_query",
            "ofl.research.analytics.x_number",
            "ofl.research.analytics.x_portfolio",
            "ofl.research.analytics.x_compare_quotes",
            "ofl.research.cotahist.x__date",
            "ofl.research.cotahist.x_parse_record",
            "ofl.research.cotahist.x_stage",
            "ofl.research.review.x_observation_period",
            "ofl.research.review.x_normalize",
        ],
    ),
    "evidence": (
        0.95,
        [
            "ofl.research.reference_review.x_text_hash",
            "ofl.research.reference_review.x_reference_reviews",
            "ofl.research.dataset_bundle.x_bundle",
            "ofl.research.dataset_replay.x_replay",
        ],
    ),
    "other": (
        0.85,
        [
            "ofl.research.workspaces.x_evidence",
            "ofl.research.workspaces.x_validate",
            "ofl.research.analytics.x_release_diff",
            "ofl.research.analytics.x_prepare_dataset",
        ],
    ),
}

_HEADERS = "request headers are a case-insensitive mapping (Starlette Headers)"
_ZIP = "zip() over sequences whose equal length is guaranteed a few lines earlier"
_DEFAULT_SAME_ERROR = "missing key: None fails the same type check with the same message"
_CODEC = "Python codec names are case-insensitive"
_DUCKDB = "DuckDB settings and view names are case-insensitive"
_DUCKDB_SQL = "DuckDB keywords, settings and identifiers are case-insensitive"
_EXPONENT = "Decimal exponent marker is case-insensitive: same value"

# Mutant number -> reason, per gated function. Numbers are stable while the function's
# source is unchanged; mutmut regenerates them when it changes, so review this table then.
EQUIVALENT = {
    "ofl.research.api.x_authenticate": {
        7: _HEADERS,
        21: _HEADERS,
        86: _HEADERS,
        114: _HEADERS,
        8: "default only feeds startswith('Bearer '); 'XXXX' and '' both fail it",
        82: "absent identity: None and '' are both refused by the empty-identity check",
        84: "absent identity: None and '' are both refused by the empty-identity check",
    },
    "ofl.research.query.x_per_unit": {
        3: "factor 0 is already caught by `not factor`",
        4: "quotation factor is a 7-digit unsigned integer: < 1 and < 0 differ only at 0",
        6: "38 vs 39 digits differ only after >= 25 consecutive 9s in close/factor; with "
        "two-decimal closes and factors <= 9,999,999 the period bounds runs of 9s to 9 digits",
    },
    "ofl.research.query.x_quote_query": {
        **dict.fromkeys((41, 42, 48, 51, 53, 55, 60, 64, 78, 80), _DUCKDB_SQL),
        **dict.fromkeys((95, 98, 103), "zip() over two columns of one Arrow table"),
        **dict.fromkeys((128, 131, 132), "zip() over cursor column names and one row"),
    },
    "ofl.research.analytics.x_number": {21: _EXPONENT},
    "ofl.research.analytics.x_portfolio": {
        3: _DEFAULT_SAME_ERROR,
        5: _DEFAULT_SAME_ERROR,
        36: "missing date: str(None) fails fromisoformat with the same message",
        38: "missing date: str(None) fails fromisoformat with the same message",
        41: "missing date: 'XXXX' fails fromisoformat with the same message",
        191: _DEFAULT_SAME_ERROR,
        193: _DEFAULT_SAME_ERROR,
    },
    "ofl.research.analytics.x_prepare_dataset": {
        19: _ZIP,
        22: _ZIP,
        23: _ZIP,
        129: _ZIP,
        132: _ZIP,
        135: _ZIP,
        170: "unit default only feeds a membership test; 'none' is not a percent unit",
        172: "unit default only feeds a membership test; 'none' is not a percent unit",
        177: "unit default only feeds a membership test; 'xxxx' is not a percent unit",
    },
    "ofl.research.cotahist.x_parse_record": {7: _CODEC},
    "ofl.research.cotahist.x_stage": {
        170: "the dropped leading digit is non-zero only for >= 10^10 lines (2.4 TB)",
        182: _CODEC,
        188: _CODEC,
        281: _DUCKDB,
        282: _DUCKDB,
        285: _DUCKDB,
        286: _DUCKDB,
        292: _DUCKDB,
        293: _DUCKDB,
        301: _DUCKDB,
    },
    "ofl.research.dataset_bundle.x_bundle": {
        15: "json.dumps treats ensure_ascii=None like False",
        24: "json.dumps treats ensure_ascii=None like False",
        107: "every member's ZipInfo sets ZIP_DEFLATED, overriding the archive default",
        114: "ZipInfo's default date_time is (1980, 1, 1, 0, 0, 0)",
        120: "ZIP stores seconds in 2-second units: 1 s is written as 0",
        122: "zipfile writes 0o600 << 16 when external_attr is falsy",
        123: "0o600 >> 16 is 0, and zipfile writes 0o600 << 16 for 0",
    },
    "ofl.research.dataset_replay.x_replay": {
        68: _ZIP,
        71: _ZIP,
        74: _ZIP,
        245: _ZIP,
        248: _ZIP,
        249: _ZIP,
    },
    "ofl.research.review.x_normalize": {
        9: "default only used when header_rows is absent; neither 1 nor 2 is negative",
        137: "no separator configured: None and '' are both falsy and differ from '.'/','",
        139: "no separator configured: None and '' are both falsy and differ from '.'/','",
        190: _EXPONENT,
    },
    "ofl.research.workspaces.x_validate": {
        26: _DEFAULT_SAME_ERROR,
        28: _DEFAULT_SAME_ERROR,
        99: _DEFAULT_SAME_ERROR,
        101: _DEFAULT_SAME_ERROR,
        191: _DEFAULT_SAME_ERROR,
        193: _DEFAULT_SAME_ERROR,
    },
}

# Functions reached by tests that read local Delta tables (quote_query calls per_unit):
# verified one mutant per new interpreter.
FRESH_PROCESS = ["ofl.research.query.x_quote_query", "ofl.research.query.x_per_unit"]

KILLED = {1, 3, -24, 24, 36, 152, 255}  # failing tests, or CPU limit (counted as killed)
SURVIVED = {0}


def wait_for_capacity(cores):
    while os.getloadavg()[0] > cores:
        print(f"load {os.getloadavg()[0]:.1f} exceeds {cores} cores; waiting 60 s", flush=True)
        time.sleep(60)


def gated_functions():
    return [f for _, functions in SCOPES.values() for f in functions]


def meta_path(function):
    module = function.rpartition(".")[0]
    return MUTANTS / (module.replace(".", "/") + ".py.meta")


def run_problems():
    """A failed or missing mutmut run must fail the gate, never score as a pass."""
    if not STATUS.exists():
        return ["no completed mutation run in mutants/ (run without --report-only)"]
    returncode = json.loads(STATUS.read_text())["returncode"]
    if returncode != 0:
        log = (MUTANTS / "run.log").read_text(errors="replace").splitlines()[-15:]
        return [f"mutmut exited with {returncode}; end of mutants/run.log:", *log]
    missing = sorted({str(meta_path(f)) for f in gated_functions() if not meta_path(f).exists()})
    problems = [f"mutation metadata missing: {path}" for path in missing]
    if FRESH_PROCESS and not FRESH.exists():
        problems.append(
            "fresh-process check missing or failed: unmutated code must pass "
            + ", ".join(FRESH_PROCESS)
        )
    return problems


def results():
    codes = {}
    for meta in MUTANTS.rglob("*.meta"):
        codes.update(json.loads(meta.read_text())["exit_code_by_key"])
    if FRESH.exists():
        codes.update(json.loads(FRESH.read_text()))
    return codes


def fresh_process_check(workers, pinned):
    """Re-run mutants of FRESH_PROCESS functions, each in a new interpreter.

    Their tests read Delta tables; the Delta reader's runtime does not survive the fork
    mutmut uses, so forked results for these functions are replaced. The unmutated code
    must pass the same tests first, or the gate stops.
    """
    stats = json.loads((MUTANTS / "mutmut-stats.json").read_text())
    codes = results()
    env = {**os.environ, "OFL_RESEARCH_TEST_DATABASE_URL": "", "PYTHONPATH": str(MUTANTS)}

    def run(function, mutant):
        tests = sorted(stats["tests_by_mangled_function_name"].get(function, []))
        if not tests:
            return mutant, 33
        command = ["nice", "-n", "10", sys.executable, "-m", "pytest", "-x", "-q"]
        try:
            return mutant, subprocess.run(
                [*command, "-p", "no:cacheprovider", *tests],
                cwd=MUTANTS,
                env={**env, "MUTANT_UNDER_TEST": mutant},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=600,
                preexec_fn=lambda: os.sched_setaffinity(0, pinned),
            ).returncode
        except subprocess.TimeoutExpired:
            return mutant, 36

    problems, fresh = [], {}
    for function in FRESH_PROCESS:
        if run(function, "")[1] != 0:
            problems.append(f"unmutated {function} fails its tests in a fresh process")
            continue
        names = [n for n in codes if n.startswith(function + "__mutmut_")]
        with ThreadPoolExecutor(workers) as pool:
            fresh.update(pool.map(lambda name, f=function: run(f, name), names))
    FRESH.write_text(json.dumps(fresh))
    return problems


def missing_functions(codes):
    """Every configured function must produce at least one mutant, all of them run."""
    problems = []
    for function in gated_functions():
        names = [n for n in codes if n.startswith(function + "__mutmut_")]
        if not names:
            problems.append(f"no mutants generated for {function}")
        elif any(codes[n] is None for n in names):
            problems.append(f"mutants of {function} were not run")
    return problems


def identical_source():
    """Mutants whose function source equals the original (e.g. upper-casing '00')."""
    same = set()
    for path in MUTANTS.joinpath("ofl").rglob("*.py"):
        text = path.read_text()
        if "__mutmut_orig" not in text:
            continue
        module = ".".join(path.relative_to(MUTANTS).with_suffix("").parts)
        bodies = {
            node.name: ast.dump(node.args) + ast.dump(ast.Module(body=node.body, type_ignores=[]))
            for node in ast.parse(text).body
            if isinstance(node, ast.FunctionDef) and "__mutmut_" in node.name
        }
        for name, body in bodies.items():
            base, _, number = name.partition("__mutmut_")
            if number != "orig" and body == bodies.get(base + "__mutmut_orig"):
                same.add(f"{module}.{name}")
    return same


def equivalent_reason(name, same):
    if name in same:
        return "identical source"
    function, _, number = name.rpartition("__mutmut_")
    return EQUIVALENT.get(function, {}).get(int(number))


def scope_names(codes, functions):
    return sorted(n for n in codes if any(n.startswith(f + "__mutmut_") for f in functions))


def report(codes, verbose):
    same = identical_source()
    failed = False
    print(
        f"{'scope':<10}{'target':>7}{'mutants':>9}{'killed':>8}{'survived':>10}"
        f"{'equivalent':>12}{'untested':>10}{'score':>8}"
    )
    for scope, (target, functions) in SCOPES.items():
        names = scope_names(codes, functions)
        equivalent = [n for n in names if equivalent_reason(n, same)]
        live = [n for n in names if n not in equivalent]
        killed = [n for n in live if codes[n] in KILLED]
        survived = [n for n in live if codes[n] in SURVIVED]
        untested = [n for n in live if codes[n] not in KILLED | SURVIVED]
        score = len(killed) / len(live) if live else 0.0  # nothing testable is not a pass
        failed |= score < target
        print(
            f"{scope:<10}{target:>7.0%}{len(names):>9}{len(killed):>8}{len(survived):>10}"
            f"{len(equivalent):>12}{len(untested):>10}{score:>8.1%}"
        )
        if verbose:
            for name in survived + untested:
                print(f"    {'survived' if name in survived else 'not run'}  {name}")
            for name in equivalent:
                print(f"    equivalent  {name}: {equivalent_reason(name, same)}")
    return failed


def probe(job_file):
    """Inside mutants/: for each test, stop at the first mutant it kills."""
    import contextlib
    import io

    import pytest

    jobs = json.loads(Path(job_file).read_text())
    found = {}
    for test, candidates in jobs.items():
        found[test] = None
        for mutant in candidates:
            os.environ["MUTANT_UNDER_TEST"] = mutant
            with (
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                code = pytest.main(["-x", "-q", "-p", "no:cacheprovider", test])
            if code == 1:
                found[test] = mutant
                break
    Path(job_file).with_suffix(".out").write_text(json.dumps(found))


def idle_tests(codes, workers):
    stats = json.loads((MUTANTS / "mutmut-stats.json").read_text())
    gated = {f for _, functions in SCOPES.values() for f in functions}
    killed_by_function = {}
    for name, code in codes.items():
        function = name.rpartition("__mutmut_")[0]
        if function in gated and code in KILLED:
            killed_by_function.setdefault(function, []).append(name)
    candidates = {}
    for function, tests in stats["tests_by_mangled_function_name"].items():
        for test in tests:
            candidates.setdefault(test, []).extend(killed_by_function.get(function, []))
    tests = sorted(candidates)
    chunks = [tests[i::workers] for i in range(workers)]
    env = {**os.environ, "OFL_RESEARCH_TEST_DATABASE_URL": "", "PYTHONPATH": str(MUTANTS)}

    def run(index):
        job = MUTANTS / f"idle-{index}.json"
        job.write_text(json.dumps({t: candidates[t] for t in chunks[index]}))
        subprocess.run(
            [
                "nice",
                "-n",
                "10",
                sys.executable,
                str(Path(__file__).resolve()),
                "--probe",
                str(job),
            ],
            cwd=MUTANTS,
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        return json.loads(job.with_suffix(".out").read_text())

    found = {}
    with ThreadPoolExecutor(workers) as pool:
        for part in pool.map(run, range(len(chunks))):
            found.update(part)
    idle = sorted(t for t, mutant in found.items() if mutant is None)
    print(f"tests touching gated functions: {len(found)}; killing no mutant: {len(idle)}")
    for test in idle:
        print(f"    {test}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report-only", action="store_true", help="read the last run")
    parser.add_argument("--idle-tests", action="store_true", help="list tests that kill nothing")
    parser.add_argument("-v", "--verbose", action="store_true", help="list each non-killed mutant")
    parser.add_argument("--probe", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.probe:
        return probe(args.probe)
    os.chdir(ROOT)
    cores = sorted(os.sched_getaffinity(0))
    workers = int(os.environ.get("MUTATION_WORKERS", max(1, len(cores) // 2)))
    if not args.report_only:
        wait_for_capacity(len(cores))
        shutil.rmtree(MUTANTS, ignore_errors=True)
        MUTANTS.mkdir()
        patterns = [f + "__mutmut_*" for f in gated_functions()]
        # Offline tests only: the PostgreSQL tests publish Delta tables, and the Delta
        # writer's runtime does not survive the fork that mutmut makes for each mutant.
        env = {**os.environ, "OFL_RESEARCH_TEST_DATABASE_URL": ""}
        command = [
            "nice",
            "-n",
            "10",
            shutil.which("mutmut") or "mutmut",
            "run",
            "--max-children",
            str(workers),
            *patterns,
        ]
        pinned = set(cores[-workers:])
        started = time.monotonic()
        with open(MUTANTS / "run.log", "w") as log:
            returncode = subprocess.run(
                command,
                env=env,
                check=False,
                stdout=log,
                stderr=subprocess.STDOUT,
                preexec_fn=lambda: os.sched_setaffinity(0, pinned),
            ).returncode
        STATUS.write_text(json.dumps({"returncode": returncode}))
        print(f"mutation run: {time.monotonic() - started:.0f} s, {workers} workers")
        if returncode == 0:
            fresh_problems = fresh_process_check(workers, pinned)
            for problem in fresh_problems:
                print(f"gate error: {problem}")
            if fresh_problems:
                FRESH.unlink()
            print(f"fresh-process check: {time.monotonic() - started:.0f} s total")
    problems = run_problems()
    codes = results() if not problems else {}
    problems += missing_functions(codes)
    if problems:
        for problem in problems:
            print(f"gate error: {problem}")
        sys.exit(2)
    failed = report(codes, args.verbose)
    if args.idle_tests:
        idle_tests(codes, workers)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
