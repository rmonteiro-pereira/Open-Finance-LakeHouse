"""What the research image must contain for its commands to run without extra settings."""

import os
import re
from pathlib import Path, PurePosixPath

from ofl.documents.sources import sources_dir

REPO_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DOCKERFILE = REPO_ROOT / "docker" / "Dockerfile.research"
DOCUMENTS_PACKAGE = REPO_ROOT / "ofl" / "documents"

# The image installs the project editable from /app, so sources_dir() resolves here.
IMAGE_PROJECT_DIR = "/app"
IMAGE_SOURCES_DIR = f"{IMAGE_PROJECT_DIR}/sources"

# Matches `sources_dir() / "letters.yml"`, the one way the documents lane opens a source list.
SOURCE_LIST_READ = re.compile(r"""sources_dir\(\)\s*/\s*["']([\w.-]+)["']""")


def source_lists_read_by_documents_lane() -> set[str]:
    """File names under ``sources/`` that the documents lane opens at run time."""
    names = set()
    for module in DOCUMENTS_PACKAGE.rglob("*.py"):
        names.update(SOURCE_LIST_READ.findall(module.read_text(encoding="utf-8")))
    return names


def image_paths_for_copy(sources: list[str], destination: PurePosixPath, as_folder: bool):
    """Pairs of (build-context file, image path) for one ``COPY`` line.

    A source folder copies its contents into the destination, as Docker does.
    """
    for source in sources:
        context_path = REPO_ROOT / source
        if context_path.is_dir():
            for file in context_path.rglob("*"):
                if file.is_file():
                    relative = file.relative_to(REPO_ROOT).as_posix()
                    yield relative, destination / file.relative_to(context_path).as_posix()
        elif as_folder:
            yield source, destination / PurePosixPath(source).name
        else:
            yield source, destination


def files_copied_into_research_image() -> dict[str, str]:
    """Image path of every copied file, mapped to the build-context file it came from.

    Destinations are resolved against the ``WORKDIR`` in force at that line. A
    destination ending in ``/``, or one that receives several files, is a folder.
    """
    workdir = PurePosixPath("/")
    copied = {}
    for line in RESEARCH_DOCKERFILE.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "WORKDIR":
            workdir = workdir / parts[1]
            continue
        if parts[0] != "COPY" or any(part.startswith("--from") for part in parts):
            continue
        sources = [part for part in parts[1:-1] if not part.startswith("--")]
        destination = workdir / parts[-1]
        as_folder = parts[-1].endswith("/") or len(sources) > 1
        for source, target in image_paths_for_copy(sources, destination, as_folder):
            copied[os.path.normpath(target)] = os.path.normpath(source)
    return copied


def test_the_documents_lane_reads_source_lists():
    # Guards the scan itself: if the read pattern changes, the image check below
    # would otherwise pass while checking nothing.
    assert {"letters.yml", "news.yml"} <= source_lists_read_by_documents_lane()


def test_research_image_contains_every_source_list_the_documents_lane_reads():
    copied = files_copied_into_research_image()
    required = source_lists_read_by_documents_lane()
    missing = sorted(
        name for name in required if copied.get(f"{IMAGE_SOURCES_DIR}/{name}") != f"sources/{name}"
    )
    assert not missing, f"copy into {IMAGE_SOURCES_DIR} in docker/Dockerfile.research: {missing}"


def test_source_lists_resolve_next_to_the_package_without_settings(monkeypatch):
    # With no OFL_REGISTRY the lists are found next to the package. That is /app/sources
    # in the image only while the package is copied to /app and installed editable.
    monkeypatch.delenv("OFL_REGISTRY", raising=False)
    assert sources_dir() == REPO_ROOT / "sources"
    assert f"{IMAGE_PROJECT_DIR}/ofl/documents/sources.py" in files_copied_into_research_image()
    assert "--no-editable" not in RESEARCH_DOCKERFILE.read_text(encoding="utf-8")
