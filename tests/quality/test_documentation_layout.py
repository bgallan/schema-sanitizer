"""Keep user documentation centralized and free of retired planning files."""

from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCS_ROOT = PROJECT_ROOT / "docs"
DOCUMENTATION = (PROJECT_ROOT / "README.md", *sorted(DOCS_ROOT.glob("*.md")))
AUDIT_RECORDS = tuple(sorted(DOCS_ROOT.glob("memory-concurrency-hardening-pass*.md")))
PUBLIC_DOCUMENTATION = tuple(path for path in DOCUMENTATION if path not in AUDIT_RECORDS)


def _anchor(title: str) -> str:
    """Build the subset of GitHub heading anchors used by project docs."""
    plain = title.replace("`", "").lower()
    plain = re.sub(r"[^\w\- ]", "", plain)
    return re.sub(r"\s+", "-", plain.strip())


def test_only_introductory_readme_remains_at_repository_root() -> None:
    """Detailed Markdown belongs under docs instead of the repository root."""
    root_markdown = sorted(path.name for path in PROJECT_ROOT.glob("*.md"))

    assert root_markdown == ["README.md"]


def test_documentation_index_covers_public_guides() -> None:
    """The documentation index must link every main user-facing guide."""
    index = (DOCS_ROOT / "README.md").read_text(encoding="utf-8")
    expected = {
        "bigquery.md",
        "ci-cd.md",
        "compatibility.md",
        "concurrency-memory-hardening.md",
        "development.md",
        "getting-started.md",
        "heuristics.md",
        "inputs-and-filesystems.md",
        "options.md",
        "pipelines.md",
        "python-api.md",
        "reader-security-limits.md",
    }

    assert all(f"({name})" in index for name in expected)


def test_documentation_index_covers_hardening_audit_records() -> None:
    """Every internal hardening record remains discoverable from the index."""
    index = (DOCS_ROOT / "README.md").read_text(encoding="utf-8")

    assert AUDIT_RECORDS
    assert all(f"({path.name})" in index for path in AUDIT_RECORDS)


def test_hardening_audit_record_titles_match_their_pass_number() -> None:
    """Audit records remain non-empty, self-identifying historical artifacts."""
    filename = re.compile(r"memory-concurrency-hardening-pass(\d+)\.md")
    for path in AUDIT_RECORDS:
        match = filename.fullmatch(path.name)
        first_line = path.read_text(encoding="utf-8").splitlines()[0]

        assert match is not None
        assert first_line.startswith("# ")
        assert re.search(rf"\bpass\s+{match.group(1)}\b", first_line, re.IGNORECASE)


def test_retired_documentation_and_references_are_absent() -> None:
    """Completed TODOs and old root-document paths must not return."""
    retired = {
        "COMPATIBILITY.md",
        "HEURISTICS.md",
        "MODIFIED_TIME_CSV_TODO.md",
        "READER_HARDENING_TODO.md",
    }
    assert all(not (PROJECT_ROOT / name).exists() for name in retired)
    for path in DOCUMENTATION:
        text = path.read_text(encoding="utf-8")
        assert not any(f"](../{name})" in text or f"]({name})" in text for name in retired)


def test_every_document_has_bidirectional_index_links() -> None:
    """Each index reaches every section and every section title returns to it."""
    heading = re.compile(r"^(#{2,6}) \[(.+)\]\(#index\)$", re.MULTILINE)
    for path in PUBLIC_DOCUMENTATION:
        text = path.read_text(encoding="utf-8")
        assert "## Index" in text, path
        sections = heading.findall(text)
        assert sections, path
        for _marks, title in sections:
            assert f"](#{_anchor(title)})" in text, (path, title)
