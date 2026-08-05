"""Execute the tutorial notebook code cells against the public API."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS = tuple(sorted((ROOT / "examples").glob("*.ipynb")))


def test_tutorial_notebook_code_cells_execute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run each tutorial notebook in an isolated notebook-style working directory."""
    pytest.importorskip("pyarrow")
    monkeypatch.chdir(tmp_path)

    for notebook_path in NOTEBOOKS:
        notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
        namespace = {"__name__": "__main__"}
        for index, cell in enumerate(notebook.get("cells", [])):
            if cell.get("cell_type") != "code":
                continue
            source = "".join(cell.get("source", []))
            filename = f"{notebook_path.name}:cell-{index}"
            exec(compile(source, filename, "exec"), namespace)
