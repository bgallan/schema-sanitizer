"""Release-version, PyPI, and GitHub preflight contracts.

It covers version agreement, trusted PyPI and GitHub endpoints, bounded retries,
workflow inputs, release state, and command failure modes.
"""

from __future__ import annotations

import importlib.util
import io
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError, URLError

import pytest

ROOT = Path(__file__).resolve().parents[2]
RELEASE = ROOT / "meta/ci/release"
GITHUB_URL = "https://api.github.com/repos/bgallan/project/git/ref/heads/main"


def _module(name: str) -> ModuleType:
    """Load the CI helper module under test from its repository path."""
    path = RELEASE / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(RELEASE))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


class _Response(io.BytesIO):
    def __enter__(self) -> _Response:
        """Return the managed response value from context entry."""
        return self

    def __exit__(self, *_args: object) -> None:
        """Finalize the response context without suppressing exceptions."""
        self.close()


class _Responses:
    def __init__(self, *values: object) -> None:
        """Initialize responses state for values and calls."""
        self.values = list(values)
        self.calls = 0

    def __call__(self, _request: object, *, timeout: int) -> _Response:
        """Return successive HTTP responses while enforcing the release-check timeout."""
        assert timeout == 20
        self.calls += 1
        value = self.values.pop(0)
        if isinstance(value, Exception):
            raise value
        return _Response(json.dumps(value).encode())


def test_validate_release_version_accepts_matching_optional_version(tmp_path: Path) -> None:
    """Verify validate release version accepts matching optional version."""
    version_file = tmp_path / "VERSION"
    version_file.write_text("0.3.8\n", encoding="utf-8")
    validator = _module("validate_release_version")
    assert validator.validate_release_version(version_file) == "0.3.8"
    assert validator.validate_release_version(version_file, "0.3.8") == "0.3.8"


def test_validate_release_version_rejects_invalid_or_mismatched_version(tmp_path: Path) -> None:
    """Verify validate release version rejects invalid or mismatched version."""
    version_file = tmp_path / "VERSION"
    validator = _module("validate_release_version")
    version_file.write_text("release-0.3.8\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid"):
        validator.validate_release_version(version_file)
    version_file.write_text("0.3.8\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match"):
        validator.validate_release_version(version_file, "0.3.7")


def test_release_version_is_read_from_manual_workflow_event(tmp_path: Path) -> None:
    """Verify release version is read from manual workflow event."""
    event = tmp_path / "event.json"
    event.write_text('{"inputs":{"release_version":"0.3.8"}}', encoding="utf-8")
    assert _module("validate_release_version").release_version_from_event(event) == "0.3.8"


def test_required_release_version_fails_closed_in_cli(tmp_path: Path) -> None:
    """Verify required release version fails closed in CLI."""
    version = tmp_path / "VERSION"
    version.write_text("0.3.8\n", encoding="utf-8")
    event = tmp_path / "event.json"
    event.write_text('{"inputs":{}}', encoding="utf-8")
    command = [
        sys.executable,
        str(RELEASE / "validate_release_version.py"),
        "--version-file",
        str(version),
        "--github-event",
        str(event),
        "--require-release-version",
    ]
    rejected = subprocess.run(command, check=False, capture_output=True, text=True)
    assert rejected.returncode != 0
    assert "release_version is required" in rejected.stderr
    event.write_text('{"inputs":{"release_version":"0.3.8"}}', encoding="utf-8")
    accepted = subprocess.run(command, check=False, capture_output=True, text=True)
    assert accepted.returncode == 0
    assert accepted.stdout == "package-version=0.3.8\n"


def test_publish_confirmation_is_read_from_manual_workflow_event(tmp_path: Path) -> None:
    """Verify publish confirmation is read from manual workflow event."""
    validator = _module("validate_release_version")
    event = tmp_path / "event.json"
    event.write_text('{"inputs":{"confirm_publish":"publish schema-sanitizer"}}', encoding="utf-8")
    validator.require_publish_confirmation(event)
    event.write_text('{"inputs":{"confirm_publish":"no"}}', encoding="utf-8")
    with pytest.raises(ValueError, match="Refusing upload"):
        validator.require_publish_confirmation(event)


def test_tag_shaped_release_version_is_rejected(tmp_path: Path) -> None:
    """Verify tag shaped release version is rejected."""
    version = tmp_path / "VERSION"
    version.write_text("0.3.8\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match"):
        _module("validate_release_version").validate_release_version(version, "v0.3.8")


def test_pypi_lookup_distinguishes_existing_and_available_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify PyPI lookup distinguishes existing and available versions."""
    checker = _module("check_pypi_version")
    existing = _Responses({"info": {"version": "0.4.2"}})
    monkeypatch.setattr(checker, "urlopen", existing)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.2") is True

    def missing(request: object, *, timeout: int) -> _Response:
        """Return the simulated GitHub response for a missing release resource."""
        raise HTTPError(request.full_url, 404, "not found", {}, None)

    monkeypatch.setattr(checker, "urlopen", missing)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3") is False


def test_pypi_lookup_fails_closed_on_service_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify PyPI lookup fails closed on service errors."""
    checker = _module("check_pypi_version")
    unavailable = _Responses(*(HTTPError("url", 503, "unavailable", {}, None) for _ in range(3)))
    monkeypatch.setattr(checker, "urlopen", unavailable)
    with pytest.raises(RuntimeError, match="HTTP 503"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=lambda _: None)
    assert unavailable.calls == 3


def test_pypi_lookup_retries_transport_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify PyPI lookup retries transport then succeeds."""
    checker = _module("check_pypi_version")
    responses = _Responses(URLError("reset"), {"info": {"version": "0.4.3"}})
    delays: list[float] = []
    monkeypatch.setattr(checker, "urlopen", responses)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=delays.append)
    assert delays == [1.0]


def test_pypi_lookup_does_not_retry_semantic_client_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify PyPI lookup does not retry semantic client errors."""
    checker = _module("check_pypi_version")
    forbidden = _Responses(HTTPError("url", 403, "forbidden", {}, None))
    delays: list[float] = []
    monkeypatch.setattr(checker, "urlopen", forbidden)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=delays.append)
    assert forbidden.calls == 1
    assert delays == []


def test_pypi_lookup_retries_rate_limit_before_exact_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify PyPI lookup retries rate limit before exact 404."""
    checker = _module("check_pypi_version")
    responses = _Responses(
        HTTPError("url", 429, "rate", {}, None), HTTPError("url", 404, "missing", {}, None)
    )
    delays: list[float] = []
    monkeypatch.setattr(checker, "urlopen", responses)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=delays.append) is False
    assert delays == [1.0]


@pytest.mark.parametrize(
    "index_url", ("http://pypi.org/pypi", "file:///tmp/pypi", "https://example.invalid/pypi")
)
def test_pypi_lookup_rejects_untrusted_index_urls(index_url: str) -> None:
    """Verify PyPI lookup rejects untrusted index urls."""
    with pytest.raises(RuntimeError, match="non-PyPI HTTPS"):
        _module("check_pypi_version").pypi_release_exists(
            "schema-sanitizer", "0.4.3", index_url=index_url
        )


@pytest.mark.parametrize("payload", ([], {}, {"info": []}, {"info": {"version": "0.4.2"}}))
def test_pypi_lookup_never_treats_an_unexpected_200_as_available(
    monkeypatch: pytest.MonkeyPatch, payload: object
) -> None:
    """Verify PyPI lookup never treats an unexpected 200 as available."""
    checker = _module("check_pypi_version")
    monkeypatch.setattr(checker, "urlopen", _Responses(payload))
    with pytest.raises(RuntimeError, match="malformed JSON|exact resource"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3")


def test_main_sha_uses_the_retrying_api_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main sha uses the retrying API lookup."""
    checker = _module("check_github_release_state")
    expected = "a" * 40
    captured: list[str] = []

    def reference(url: str, _token: str) -> dict[str, object]:
        """Return the simulated GitHub response for the target reference."""
        captured.append(url)
        return {"object": {"sha": expected}}

    monkeypatch.setattr(checker, "_github_json", reference)
    checker.validate_main_sha("bgallan/project", expected, token="token")
    assert captured == [GITHUB_URL]


def test_main_sha_fails_closed_if_the_branch_moved(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify main sha fails closed if the branch moved."""
    checker = _module("check_github_release_state")
    monkeypatch.setattr(
        checker, "_github_json", lambda *_args, **_kwargs: {"object": {"sha": "b" * 40}}
    )
    with pytest.raises(RuntimeError, match="main moved"):
        checker.validate_main_sha("bgallan/project", "a" * 40)


def test_github_lookup_retries_transport_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify GitHub lookup retries transport then succeeds."""
    checker = _module("check_github_release_state")
    payload = {"object": {"sha": "a" * 40}}
    responses = _Responses(URLError("reset"), payload)
    delays: list[float] = []
    monkeypatch.setattr(checker, "urlopen", responses)
    assert checker._github_json(GITHUB_URL, sleeper=delays.append) == payload
    assert delays == [1.0]


@pytest.mark.parametrize("status", (429, 500, 503))
def test_github_lookup_retries_transient_http_statuses(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    """Verify GitHub lookup retries transient HTTP statuses."""
    checker = _module("check_github_release_state")
    payload = {"object": {"sha": "a" * 40}}
    responses = _Responses(HTTPError("url", status, "transient", {}, None), payload)
    delays: list[float] = []
    monkeypatch.setattr(checker, "urlopen", responses)
    assert checker._github_json(GITHUB_URL, sleeper=delays.append) == payload
    assert responses.calls == 2
    assert delays == [1.0]


@pytest.mark.parametrize(
    ("headers", "now", "expected_delay"),
    (
        ({"Retry-After": "12"}, 0.0, 12.0),
        (
            {"Retry-After": "Thu, 13 Aug 2026 15:00:00 GMT"},
            datetime(2026, 8, 13, 14, 59, 50, tzinfo=timezone.utc).timestamp(),
            10.0,
        ),
        ({"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "115"}, 100.0, 15.0),
        ({"Retry-After": "300"}, 0.0, 30.0),
    ),
)
def test_github_lookup_retries_documented_rate_limited_403(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
    now: float,
    expected_delay: float,
) -> None:
    """Verify GitHub lookup retries documented rate limited 403."""
    checker = _module("check_github_release_state")
    payload = {"object": {"sha": "a" * 40}}
    responses = _Responses(HTTPError("url", 403, "rate limited", headers, None), payload)
    delays: list[float] = []
    monkeypatch.setattr(checker, "urlopen", responses)
    assert checker._github_json(GITHUB_URL, sleeper=delays.append, clock=lambda: now) == payload
    assert responses.calls == 2
    assert delays == [expected_delay]


@pytest.mark.parametrize("headers", ({}, {"Retry-After": "invalid"}))
def test_github_lookup_rejects_unqualified_403_without_retry(
    monkeypatch: pytest.MonkeyPatch, headers: dict[str, str]
) -> None:
    """Verify GitHub lookup rejects unqualified 403 without retry."""
    checker = _module("check_github_release_state")
    responses = _Responses(HTTPError("url", 403, "forbidden", headers, None))
    delays: list[float] = []
    monkeypatch.setattr(checker, "urlopen", responses)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        checker._github_json(GITHUB_URL, sleeper=delays.append)
    assert responses.calls == 1
    assert delays == []


@pytest.mark.parametrize("payload", ([], "main", None))
def test_github_lookup_rejects_non_object_json(
    monkeypatch: pytest.MonkeyPatch, payload: object
) -> None:
    """Verify GitHub lookup rejects non object JSON."""
    checker = _module("check_github_release_state")
    monkeypatch.setattr(checker, "urlopen", _Responses(payload))
    with pytest.raises(RuntimeError, match="non-object"):
        checker._github_json(GITHUB_URL)


def test_github_lookup_rejects_malformed_json_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify GitHub lookup rejects malformed JSON without retry."""
    checker = _module("check_github_release_state")
    responses = _Responses(b"{")

    def malformed(_request: object, *, timeout: int) -> _Response:
        """Return the malformed workflow input used by the validation case."""
        responses.calls += 1
        return _Response(b"{")

    delays: list[float] = []
    monkeypatch.setattr(checker, "urlopen", malformed)
    with pytest.raises(RuntimeError, match="verify GitHub release state"):
        checker._github_json(GITHUB_URL, sleeper=delays.append)
    assert responses.calls == 1
    assert delays == []


@pytest.mark.parametrize("repository", ("", "owner", "../owner/repository", "owner/../repo"))
def test_main_sha_rejects_invalid_repository_identifiers(repository: str) -> None:
    """Verify main sha rejects invalid repository identifiers."""
    with pytest.raises(RuntimeError, match="invalid GitHub repository identifier"):
        _module("check_github_release_state").validate_main_sha(repository, "a" * 40)


@pytest.mark.parametrize(
    "api_url", ("http://api.github.com", "file:///tmp/github", "https://example.invalid")
)
def test_main_sha_rejects_untrusted_api_urls(api_url: str) -> None:
    """Verify main sha rejects untrusted API urls."""
    with pytest.raises(RuntimeError, match="non-GitHub HTTPS"):
        _module("check_github_release_state").validate_main_sha(
            "bgallan/project", "a" * 40, api_url=api_url
        )
