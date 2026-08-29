"""Release-version, PyPI, and GitHub preflight contracts.

The tests cover workflow identity, literal-host HTTPS transports, strict endpoint
validation, bounded retries, fail-closed response handling, and release state.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
RELEASE = ROOT / "meta" / "ci" / "release"
GITHUB_URL = "https://api.github.com/repos/example/project/git/ref/heads/main"


def _module(name: str) -> ModuleType:
    """Load one standalone release helper from its repository path."""
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


def test_validate_release_version_accepts_matching_optional_version(tmp_path: Path) -> None:
    """The canonical version file may be checked against a workflow value."""
    version_file = tmp_path / "VERSION"
    version_file.write_text("0.3.8\n", encoding="utf-8")
    validator = _module("validate_release_version")
    assert validator.validate_release_version(version_file) == "0.3.8"
    assert validator.validate_release_version(version_file, "0.3.8") == "0.3.8"


def test_validate_release_version_rejects_invalid_or_mismatched_version(tmp_path: Path) -> None:
    """Malformed and stale workflow versions fail before release work starts."""
    version_file = tmp_path / "VERSION"
    validator = _module("validate_release_version")
    version_file.write_text("release-0.3.8\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid"):
        validator.validate_release_version(version_file)
    version_file.write_text("0.3.8\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match"):
        validator.validate_release_version(version_file, "0.3.7")


def test_release_version_is_read_from_manual_workflow_event(tmp_path: Path) -> None:
    """Manual dispatch input remains the release version authority."""
    event = tmp_path / "event.json"
    event.write_text('{"inputs":{"release_version":"0.3.8"}}', encoding="utf-8")
    assert _module("validate_release_version").release_version_from_event(event) == "0.3.8"


def test_required_release_version_fails_closed_in_cli(tmp_path: Path) -> None:
    """The command-line gate rejects a dispatch that omits release identity."""
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
    """Publication requires the exact explicit manual confirmation phrase."""
    validator = _module("validate_release_version")
    event = tmp_path / "event.json"
    event.write_text('{"inputs":{"confirm_publish":"publish schema-sanitizer"}}', encoding="utf-8")
    validator.require_publish_confirmation(event)
    event.write_text('{"inputs":{"confirm_publish":"no"}}', encoding="utf-8")
    with pytest.raises(ValueError, match="Refusing upload"):
        validator.require_publish_confirmation(event)


def test_tag_shaped_release_version_is_rejected(tmp_path: Path) -> None:
    """Tag syntax cannot substitute for the canonical package version."""
    version = tmp_path / "VERSION"
    version.write_text("0.3.8\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match"):
        _module("validate_release_version").validate_release_version(version, "v0.3.8")


@pytest.mark.parametrize("hostname", ("pypi.org", "test.pypi.org"))
def test_pypi_transport_connects_only_to_literal_https_origins(
    monkeypatch: pytest.MonkeyPatch,
    hostname: str,
) -> None:
    """The low-level transport fixes the remote origin before making a request."""
    checker = _module("check_pypi_version")
    observed: list[object] = []

    class Response:
        """Provide one successful PyPI JSON response."""

        status = 200

        @staticmethod
        def read() -> bytes:
            """Return the exact simulated response body."""
            return b'{"info":{"version":"0.4.2"}}'

    class Connection:
        """Record the literal connection and origin-form request target."""

        def __init__(self, host: str, *, timeout: int) -> None:
            """Capture the selected TLS host and timeout."""
            observed.append(("connect", host, timeout))

        def request(self, method: str, target: str, *, headers: dict[str, str]) -> None:
            """Capture the method, target, and request headers."""
            observed.append(("request", method, target, headers))

        @staticmethod
        def getresponse() -> Response:
            """Return the simulated successful response."""
            return Response()

        def close(self) -> None:
            """Record deterministic connection cleanup."""
            observed.append(("close",))

    monkeypatch.setattr(checker, "HTTPSConnection", Connection)
    status, payload = checker._pypi_https_json(
        hostname,
        "/pypi/schema-sanitizer/0.4.2/json",
        {"Accept": "application/json"},
    )

    assert status == 200
    assert payload == {"info": {"version": "0.4.2"}}
    assert observed == [
        ("connect", hostname, 20),
        (
            "request",
            "GET",
            "/pypi/schema-sanitizer/0.4.2/json",
            {"Accept": "application/json"},
        ),
        ("close",),
    ]


def test_pypi_transport_rejects_an_unlisted_host_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Internal misuse cannot turn the release preflight into an HTTPS proxy."""
    checker = _module("check_pypi_version")
    monkeypatch.setattr(
        checker,
        "HTTPSConnection",
        lambda *_args, **_kwargs: pytest.fail("unexpected connection"),
    )

    with pytest.raises(RuntimeError, match="non-PyPI HTTPS host"):
        checker._pypi_https_json("example.invalid", "/", {})


def test_pypi_lookup_distinguishes_existing_and_available_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exact JSON resource blocks release while a 404 proves availability."""
    checker = _module("check_pypi_version")
    captured: list[str] = []

    def existing(hostname: str, target: str, _headers: object) -> tuple[int, object]:
        """Return a matching release and capture its fixed-origin resource."""
        captured.append(f"https://{hostname}{target}")
        return 200, {"info": {"version": "0.4.2"}}

    monkeypatch.setattr(checker, "_pypi_https_json", existing)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.2") is True
    assert captured == ["https://pypi.org/pypi/schema-sanitizer/0.4.2/json"]

    def missing(_hostname: str, _target: str, _headers: object) -> tuple[int, object]:
        """Return the only response that authorizes a new version."""
        return 404, None

    monkeypatch.setattr(checker, "_pypi_https_json", missing)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3") is False


def test_pypi_lookup_fails_closed_on_service_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """An inconclusive index response cannot authorize publication."""
    checker = _module("check_pypi_version")
    calls = 0

    def unavailable(_hostname: str, _target: str, _headers: object) -> tuple[int, object]:
        """Count and return one transient service failure."""
        nonlocal calls
        calls += 1
        return 503, None

    monkeypatch.setattr(checker, "_pypi_https_json", unavailable)
    with pytest.raises(RuntimeError, match="HTTP 503"):
        checker.pypi_release_exists(
            "schema-sanitizer",
            "0.4.3",
            sleeper=lambda _delay: None,
        )
    assert calls == 3


def test_pypi_lookup_retries_transport_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bounded retry absorbs one transient transport failure."""
    checker = _module("check_pypi_version")
    responses: list[object] = [OSError("reset"), {"info": {"version": "0.4.3"}}]
    delays: list[float] = []

    def flaky(_hostname: str, _target: str, _headers: object) -> tuple[int, object]:
        """Raise the first response and return the second."""
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return 200, response

    monkeypatch.setattr(checker, "_pypi_https_json", flaky)
    assert checker.pypi_release_exists(
        "schema-sanitizer",
        "0.4.3",
        sleeper=delays.append,
    )
    assert delays == [1.0]


def test_pypi_lookup_does_not_retry_semantic_client_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Authentication and policy failures terminate immediately."""
    checker = _module("check_pypi_version")
    calls = 0
    delays: list[float] = []

    def forbidden(_hostname: str, _target: str, _headers: object) -> tuple[int, object]:
        """Count and return one non-retryable client failure."""
        nonlocal calls
        calls += 1
        return 403, None

    monkeypatch.setattr(checker, "_pypi_https_json", forbidden)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=delays.append)
    assert calls == 1
    assert delays == []


def test_pypi_lookup_retries_rate_limit_before_exact_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rate-limited lookup may retry before an exact missing result."""
    checker = _module("check_pypi_version")
    statuses = iter((429, 404))
    delays: list[float] = []

    def response(_hostname: str, _target: str, _headers: object) -> tuple[int, object]:
        """Return successive retryable and authoritative statuses."""
        return next(statuses), None

    monkeypatch.setattr(checker, "_pypi_https_json", response)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=delays.append) is False
    assert delays == [1.0]


@pytest.mark.parametrize(
    "index_url",
    (
        "http://pypi.org/pypi",
        "file:///tmp/pypi",
        "https://example.invalid/pypi",
        "https://user@pypi.org/pypi",
        "https://pypi.org:444/pypi",
        "https://pypi.org/simple",
        "https://pypi.org/pypi?mirror=other",
        "https://pypi.org/pypi#fragment",
    ),
)
def test_pypi_lookup_rejects_untrusted_index_urls(index_url: str) -> None:
    """Preflight accepts only the exact credential-free PyPI JSON base URL."""
    with pytest.raises(RuntimeError, match="non-PyPI HTTPS"):
        _module("check_pypi_version").pypi_release_exists(
            "schema-sanitizer",
            "0.4.3",
            index_url=index_url,
        )


@pytest.mark.parametrize("payload", ([], {}, {"info": []}, {"info": {"version": "0.4.2"}}))
def test_pypi_lookup_never_treats_an_unexpected_200_as_available(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
) -> None:
    """Malformed or inconsistent successful responses fail closed."""
    checker = _module("check_pypi_version")

    def unexpected(_hostname: str, _target: str, _headers: object) -> tuple[int, object]:
        """Return the parametrized unexpected successful payload."""
        return 200, payload

    monkeypatch.setattr(checker, "_pypi_https_json", unexpected)
    with pytest.raises(RuntimeError, match="malformed JSON|exact resource"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3")


def test_github_transport_connects_to_the_literal_api_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The low-level transport fixes GitHub's origin independently of its target."""
    checker = _module("check_github_release_state")
    observed: list[object] = []

    class Response:
        """Provide one successful GitHub JSON response."""

        status = 200

        @staticmethod
        def getheaders() -> list[tuple[str, str]]:
            """Return one response header for normalization."""
            return [("X-RateLimit-Remaining", "42")]

        @staticmethod
        def read() -> bytes:
            """Return the exact simulated response body."""
            return b'{"object":{"sha":"abc"}}'

    class Connection:
        """Record the literal connection and origin-form request target."""

        def __init__(self, host: str, *, timeout: int) -> None:
            """Capture the selected TLS host and timeout."""
            observed.append(("connect", host, timeout))

        def request(self, method: str, target: str, *, headers: dict[str, str]) -> None:
            """Capture the method, target, and request headers."""
            observed.append(("request", method, target, headers))

        @staticmethod
        def getresponse() -> Response:
            """Return the simulated successful response."""
            return Response()

        def close(self) -> None:
            """Record deterministic connection cleanup."""
            observed.append(("close",))

    monkeypatch.setattr(checker, "HTTPSConnection", Connection)
    status, headers, payload = checker._github_https_json(
        "/repos/example/project/git/ref/heads/main",
        {"Accept": "application/json"},
    )

    assert (status, headers, payload) == (
        200,
        {"x-ratelimit-remaining": "42"},
        {"object": {"sha": "abc"}},
    )
    assert observed == [
        ("connect", "api.github.com", 20),
        (
            "request",
            "GET",
            "/repos/example/project/git/ref/heads/main",
            {"Accept": "application/json"},
        ),
        ("close",),
    ]


def test_main_sha_uses_the_retrying_api_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Release identity is checked against the authenticated remote main ref."""
    checker = _module("check_github_release_state")
    expected = "a" * 40
    captured: list[str] = []

    def reference(url: str, _token: str) -> dict[str, object]:
        """Return and capture the simulated main reference."""
        captured.append(url)
        return {"object": {"sha": expected}}

    monkeypatch.setattr(checker, "_github_json", reference)
    checker.validate_main_sha("example/project", expected, token="token")
    assert captured == [GITHUB_URL]


def test_main_sha_fails_closed_if_the_branch_moved(monkeypatch: pytest.MonkeyPatch) -> None:
    """A release cannot proceed from a stale dispatch commit."""
    checker = _module("check_github_release_state")
    monkeypatch.setattr(
        checker,
        "_github_json",
        lambda *_args, **_kwargs: {"object": {"sha": "b" * 40}},
    )
    with pytest.raises(RuntimeError, match="main moved"):
        checker.validate_main_sha("example/project", "a" * 40)


def test_github_lookup_retries_transport_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A temporary API transport failure consumes one bounded retry."""
    checker = _module("check_github_release_state")
    payload = {"object": {"sha": "a" * 40}}
    responses: list[object] = [OSError("reset"), payload]
    delays: list[float] = []

    def flaky(_target: str, _headers: object) -> tuple[int, dict[str, str], object]:
        """Raise the first response and return the second."""
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return 200, {}, response

    monkeypatch.setattr(checker, "_github_https_json", flaky)
    assert checker._github_json(GITHUB_URL, sleeper=delays.append) == payload
    assert delays == [1.0]


@pytest.mark.parametrize("status", (429, 500, 503))
def test_github_lookup_retries_transient_http_statuses(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    """Rate limits and server failures retry without weakening the result."""
    checker = _module("check_github_release_state")
    calls = 0
    delays: list[float] = []
    payload = {"object": {"sha": "a" * 40}}

    def transient(_target: str, _headers: object) -> tuple[int, dict[str, str], object]:
        """Return one transient status before a successful payload."""
        nonlocal calls
        calls += 1
        return (status, {}, None) if calls == 1 else (200, {}, payload)

    monkeypatch.setattr(checker, "_github_https_json", transient)
    assert checker._github_json(GITHUB_URL, sleeper=delays.append) == payload
    assert calls == 2
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
    """A 403 retries only with GitHub's documented rate-limit signal."""
    checker = _module("check_github_release_state")
    calls = 0
    delays: list[float] = []
    payload = {"object": {"sha": "a" * 40}}

    def rate_limited(_target: str, _headers: object) -> tuple[int, dict[str, str], object]:
        """Return one qualified 403 before the successful payload."""
        nonlocal calls
        calls += 1
        return (403, headers, None) if calls == 1 else (200, {}, payload)

    monkeypatch.setattr(checker, "_github_https_json", rate_limited)
    assert checker._github_json(GITHUB_URL, sleeper=delays.append, clock=lambda: now) == payload
    assert calls == 2
    assert delays == [expected_delay]


@pytest.mark.parametrize("headers", ({}, {"Retry-After": "invalid"}))
def test_github_lookup_rejects_unqualified_403_without_retry(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
) -> None:
    """Permission failures cannot be disguised as transient rate limits."""
    checker = _module("check_github_release_state")
    calls = 0
    delays: list[float] = []

    def forbidden(_target: str, _headers: object) -> tuple[int, dict[str, str], object]:
        """Count and return one unqualified permission failure."""
        nonlocal calls
        calls += 1
        return 403, headers, None

    monkeypatch.setattr(checker, "_github_https_json", forbidden)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        checker._github_json(GITHUB_URL, sleeper=delays.append)
    assert calls == 1
    assert delays == []


@pytest.mark.parametrize("payload", ([], "main", None))
def test_github_lookup_rejects_non_object_json(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
) -> None:
    """A structurally invalid successful response fails closed."""
    checker = _module("check_github_release_state")

    def invalid(_target: str, _headers: object) -> tuple[int, dict[str, str], object]:
        """Return the parametrized invalid payload."""
        return 200, {}, payload

    monkeypatch.setattr(checker, "_github_https_json", invalid)
    with pytest.raises(RuntimeError, match="non-object"):
        checker._github_json(GITHUB_URL)


def test_github_lookup_rejects_malformed_json_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed data is semantic corruption rather than transport failure."""
    checker = _module("check_github_release_state")
    calls = 0
    delays: list[float] = []

    def malformed(_target: str, _headers: object) -> tuple[int, dict[str, str], object]:
        """Raise one JSON parse failure and count transport calls."""
        nonlocal calls
        calls += 1
        raise json.JSONDecodeError("invalid payload", "{", 1)

    monkeypatch.setattr(checker, "_github_https_json", malformed)
    with pytest.raises(RuntimeError, match="verify GitHub release state"):
        checker._github_json(GITHUB_URL, sleeper=delays.append)
    assert calls == 1
    assert delays == []


@pytest.mark.parametrize("repository", ("", "owner", "../owner/repository", "owner/../repo"))
def test_main_sha_rejects_invalid_repository_identifiers(repository: str) -> None:
    """Repository input cannot alter the intended GitHub API path."""
    with pytest.raises(RuntimeError, match="invalid GitHub repository identifier"):
        _module("check_github_release_state").validate_main_sha(repository, "a" * 40)


@pytest.mark.parametrize(
    "api_url",
    (
        "http://api.github.com",
        "file:///tmp/github",
        "https://example.invalid",
        "https://user@api.github.com",
        "https://api.github.com:444",
        "https://api.github.com/base",
        "https://api.github.com?redirect=other",
        "https://api.github.com#fragment",
    ),
)
def test_main_sha_rejects_untrusted_api_urls(api_url: str) -> None:
    """Release preflight accepts only GitHub's credential-free API root."""
    with pytest.raises(RuntimeError, match="non-GitHub HTTPS"):
        _module("check_github_release_state").validate_main_sha(
            "example/project",
            "a" * 40,
            api_url=api_url,
        )
