"""Release-version, PyPI, and GitHub preflight contracts.

The tests cover workflow identity, literal-host HTTPS transports, strict endpoint
validation, bounded retries, fail-closed response handling, and release state.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

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


def _recovery_fixture(tmp_path: Path) -> tuple[Path, Path, dict[str, str]]:
    """Create canonical manifest evidence and five matching package files."""
    packages = tmp_path / "packages"
    packages.mkdir()
    digests: dict[str, str] = {}
    entries: list[dict[str, object]] = []
    filenames = {
        "schema_sanitizer-0.4.2.tar.gz",
        *(f"schema_sanitizer-0.4.2-artifact-{ordinal}.whl" for ordinal in range(4)),
    }
    for ordinal, filename in enumerate(sorted(filenames)):
        payload = f"artifact {ordinal}\n".encode()
        (packages / filename).write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        digests[filename] = digest
        entries.append({"filename": filename, "sha256": digest, "size": len(payload)})
    manifest = tmp_path / "release-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "artifacts": entries,
                "format": "schema-sanitizer-release-manifest-v1",
                "project": "schema-sanitizer",
                "provenance": {
                    "git_sha": "a" * 40,
                    "github_run_attempt": 1,
                    "github_run_id": 42,
                },
                "version": "0.4.2",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest, packages, digests


def _verified_attestation(checker: ModuleType, **overrides: object) -> object:
    """Build one authenticated attestation-policy record with safe defaults."""
    values: dict[str, object] = {
        "github_publisher": True,
        "repository": "bgallan/schema-sanitizer",
        "workflow": "publish.yml",
        "environment": "pypi",
        "predicate_type": "https://docs.pypi.org/attestations/publish/v1",
        "predicate": {},
        "claims": {
            "1.3.6.1.4.1.57264.1.13": "a" * 40,
            "1.3.6.1.4.1.57264.1.14": "refs/heads/main",
        },
    }
    values.update(overrides)
    return checker._VerifiedAttestation(**values)


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


def test_release_inputs_reject_symlink_aliases(tmp_path: Path) -> None:
    """Version and workflow-event trust inputs must be regular files."""
    validator = _module("validate_release_version")
    version = tmp_path / "VERSION"
    version.write_text("0.4.2\n", encoding="utf-8")
    linked = tmp_path / "VERSION.link"
    try:
        linked.symlink_to(version)
    except OSError:
        pytest.skip("symlinks are unavailable")

    with pytest.raises(ValueError, match="regular file"):
        validator.validate_release_version(linked)


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
        def getheaders() -> list[tuple[str, str]]:
            """Return one retry header for normalization coverage."""
            return [("Retry-After", "2")]

        @staticmethod
        def read(_amount: int) -> bytes:
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
    status, headers, payload = checker._pypi_https_json(
        hostname,
        "/pypi/schema-sanitizer/0.4.2/json",
        {"Accept": "application/json"},
    )

    assert status == 200
    assert headers == {"retry-after": "2"}
    assert payload == {"info": {"version": "0.4.2"}}
    assert observed == [
        ("connect", hostname, 10),
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


def test_pypi_transport_bounds_response_bytes_and_still_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An oversized CDN response fails closed while releasing its connection."""
    checker = _module("check_pypi_version")
    closed: list[bool] = []

    class Response:
        """Provide a response one byte beyond the fixed body limit."""

        status = 200

        @staticmethod
        def getheaders() -> list[tuple[str, str]]:
            """Return a bounded empty header set."""
            return []

        @staticmethod
        def read(amount: int) -> bytes:
            """Return exactly the bounded read size requested by the helper."""
            return b"x" * amount

    class Connection:
        """Expose the oversized response and record deterministic cleanup."""

        def __init__(self, _host: str, *, timeout: int) -> None:
            """Require the fixed connection timeout."""
            assert timeout == 10

        @staticmethod
        def request(_method: str, _target: str, *, headers: object) -> None:
            """Accept the fixed request without side effects."""
            assert headers == {}

        @staticmethod
        def getresponse() -> Response:
            """Return the oversized response fixture."""
            return Response()

        @staticmethod
        def close() -> None:
            """Record cleanup after the bounded read."""
            closed.append(True)

    monkeypatch.setattr(checker, "HTTPSConnection", Connection)

    with pytest.raises(RuntimeError, match="response exceeded the byte limit"):
        checker._pypi_https_json("pypi.org", "/pypi/project/1/json", {})

    assert closed == [True]


def test_pypi_lookup_distinguishes_existing_and_available_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exact JSON resource blocks release while a 404 proves availability."""
    checker = _module("check_pypi_version")
    captured: list[str] = []
    request_headers: list[object] = []

    def existing(hostname: str, target: str, headers: object) -> tuple[int, dict[str, str], object]:
        """Return a matching release and capture its fixed-origin resource."""
        captured.append(f"https://{hostname}{target}")
        request_headers.append(headers)
        return 200, {}, {"info": {"version": "0.4.2"}}

    monkeypatch.setattr(checker, "_pypi_https_json", existing)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.2") is True
    assert captured == ["https://pypi.org/pypi/schema-sanitizer/0.4.2/json"]
    assert request_headers == [
        {
            "Accept": "application/json",
            "Cache-Control": "no-cache, max-age=0",
            "Pragma": "no-cache",
            "User-Agent": "schema-sanitizer-release-preflight",
        }
    ]

    def missing(
        _hostname: str, _target: str, _headers: object
    ) -> tuple[int, dict[str, str], object]:
        """Return the only response that authorizes a new version."""
        return 404, {}, None

    monkeypatch.setattr(checker, "_pypi_https_json", missing)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3") is False


def test_pypi_lookup_fails_closed_on_service_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """An inconclusive index response cannot authorize publication."""
    checker = _module("check_pypi_version")
    calls = 0

    def unavailable(
        _hostname: str, _target: str, _headers: object
    ) -> tuple[int, dict[str, str], object]:
        """Count and return one transient service failure."""
        nonlocal calls
        calls += 1
        return 503, {}, None

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

    def flaky(_hostname: str, _target: str, _headers: object) -> tuple[int, dict[str, str], object]:
        """Raise the first response and return the second."""
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return 200, {}, response

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

    def forbidden(
        _hostname: str, _target: str, _headers: object
    ) -> tuple[int, dict[str, str], object]:
        """Count and return one non-retryable client failure."""
        nonlocal calls
        calls += 1
        return 403, {}, None

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

    def response(
        _hostname: str, _target: str, _headers: object
    ) -> tuple[int, dict[str, str], object]:
        """Return successive retryable and authoritative statuses."""
        return next(statuses), {}, None

    monkeypatch.setattr(checker, "_pypi_https_json", response)
    assert checker.pypi_release_exists("schema-sanitizer", "0.4.3", sleeper=delays.append) is False
    assert delays == [1.0]


def test_pypi_lookup_bounds_server_directed_retry_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry-After may guide a retry but cannot impose an unbounded wait."""
    checker = _module("check_pypi_version")
    responses = iter(((429, {"Retry-After": "300"}, None), (404, {}, None)))
    delays: list[float] = []

    def response(
        _hostname: str, _target: str, _headers: object
    ) -> tuple[int, dict[str, str], object]:
        """Return one server-directed retry followed by exact absence."""
        return next(responses)

    monkeypatch.setattr(checker, "_pypi_https_json", response)
    assert (
        checker.pypi_release_exists(
            "schema-sanitizer",
            "0.4.3",
            sleeper=delays.append,
            clock=lambda: 0.0,
        )
        is False
    )
    assert delays == [30.0]


def test_integrity_lookup_retries_temporary_disablement_with_exact_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Integrity API retries temporary unavailability and requests fresh JSON."""
    checker = _module("check_pypi_version")
    responses: list[tuple[int, dict[str, str], object]] = [
        (403, {}, None),
        (200, {"Cache-Control": "max-age=600"}, {"version": 1}),
    ]
    observed: list[object] = []

    def response(hostname: str, target: str, headers: object) -> tuple[int, dict[str, str], object]:
        """Capture the fixed endpoint and return one transient then success."""
        observed.append((hostname, target, headers))
        return responses.pop(0)

    monkeypatch.setattr(checker, "_pypi_https_json", response)
    delays: list[float] = []
    payload = checker._pypi_provenance_payload(
        "schema-sanitizer",
        "0.4.2",
        "schema_sanitizer-0.4.2.tar.gz",
        sleeper=delays.append,
    )

    assert payload == {"version": 1}
    assert delays == [1.0]
    assert observed == [
        (
            "pypi.org",
            "/integrity/schema-sanitizer/0.4.2/schema_sanitizer-0.4.2.tar.gz/provenance",
            {
                "Accept": "application/vnd.pypi.integrity.v1+json",
                "Cache-Control": "no-cache, max-age=0",
                "Pragma": "no-cache",
                "User-Agent": "schema-sanitizer-release-verifier",
            },
        ),
        (
            "pypi.org",
            "/integrity/schema-sanitizer/0.4.2/schema_sanitizer-0.4.2.tar.gz/provenance",
            {
                "Accept": "application/vnd.pypi.integrity.v1+json",
                "Cache-Control": "no-cache, max-age=0",
                "Pragma": "no-cache",
                "User-Agent": "schema-sanitizer-release-verifier",
            },
        ),
    ]


def test_pypi_freshness_metadata_rejects_ambiguous_cache_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed freshness metadata cannot make a cached state look authoritative."""
    checker = _module("check_pypi_version")
    monkeypatch.setattr(
        checker,
        "_pypi_https_json",
        lambda *_args, **_kwargs: (
            200,
            {"Cache-Control": "max-age=900", "Age": "unknown"},
            {"info": {"version": "0.4.2"}},
        ),
    )

    with pytest.raises(RuntimeError, match="malformed Age"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.2")


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

    def unexpected(
        _hostname: str, _target: str, _headers: object
    ) -> tuple[int, dict[str, str], object]:
        """Return the parametrized unexpected successful payload."""
        return 200, {}, payload

    monkeypatch.setattr(checker, "_pypi_https_json", unexpected)
    with pytest.raises(RuntimeError, match="malformed JSON|exact resource"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3")


def test_pypi_lookup_rejects_mismatched_project_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical project metadata must agree with the exact requested resource."""
    checker = _module("check_pypi_version")

    def mismatched(
        _hostname: str, _target: str, _headers: object
    ) -> tuple[int, dict[str, str], object]:
        """Return a valid version under the wrong project identity."""
        return 200, {}, {"info": {"name": "other-project", "version": "0.4.3"}}

    monkeypatch.setattr(checker, "_pypi_https_json", mismatched)
    with pytest.raises(RuntimeError, match="returned project"):
        checker.pypi_release_exists("schema-sanitizer", "0.4.3")


def test_pypi_preflight_can_defer_existing_files_to_manifest_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The explicit recovery preflight mode inspects but does not reject existence."""
    checker = _module("check_pypi_version")
    version = tmp_path / "VERSION"
    version.write_text("0.4.3\n", encoding="utf-8")
    monkeypatch.setattr(checker, "pypi_release_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(RELEASE / "check_pypi_version.py"),
            "--version-file",
            str(version),
            "--allow-existing-for-recovery",
        ],
    )

    checker.main()

    assert "requires manifest reconciliation" in capsys.readouterr().out


def test_pypi_manifest_cli_requires_the_current_workflow_run_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Manifest-backed CLI modes fail before I/O when run provenance is omitted."""
    checker = _module("check_pypi_version")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(RELEASE / "check_pypi_version.py"),
            "--manifest",
            str(tmp_path / "release-manifest.json"),
            "--packages-dir",
            str(tmp_path / "packages"),
            "--publish-dir",
            str(tmp_path / "publish"),
            "--state-output",
            str(tmp_path / "state.json"),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        checker.main()

    assert exc_info.value.code == 2
    assert "needs github-run-id and github-run-attempt" in capsys.readouterr().err


def test_release_manifest_accepts_an_earlier_attempt_from_the_same_run(tmp_path: Path) -> None:
    """A selective rerun may consume immutable evidence from an earlier run attempt."""
    checker = _module("check_pypi_version")
    manifest, packages, _digests = _recovery_fixture(tmp_path)

    artifacts, git_sha = checker._local_manifest_artifacts(
        manifest,
        packages,
        project="schema-sanitizer",
        version="0.4.2",
        expected_github_run_id=42,
        current_github_run_attempt=2,
    )

    assert len(artifacts) == 5
    assert git_sha == "a" * 40


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("github_run_id", 43, "current GitHub workflow run"),
        ("github_run_attempt", 2, "future GitHub workflow run attempt"),
        ("github_run_id", True, "malformed provenance"),
        ("github_run_attempt", True, "malformed provenance"),
    ),
)
def test_release_manifest_rejects_wrong_future_or_boolean_provenance(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    """Manifest evidence must originate in this run and no later than this attempt."""
    checker = _module("check_pypi_version")
    manifest, packages, _digests = _recovery_fixture(tmp_path)
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["provenance"][field] = value
    manifest.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match=message):
        checker._local_manifest_artifacts(
            manifest,
            packages,
            project="schema-sanitizer",
            version="0.4.2",
            expected_github_run_id=42,
            current_github_run_attempt=1,
        )


@pytest.mark.parametrize(
    ("run_id", "run_attempt"),
    ((True, 1), (42, False), (42.0, 1), (42, 1.0), (0, 1), (42, 0)),
)
def test_release_manifest_rejects_invalid_current_run_identity(
    tmp_path: Path,
    run_id: object,
    run_attempt: object,
) -> None:
    """Programmatic callers cannot supply boolean, coercible, or nonpositive identity."""
    checker = _module("check_pypi_version")
    manifest, packages, _digests = _recovery_fixture(tmp_path)

    with pytest.raises(RuntimeError, match="must be positive integers"):
        checker._local_manifest_artifacts(
            manifest,
            packages,
            project="schema-sanitizer",
            version="0.4.2",
            expected_github_run_id=run_id,
            current_github_run_attempt=run_attempt,
        )


def test_pypi_recovery_stages_only_manifest_matched_missing_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial matching upload yields an exact idempotent missing-file set."""
    checker = _module("check_pypi_version")
    manifest, packages, digests = _recovery_fixture(tmp_path)
    published_names = sorted(digests)[:2]
    payload = {
        "info": {"version": "0.4.2"},
        "urls": [
            {"filename": name, "digests": {"sha256": digests[name]}, "yanked": False}
            for name in reversed(published_names)
        ],
    }
    monkeypatch.setattr(checker, "_pypi_release_payload", lambda *_args, **_kwargs: payload)
    publish_dir = tmp_path / "publish"
    state_output = tmp_path / "recovery.json"

    state = checker.prepare_pypi_publish_recovery(
        manifest,
        packages,
        publish_dir,
        state_output,
        project="schema-sanitizer",
        version="0.4.2",
        github_run_id=42,
        github_run_attempt=1,
    )

    missing_names = sorted(set(digests) - set(published_names))
    assert state["status"] == "publish-required"
    assert [entry["filename"] for entry in state["published"]] == published_names
    assert [entry["filename"] for entry in state["missing"]] == missing_names
    assert sorted(path.name for path in publish_dir.iterdir()) == missing_names
    assert all(
        (publish_dir / name).read_bytes() == (packages / name).read_bytes()
        for name in missing_names
    )
    github_output = tmp_path / "github-output.txt"
    checker.write_github_recovery_outputs(github_output, state)
    assert github_output.read_text(encoding="utf-8") == (
        "missing-count=3\npublished-count=2\npublish-required=true\nstatus=publish-required\n"
    )
    state_mtime = state_output.stat().st_mtime_ns
    package_mtimes = {path.name: path.stat().st_mtime_ns for path in publish_dir.iterdir()}

    repeated = checker.prepare_pypi_publish_recovery(
        manifest,
        packages,
        publish_dir,
        state_output,
        project="schema-sanitizer",
        version="0.4.2",
        github_run_id=42,
        github_run_attempt=1,
    )
    assert repeated == state
    assert state_output.stat().st_mtime_ns == state_mtime
    assert {path.name: path.stat().st_mtime_ns for path in publish_dir.iterdir()} == package_mtimes


def test_pypi_recovery_keeps_state_outside_validated_package_evidence(
    tmp_path: Path,
) -> None:
    """Recovery metadata cannot mutate the exact source package directory."""
    checker = _module("check_pypi_version")
    manifest, packages, _digests = _recovery_fixture(tmp_path)

    with pytest.raises(RuntimeError, match="outside the source packages"):
        checker.prepare_pypi_publish_recovery(
            manifest,
            packages,
            tmp_path / "publish",
            packages / "recovery.json",
            project="schema-sanitizer",
            version="0.4.2",
            github_run_id=42,
            github_run_attempt=1,
        )


def test_pypi_recovery_rejects_remote_drift_before_replacing_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown or hash-mismatched immutable files stop recovery without staging."""
    checker = _module("check_pypi_version")
    manifest, packages, digests = _recovery_fixture(tmp_path)
    filename = sorted(digests)[0]
    payload = {
        "info": {"version": "0.4.2"},
        "urls": [
            {"filename": filename, "digests": {"sha256": "0" * 64}, "yanked": False},
            {
                "filename": "unknown.whl",
                "digests": {"sha256": "1" * 64},
                "yanked": False,
            },
        ],
    }
    monkeypatch.setattr(checker, "_pypi_release_payload", lambda *_args, **_kwargs: payload)
    publish_dir = tmp_path / "publish"
    publish_dir.mkdir()
    sentinel = publish_dir / "preserve"
    sentinel.write_text("old state\n", encoding="utf-8")
    state_output = tmp_path / "recovery.json"

    with pytest.raises(RuntimeError, match="differs from the local manifest"):
        checker.prepare_pypi_publish_recovery(
            manifest,
            packages,
            publish_dir,
            state_output,
            project="schema-sanitizer",
            version="0.4.2",
            github_run_id=42,
            github_run_attempt=1,
        )

    assert sentinel.read_text(encoding="utf-8") == "old state\n"
    assert not state_output.exists()


def test_pypi_recovery_marks_a_complete_matching_release_without_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fully matching immutable release produces an empty no-publish directory."""
    checker = _module("check_pypi_version")
    manifest, packages, digests = _recovery_fixture(tmp_path)
    payload = {
        "info": {"version": "0.4.2"},
        "urls": [
            {"filename": name, "digests": {"sha256": digest}, "yanked": False}
            for name, digest in reversed(sorted(digests.items()))
        ],
    }
    monkeypatch.setattr(checker, "_pypi_release_payload", lambda *_args, **_kwargs: payload)
    publish_dir = tmp_path / "publish"

    state = checker.prepare_pypi_publish_recovery(
        manifest,
        packages,
        publish_dir,
        tmp_path / "recovery.json",
        project="schema-sanitizer",
        version="0.4.2",
        github_run_id=42,
        github_run_attempt=1,
    )

    assert state["status"] == "already-complete"
    assert state["missing"] == []
    assert list(publish_dir.iterdir()) == []


@pytest.mark.parametrize("yanked", (True, None, "false"))
def test_pypi_recovery_rejects_yanked_or_malformed_yank_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    yanked: object,
) -> None:
    """Recovery never treats yanked or ambiguously described files as complete."""
    checker = _module("check_pypi_version")
    manifest, packages, digests = _recovery_fixture(tmp_path)
    filename, digest = sorted(digests.items())[0]
    payload = {
        "info": {"version": "0.4.2"},
        "urls": [{"filename": filename, "digests": {"sha256": digest}, "yanked": yanked}],
    }
    monkeypatch.setattr(checker, "_pypi_release_payload", lambda *_args, **_kwargs: payload)

    message = "yanked file" if yanked is True else "malformed file entry"
    with pytest.raises(RuntimeError, match=message):
        checker.prepare_pypi_publish_recovery(
            manifest,
            packages,
            tmp_path / "publish",
            tmp_path / "recovery.json",
            project="schema-sanitizer",
            version="0.4.2",
            github_run_id=42,
            github_run_attempt=1,
        )


def test_pypi_publish_attestation_policy_accepts_one_exact_authenticated_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The policy consumes the cryptographic boundary and accepts the exact identity."""
    checker = _module("check_pypi_version")
    package = tmp_path / "schema_sanitizer-0.4.2.tar.gz"
    package.write_bytes(b"distribution\n")
    observed: list[object] = []

    def authenticate(payload: object, candidate: Path) -> tuple[object, ...]:
        """Record the cryptographic-boundary inputs and return one valid record."""
        observed.extend((payload, candidate))
        return (_verified_attestation(checker),)

    monkeypatch.setattr(checker, "_cryptographically_verified_attestations", authenticate)
    payload: dict[str, object] = {"attestation_bundles": []}

    checker._verify_publish_provenance(payload, package, expected_git_sha="a" * 40)

    assert observed == [payload, package]


def test_pypi_attestation_boundary_invokes_crypto_verification_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pinned API verifies file bytes and identity rather than inspecting JSON."""
    checker = _module("check_pypi_version")
    package = tmp_path / "schema_sanitizer-0.4.2.tar.gz"
    package.write_bytes(b"distribution\n")
    calls: list[tuple[object, ...]] = []

    class FakeProvenance:
        """Expose the already-validated test provenance object."""

        @classmethod
        def model_validate(cls, payload: object) -> object:
            """Record schema validation and return the fixture payload."""
            calls.append(("model-validate", payload))
            return payload

    class FakeDistribution:
        """Represent construction of a distribution from exact local bytes."""

        @classmethod
        def from_file(cls, path: Path) -> object:
            """Record the local file used as the signature subject."""
            calls.append(("distribution", path))
            return SimpleNamespace(name=path.name)

    class FakePublisher:
        """Represent the exact GitHub Trusted Publisher policy."""

        def __init__(self, *, repository: str, workflow: str, environment: object) -> None:
            """Store the publisher fields passed to cryptographic verification."""
            self.repository = repository
            self.workflow = workflow
            self.environment = environment

    class FakeAttestationError(ValueError):
        """Represent a cryptographic verification failure type."""

    class FakeAttestation:
        """Record invocation of the cryptographic verification API."""

        certificate_claims = {
            "1.3.6.1.4.1.57264.1.13": "a" * 40,
            "1.3.6.1.4.1.57264.1.14": "refs/heads/main",
        }

        @staticmethod
        def verify(publisher: object, distribution: object, *, offline: bool) -> tuple[str, object]:
            """Return a verified publish predicate after recording strict inputs."""
            calls.append(("verify", publisher, distribution, offline))
            return "https://docs.pypi.org/attestations/publish/v1", {}

    publisher = FakePublisher(
        repository="bgallan/schema-sanitizer",
        workflow="publish.yml",
        environment="pypi",
    )
    provenance = SimpleNamespace(
        attestation_bundles=[SimpleNamespace(publisher=publisher, attestations=[FakeAttestation()])]
    )
    api = SimpleNamespace(
        Provenance=FakeProvenance,
        Distribution=FakeDistribution,
        GitHubPublisher=FakePublisher,
        AttestationType=SimpleNamespace(
            PYPI_PUBLISH_V1=SimpleNamespace(value="https://docs.pypi.org/attestations/publish/v1")
        ),
        AttestationError=FakeAttestationError,
    )
    monkeypatch.setattr(checker, "_attestation_module", lambda: api)

    records = checker._cryptographically_verified_attestations(provenance, package)

    assert len(records) == 1
    assert calls[:2] == [("model-validate", provenance), ("distribution", package)]
    assert calls[2][0] == "verify"
    assert calls[2][1].environment == "pypi"
    assert calls[2][3] is True


@pytest.mark.parametrize(
    ("records", "message"),
    (
        ((), "no authenticated attestations"),
        (({"predicate_type": "https://slsa.dev/provenance/v1"},), "unexpected predicate"),
        (({"repository": "other/project"},), "unexpected GitHub publisher identity"),
        (({"environment": None},), "unexpected GitHub publisher identity"),
        (
            (
                {
                    "claims": {
                        "1.3.6.1.4.1.57264.1.13": "b" * 40,
                        "1.3.6.1.4.1.57264.1.14": "refs/heads/main",
                    }
                },
            ),
            "source repository digest",
        ),
        (
            (
                {
                    "claims": {
                        "1.3.6.1.4.1.57264.1.13": "a" * 40,
                        "1.3.6.1.4.1.57264.1.14": "refs/tags/v0.4.2",
                    }
                },
            ),
            "source repository ref",
        ),
    ),
    ids=("missing", "predicate", "publisher", "environment", "source-sha", "source-ref"),
)
def test_pypi_publish_attestation_policy_rejects_missing_or_wrong_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    records: tuple[dict[str, object], ...],
    message: str,
) -> None:
    """Missing, wrong-predicate, or wrong-source attestations fail immediately."""
    checker = _module("check_pypi_version")
    package = tmp_path / "schema_sanitizer-0.4.2.tar.gz"
    package.write_bytes(b"distribution\n")
    authenticated = tuple(_verified_attestation(checker, **record) for record in records)
    monkeypatch.setattr(
        checker,
        "_cryptographically_verified_attestations",
        lambda *_args, **_kwargs: authenticated,
    )

    with pytest.raises(RuntimeError, match=message):
        checker._verify_publish_provenance({}, package, expected_git_sha="a" * 40)


def test_pypi_completion_verification_retries_visibility_then_matches_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-publish certification tolerates bounded cache lag without weakening digests."""
    checker = _module("check_pypi_version")
    manifest, packages, digests = _recovery_fixture(tmp_path)
    names = sorted(digests)

    def payload(visible: list[str]) -> dict[str, object]:
        """Build one exact simulated PyPI visibility snapshot."""
        return {
            "info": {"version": "0.4.2"},
            "urls": [
                {
                    "filename": name,
                    "digests": {"sha256": digests[name]},
                    "yanked": False,
                }
                for name in visible
            ],
        }

    responses = iter((payload(names[:2]), payload(names[:2]), payload(names)))
    monkeypatch.setattr(
        checker,
        "_pypi_release_payload",
        lambda *_args, **_kwargs: next(responses),
    )
    monkeypatch.setattr(checker, "_pypi_provenance_payload", lambda *_args, **_kwargs: {})
    verified: list[str] = []
    monkeypatch.setattr(
        checker,
        "_verify_publish_provenance",
        lambda _payload, package, **_kwargs: verified.append(package.name),
    )
    delays: list[float] = []

    checker.verify_pypi_release_complete(
        manifest,
        packages,
        project="schema-sanitizer",
        version="0.4.2",
        github_run_id=42,
        github_run_attempt=1,
        sleeper=delays.append,
    )

    assert delays == [300.0, 300.0]
    assert verified == names


def test_pypi_completion_verification_retries_missing_integrity_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A temporarily absent provenance object receives the same fixed cache budget."""
    checker = _module("check_pypi_version")
    manifest, packages, digests = _recovery_fixture(tmp_path)
    payload = {
        "info": {"version": "0.4.2"},
        "urls": [
            {"filename": name, "digests": {"sha256": digest}, "yanked": False}
            for name, digest in sorted(digests.items())
        ],
    }
    monkeypatch.setattr(checker, "_pypi_release_payload", lambda *_args, **_kwargs: payload)
    integrity_calls = 0

    def provenance(*_args: object, **_kwargs: object) -> dict[str, object] | None:
        """Hide the first provenance object and expose all subsequent reads."""
        nonlocal integrity_calls
        integrity_calls += 1
        return None if integrity_calls == 1 else {}

    monkeypatch.setattr(checker, "_pypi_provenance_payload", provenance)
    monkeypatch.setattr(checker, "_verify_publish_provenance", lambda *_args, **_kwargs: None)
    delays: list[float] = []

    checker.verify_pypi_release_complete(
        manifest,
        packages,
        project="schema-sanitizer",
        version="0.4.2",
        github_run_id=42,
        github_run_attempt=1,
        sleeper=delays.append,
    )

    assert delays == [300.0]
    assert integrity_calls == 10


def test_pypi_completion_verification_retries_a_transient_snapshot_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bounded transient read consumes one fixed visibility interval."""
    checker = _module("check_pypi_version")
    manifest, packages, digests = _recovery_fixture(tmp_path)
    complete = {
        "info": {"version": "0.4.2"},
        "urls": [
            {"filename": name, "digests": {"sha256": digest}, "yanked": False}
            for name, digest in sorted(digests.items())
        ],
    }
    responses: list[object] = [checker._PyPITransientError("temporary outage"), complete]

    def release(*_args: object, **_kwargs: object) -> dict[str, object]:
        """Raise one transient error before returning the complete snapshot."""
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        assert isinstance(response, dict)
        return response

    monkeypatch.setattr(checker, "_pypi_release_payload", release)
    monkeypatch.setattr(checker, "_pypi_provenance_payload", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(checker, "_verify_publish_provenance", lambda *_args, **_kwargs: None)
    delays: list[float] = []

    checker.verify_pypi_release_complete(
        manifest,
        packages,
        project="schema-sanitizer",
        version="0.4.2",
        github_run_id=42,
        github_run_attempt=1,
        sleeper=delays.append,
    )

    assert delays == [300.0]
    assert responses == []


def test_pypi_completion_verification_does_not_retry_semantic_provenance_conflicts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An authenticated identity conflict fails immediately without a lucky retry."""
    checker = _module("check_pypi_version")
    manifest, packages, digests = _recovery_fixture(tmp_path)
    complete = {
        "info": {"version": "0.4.2"},
        "urls": [
            {"filename": name, "digests": {"sha256": digest}, "yanked": False}
            for name, digest in sorted(digests.items())
        ],
    }
    monkeypatch.setattr(checker, "_pypi_release_payload", lambda *_args, **_kwargs: complete)
    monkeypatch.setattr(checker, "_pypi_provenance_payload", lambda *_args, **_kwargs: {})

    def conflict(*_args: object, **_kwargs: object) -> None:
        """Raise one deterministic authenticated-identity conflict."""
        raise RuntimeError("wrong publisher")

    monkeypatch.setattr(checker, "_verify_publish_provenance", conflict)
    delays: list[float] = []

    with pytest.raises(RuntimeError, match="wrong publisher"):
        checker.verify_pypi_release_complete(
            manifest,
            packages,
            project="schema-sanitizer",
            version="0.4.2",
            github_run_id=42,
            github_run_attempt=1,
            sleeper=delays.append,
        )

    assert delays == []


def test_pypi_completion_verification_has_a_fixed_visibility_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persistently incomplete index fails after the exact bounded retry schedule."""
    checker = _module("check_pypi_version")
    manifest, packages, _digests = _recovery_fixture(tmp_path)
    monkeypatch.setattr(checker, "_pypi_release_payload", lambda *_args, **_kwargs: None)
    delays: list[float] = []

    with pytest.raises(RuntimeError, match="manifest-and-provenance postcondition"):
        checker.verify_pypi_release_complete(
            manifest,
            packages,
            project="schema-sanitizer",
            version="0.4.2",
            github_run_id=42,
            github_run_attempt=1,
            sleeper=delays.append,
        )

    assert delays == [300.0, 300.0, 300.0]


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
        def read(_amount: int) -> bytes:
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
