#!/usr/bin/env python3
"""Reconcile and certify immutable PyPI release state.

The helper performs bounded fixed-origin API reads, validates local manifest evidence,
stages only exact files missing from a partial upload, and post-verifies every published
digest and PEP 740 Trusted Publisher identity against the original release commit.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import shutil
import stat
import tempfile
import time
from collections.abc import Callable, Mapping
from email.utils import parsedate_to_datetime
from http.client import HTTPException, HTTPSConnection
from pathlib import Path
from types import ModuleType
from urllib.parse import quote, urlsplit

_RETRY_DELAYS_SECONDS = (1.0, 2.0)
# PyPI's release JSON is served with a documented 15-minute CDN max-age.  The
# verifier therefore observes the initial state plus three fixed revalidations
# spanning the full cache lifetime; a workflow timeout bounds the entire job.
_COMPLETE_VISIBILITY_DELAYS_SECONDS = (300.0, 300.0, 300.0)
_MAX_SERVER_RETRY_DELAY_SECONDS = 30.0
_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_RESPONSE_HEADERS = 64
_MAX_RESPONSE_HEADER_BYTES = 64 * 1024
_HTTP_TIMEOUT_SECONDS = 10
_TRANSIENT_HTTP_STATUSES = {429, *range(500, 600)}
_INTEGRITY_TRANSIENT_HTTP_STATUSES = {403, *_TRANSIENT_HTTP_STATUSES}
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_EXPECTED_GITHUB_REPOSITORY = "bgallan/schema-sanitizer"
_EXPECTED_GITHUB_WORKFLOW = "publish.yml"
_EXPECTED_PUBLISH_ENVIRONMENT = "pypi"
_EXPECTED_GITHUB_REF = "refs/heads/main"
_SOURCE_REPOSITORY_DIGEST_OID = "1.3.6.1.4.1.57264.1.13"
_SOURCE_REPOSITORY_REF_OID = "1.3.6.1.4.1.57264.1.14"

if __package__:
    from .validate_release_version import validate_release_version
else:
    from validate_release_version import validate_release_version


class _PyPITransientError(RuntimeError):
    """Report a bounded transport or service failure that may be retried."""


class _VerifiedAttestation:
    """Describe authenticated attestation fields needed by the release policy."""

    __slots__ = (
        "claims",
        "environment",
        "github_publisher",
        "predicate",
        "predicate_type",
        "repository",
        "workflow",
    )

    def __init__(
        self,
        *,
        github_publisher: bool,
        repository: object,
        workflow: object,
        environment: object,
        predicate_type: object,
        predicate: object,
        claims: Mapping[str, str],
    ) -> None:
        """Store one already-authenticated record without changing its values."""
        self.github_publisher = github_publisher
        self.repository = repository
        self.workflow = workflow
        self.environment = environment
        self.predicate_type = predicate_type
        self.predicate = predicate
        self.claims = claims


def _pypi_https_json(
    hostname: str, target: str, headers: Mapping[str, str]
) -> tuple[int, dict[str, str], object]:
    """Issue one bounded request to an explicitly allowed PyPI HTTPS origin."""
    if hostname == "pypi.org":
        connection = HTTPSConnection("pypi.org", timeout=_HTTP_TIMEOUT_SECONDS)
    elif hostname == "test.pypi.org":
        connection = HTTPSConnection("test.pypi.org", timeout=_HTTP_TIMEOUT_SECONDS)
    else:
        raise RuntimeError(f"refusing non-PyPI HTTPS host: {hostname!r}")
    try:
        connection.request("GET", target, headers=dict(headers))
        response = connection.getresponse()
        status = response.status
        raw_headers = response.getheaders()
        if (
            len(raw_headers) > _MAX_RESPONSE_HEADERS
            or sum(len(name) + len(value) for name, value in raw_headers)
            > _MAX_RESPONSE_HEADER_BYTES
        ):
            raise RuntimeError("PyPI response headers exceeded the byte limit")
        response_headers = {name.lower(): value for name, value in raw_headers}
        body = response.read(_MAX_RESPONSE_BYTES + 1)
    finally:
        connection.close()
    if len(body) > _MAX_RESPONSE_BYTES:
        raise RuntimeError("PyPI response exceeded the byte limit")
    payload: object = json.loads(body) if status == 200 else None
    return status, response_headers, payload


def _validated_pypi_origin(index_url: str) -> str:
    """Return the literal host for the one accepted PyPI JSON API base URL."""
    try:
        parsed_index = urlsplit(index_url)
        port = parsed_index.port
    except ValueError as exc:
        raise RuntimeError(f"refusing non-PyPI HTTPS index URL: {index_url!r}") from exc
    if (
        parsed_index.scheme != "https"
        or parsed_index.hostname not in {"pypi.org", "test.pypi.org"}
        or parsed_index.username is not None
        or parsed_index.password is not None
        or port not in {None, 443}
        or parsed_index.path.rstrip("/") != "/pypi"
        or parsed_index.query
        or parsed_index.fragment
    ):
        raise RuntimeError(f"refusing non-PyPI HTTPS index URL: {index_url!r}")
    return parsed_index.hostname


def _cache_max_age(value: str) -> int | None:
    """Parse a single nonnegative max-age directive when one is present."""
    matches = re.findall(r"(?:^|,)\s*max-age\s*=\s*([0-9]+)\s*(?:,|$)", value, re.I)
    if not matches:
        return None
    if len(matches) != 1:
        raise RuntimeError("PyPI returned ambiguous Cache-Control max-age metadata")
    return int(matches[0])


def _log_freshness_metadata(resource: str, headers: Mapping[str, str]) -> None:
    """Validate and log bounded CDN freshness evidence without trusting it."""
    cache_control = headers.get("cache-control")
    max_age = _cache_max_age(cache_control) if cache_control is not None else None
    age = headers.get("age")
    if age is not None and re.fullmatch(r"[0-9]+", age) is None:
        raise RuntimeError("PyPI returned malformed Age freshness metadata")
    date = headers.get("date")
    if date is not None:
        try:
            parsed_date = parsedate_to_datetime(date)
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError("PyPI returned malformed Date freshness metadata") from exc
        if parsed_date.tzinfo is None:
            raise RuntimeError("PyPI returned timezone-free Date freshness metadata")
    serial = headers.get("x-pypi-last-serial")
    if serial is not None and re.fullmatch(r"[0-9]+", serial) is None:
        raise RuntimeError("PyPI returned malformed X-PyPI-Last-Serial metadata")
    details = {
        key: headers[key]
        for key in ("age", "date", "etag", "x-cache", "x-cache-hits", "x-pypi-last-serial")
        if key in headers
    }
    details["max-age"] = "unreported" if max_age is None else str(max_age)
    print(f"PyPI freshness metadata for {resource}: {json.dumps(details, sort_keys=True)}")


def _retry_after_delay(value: str, now: float) -> float | None:
    """Parse one Retry-After value into a nonnegative delay."""
    value = value.strip()
    if re.fullmatch(r"[0-9]+", value):
        return float(value)
    try:
        deadline = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if deadline.tzinfo is None:
        return None
    return max(0.0, deadline.timestamp() - now)


def _http_retry_delay(headers: Mapping[str, str], fallback: float, now: float) -> float:
    """Return a bounded server-directed delay or the deterministic fallback."""
    value = headers.get("retry-after")
    if isinstance(value, str):
        delay = _retry_after_delay(value, now)
        if delay is not None:
            return min(delay, _MAX_SERVER_RETRY_DELAY_SECONDS)
    return fallback


def _canonical_project_name(value: str) -> str:
    """Return the normalized project identity used by package indexes."""
    return re.sub(r"[-_.]+", "-", value).lower()


def _pypi_release_payload(
    project: str,
    version: str,
    *,
    index_url: str = "https://pypi.org/pypi",
    sleeper: Callable[[float], object] = time.sleep,
    clock: Callable[[], float] = time.time,
) -> dict[str, object] | None:
    """Return one exact PyPI release payload or ``None`` for authoritative absence."""
    hostname = _validated_pypi_origin(index_url)
    target = f"/pypi/{quote(project, safe='')}/{quote(version, safe='')}/json"
    headers = {
        "Accept": "application/json",
        "Cache-Control": "no-cache, max-age=0",
        "Pragma": "no-cache",
        "User-Agent": "schema-sanitizer-release-preflight",
    }
    attempts = len(_RETRY_DELAYS_SECONDS) + 1
    payload: object = None
    for attempt in range(attempts):
        try:
            status, response_headers, payload = _pypi_https_json(hostname, target, headers)
            response_headers = {name.lower(): value for name, value in response_headers.items()}
            _log_freshness_metadata("release JSON", response_headers)
            if status == 200:
                break
            if status == 404:
                return None
            if status not in _TRANSIENT_HTTP_STATUSES or attempt == attempts - 1:
                error = f"PyPI returned HTTP {status} for {project} {version}"
                if status in _TRANSIENT_HTTP_STATUSES:
                    raise _PyPITransientError(error)
                raise RuntimeError(error)
            retry_delay = _http_retry_delay(
                response_headers, _RETRY_DELAYS_SECONDS[attempt], clock()
            )
        except (OSError, HTTPException) as exc:
            if attempt == attempts - 1:
                raise _PyPITransientError(
                    f"could not verify {project} {version} on PyPI: {exc}"
                ) from exc
            retry_delay = _RETRY_DELAYS_SECONDS[attempt]
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"could not verify {project} {version} on PyPI: {exc}") from exc
        sleeper(retry_delay)
    if not isinstance(payload, dict) or not isinstance(payload.get("info"), dict):
        raise RuntimeError(f"PyPI returned malformed JSON for {project} {version}")
    info = payload["info"]
    reported_version = info.get("version")
    if reported_version != version:
        raise RuntimeError(
            f"PyPI returned version {reported_version!r} for exact resource {project} {version}"
        )
    reported_project = info.get("name")
    if reported_project is not None and (
        not isinstance(reported_project, str)
        or _canonical_project_name(reported_project) != _canonical_project_name(project)
    ):
        raise RuntimeError(
            f"PyPI returned project {reported_project!r} for exact resource {project} {version}"
        )
    return payload


def _pypi_provenance_payload(
    project: str,
    version: str,
    filename: str,
    *,
    index_url: str = "https://pypi.org/pypi",
    sleeper: Callable[[float], object] = time.sleep,
    clock: Callable[[], float] = time.time,
) -> dict[str, object] | None:
    """Return one exact Integrity API provenance object or ``None`` for absence."""
    hostname = _validated_pypi_origin(index_url)
    target = "/integrity/{}/{}/{}/provenance".format(
        quote(project, safe=""),
        quote(version, safe=""),
        quote(filename, safe=""),
    )
    headers = {
        "Accept": "application/vnd.pypi.integrity.v1+json",
        "Cache-Control": "no-cache, max-age=0",
        "Pragma": "no-cache",
        "User-Agent": "schema-sanitizer-release-verifier",
    }
    attempts = len(_RETRY_DELAYS_SECONDS) + 1
    payload: object = None
    for attempt in range(attempts):
        try:
            status, response_headers, payload = _pypi_https_json(hostname, target, headers)
            response_headers = {name.lower(): value for name, value in response_headers.items()}
            _log_freshness_metadata(f"Integrity API {filename}", response_headers)
            if status == 200:
                break
            if status == 404:
                return None
            if status not in _INTEGRITY_TRANSIENT_HTTP_STATUSES:
                raise RuntimeError(f"PyPI Integrity API returned HTTP {status} for {filename}")
            if attempt == attempts - 1:
                raise _PyPITransientError(
                    f"PyPI Integrity API returned HTTP {status} for {filename}"
                )
            retry_delay = _http_retry_delay(
                response_headers, _RETRY_DELAYS_SECONDS[attempt], clock()
            )
        except (OSError, HTTPException) as exc:
            if attempt == attempts - 1:
                raise _PyPITransientError(
                    f"could not retrieve PyPI provenance for {filename}: {exc}"
                ) from exc
            retry_delay = _RETRY_DELAYS_SECONDS[attempt]
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(
                f"PyPI Integrity API returned malformed JSON for {filename}: {exc}"
            ) from exc
        sleeper(retry_delay)
    if not isinstance(payload, dict):
        raise RuntimeError(f"PyPI Integrity API returned malformed JSON for {filename}")
    return payload


def _attestation_module() -> ModuleType:
    """Load the pinned cryptographic verifier only in the post-publish job."""
    try:
        module = importlib.import_module("pypi_attestations")
    except ImportError as exc:
        raise RuntimeError("pypi-attestations is required for release verification") from exc
    if getattr(module, "__version__", None) != "0.0.30":
        raise RuntimeError("release verification requires pypi-attestations==0.0.30")
    return module


def _cryptographically_verified_attestations(
    payload: object,
    package: Path,
) -> tuple[_VerifiedAttestation, ...]:
    """Authenticate provenance and return only cryptographically valid records."""
    api = _attestation_module()
    provenance_type = getattr(api, "Provenance")
    distribution_type = getattr(api, "Distribution")
    github_publisher_type = getattr(api, "GitHubPublisher")
    attestation_type = getattr(api, "AttestationType")
    attestation_error = getattr(api, "AttestationError")
    try:
        provenance = provenance_type.model_validate(payload)
        distribution = distribution_type.from_file(package)
        expected_publisher = github_publisher_type(
            repository=_EXPECTED_GITHUB_REPOSITORY,
            workflow=_EXPECTED_GITHUB_WORKFLOW,
            environment=_EXPECTED_PUBLISH_ENVIRONMENT,
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"malformed PyPI provenance for {package.name}: {exc}") from exc

    failures: list[str] = []
    verified: list[_VerifiedAttestation] = []
    for bundle in provenance.attestation_bundles:
        publisher = bundle.publisher
        for attestation in bundle.attestations:
            try:
                predicate_type, predicate = attestation.verify(
                    expected_publisher,
                    distribution,
                    offline=True,
                )
                claims = attestation.certificate_claims
            except attestation_error as exc:
                failures.append(f"cryptographic verification failed: {exc}")
                continue
            verified.append(
                _VerifiedAttestation(
                    github_publisher=isinstance(publisher, github_publisher_type),
                    repository=getattr(publisher, "repository", None),
                    workflow=getattr(publisher, "workflow", None),
                    environment=getattr(publisher, "environment", None),
                    predicate_type=predicate_type,
                    predicate=predicate,
                    claims=dict(claims),
                )
            )
    if not verified and failures:
        raise RuntimeError(
            f"no cryptographically valid attestation for {package.name}: {'; '.join(failures)}"
        )
    # Accessing the enum through the pinned API makes an incompatible upgrade
    # fail at the same boundary as the rest of the verifier contract.
    expected_predicate = attestation_type.PYPI_PUBLISH_V1.value
    if expected_predicate != "https://docs.pypi.org/attestations/publish/v1":
        raise RuntimeError("pypi-attestations exposes an unexpected publish predicate")
    return tuple(verified)


def _verify_publish_provenance(
    payload: object,
    package: Path,
    *,
    expected_git_sha: str,
) -> None:
    """Require one valid PyPI publish attestation for the exact file and source."""
    failures: list[str] = []
    for attestation in _cryptographically_verified_attestations(payload, package):
        if (
            not attestation.github_publisher
            or attestation.repository != _EXPECTED_GITHUB_REPOSITORY
            or attestation.workflow != _EXPECTED_GITHUB_WORKFLOW
            or attestation.environment != _EXPECTED_PUBLISH_ENVIRONMENT
        ):
            failures.append("unexpected GitHub publisher identity")
            continue
        if attestation.predicate_type != "https://docs.pypi.org/attestations/publish/v1":
            failures.append(f"unexpected predicate {attestation.predicate_type!r}")
            continue
        if attestation.predicate not in ({}, None):
            failures.append(f"nonempty publish predicate {attestation.predicate!r}")
            continue
        if attestation.claims.get(_SOURCE_REPOSITORY_DIGEST_OID) != expected_git_sha:
            failures.append("source repository digest does not match the release manifest")
            continue
        if attestation.claims.get(_SOURCE_REPOSITORY_REF_OID) != _EXPECTED_GITHUB_REF:
            failures.append("source repository ref is not refs/heads/main")
            continue
        return
    detail = "; ".join(failures) if failures else "no authenticated attestations"
    raise RuntimeError(f"no valid matching PyPI publish attestation for {package.name}: {detail}")


def pypi_release_exists(
    project: str,
    version: str,
    *,
    index_url: str = "https://pypi.org/pypi",
    sleeper: Callable[[float], object] = time.sleep,
    clock: Callable[[], float] = time.time,
) -> bool:
    """Return whether PyPI exposes an exact project/version JSON resource."""
    return (
        _pypi_release_payload(
            project,
            version,
            index_url=index_url,
            sleeper=sleeper,
            clock=clock,
        )
        is not None
    )


def _sha256(path: Path) -> str:
    """Return the SHA-256 digest of one stable regular package file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_distribution_filename(value: object) -> str | None:
    """Return one single-component distribution filename or ``None``."""
    if not isinstance(value, str) or not value or value in {".", ".."}:
        return None
    if "/" in value or "\\" in value or "\0" in value or Path(value).name != value:
        return None
    return value


def _local_manifest_artifacts(
    manifest_file: Path,
    packages_dir: Path,
    *,
    project: str,
    version: str,
) -> tuple[dict[str, tuple[Path, str]], str]:
    """Validate local release evidence and return files plus the source commit."""
    if manifest_file.is_symlink() or not manifest_file.is_file():
        raise RuntimeError(f"release manifest must be a regular file: {manifest_file}")
    serialized = manifest_file.read_text(encoding="utf-8")
    manifest = json.loads(serialized)
    if (
        not isinstance(manifest, dict)
        or serialized != json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ):
        raise RuntimeError(f"release manifest is not canonical JSON: {manifest_file}")
    if set(manifest) != {"artifacts", "format", "project", "provenance", "version"}:
        raise RuntimeError("release manifest contains an unexpected schema")
    if (
        manifest.get("format") != f"{project}-release-manifest-v1"
        or manifest.get("project") != project
        or manifest.get("version") != version
    ):
        raise RuntimeError("release manifest identity does not match the requested PyPI release")
    entries = manifest.get("artifacts")
    if not isinstance(entries, list) or len(entries) != 5:
        raise RuntimeError("release manifest must describe exactly five artifacts")
    provenance = manifest.get("provenance")
    if (
        not isinstance(provenance, dict)
        or set(provenance) != {"git_sha", "github_run_attempt", "github_run_id"}
        or not isinstance(provenance.get("git_sha"), str)
        or re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", provenance["git_sha"]) is None
        or isinstance(provenance.get("github_run_attempt"), bool)
        or not isinstance(provenance.get("github_run_attempt"), int)
        or provenance["github_run_attempt"] < 1
        or isinstance(provenance.get("github_run_id"), bool)
        or not isinstance(provenance.get("github_run_id"), int)
        or provenance["github_run_id"] < 1
    ):
        raise RuntimeError("release manifest contains malformed provenance")
    if packages_dir.is_symlink() or not packages_dir.is_dir():
        raise RuntimeError(f"packages directory must be a regular directory: {packages_dir}")

    artifacts: dict[str, tuple[Path, str]] = {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"filename", "sha256", "size"}:
            raise RuntimeError("release manifest contains a malformed artifact entry")
        filename = _safe_distribution_filename(entry.get("filename"))
        expected_digest = entry.get("sha256")
        expected_size = entry.get("size")
        if (
            filename is None
            or not isinstance(expected_digest, str)
            or _SHA256_PATTERN.fullmatch(expected_digest) is None
            or isinstance(expected_size, bool)
            or not isinstance(expected_size, int)
            or expected_size < 0
            or filename in artifacts
        ):
            raise RuntimeError("release manifest contains a malformed artifact entry")
        package = packages_dir / filename
        if package.is_symlink() or not package.is_file():
            raise RuntimeError(f"manifest package must be a regular file: {package}")
        identity = package.stat()
        if identity.st_size != expected_size or _sha256(package) != expected_digest:
            raise RuntimeError(f"local package does not match release manifest: {filename}")
        artifacts[filename] = (package, expected_digest)

    if list(artifacts) != sorted(artifacts):
        raise RuntimeError("release manifest artifacts are not in canonical filename order")
    if (
        sum(name.endswith(".tar.gz") for name in artifacts) != 1
        or sum(name.endswith(".whl") for name in artifacts) != 4
    ):
        raise RuntimeError("release manifest must contain one sdist and four wheels")
    actual_names = sorted(path.name for path in packages_dir.iterdir())
    if actual_names != sorted(artifacts):
        raise RuntimeError("packages directory does not exactly match the release manifest")
    return artifacts, provenance["git_sha"]


def _published_artifact_digests(payload: dict[str, object] | None) -> dict[str, str]:
    """Return unique immutable PyPI filenames and SHA-256 digests."""
    if payload is None:
        return {}
    urls = payload.get("urls")
    if not isinstance(urls, list) or not urls:
        raise RuntimeError("PyPI release JSON does not contain a non-empty file list")
    published: dict[str, str] = {}
    for entry in urls:
        if not isinstance(entry, dict):
            raise RuntimeError("PyPI release JSON contains a malformed file entry")
        filename = _safe_distribution_filename(entry.get("filename"))
        digests = entry.get("digests")
        digest = digests.get("sha256") if isinstance(digests, dict) else None
        yanked = entry.get("yanked")
        if (
            filename is None
            or not isinstance(digest, str)
            or _SHA256_PATTERN.fullmatch(digest) is None
            or not isinstance(yanked, bool)
            or filename in published
        ):
            raise RuntimeError("PyPI release JSON contains a malformed file entry")
        if yanked:
            raise RuntimeError(f"PyPI release contains a yanked file: {filename}")
        published[filename] = digest
    return published


def _reconcile_remote_artifacts(
    artifacts: Mapping[str, tuple[Path, str]],
    payload: dict[str, object] | None,
) -> tuple[dict[str, str], dict[str, tuple[Path, str]]]:
    """Return exact published and missing sets or reject immutable remote drift."""
    published = _published_artifact_digests(payload)
    unknown = sorted(set(published) - set(artifacts))
    mismatched = sorted(
        filename
        for filename, digest in published.items()
        if filename in artifacts and digest != artifacts[filename][1]
    )
    if unknown or mismatched:
        raise RuntimeError(
            f"PyPI release differs from the local manifest: unknown={unknown}, "
            f"digest_mismatches={mismatched}"
        )
    missing = {name: artifacts[name] for name in sorted(set(artifacts) - set(published))}
    return published, missing


def _same_staged_files(directory: Path, expected: Mapping[str, tuple[Path, str]]) -> bool:
    """Return whether a regular directory already contains the expected bytes."""
    if directory.is_symlink() or not directory.is_dir():
        return False
    paths = sorted(directory.iterdir(), key=lambda path: path.name)
    return [path.name for path in paths] == sorted(expected) and all(
        not path.is_symlink() and path.is_file() and _sha256(path) == expected[path.name][1]
        for path in paths
    )


def _stage_missing_artifacts(
    publish_dir: Path,
    missing: Mapping[str, tuple[Path, str]],
) -> None:
    """Atomically stage an exact idempotent directory of missing artifacts."""
    if publish_dir.is_symlink():
        raise RuntimeError(f"publish directory must not be a symlink: {publish_dir}")
    if publish_dir.exists() and not publish_dir.is_dir():
        raise RuntimeError(f"publish output must be a directory: {publish_dir}")
    if _same_staged_files(publish_dir, missing):
        for path in publish_dir.iterdir():
            path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        return
    publish_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{publish_dir.name}.", dir=publish_dir.parent))
    try:
        for filename in sorted(missing):
            source, _digest = missing[filename]
            destination = temporary / filename
            shutil.copyfile(source, destination)
            destination.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        if publish_dir.exists():
            shutil.rmtree(publish_dir)
        os.replace(temporary, publish_dir)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def _write_text_atomically(destination: Path, content: str) -> None:
    """Replace one regular text output atomically and skip unchanged bytes."""
    payload = content.encode("utf-8")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise RuntimeError(f"text output must be a regular file: {destination}")
    if destination.is_file() and destination.read_bytes() == payload:
        return
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def prepare_pypi_publish_recovery(
    manifest_file: Path,
    packages_dir: Path,
    publish_dir: Path,
    state_output: Path,
    *,
    project: str,
    version: str,
    index_url: str = "https://pypi.org/pypi",
) -> dict[str, object]:
    """Verify partial PyPI state and stage only manifest-matched missing files."""
    source_root = packages_dir.resolve()
    publish_root = publish_dir.resolve()
    if (
        publish_root == source_root
        or publish_root.is_relative_to(source_root)
        or source_root.is_relative_to(publish_root)
    ):
        raise RuntimeError("publish output and source packages must be disjoint directories")
    if state_output.resolve().is_relative_to(publish_root):
        raise RuntimeError("recovery state output must stay outside the publish directory")
    if state_output.resolve().is_relative_to(source_root):
        raise RuntimeError("recovery state output must stay outside the source packages")
    artifacts, _git_sha = _local_manifest_artifacts(
        manifest_file,
        packages_dir,
        project=project,
        version=version,
    )
    payload = _pypi_release_payload(project, version, index_url=index_url)
    published, missing = _reconcile_remote_artifacts(artifacts, payload)
    _stage_missing_artifacts(publish_dir, missing)
    state: dict[str, object] = {
        "format": f"{project}-pypi-recovery-v1",
        "project": project,
        "version": version,
        "status": "already-complete" if not missing else "publish-required",
        "publish_required": bool(missing),
        "published_count": len(published),
        "missing_count": len(missing),
        "published": [{"filename": name, "sha256": published[name]} for name in sorted(published)],
        "missing": [{"filename": name, "sha256": artifacts[name][1]} for name in sorted(missing)],
    }
    _write_text_atomically(
        state_output,
        json.dumps(state, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
    )
    return state


def verify_pypi_release_complete(
    manifest_file: Path,
    packages_dir: Path,
    *,
    project: str,
    version: str,
    index_url: str = "https://pypi.org/pypi",
    sleeper: Callable[[float], object] = time.sleep,
) -> None:
    """Require exact remote files and verified provenance within one fixed deadline."""
    artifacts, expected_git_sha = _local_manifest_artifacts(
        manifest_file,
        packages_dir,
        project=project,
        version=version,
    )
    pending_reason = "release state was not checked"
    for attempt in range(len(_COMPLETE_VISIBILITY_DELAYS_SECONDS) + 1):
        try:
            payload = _pypi_release_payload(project, version, index_url=index_url)
        except _PyPITransientError as exc:
            pending_reason = str(exc)
        else:
            _published, missing = _reconcile_remote_artifacts(artifacts, payload)
            if missing:
                pending_reason = f"missing release files: {sorted(missing)}"
            else:
                missing_provenance: list[str] = []
                try:
                    for filename, (package, _digest) in artifacts.items():
                        provenance = _pypi_provenance_payload(
                            project,
                            version,
                            filename,
                            index_url=index_url,
                        )
                        if provenance is None:
                            missing_provenance.append(filename)
                            continue
                        _verify_publish_provenance(
                            provenance,
                            package,
                            expected_git_sha=expected_git_sha,
                        )
                except _PyPITransientError as exc:
                    pending_reason = str(exc)
                else:
                    if not missing_provenance:
                        print(
                            "Verified exact PyPI files and publish attestations: "
                            f"{project} {version} ({len(artifacts)} artifacts)"
                        )
                        return
                    pending_reason = f"missing provenance: {missing_provenance}"
        if attempt < len(_COMPLETE_VISIBILITY_DELAYS_SECONDS):
            delay = _COMPLETE_VISIBILITY_DELAYS_SECONDS[attempt]
            print(
                f"PyPI release verification pending ({pending_reason}); "
                f"revalidating in {int(delay)} seconds"
            )
            sleeper(delay)
    raise RuntimeError(
        "PyPI release did not expose a complete manifest-and-provenance postcondition "
        f"after {_COMPLETE_VISIBILITY_DELAYS_SECONDS!r}: {pending_reason}"
    )


def write_github_recovery_outputs(destination: Path, state: Mapping[str, object]) -> None:
    """Write stable scalar recovery results for one GitHub Actions step."""
    status = state.get("status")
    missing_count = state.get("missing_count")
    published_count = state.get("published_count")
    publish_required = state.get("publish_required")
    if (
        status not in {"already-complete", "publish-required"}
        or isinstance(missing_count, bool)
        or not isinstance(missing_count, int)
        or isinstance(published_count, bool)
        or not isinstance(published_count, int)
        or not isinstance(publish_required, bool)
    ):
        raise RuntimeError("recovery state cannot be represented as GitHub outputs")
    _write_text_atomically(
        destination,
        "\n".join(
            (
                f"missing-count={missing_count}",
                f"published-count={published_count}",
                f"publish-required={str(publish_required).lower()}",
                f"status={status}",
                "",
            )
        ),
    )


def main() -> None:
    """Run initial-availability or manifest-verified recovery preflight."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version-file", type=Path, default=Path("meta/VERSION"))
    parser.add_argument("--project", default="schema-sanitizer")
    parser.add_argument("--index-url", default="https://pypi.org/pypi")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--packages-dir", type=Path)
    parser.add_argument("--publish-dir", type=Path)
    parser.add_argument("--state-output", type=Path)
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--allow-existing-for-recovery", action="store_true")
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    recovery_paths = (args.publish_dir, args.state_output)
    if args.require_complete:
        if args.manifest is None or args.packages_dir is None:
            parser.error("require-complete needs manifest and packages-dir")
        if any(path is not None for path in (*recovery_paths, args.github_output)):
            parser.error("require-complete does not accept recovery output paths")
    elif (
        (args.manifest is None) != (args.packages_dir is None)
        or (args.manifest is not None and not all(path is not None for path in recovery_paths))
        or (args.manifest is None and any(path is not None for path in recovery_paths))
    ):
        parser.error(
            "manifest, packages-dir, publish-dir, and state-output must be provided together"
        )
    if args.github_output is not None and args.manifest is None:
        parser.error("github-output requires recovery mode")
    if args.allow_existing_for_recovery and args.manifest is not None:
        parser.error("allow-existing-for-recovery is only valid in initial preflight mode")
    try:
        version = validate_release_version(args.version_file)
        if args.require_complete:
            verify_pypi_release_complete(
                args.manifest,
                args.packages_dir,
                project=args.project,
                version=version,
                index_url=args.index_url,
            )
            print(f"PyPI release is complete and manifest-matched: {args.project} {version}")
            return
        if args.manifest is not None:
            state = prepare_pypi_publish_recovery(
                args.manifest,
                args.packages_dir,
                args.publish_dir,
                args.state_output,
                project=args.project,
                version=version,
                index_url=args.index_url,
            )
            if args.github_output is not None:
                write_github_recovery_outputs(args.github_output, state)
            print(
                "PyPI recovery state: "
                f"published={state['published_count']}, missing={state['missing_count']}, "
                f"status={state['status']}"
            )
            return
        exists = pypi_release_exists(args.project, version, index_url=args.index_url)
    except (json.JSONDecodeError, OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    if exists:
        if args.allow_existing_for_recovery:
            print(
                f"PyPI version exists and requires manifest reconciliation: "
                f"{args.project} {version}"
            )
            return
        raise SystemExit(
            f"Refusing release: {args.project} {version} already exists on PyPI; "
            "published files are immutable. Increment meta/VERSION."
        )
    print(f"PyPI version is available: {args.project} {version}")


if __name__ == "__main__":
    main()
