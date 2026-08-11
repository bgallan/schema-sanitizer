"""Directory discovery values, listing, reading, and scoped context."""

from __future__ import annotations

import contextlib
import os
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from operator import attrgetter
from pathlib import Path
from typing import Generic, Protocol, TypeVar, cast

from schema_sanitizer.core_impl.fork_safety import quarantine_inherited_state

from ..core_impl.generated_bytes import (
    _secure_cleanup_enabled,
    _zero_bytearray_range,
)
from ..core_impl.governed_sort import governed_sort
from ..core_impl.process_resources import open_governed_file
from ..core_impl.uris import (
    local_path_from_file_uri,
    looks_like_file_uri,
    looks_like_remote_uri,
)
from ..errors import SchemaSanitizerResourceError
from ..sources.models import RemoteFile as _RemoteFile
from ..sources.models import remote_file_sort_key
from .directory_metadata_budget import (
    DirectoryMetadataBudget,
    TransientDirectoryMetadataReservation,
)

FOLDER_READ_CHUNK_BYTES = 1024 * 1024


class BinaryReader(Protocol):
    """Minimal binary stream interface used by discovered files."""

    def read(self, size: int = -1, /) -> bytes:
        """Read up to size bytes from the stream."""
        ...

    def close(self) -> object:
        """Close the stream."""
        ...


@dataclass(frozen=True)
class FolderFile:
    """A direct folder child that can be read as bytes."""

    display_name: str
    name: str
    size: int | None
    open_binary: Callable[[], BinaryReader]
    native_path: str | None = None
    _metadata_owner: object | None = field(default=None, compare=False, repr=False)


_DIRECTORY_METADATA_BUDGET: ContextVar[DirectoryMetadataBudget | None] = ContextVar(
    "schema_sanitizer_directory_metadata_budget", default=None
)
_FORKED_DIRECTORY_CONTEXT_KEEPALIVE: list[object] = []


@contextlib.contextmanager
def activate_directory_metadata_budget(
    budget: DirectoryMetadataBudget,
) -> Iterator[DirectoryMetadataBudget]:
    """Activate an existing shared directory-metadata budget."""
    owner_pid = os.getpid()
    token = _DIRECTORY_METADATA_BUDGET.set(budget)
    try:
        yield budget
    finally:
        if os.getpid() == owner_pid:
            _DIRECTORY_METADATA_BUDGET.reset(token)
        else:
            _reset_directory_contexts_after_fork()


@contextlib.contextmanager
def directory_metadata_budget_scope(
    memory_limit_bytes: int | None,
    *,
    budget: DirectoryMetadataBudget | None = None,
) -> Iterator[DirectoryMetadataBudget]:
    """Reuse the active operation budget or create one local scope."""
    current = _DIRECTORY_METADATA_BUDGET.get()
    if current is not None:
        yield current
        return
    selected = budget or DirectoryMetadataBudget(memory_limit_bytes)
    with activate_directory_metadata_budget(selected):
        yield selected


def current_directory_metadata_budget(
    memory_limit_bytes: int | None,
) -> DirectoryMetadataBudget:
    """Return the active operation budget or a bounded local fallback."""
    return _DIRECTORY_METADATA_BUDGET.get() or DirectoryMetadataBudget(memory_limit_bytes)


def split_parent_child(path: str) -> tuple[str, str] | None:
    """Return the parent prefix and final child segment of an object path."""
    normalized = path.rstrip("/")
    if not normalized:
        return None
    parent, _separator, child = normalized.rpartition("/")
    return (parent, child) if child else None


DirectoryFileT = TypeVar("DirectoryFileT")
_MAX_URI_MATCH_OBSERVATIONS = 65_536


@dataclass(frozen=True, slots=True)
class DirectoryDiscovery(Generic[DirectoryFileT]):
    """Directory existence flags and reusable direct-child listings."""

    exists_by_uri: dict[str, bool]
    files_by_uri: dict[str, list[DirectoryFileT]]


@dataclass(slots=True)
class DirectoryDiscoveryBuilder(Generic[DirectoryFileT]):
    """Accumulate deterministic directory-discovery results."""

    exists_by_uri: dict[str, bool]
    files_by_uri: dict[str, list[DirectoryFileT]]
    metadata_budget: DirectoryMetadataBudget | None = None
    transient_group_reservation: TransientDirectoryMetadataReservation | None = None

    @classmethod
    def from_uris(
        cls,
        uris: Iterable[str],
        *,
        metadata_budget: DirectoryMetadataBudget | None = None,
    ) -> DirectoryDiscoveryBuilder[DirectoryFileT]:
        """Create an empty result preserving the requested URI keys."""
        uri_values = tuple(uris) if metadata_budget is None else metadata_budget.charge_uris(uris)
        return cls(
            exists_by_uri=dict.fromkeys(uri_values, False),
            files_by_uri={uri: [] for uri in uri_values},
            metadata_budget=metadata_budget,
            transient_group_reservation=(
                None if metadata_budget is None else metadata_budget.transient_group_associations()
            ),
        )

    def _requested_uris(self, uris: Iterable[str]) -> tuple[str, ...]:
        """Return unique known keys without consuming an unbounded duplicate source."""
        if not self.files_by_uri:
            return ()
        values: list[str] = []
        seen: set[str] = set()
        observation_limit = min(
            _MAX_URI_MATCH_OBSERVATIONS,
            max(64, len(self.files_by_uri) * 8),
        )
        for observed, uri in enumerate(uris, start=1):
            if observed > observation_limit:
                raise SchemaSanitizerResourceError(
                    "directory URI association source exceeded its bounded scan window",
                    detail={
                        "stage": "directory_metadata",
                        "limit_name": "directory_uri_match_observations",
                        "limit_items": observation_limit,
                        "actual_items": observed,
                    },
                )
            if uri not in self.files_by_uri:
                raise KeyError(uri)
            if uri in seen:
                continue
            seen.add(uri)
            values.append(uri)
            if len(seen) == len(self.files_by_uri):
                break
        return tuple(values)

    def publish_group_association(self, publisher: Callable[[], None]) -> None:
        """Charge transient grouping scratch transactionally around publication."""
        reservation = self.transient_group_reservation
        if reservation is None:
            publisher()
            return
        reservation.charge_before_publish()
        try:
            publisher()
        except BaseException:
            reservation.rollback_publish()
            raise

    def add(self, uris: Iterable[str], file: DirectoryFileT) -> None:
        """Attach one discovered file to every matching requested directory."""
        uri_values = self._requested_uris(uris)
        if not uri_values:
            return
        if self.metadata_budget is not None and isinstance(file, (FolderFile, _RemoteFile)):
            self.metadata_budget.charge_file(file, associations=len(uri_values))
        for uri in uri_values:
            self.exists_by_uri[uri] = True
            self.files_by_uri[uri].append(file)

    def extend(self, uris: Iterable[str], files: Iterable[DirectoryFileT]) -> None:
        """Attach discovered files to every matching requested directory."""
        uri_values = self._requested_uris(uris)
        if not uri_values:
            return
        if self.metadata_budget is None:
            file_list = list(files)
        else:
            # ``folder_files`` already charged each retained file object. Bulk
            # extension charges and bounds only the new directory references.
            file_list = list(
                cast(
                    tuple[DirectoryFileT, ...],
                    self.metadata_budget.charge_references(
                        files, references_per_item=len(uri_values)
                    ),
                )
            )
        if not file_list:
            return
        for uri in uri_values:
            self.exists_by_uri[uri] = True
            self.files_by_uri[uri].extend(file_list)

    def finish(self, *, sort_files: bool = True) -> DirectoryDiscovery[DirectoryFileT]:
        """Finalize results and retire provider-grouping scratch immediately."""
        try:
            if sort_files:
                for files in self.files_by_uri.values():
                    if len(files) > 1:
                        if all(isinstance(file, _RemoteFile) for file in files):
                            governed_sort(
                                files,
                                key=lambda file: remote_file_sort_key(cast(_RemoteFile, file)),
                                stage="directory_metadata_sort",
                            )
                        else:
                            governed_sort(
                                files, key=attrgetter("name"), stage="directory_metadata_sort"
                            )
            return DirectoryDiscovery(
                exists_by_uri=self.exists_by_uri,
                files_by_uri=self.files_by_uri,
            )
        finally:
            reservation = self.transient_group_reservation
            if reservation is not None:
                reservation.close()
                self.transient_group_reservation = None


@dataclass(frozen=True, slots=True)
class DiscoveredDirectoryInput:
    """Directory child files already found by pipeline source discovery."""

    input_format: str
    local_files: tuple[FolderFile, ...] = ()
    remote_files: tuple[_RemoteFile, ...] = ()
    _metadata_owner: object | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        # Attach the stable owner before the enclosing discovery scope can
        # transfer its memory lease. All copies sharing this object graph keep
        # the same lifetime charge alive until the final reference disappears.
        if self._metadata_owner is None:
            budget = _DIRECTORY_METADATA_BUDGET.get()
            if budget is not None:
                object.__setattr__(self, "_metadata_owner", budget.retention_owner)


_DISCOVERED_DIRECTORY_INPUTS: ContextVar[Mapping[str, DiscoveredDirectoryInput] | None] = (
    ContextVar("schema_sanitizer_discovered_directory_inputs", default=None)
)


@contextlib.contextmanager
def discovered_directory_inputs(
    inputs: Mapping[str, DiscoveredDirectoryInput],
) -> Iterator[None]:
    """Temporarily provide pre-discovered directory files to public input prep."""
    owner_pid = os.getpid()
    token = _DISCOVERED_DIRECTORY_INPUTS.set(inputs)
    try:
        yield
    finally:
        if os.getpid() == owner_pid:
            _DISCOVERED_DIRECTORY_INPUTS.reset(token)
        else:
            _reset_directory_contexts_after_fork()


@contextlib.contextmanager
def discovered_directory_input_context(
    path: str | os.PathLike[str],
    discovered: DiscoveredDirectoryInput | None,
) -> Iterator[None]:
    """Expose one pre-discovered directory input for the duration of preparation."""
    if discovered is None:
        yield
        return
    with discovered_directory_inputs({os.fspath(path): discovered}):
        yield


def _reset_directory_contexts_after_fork() -> None:
    """Detach inherited directory state without finalizing parent resources."""
    budget = _DIRECTORY_METADATA_BUDGET.get()
    discovered = _DISCOVERED_DIRECTORY_INPUTS.get()
    quarantine_inherited_state("directory-contexts", budget, discovered)
    _DIRECTORY_METADATA_BUDGET.set(None)
    _DISCOVERED_DIRECTORY_INPUTS.set(None)


from ..core_impl.fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("directory-inputs", mode="quarantine_only")


def discovered_directory_input_for(
    path: str | os.PathLike[str],
    input_format: str,
    *,
    normalize_input_format: Callable[[str], str],
) -> DiscoveredDirectoryInput | None:
    """Return a matching pre-discovered input for this path and format."""
    inputs = _DISCOVERED_DIRECTORY_INPUTS.get()
    if not inputs:
        return None
    raw = os.fspath(path)
    discovered = inputs.get(raw)
    if discovered is None and isinstance(path, str) and looks_like_file_uri(path):
        discovered = inputs.get(local_path_from_file_uri(path))
    if discovered is None:
        return None
    if normalize_input_format(discovered.input_format) != input_format:
        return None
    return discovered


def _path_size(path: Path) -> int | None:
    """Return a local file size, or None if the platform cannot provide it."""
    try:
        return path.stat().st_size
    except OSError:
        return None


def _local_binary_opener(path: Path) -> Callable[[], BinaryReader]:
    """Return a stable opener for a local folder child."""

    def _open() -> BinaryReader:
        """Open the captured local child path."""
        return open_governed_file(path, "rb")

    return _open


def _normalized_suffixes(suffixes: str | Sequence[str]) -> tuple[str, ...]:
    """Return normalized lowercase suffixes with leading dots."""
    values = (suffixes,) if isinstance(suffixes, str) else tuple(suffixes)
    if not values:
        raise ValueError("At least one file suffix is required")
    return tuple(
        suffix if (suffix := value.lower()).startswith(".") else f".{suffix}" for value in values
    )


def folder_files(
    path: str | os.PathLike[str],
    *,
    suffix: str | Sequence[str],
    reader_name: str,
    memory_limit_bytes: int | None = None,
) -> list[FolderFile]:
    """Return deterministic non-recursive files from a local folder."""
    if looks_like_remote_uri(path):
        raise ValueError(f"{reader_name} remote directories must be staged before listing")
    accepted = _normalized_suffixes(suffix)
    folder = Path(os.fspath(path))
    if not folder.is_dir():
        raise NotADirectoryError(f"{reader_name} requires a directory: {folder}")
    metadata_budget = current_directory_metadata_budget(memory_limit_bytes)
    files: list[FolderFile] = []
    for child in folder.iterdir():
        if not child.is_file() or child.suffix.lower() not in accepted:
            continue
        file = FolderFile(
            display_name=str(child),
            name=child.name,
            size=_path_size(child),
            open_binary=_local_binary_opener(child),
            native_path=os.fspath(child),
        )
        # Charge before retaining the object in the sortable result list.
        metadata_budget.charge_file(file)
        files.append(file)
    if not files:
        expected = " or ".join(accepted)
        raise ValueError(f"{reader_name} found no {expected} files in: {folder}")
    governed_sort(files, key=attrgetter("name"), stage="directory_metadata_sort")
    return files


def check_document_size(
    display_name: str,
    size: int | None,
    *,
    memory_limit_bytes: int | None,
    stage: str,
) -> None:
    """Reject a folder child that exceeds the configured memory budget."""
    if memory_limit_bytes is None or memory_limit_bytes <= 0:
        return
    if size is None or size <= memory_limit_bytes:
        return
    raise SchemaSanitizerResourceError(
        f"memory_limit_bytes limit exceeded during {stage}: "
        f"{size} bytes > {memory_limit_bytes} bytes; file: {display_name}",
        detail={
            "stage": stage,
            "limit_name": "memory_limit_bytes",
            "limit_bytes": memory_limit_bytes,
            "actual_bytes": size,
            "file": display_name,
        },
    )


def read_folder_file_bytes(
    file: FolderFile,
    *,
    memory_limit_bytes: int | None,
    stage: str,
) -> bytes:
    """Read one folder child without crossing the configured byte budget."""
    check_document_size(
        file.display_name,
        file.size,
        memory_limit_bytes=memory_limit_bytes,
        stage=stage,
    )
    stream = file.open_binary()
    try:
        if memory_limit_bytes is None or memory_limit_bytes <= 0:
            return stream.read()
        payload = bytearray()
        total = 0
        try:
            while True:
                remaining = memory_limit_bytes + 1 - total
                chunk = stream.read(min(FOLDER_READ_CHUNK_BYTES, remaining))
                if not chunk:
                    break
                chunk_size = len(chunk)
                next_total = total + chunk_size
                check_document_size(
                    file.display_name,
                    next_total,
                    memory_limit_bytes=memory_limit_bytes,
                    stage=stage,
                )
                payload.extend(chunk)
                total = next_total
            return bytes(payload)
        finally:
            if payload and _secure_cleanup_enabled():
                _zero_bytearray_range(payload, 0, len(payload))
            payload.clear()
    finally:
        close = getattr(stream, "close", None)
        if close is not None:
            close()
