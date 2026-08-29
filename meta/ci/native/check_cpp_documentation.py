"""Validate file summaries and callable documentation across maintained native code.

The checker augments a compile database for auxiliary targets and uses Clang's AST
to cover private callables while excluding generated or vendored source files.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shlex
import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[3]
_BUILD_ROOT = (_REPO_ROOT / ".work" / "build").resolve()
_MAINTAINED_CPP_ROOTS = (
    _REPO_ROOT / "cpp" / "src",
    _REPO_ROOT / "cpp" / "tests",
    _REPO_ROOT / "cpp" / "fuzz",
    _REPO_ROOT / "meta" / "ci" / "sanitizers",
)
_CPP_SUFFIXES = frozenset(
    {".c", ".cc", ".cpp", ".cxx", ".def", ".h", ".hh", ".hpp", ".hxx", ".inc"}
)
_TRANSLATION_UNIT_SUFFIXES = frozenset({".c", ".cc", ".cpp", ".cxx"})
_AUXILIARY_TRANSLATION_UNITS = tuple(
    path
    for root in _MAINTAINED_CPP_ROOTS[1:]
    for path in sorted(root.rglob("*"))
    if path.is_file() and path.suffix in _TRANSLATION_UNIT_SUFFIXES
)

_CALLABLE_AUDITOR_SOURCE = r"""// Audits explicit native callables through Clang's complete semantic AST.
// Stable USRs join declarations to definitions while file callbacks prove source reachability.

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "clang/AST/ASTConsumer.h"
#include "clang/AST/RecursiveASTVisitor.h"
#include "clang/Frontend/CompilerInstance.h"
#include "clang/Frontend/FrontendActions.h"
#include "clang/Index/USRGeneration.h"
#include "clang/Lex/PPCallbacks.h"
#include "clang/Tooling/ArgumentsAdjusters.h"
#include "clang/Tooling/CompilationDatabase.h"
#include "clang/Tooling/Tooling.h"
#include "llvm/ADT/SmallString.h"
#include "llvm/Support/FileSystem.h"

namespace {

std::string repository_root;

/// Canonicalizes a source path before maintained-root comparisons and reporting.
std::string NormalizePath(llvm::StringRef value) {
  llvm::SmallString<512> canonical;
  const auto path = llvm::sys::fs::real_path(value, canonical)
                        ? value
                        : llvm::StringRef(canonical);
  std::string normalized(path);
  std::replace(normalized.begin(), normalized.end(), '\\', '/');
  while (normalized.size() > 1U && normalized.back() == '/') {
    normalized.pop_back();
  }
  return normalized;
}

/// Reports whether a source path belongs to a maintained native-code root.
bool MaintainedPath(llvm::StringRef value) {
  const auto path = NormalizePath(value);
  return path.starts_with(repository_root + "/cpp/src/") ||
         path.starts_with(repository_root + "/cpp/tests/") ||
         path.starts_with(repository_root + "/cpp/fuzz/") ||
         path.starts_with(repository_root + "/meta/ci/sanitizers/");
}

/// Replaces field separators so each audit record remains one TSV row.
std::string Escape(llvm::StringRef value) {
  std::string escaped(value);
  for (char &character : escaped) {
    if (character == '\t' || character == '\n' || character == '\r') {
      character = ' ';
    }
  }
  return escaped;
}

/// Reports whether a Doxygen comment reaches a declaration without a blank line.
bool IsAttachedDocumentation(const clang::RawComment &comment,
                             const clang::FunctionDecl &declaration,
                             const clang::SourceManager &sources) {
  if (!comment.isDocumentation()) {
    return false;
  }
  const auto comment_end = sources.getSpellingLoc(comment.getEndLoc());
  const auto declaration_begin =
      sources.getExpansionLoc(declaration.getBeginLoc());
  if (comment_end.isInvalid() || declaration_begin.isInvalid()) {
    return false;
  }
  const auto file_id = sources.getFileID(comment_end);
  if (file_id != sources.getFileID(declaration_begin)) {
    return false;
  }
  const auto comment_offset = sources.getFileOffset(comment_end);
  const auto declaration_offset = sources.getFileOffset(declaration_begin);
  if (comment_offset >= declaration_offset) {
    return false;
  }
  bool invalid_buffer = false;
  const auto buffer = sources.getBufferData(file_id, &invalid_buffer);
  if (invalid_buffer || declaration_offset > buffer.size()) {
    return false;
  }
  const auto between = buffer.slice(comment_offset, declaration_offset);
  for (std::size_t index = 0; index < between.size(); ++index) {
    if (between[index] != '\n') {
      continue;
    }
    auto next = index + 1U;
    while (next < between.size() &&
           (between[next] == ' ' || between[next] == '\t' ||
            between[next] == '\r')) {
      ++next;
    }
    if (next < between.size() && between[next] == '\n') {
      return false;
    }
  }
  return true;
}

class CallableVisitor final
    : public clang::RecursiveASTVisitor<CallableVisitor> {
 public:
  /// Binds callable traversal to the current translation unit's AST and sources.
  explicit CallableVisitor(clang::ASTContext &context)
      : context_(context), sources_(context.getSourceManager()) {}

  /// Emits one record for each explicit, non-lambda callable declaration.
  bool VisitFunctionDecl(clang::FunctionDecl *declaration) {
    if (declaration == nullptr || declaration->isImplicit() ||
        declaration->getTemplateSpecializationKind() ==
            clang::TSK_ImplicitInstantiation) {
      return true;
    }
    if (const auto *method =
            llvm::dyn_cast<clang::CXXMethodDecl>(declaration);
        method != nullptr && method->getParent() != nullptr &&
        method->getParent()->isLambda()) {
      return true;
    }

    const auto location = declaration->getLocation();
    if (location.isInvalid() || location.isMacroID()) {
      return true;
    }
    const auto spelling = sources_.getSpellingLoc(location);
    const auto file = sources_.getFilename(spelling);
    if (spelling.isInvalid() || !MaintainedPath(file)) {
      return true;
    }

    llvm::SmallString<256> usr;
    if (clang::index::generateUSRForDecl(declaration, usr)) {
      return true;
    }

    bool documented = false;
    for (const auto *redeclared : declaration->redecls()) {
      if (redeclared->getLocation().isMacroID()) {
        continue;
      }
      const auto *comment =
          context_.getRawCommentForDeclNoCache(redeclared);
      documented |= comment != nullptr &&
                    IsAttachedDocumentation(*comment, *redeclared, sources_);
    }

    const auto presumed = sources_.getPresumedLoc(spelling);
    if (presumed.isInvalid()) {
      return true;
    }
    std::cout << Escape(usr) << '\t' << Escape(NormalizePath(file)) << '\t'
              << presumed.getLine() << '\t'
              << Escape(declaration->getNameAsString()) << '\t'
              << documented << '\t'
              << declaration->doesThisDeclarationHaveABody() << '\n';
    return true;
  }

 private:
  clang::ASTContext &context_;
  clang::SourceManager &sources_;
};

class CallableConsumer final : public clang::ASTConsumer {
 public:
  /// Creates a callable visitor for one translation unit.
  explicit CallableConsumer(clang::ASTContext &context) : visitor_(context) {}

  /// Traverses every declaration after semantic analysis completes.
  void HandleTranslationUnit(clang::ASTContext &context) override {
    visitor_.TraverseDecl(context.getTranslationUnitDecl());
  }

 private:
  CallableVisitor visitor_;
};

class MaintainedFileCallbacks final : public clang::PPCallbacks {
 public:
  /// Binds preprocessing file events to the translation unit's source manager.
  explicit MaintainedFileCallbacks(clang::SourceManager &sources)
      : sources_(sources) {}

  /// Emits every maintained file entered by preprocessing.
  void FileChanged(clang::SourceLocation location, FileChangeReason reason,
                   clang::SrcMgr::CharacteristicKind, clang::FileID) override {
    if (reason != EnterFile) {
      return;
    }
    const auto spelling = sources_.getSpellingLoc(location);
    const auto file = sources_.getFilename(spelling);
    if (!spelling.isInvalid() && MaintainedPath(file)) {
      std::cout << "@FILE\t" << Escape(NormalizePath(file)) << '\n';
    }
  }

 private:
  clang::SourceManager &sources_;
};

class AuditAction final : public clang::ASTFrontendAction {
 public:
  /// Registers maintained-file callbacks before preprocessing begins.
  bool BeginSourceFileAction(clang::CompilerInstance &compiler) override {
    compiler.getPreprocessor().addPPCallbacks(
        std::make_unique<MaintainedFileCallbacks>(compiler.getSourceManager()));
    return true;
  }

  /// Creates the semantic callable consumer for one translation unit.
  std::unique_ptr<clang::ASTConsumer> CreateASTConsumer(
      clang::CompilerInstance &compiler, llvm::StringRef) override {
    return std::make_unique<CallableConsumer>(compiler.getASTContext());
  }
};

}  // namespace

/// Runs the callable audit over every translation unit in a compile database.
int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "usage: callable-auditor COMPILE_DB_DIR REPOSITORY_ROOT\n";
    return 2;
  }
  repository_root = NormalizePath(argv[2]);
  std::string error;
  auto database =
      clang::tooling::CompilationDatabase::loadFromDirectory(argv[1], error);
  if (database == nullptr) {
    std::cerr << error << '\n';
    return 2;
  }
  clang::tooling::ClangTool tool(*database, database->getAllFiles());
  tool.appendArgumentsAdjuster(clang::tooling::getClangSyntaxOnlyAdjuster());
  return tool.run(
      clang::tooling::newFrontendActionFactory<AuditAction>().get());
}
"""


async def _invoke_command(
    arguments: Sequence[str | Path],
    *,
    cwd: Path | None = None,
    stdout: Any = None,
) -> tuple[int, bytes, bytes]:
    """Run one argv-only command and return its status and captured streams."""
    process = await asyncio.create_subprocess_exec(
        *(str(argument) for argument in arguments),
        cwd=str(cwd) if cwd is not None else None,
        stdout=asyncio.subprocess.PIPE if stdout is None else stdout,
        stderr=asyncio.subprocess.PIPE,
    )
    captured_stdout, captured_stderr = await process.communicate()
    return process.returncode, captured_stdout or b"", captured_stderr or b""


def _command_output(arguments: Sequence[str | Path]) -> str:
    """Return UTF-8 output from one successful argv-only command."""
    returncode, stdout, stderr = asyncio.run(_invoke_command(arguments))
    if returncode:
        diagnostics = stderr.decode(errors="replace")[-12_000:]
        raise RuntimeError(f"command failed with status {returncode}:\n{diagnostics}")
    return stdout.decode()


def _run_checked_command(arguments: Sequence[str | Path], *, cwd: Path) -> None:
    """Run one trusted argv and raise with bounded diagnostics on failure."""
    returncode, _stdout, stderr = asyncio.run(_invoke_command(arguments, cwd=cwd))
    if returncode:
        diagnostics = stderr.decode(errors="replace")[-12_000:]
        raise RuntimeError(f"command failed with status {returncode}:\n{diagnostics}")


def _llvm_config_version(executable: str | Path) -> tuple[int, ...]:
    """Return the numeric LLVM version reported by one llvm-config candidate."""
    output = _command_output((executable, "--version")).strip()
    match = re.match(r"(\d+(?:\.\d+)*)", output)
    return tuple(int(part) for part in match.group(1).split(".")) if match else ()


def _find_llvm_config(explicit: Path | None = None) -> str:
    """Return the requested or newest available llvm-config executable."""
    if explicit is not None:
        executable = explicit.expanduser().resolve()
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise FileNotFoundError(f"llvm-config was not found at {executable}")
        return str(executable)
    candidates = {
        Path(executable).resolve()
        for executable in (shutil.which("llvm-config"),)
        if executable is not None
    }
    candidates.update(Path("/usr/lib").glob("llvm-*/bin/llvm-config"))
    if candidates:
        return str(max(candidates, key=_llvm_config_version))
    raise FileNotFoundError("llvm-config was not found")


def _find_compile_database(build_root: Path = _BUILD_ROOT) -> Path:
    """Return the newest compile database below one trusted build root."""
    root = build_root.expanduser().resolve(strict=True)
    candidates = [root / "compile_commands.json"]
    candidates.extend(sorted(root.glob("*/compile_commands.json")))
    candidates = [path for path in candidates if path.is_file()]
    if not candidates:
        raise FileNotFoundError(f"no compile_commands.json was found below {root}")
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def _validated_compile_database(candidate: Path, build_root: Path = _BUILD_ROOT) -> Path:
    """Resolve a regular compile database confined to an approved build root."""
    root = build_root.expanduser().resolve(strict=True)
    database = candidate.expanduser().resolve(strict=True)
    try:
        database.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"compile database must be located below {root}: {database}") from exc
    if database.name != "compile_commands.json" or not database.is_file():
        raise FileNotFoundError(f"compile database is not a regular expected file: {database}")
    return database


def _maintained_cpp_files() -> tuple[Path, ...]:
    """Return maintained native files while excluding vendored dependencies."""
    return tuple(
        path
        for root in _MAINTAINED_CPP_ROOTS
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.suffix in _CPP_SUFFIXES
    )


def _leading_file_summary(path: Path) -> str:
    """Return the leading line or block comment from one native file."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return ""
    first = lines[0].lstrip()
    if first.startswith("//") and not first.startswith(("///", "//!")):
        summary: list[str] = []
        for line in lines:
            stripped = line.lstrip()
            if not stripped.startswith("//") or stripped.startswith(("///", "//!")):
                break
            summary.append(stripped.removeprefix("//").strip())
        return " ".join(part for part in summary if part)
    if not first.startswith("/*"):
        return ""
    summary = []
    for line in lines:
        stripped = line.strip()
        summary.append(stripped.removeprefix("/*").removesuffix("*/").strip(" *"))
        if "*/" in line:
            break
    return " ".join(part for part in summary if part)


def _file_summary_has_separator(path: Path) -> bool:
    """Report whether a blank line separates the file summary from native code."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return False
    index = 0
    first = lines[0].lstrip()
    if first.startswith("//") and not first.startswith(("///", "//!")):
        while index < len(lines):
            stripped = lines[index].lstrip()
            if not stripped.startswith("//") or stripped.startswith(("///", "//!")):
                break
            index += 1
    elif first.startswith("/*"):
        while index < len(lines):
            closed = "*/" in lines[index]
            index += 1
            if closed:
                break
    else:
        return False
    return index < len(lines) and not lines[index].strip()


def _file_summary_gaps() -> list[str]:
    """Return native files without concise two- or three-sentence summaries."""
    gaps = []
    for path in _maintained_cpp_files():
        summary = _leading_file_summary(path)
        sentence_count = len(re.findall(r"[.!?](?=\s|$)", summary))
        word_count = len(re.findall(r"\b[\w'-]+\b", summary))
        separated = _file_summary_has_separator(path)
        if not 2 <= sentence_count <= 3 or word_count < 12 or not separated:
            relative = path.relative_to(_REPO_ROOT).as_posix()
            gaps.append(
                f"{relative}: sentences={sentence_count}, words={word_count}, "
                f"separator={'present' if separated else 'missing'}"
            )
    return gaps


def _command_arguments(entry: dict[str, Any]) -> list[str]:
    """Return one compile database entry as an argument vector."""
    arguments = entry.get("arguments")
    if arguments is not None:
        return list(arguments)
    return shlex.split(entry["command"])


def _command_source(entry: dict[str, Any]) -> Path:
    """Resolve a compile database source against its declared working directory."""
    source = Path(entry["file"])
    if source.is_absolute():
        return source.resolve()
    return (Path(entry["directory"]) / source).resolve()


def _is_maintained_cpp_path(path: Path) -> bool:
    """Report whether a resolved path belongs to a maintained native root."""
    return any(path.is_relative_to(root) for root in _MAINTAINED_CPP_ROOTS)


def _augmented_compile_database(compile_commands: Path, output_dir: Path) -> Path:
    """Add maintained test, fuzz, and launcher translation units to a database."""
    entries = [
        entry
        for entry in json.loads(compile_commands.read_text(encoding="utf-8"))
        if _is_maintained_cpp_path(_command_source(entry))
    ]
    template = next(
        entry
        for entry in entries
        if _command_source(entry).is_relative_to(_REPO_ROOT / "cpp" / "src")
    )
    template_file = template["file"]
    template_source = str(_command_source(template))
    template_arguments = _command_arguments(template)
    existing_sources = {_command_source(entry) for entry in entries}
    object_dir = output_dir / "objects"
    object_dir.mkdir(parents=True)
    for index, source in enumerate(_AUXILIARY_TRANSLATION_UNITS):
        if source.resolve() in existing_sources:
            continue
        arguments = list(template_arguments)
        arguments = [
            str(source) if argument in {template_file, template_source} else argument
            for argument in arguments
        ]
        for position, argument in enumerate(arguments[:-1]):
            if argument == "-o":
                arguments[position + 1] = str(object_dir / f"auxiliary-{index}.o")
                break
        entries.append(
            {
                "directory": template["directory"],
                "file": str(source),
                "arguments": arguments,
            }
        )
    augmented = output_dir / "compile_commands.json"
    augmented.write_text(json.dumps(entries), encoding="utf-8")
    return augmented


def _llvm_config_arguments(executable: str, *options: str) -> list[str]:
    """Return shell-split compiler or linker flags reported by llvm-config."""
    output = _command_output((executable, *options))
    return shlex.split(output)


def _find_clang_cpp_library(llvm_config: str) -> Path:
    """Return the monolithic Clang library associated with llvm-config."""
    library_dir = Path(_command_output((llvm_config, "--libdir")).strip())
    candidates = tuple(
        path
        for pattern in ("libclang-cpp.so*", "libclang-cpp*.dylib", "clang-cpp.lib")
        for path in library_dir.glob(pattern)
        if path.is_file()
    )
    if not candidates:
        raise FileNotFoundError(f"libclang-cpp was not found below {library_dir}")
    return max(candidates, key=lambda path: (not path.is_symlink(), path.name))


def _compile_callable_auditor(output_dir: Path, llvm_config_path: Path | None) -> Path:
    """Compile the small LibTooling program used for exact callable discovery."""
    llvm_config = _find_llvm_config(llvm_config_path)
    binary_dir = Path(_command_output((llvm_config, "--bindir")).strip())
    compiler = binary_dir / "clang++"
    if not compiler.is_file():
        compiler = binary_dir / "clang++.exe"
    if not compiler.is_file():
        raise FileNotFoundError(f"clang++ was not found below {binary_dir}")

    source = output_dir / "callable_auditor.cc"
    executable_name = "callable-auditor.exe" if compiler.suffix == ".exe" else "callable-auditor"
    executable = output_dir / executable_name
    source.write_text(_CALLABLE_AUDITOR_SOURCE, encoding="utf-8")
    _run_checked_command(
        (
            str(compiler),
            *_llvm_config_arguments(llvm_config, "--cxxflags"),
            "-std=c++20",
            "-O2",
            str(source),
            str(_find_clang_cpp_library(llvm_config)),
            *_llvm_config_arguments(llvm_config, "--ldflags", "--system-libs", "--libs"),
            "-o",
            str(executable),
        ),
        cwd=_REPO_ROOT,
    )
    return executable


def _relative_audit_path(value: str) -> str:
    """Return one absolute auditor path relative to the repository root."""
    return Path(value).resolve().relative_to(_REPO_ROOT).as_posix()


def _ast_documentation_gaps(
    executable: Path, compile_commands: Path
) -> tuple[list[str], list[str], int]:
    """Return callable gaps, unreached files, and the unique callable count."""
    audit_output = compile_commands.parent / "callables.tsv"
    with audit_output.open("w", encoding="utf-8") as output_stream:
        returncode, _stdout, stderr = asyncio.run(
            _invoke_command(
                (executable, compile_commands.parent, _REPO_ROOT),
                cwd=_REPO_ROOT,
                stdout=output_stream,
            )
        )
    if returncode:
        diagnostics = stderr.decode(errors="replace")[-12_000:]
        raise RuntimeError(f"Clang AST documentation audit failed:\n{diagnostics}")

    reached_files: set[str] = set()
    callables: dict[str, dict[str, Any]] = {}
    with audit_output.open(encoding="utf-8") as input_stream:
        for raw_line in input_stream:
            line = raw_line.rstrip("\n")
            fields = line.split("\t")
            if fields[0] == "@FILE" and len(fields) == 2:
                reached_files.add(_relative_audit_path(fields[1]))
                continue
            if len(fields) != 6:
                raise RuntimeError(f"Malformed callable-auditor record: {line!r}")
            usr, filename, line_number, name, documented, has_body = fields
            location = f"{_relative_audit_path(filename)}:{line_number}:{name}"
            record = callables.setdefault(
                usr, {"documented": False, "locations": set(), "bodies": {}}
            )
            record["documented"] |= documented == "1"
            record["locations"].add(location)
            if has_body == "1":
                record["bodies"][location] = (
                    record["bodies"].get(location, False) or documented == "1"
                )

    gaps = []
    callable_count = 0
    for record in callables.values():
        bodies = record["bodies"]
        if len(bodies) > 1:
            callable_count += len(bodies)
            gaps.extend(location for location, documented in bodies.items() if not documented)
            continue
        callable_count += 1
        if not record["documented"]:
            gaps.append(";".join(sorted(record["locations"])))
    expected_files = {path.relative_to(_REPO_ROOT).as_posix() for path in _maintained_cpp_files()}
    return sorted(gaps), sorted(expected_files - reached_files), callable_count


def main() -> int:
    """Validate native summaries and named callables across maintained files."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--compile-commands",
        type=Path,
        default=None,
        help="Path to compile_commands.json; defaults to the latest build directory.",
    )
    parser.add_argument(
        "--build-root",
        type=Path,
        default=_BUILD_ROOT,
        help="Trusted build root containing the compile database.",
    )
    parser.add_argument(
        "--source-only",
        action="store_true",
        help="Validate maintained native file summaries without invoking Clang.",
    )
    parser.add_argument(
        "--llvm-config",
        type=Path,
        default=None,
        help="Explicit llvm-config matching the Clang LibTooling installation.",
    )
    args = parser.parse_args()

    summary_gaps = _file_summary_gaps()
    if args.source_only:
        if summary_gaps:
            print("C/C++ files without concise summaries:")
            print("\n".join(summary_gaps))
        if summary_gaps:
            return 1
        print("All maintained C/C++ file summaries are present.")
        return 0

    compile_commands = _validated_compile_database(
        args.compile_commands or _find_compile_database(args.build_root), args.build_root
    )
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-cpp-docs-") as tmp:
        temporary_root = Path(tmp)
        augmented_commands = _augmented_compile_database(compile_commands, temporary_root)
        auditor = _compile_callable_auditor(temporary_root, args.llvm_config)
        gaps, unreached_files, callable_count = _ast_documentation_gaps(auditor, augmented_commands)

    if summary_gaps:
        print("C/C++ files without concise summaries:")
        print("\n".join(summary_gaps))
    if gaps:
        print("Undocumented C/C++ functions:")
        print("\n".join(gaps))
    if unreached_files:
        print("Maintained C/C++ files not reached by the compile database:")
        print("\n".join(unreached_files))
    if summary_gaps or gaps or unreached_files:
        return 1
    print(
        f"All {len(_maintained_cpp_files())} maintained C/C++ files and "
        f"{callable_count} explicit named callables are documented."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
