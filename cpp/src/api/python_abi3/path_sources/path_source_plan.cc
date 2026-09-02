// Implements Python-owned path-source plan parsing and capsule lifetime
// management. The helpers preserve source order, format grouping, and bounded
// ownership across multi-file operations.

#include "api/python_abi3/path_sources/path_sources.hh"

#include "internal/runtime/process_fd_governor.hh"
#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "frontends/builtin_frontends.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/parsing/streaming/csv/scanner.hh"
#include "internal/runtime/process_identity.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/registry/registry.hh"

namespace core_abi3_internal {
namespace {
constexpr const char *kPathSourcePlanCapsuleName =
    "schema_sanitizer.path_source_plan";

struct PathSourcePlanCapsule {
  std::vector<PathSourceSpec> sources;
};

/// Deletes the native payload owned by the corresponding Python capsule.
void path_source_plan_capsule_destructor(PyObject *capsule) {
  if (!sanitize::internal::runtime_owner_process()) {
    return;
  }
  auto *plan = static_cast<PathSourcePlanCapsule *>(
      PyCapsule_GetPointer(capsule, kPathSourcePlanCapsuleName));
  if (!plan) {
    PyErr_Clear();
    return;
  }
  delete plan;
}

/// Recovers a path-source plan from its typed Python capsule and validates
/// capsule identity.
PathSourcePlanCapsule *unwrap_path_source_plan(PyObject *obj) {
  if (!PyCapsule_CheckExact(obj)) {
    return nullptr;
  }
  return static_cast<PathSourcePlanCapsule *>(
      PyCapsule_GetPointer(obj, kPathSourcePlanCapsuleName));
}

/// Copies one Python Unicode field into a native path-source string.
bool py_path_source_text(PyObject *obj, const char *name, std::string *out) {
  if (!PyUnicode_Check(obj)) {
    PyErr_Format(PyExc_TypeError, "%s must be a string", name);
    return false;
  }
  Py_ssize_t size = 0;
  const char *data = PyUnicode_AsUTF8AndSize(obj, &size);
  if (!data) {
    return false;
  }
  out->assign(data, static_cast<std::size_t>(size));
  return true;
}

/// Encodes a Python path-like object into native filesystem bytes.
bool py_path_to_string(PyObject *obj, std::string *out) {
  PyObject *encoded = fsencode_path(obj);
  if (!encoded) {
    return false;
  }
  const char *path = PyBytes_AsString(encoded);
  const Py_ssize_t size = PyBytes_Size(encoded);
  if (!path || size < 0) {
    Py_DECREF(encoded);
    PyErr_SetString(PyExc_ValueError, "invalid source path");
    return false;
  }
  out->assign(path, static_cast<std::size_t>(size));
  Py_DECREF(encoded);
  return true;
}

/// Rejects readable path sources larger than the configured memory limit.
sanitize::Status
validate_path_source_sizes(const std::vector<PathSourceSpec> &sources,
                           long long memory_limit_bytes,
                           std::string_view stage) {
  if (memory_limit_bytes <= 0) {
    return sanitize::Status::OK();
  }
  for (const PathSourceSpec &source : sources) {
    sanitize::internal::ProcessFdPermitLease fd_lease(1U);
    if (!fd_lease) {
      return sanitize::Status::IOError(
          "native file-descriptor capacity exhausted");
    }
    std::ifstream input(source.path, std::ios::binary | std::ios::ate);
    if (!input) {
      continue;
    }
    fd_lease.mark_opened();
    sanitize::internal::ProcessFdStreamCloseGuard<std::ifstream> close_guard(
        input, fd_lease);
    const auto end = input.tellg();
    if (end == std::ifstream::pos_type(-1)) {
      continue;
    }
    const auto size =
        static_cast<std::uintmax_t>(static_cast<std::streamoff>(end));
    if (size > static_cast<std::uintmax_t>(memory_limit_bytes)) {
      return sanitize::Status::Invalid(
          "memory_limit_bytes limit exceeded during ", stage, ": ", size,
          " bytes > ", memory_limit_bytes,
          " bytes; file: ", source.source_file);
    }
  }
  return sanitize::Status::OK();
}
} // namespace

/// Parses owned path-source specifications from a capsule or Python sequence.
bool parse_path_sources(PyObject *sources_obj,
                        std::vector<PathSourceSpec> *out) {
  if (PyCapsule_CheckExact(sources_obj)) {
    auto *plan = unwrap_path_source_plan(sources_obj);
    if (!plan) {
      return false;
    }
    *out = plan->sources;
    if (out->empty()) {
      PyErr_SetString(PyExc_ValueError, "sources must not be empty");
      return false;
    }
    return true;
  }
  if (!PySequence_Check(sources_obj) || PyUnicode_Check(sources_obj)) {
    PyErr_SetString(PyExc_TypeError, "sources must be a sequence");
    return false;
  }
  const Py_ssize_t size = PySequence_Size(sources_obj);
  if (size < 0) {
    return false;
  }
  if (size == 0) {
    PyErr_SetString(PyExc_ValueError, "sources must not be empty");
    return false;
  }
  out->clear();
  out->reserve(static_cast<std::size_t>(size));
  for (Py_ssize_t i = 0; i < size; ++i) {
    bool borrowed = false;
    PyObject *item = sequence_item_borrowed_or_new(sources_obj, i, &borrowed);
    if (!item) {
      return false;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> item_owner(
        borrowed ? nullptr : item, Py_DECREF);
    if (!PySequence_Check(item) || PyUnicode_Check(item) ||
        PySequence_Size(item) != 3) {
      PyErr_SetString(PyExc_TypeError,
                      "each source must be (frontend, path, source_file)");
      return false;
    }
    PyObject *frontend_obj = PySequence_GetItem(item, 0);
    PyObject *path_obj = PySequence_GetItem(item, 1);
    PyObject *source_file_obj = PySequence_GetItem(item, 2);
    if (!frontend_obj || !path_obj || !source_file_obj) {
      Py_XDECREF(frontend_obj);
      Py_XDECREF(path_obj);
      Py_XDECREF(source_file_obj);
      return false;
    }
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> frontend_owner(frontend_obj,
                                                                   Py_DECREF);
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> path_owner(path_obj,
                                                               Py_DECREF);
    std::unique_ptr<PyObject, decltype(&Py_DECREF)> source_file_owner(
        source_file_obj, Py_DECREF);

    PathSourceSpec spec;
    if (!py_path_source_text(frontend_obj, "source frontend", &spec.frontend) ||
        !py_path_to_string(path_obj, &spec.path) ||
        !py_path_source_text(source_file_obj, "source_file",
                             &spec.source_file)) {
      return false;
    }
    out->push_back(std::move(spec));
  }
  return true;
}

/// Borrows capsule-backed path sources or parses an owned sequence when
/// necessary.
bool parse_path_sources_view(PyObject *sources_obj, ParsedPathSources *out) {
  out->borrowed = nullptr;
  out->owned.clear();
  if (PyCapsule_CheckExact(sources_obj)) {
    auto *plan = unwrap_path_source_plan(sources_obj);
    if (!plan) {
      return false;
    }
    if (plan->sources.empty()) {
      PyErr_SetString(PyExc_ValueError, "sources must not be empty");
      return false;
    }
    out->borrowed = &plan->sources;
    return true;
  }
  return parse_path_sources(sources_obj, &out->owned);
}

/// Validates Python path-source specifications and returns a reusable bounded
/// plan capsule.
PyObject *py_path_source_plan_create(PyObject *, PyObject *args) {
  PyObject *sources_obj = nullptr;
  long long memory_limit_bytes = -1;
  const char *stage = "memory";
  if (!PyArg_ParseTuple(args, "OLs:path_source_plan_create", &sources_obj,
                        &memory_limit_bytes, &stage)) {
    return nullptr;
  }
  auto plan = std::make_unique<PathSourcePlanCapsule>();
  if (!parse_path_sources(sources_obj, &plan->sources)) {
    return nullptr;
  }
  const sanitize::Status size_status =
      validate_path_source_sizes(plan->sources, memory_limit_bytes, stage);
  if (!size_status.ok()) {
    PyErr_SetString(PyExc_RuntimeError, size_status.message().c_str());
    return nullptr;
  }
  PyObject *capsule =
      PyCapsule_New(static_cast<void *>(plan.get()), kPathSourcePlanCapsuleName,
                    path_source_plan_capsule_destructor);
  if (!capsule) {
    return nullptr;
  }
  plan.release();
  return capsule;
}
} // namespace core_abi3_internal
