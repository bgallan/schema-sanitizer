// Manages Python ABI3 capsules for contexts, options, and streams.

#include "internal/abi/python_abi3/base.hh"
#include "internal/abi/python_abi3/capsules.hh"
#include "internal/abi/python_abi3/methods.hh"
#include "internal/abi/schema_sanitizer_c_internal.hh"

#include <cstdlib>
#include <utility>

namespace core_abi3_internal {
namespace {

// Raises runtime error.
void raise_runtime_error(const char *msg) {
  if (!msg) {
    msg = "schema-sanitizer ABI3 runtime error";
  }
  PyErr_SetString(PyExc_RuntimeError, msg);
}

constexpr const char *kContextCapsuleName = "schema_sanitizer.context";
constexpr const char *kDiagnosticsCapsuleName = "schema_sanitizer.diagnostics";
constexpr const char *kPreparedOptionsCapsuleName =
    "schema_sanitizer.prepared_options";
constexpr const char *kArrowStreamCapsuleName = "arrow_array_stream";

// Releases the execution context owned by a Python capsule.
void context_capsule_destructor(PyObject *capsule) {
  auto *ctx = static_cast<schema_sanitizer_context *>(
      PyCapsule_GetPointer(capsule, kContextCapsuleName));
  if (!ctx) {
    PyErr_Clear();
    return;
  }
  schema_sanitizer_context_free(ctx);
}

// Releases diagnostics owned by a Python capsule.
void diagnostics_capsule_destructor(PyObject *capsule) {
  auto *diagnostics = static_cast<schema_sanitizer_diagnostics *>(
      PyCapsule_GetPointer(capsule, kDiagnosticsCapsuleName));
  if (!diagnostics) {
    PyErr_Clear();
    return;
  }
  schema_sanitizer_diagnostics_free(diagnostics);
}

// Releases prepared options owned by a Python capsule.
void prepared_options_capsule_destructor(PyObject *capsule) {
  if (!capsule) {
    return;
  }
  auto *p = static_cast<schema_sanitizer_prepared_options *>(
      PyCapsule_GetPointer(capsule, kPreparedOptionsCapsuleName));
  if (!p) {
    PyErr_Clear();
    return;
  }
  schema_sanitizer_prepared_options_free(p);
}

struct StreamKeepAlive {
  PyObject *keepalive = nullptr;
};

// Releases an Arrow stream capsule and its Python keepalive reference.
void stream_capsule_destructor(PyObject *capsule) {
  auto *stream = static_cast<ArrowArrayStream *>(
      PyCapsule_GetPointer(capsule, kArrowStreamCapsuleName));
  if (!stream) {
    PyErr_Clear();
  } else {
    schema_sanitizer_stream_free(stream);
  }

  auto *ka = static_cast<StreamKeepAlive *>(PyCapsule_GetContext(capsule));
  if (!ka) {
    PyErr_Clear();
    return;
  }
  if (ka->keepalive) {
    Py_DECREF(ka->keepalive);
    ka->keepalive = nullptr;
  }
  std::free(ka);
}

} // namespace

schema_sanitizer_context *unwrap_context(PyObject *obj) {
  auto *ctx = static_cast<schema_sanitizer_context *>(
      PyCapsule_GetPointer(obj, kContextCapsuleName));
  if (!ctx) {
    return nullptr;
  }
  return ctx;
}

PyObject *wrap_context_capsule(schema_sanitizer_context *ctx) {
  PyObject *cap = PyCapsule_New(static_cast<void *>(ctx), kContextCapsuleName,
                                context_capsule_destructor);
  if (!cap) {
    if (ctx) {
      schema_sanitizer_context_free(ctx);
    }
    return nullptr;
  }
  return cap;
}

schema_sanitizer_prepared_options *unwrap_prepared_options(PyObject *obj) {
  if (obj == Py_None) {
    return nullptr;
  }
  auto *p = static_cast<schema_sanitizer_prepared_options *>(
      PyCapsule_GetPointer(obj, kPreparedOptionsCapsuleName));
  if (!p) {
    return nullptr;
  }
  return p;
}

bool resolve_prepared_options(
    PyObject *obj,
    std::shared_ptr<const sanitize::PreparedOptions> *out_prepared) {
  if (!out_prepared) {
    PyErr_SetString(PyExc_RuntimeError,
                    "internal error: null prepared-options output");
    return false;
  }
  if (obj == Py_None) {
    auto defaults = default_prepared_options();
    if (!defaults.ok()) {
      PyErr_SetString(PyExc_RuntimeError, defaults.status().ToString().c_str());
      return false;
    }
    *out_prepared = std::move(defaults).ValueOrDie();
    return true;
  }
  auto *prepared = unwrap_prepared_options(obj);
  if (!prepared) {
    return false;
  }
  *out_prepared = prepared->prepared;
  return true;
}

schema_sanitizer_diagnostics *unwrap_diagnostics(PyObject *obj) {
  auto *diagnostics = static_cast<schema_sanitizer_diagnostics *>(
      PyCapsule_GetPointer(obj, kDiagnosticsCapsuleName));
  if (!diagnostics) {
    return nullptr;
  }
  return diagnostics;
}

PyObject *wrap_prepared_options_capsule(schema_sanitizer_prepared_options *p) {
  PyObject *cap = PyCapsule_New(p, kPreparedOptionsCapsuleName,
                                prepared_options_capsule_destructor);
  if (!cap) {
    if (p) {
      schema_sanitizer_prepared_options_free(p);
    }
    return nullptr;
  }
  return cap;
}

PyObject *wrap_diagnostics_capsule(schema_sanitizer_diagnostics *diagnostics) {
  PyObject *cap =
      PyCapsule_New(static_cast<void *>(diagnostics), kDiagnosticsCapsuleName,
                    diagnostics_capsule_destructor);
  if (!cap) {
    if (diagnostics) {
      schema_sanitizer_diagnostics_free(diagnostics);
    }
    return nullptr;
  }
  return cap;
}

PyObject *wrap_stream_capsule_with_keepalive(PyObject *keepalive_obj,
                                             ArrowArrayStream *stream) {
  if (!keepalive_obj || !stream) {
    if (stream) {
      schema_sanitizer_stream_free(stream);
    }
    raise_runtime_error("internal error: null keepalive/stream");
    return nullptr;
  }

  PyObject *cap =
      PyCapsule_New(static_cast<void *>(stream), kArrowStreamCapsuleName,
                    stream_capsule_destructor);
  if (!cap) {
    schema_sanitizer_stream_free(stream);
    return nullptr;
  }

  auto *ka =
      static_cast<StreamKeepAlive *>(std::calloc(1, sizeof(StreamKeepAlive)));
  if (!ka) {
    Py_DECREF(cap);
    raise_runtime_error("out of memory");
    return nullptr;
  }
  Py_INCREF(keepalive_obj);
  ka->keepalive = keepalive_obj;

  if (PyCapsule_SetContext(cap, static_cast<void *>(ka)) != 0) {
    Py_DECREF(keepalive_obj);
    std::free(ka);
    Py_DECREF(cap);
    return nullptr;
  }

  return cap;
}

} // namespace core_abi3_internal
