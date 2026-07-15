// Implements Arrow C Stream callback lifecycle and error handling.

#include "internal/arrow_c/cdata_stream_callbacks.hh"

#include <cerrno>
#include <cstring>
#include <exception>
#include <new>
#include <string>

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

namespace sanitize::internal::cdata_stream {

sanitize::Status status_from_current_exception(const char *where) {
  try {
    throw;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(where, ": out of memory");
  } catch (const std::exception &e) {
    return sanitize::Status::IOError(where, ": ", e.what());
  } catch (...) {
    return sanitize::Status::IOError(where, ": unknown exception");
  }
}

int errno_for_status(const sanitize::Status &st) noexcept {
  switch (st.code()) {
  case sanitize::StatusCode::kOK:
    return 0;
  case sanitize::StatusCode::kInvalid:
    return EINVAL;
  case sanitize::StatusCode::kOutOfMemory:
    return ENOMEM;
  case sanitize::StatusCode::kCancelled:
#ifdef ECANCELED
    return ECANCELED;
#else
    return EIO;
#endif
  case sanitize::StatusCode::kIOError:
    return EIO;
  case sanitize::StatusCode::kNotImplemented:
#ifdef ENOTSUP
    return ENOTSUP;
#else
    return EINVAL;
#endif
  default:
    return EIO;
  }
}

void release_schema_nothrow(ArrowSchema *schema) noexcept {
  if (!schema || !schema->release) {
    return;
  }
  try {
    schema->release(schema);
  } catch (...) {
    return;
  }
}

void release_array_nothrow(ArrowArray *array) noexcept {
  if (!array || !array->release) {
    return;
  }
  try {
    array->release(array);
  } catch (...) {
    return;
  }
}

void release_stream_nothrow(ArrowArrayStream *stream) noexcept {
  if (!stream || !stream->release) {
    return;
  }
  try {
    stream->release(stream);
  } catch (...) {
    return;
  }
}

void clear_schema(ArrowSchema *schema) noexcept {
  if (schema) {
    std::memset(schema, 0, sizeof(*schema));
  }
}

void clear_array(ArrowArray *array) noexcept {
  if (array) {
    std::memset(array, 0, sizeof(*array));
  }
}

const char *last_error_ptr(const std::string &last_error) noexcept {
  return last_error.empty() ? nullptr : last_error.c_str();
}

void clear_stream(ArrowArrayStream *stream) noexcept {
  if (!stream) {
    return;
  }
  stream->get_schema = nullptr;
  stream->get_next = nullptr;
  stream->get_last_error = nullptr;
  stream->release = nullptr;
  stream->private_data = nullptr;
}

void set_last_error_nothrow(std::string &last_error,
                            const sanitize::Status &st) noexcept {
  try {
    last_error = st.ToString();
  } catch (...) {
    try {
      last_error = "Arrow C stream callback failed";
    } catch (...) {
      last_error.clear();
    }
  }
}

int fail_schema(ArrowSchema *out, std::string &last_error,
                const sanitize::Status &st) noexcept {
  release_schema_nothrow(out);
  clear_schema(out);
  set_last_error_nothrow(last_error, st);
  return errno_for_status(st);
}

int fail_array(ArrowArray *out, std::string &last_error,
               const sanitize::Status &st) noexcept {
  release_array_nothrow(out);
  clear_array(out);
  set_last_error_nothrow(last_error, st);
  return errno_for_status(st);
}

} // namespace sanitize::internal::cdata_stream
