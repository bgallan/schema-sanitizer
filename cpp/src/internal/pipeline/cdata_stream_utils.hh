// Shared helpers for Arrow C Stream callback implementations.

#pragma once

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

#include <cerrno>
#include <string>
#include <utility>

namespace sanitize::internal::cdata_stream {

// Maps the active exception to a contextual status.
sanitize::Status status_from_current_exception(const char *where);
// Maps a status to the errno convention used by Arrow C Stream callbacks.
int errno_for_status(const sanitize::Status &st) noexcept;
// Releases an ArrowSchema while suppressing callback exceptions.
void release_schema_nothrow(ArrowSchema *schema) noexcept;
// Releases an ArrowArray while suppressing callback exceptions.
void release_array_nothrow(ArrowArray *array) noexcept;
// Clears an ArrowSchema into the empty released state.
void clear_schema(ArrowSchema *schema) noexcept;
// Clears an ArrowArray into the empty released state.
void clear_array(ArrowArray *array) noexcept;
// Returns a nullable C string for an Arrow C Stream last-error callback.
const char *last_error_ptr(const std::string &last_error) noexcept;
// Clears an ArrowArrayStream into the empty released state.
void clear_stream(ArrowArrayStream *stream) noexcept;
// Stores a status message for last-error callbacks without throwing.
void set_last_error_nothrow(std::string &last_error,
                            const sanitize::Status &st) noexcept;
// Reports a schema callback failure using Arrow C Stream errno conventions.
int fail_schema(ArrowSchema *out, std::string &last_error,
                const sanitize::Status &st) noexcept;
// Reports an array callback failure using Arrow C Stream errno conventions.
int fail_array(ArrowArray *out, std::string &last_error,
               const sanitize::Status &st) noexcept;

// Runs a schema callback with common null, cleanup, and exception handling.
template <typename Fn>
inline int run_schema_callback(ArrowSchema *out, std::string &last_error,
                               const char *where, Fn &&fn) noexcept {
  if (!out) {
    set_last_error_nothrow(
        last_error,
        sanitize::Status::Invalid(where, ": output ArrowSchema is null"));
    return EINVAL;
  }
  clear_schema(out);
  sanitize::Status st;
  try {
    st = std::forward<Fn>(fn)(out);
  } catch (...) {
    st = status_from_current_exception(where);
  }
  if (!st.ok()) {
    return fail_schema(out, last_error, st);
  }
  return 0;
}

// Runs an array callback with common null, cleanup, and exception handling.
template <typename Fn>
inline int run_array_callback(ArrowArray *out, std::string &last_error,
                              const char *where, Fn &&fn) noexcept {
  if (!out) {
    set_last_error_nothrow(
        last_error,
        sanitize::Status::Invalid(where, ": output ArrowArray is null"));
    return EINVAL;
  }
  clear_array(out);
  sanitize::Status st;
  try {
    st = std::forward<Fn>(fn)(out);
  } catch (...) {
    st = status_from_current_exception(where);
  }
  if (!st.ok()) {
    return fail_array(out, last_error, st);
  }
  return 0;
}

} // namespace sanitize::internal::cdata_stream
