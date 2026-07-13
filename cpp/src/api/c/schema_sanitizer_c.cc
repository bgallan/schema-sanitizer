/*
 * C bridge context/options entry points.
 *
 * This file implements C bridge helpers for context lifecycle, options
 * preparation, and runtime metadata/diagnostic accessors.
 */
#include "internal/abi/schema_sanitizer_c_internal.hh"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <utility>

#include "internal/abi/schema_sanitizer_c_bridge.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/options/options_io.hh"

char *dup_cstr(const std::string &s) {
  char *p = static_cast<char *>(std::malloc(s.size() + 1));
  if (!p)
    return nullptr;
  std::memcpy(p, s.data(), s.size());
  p[s.size()] = '\0';
  return p;
}
void clear_out(char **out_error) {
  if (out_error)
    *out_error = nullptr;
}
int set_error(char **out_error, const std::string &msg, int code) {
  if (out_error) {
    *out_error = dup_cstr(msg);
    if (*out_error == nullptr) {
      // If we couldn't allocate the error string, prefer an OOM return.
      return SCHEMA_SANITIZER_STATUS_OUT_OF_MEMORY;
    }
  }
  return code;
}
int set_oom_error(char **out_error, const char *where) {
  return set_error(out_error, std::string(where) + ": out of memory",
                   SCHEMA_SANITIZER_STATUS_OUT_OF_MEMORY);
}
int set_exception_error(char **out_error, const char *where,
                        const std::exception &error) {
  return set_error(out_error, std::string(where) + ": " + error.what(),
                   SCHEMA_SANITIZER_STATUS_RUNTIME_ERROR);
}
int set_unknown_exception_error(char **out_error, const char *where) {
  return set_error(out_error, std::string(where) + ": unknown error",
                   SCHEMA_SANITIZER_STATUS_RUNTIME_ERROR);
}
int code_for_status(const sanitize::Status &st) {
  switch (st.code()) {
  case sanitize::StatusCode::kOK:
    return SCHEMA_SANITIZER_STATUS_OK;
  case sanitize::StatusCode::kInvalid:
    return SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT;
  case sanitize::StatusCode::kOutOfMemory:
    return SCHEMA_SANITIZER_STATUS_OUT_OF_MEMORY;
  case sanitize::StatusCode::kCancelled:
  case sanitize::StatusCode::kIOError:
  case sanitize::StatusCode::kNotImplemented:
  default:
    return SCHEMA_SANITIZER_STATUS_RUNTIME_ERROR;
  }
}
void schema_sanitizer_free_string(char *p) { std::free(p); }
sanitize::Result<sanitize::PreparedOptionsPtr> default_prepared_options() {
  static const auto prepared = sanitize::prepare_options(sanitize::Options{});
  return prepared;
}
int schema_sanitizer_options_prepare_bytes(
    const std::uint8_t *bytes, std::size_t len,
    schema_sanitizer_prepared_options **out_prepared, char **out_error) {
  static constexpr const char *kWhere =
      "schema_sanitizer_options_prepare_bytes";
  clear_out(out_error);
  if (out_prepared)
    *out_prepared = nullptr;
  if (!out_prepared) {
    return set_error(out_error, std::string(kWhere) + ": out_prepared is null",
                     SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
  }
  try {
    sanitize::PreparedOptionsPtr prepared;
    if (len == 0) {
      auto pr = default_prepared_options();
      if (!pr.ok()) {
        return set_error(out_error, pr.status().ToString(),
                         code_for_status(pr.status()));
      }
      prepared = std::move(pr).ValueOrDie();
    } else {
      if (!bytes) {
        return set_error(out_error, std::string(kWhere) + ": bytes is null",
                         SCHEMA_SANITIZER_STATUS_INVALID_ARGUMENT);
      }
      std::string_view sv(reinterpret_cast<const char *>(bytes), len);
      auto orr = sanitize::deserialize_options(sv);
      if (!orr.ok()) {
        return set_error(out_error, orr.status().ToString(),
                         code_for_status(orr.status()));
      }
      auto pr = sanitize::prepare_options(std::move(orr).ValueOrDie());
      if (!pr.ok()) {
        return set_error(out_error, pr.status().ToString(),
                         code_for_status(pr.status()));
      }
      prepared = std::move(pr).ValueOrDie();
    }
    auto h = std::make_unique<schema_sanitizer_prepared_options>();
    h->prepared = std::move(prepared);
    *out_prepared = h.release();
    return SCHEMA_SANITIZER_STATUS_OK;
  } catch (const std::bad_alloc &) {
    return set_oom_error(out_error, kWhere);
  } catch (const std::exception &e) {
    return set_exception_error(out_error, kWhere, e);
  } catch (...) {
    return set_unknown_exception_error(out_error, kWhere);
  }
}
void schema_sanitizer_prepared_options_free(
    schema_sanitizer_prepared_options *p) {
  delete p;
}
