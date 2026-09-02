// Defines Status and Result primitives for explicit native error propagation.
// Value-or-error ownership and return macros keep expected failures out of
// exceptions while preserving one stable diagnostic message.

#pragma once

#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

namespace sanitize {

enum class StatusCode : std::uint8_t {
  kOK = 0,
  kInvalid = 1,
  kOutOfMemory = 2,
  kCancelled = 3,
  kIOError = 4,
  kNotImplemented = 5,
};

class Status {
public:
  /// Creates a successful status.
  Status() = default;
  /// Creates a status with a concrete code and normalized message.
  Status(StatusCode code, std::string message)
      : code_(code), message_(std::move(message)) {
    if (code_ == StatusCode::kOK) {
      message_.clear();
    }
  }

  /// Creates a successful status.
  static Status OK() { return {}; }

  /// Creates an invalid-argument status.
  template <class... Args> static Status Invalid(Args &&...args) {
    return Status(StatusCode::kInvalid,
                  build_message(std::forward<Args>(args)...));
  }
  /// Creates an out-of-memory status.
  template <class... Args> static Status OutOfMemory(Args &&...args) {
    return Status(StatusCode::kOutOfMemory,
                  build_message(std::forward<Args>(args)...));
  }
  /// Creates a cancellation status.
  template <class... Args> static Status Cancelled(Args &&...args) {
    return Status(StatusCode::kCancelled,
                  build_message(std::forward<Args>(args)...));
  }
  /// Creates an I/O error status.
  template <class... Args> static Status IOError(Args &&...args) {
    return Status(StatusCode::kIOError,
                  build_message(std::forward<Args>(args)...));
  }
  /// Creates a not-implemented status.
  template <class... Args> static Status NotImplemented(Args &&...args) {
    return Status(StatusCode::kNotImplemented,
                  build_message(std::forward<Args>(args)...));
  }

  /// Returns whether the operation succeeded.
  [[nodiscard]] bool ok() const { return code_ == StatusCode::kOK; }
  /// Returns the status code.
  [[nodiscard]] StatusCode code() const { return code_; }

  /// Returns the status message.
  [[nodiscard]] const std::string &message() const { return message_; }

  /// Returns a string representation.
  [[nodiscard]] std::string ToString() const {
    if (ok()) {
      return "OK";
    }
    if (message_.empty()) {
      return std::string(code_name(code_));
    }
    std::string out(code_name(code_));
    out += ": ";
    out += message_;
    return out;
  }

private:
  /// Builds a message by streaming all arguments.
  template <class... Args> static std::string build_message(Args &&...args) {
    std::ostringstream oss;
    (oss << ... << std::forward<Args>(args));
    return oss.str();
  }

  /// Returns the stable name for a status code.
  static std::string_view code_name(StatusCode code) {
    switch (code) {
    case StatusCode::kOK:
      return "OK";
    case StatusCode::kInvalid:
      return "Invalid";
    case StatusCode::kOutOfMemory:
      return "OutOfMemory";
    case StatusCode::kCancelled:
      return "Cancelled";
    case StatusCode::kIOError:
      return "IOError";
    case StatusCode::kNotImplemented:
      return "NotImplemented";
    default:
      return "UnknownError";
    }
  }

  StatusCode code_ = StatusCode::kOK;
  std::string message_;
};

template <class T> class Result {
public:
  /// Creates a successful result by copying a value.
  Result(const T &value) : storage_(value) {}
  /// Creates a successful result by moving a value.
  Result(T &&value) : storage_(std::move(value)) {}

  template <class U>
    requires(std::is_constructible_v<T, U &&> &&
             !std::is_same_v<std::decay_t<U>, T> &&
             !std::is_same_v<std::decay_t<U>, Status> &&
             !std::is_same_v<std::decay_t<U>, Result<T>>)
  /// Creates a successful result from a compatible value.
  Result(U &&value) : storage_(T(std::forward<U>(value))) {}

  /// Creates a failed result by copying a status.
  Result(const Status &st) : storage_(normalize_error(st)) {}
  /// Creates a failed result by moving a status.
  Result(Status &&st) : storage_(normalize_error(std::move(st))) {}

  /// Returns whether the operation succeeded.
  [[nodiscard]] bool ok() const { return std::holds_alternative<T>(storage_); }

  /// Returns the current status.
  [[nodiscard]] Status status() const {
    if (ok()) {
      return Status::OK();
    }
    return std::get<Status>(storage_);
  }

  /// Returns the contained value or throws on an error result.
  [[nodiscard]] T &ValueOrDie() & { return std::get<T>(storage_); }
  /// Returns the contained value or throws on an error result.
  [[nodiscard]] const T &ValueOrDie() const & { return std::get<T>(storage_); }
  /// Moves the contained value or throws on an error result.
  [[nodiscard]] T ValueOrDie() && { return std::move(std::get<T>(storage_)); }

  /// Returns the contained value.
  [[nodiscard]] T &operator*() & { return std::get<T>(storage_); }
  /// Returns the contained value.
  [[nodiscard]] const T &operator*() const & { return std::get<T>(storage_); }
  /// Returns the contained value.
  [[nodiscard]] T operator*() && { return std::move(std::get<T>(storage_)); }

private:
  /// Rejects successful statuses used as failed Result states.
  static Status normalize_error(Status st) {
    if (st.ok()) {
      return Status::Invalid("sanitize::Result constructed from OK status");
    }
    return st;
  }

  std::variant<T, Status> storage_;
};

} // namespace sanitize

#ifndef SAN_DETAIL_CONCAT_IMPL
#define SAN_DETAIL_CONCAT_IMPL(x, y) x##y
#endif

#ifndef SAN_DETAIL_CONCAT
#define SAN_DETAIL_CONCAT(x, y) SAN_DETAIL_CONCAT_IMPL(x, y)
#endif

#ifndef SAN_RETURN_NOT_OK_IMPL
#define SAN_RETURN_NOT_OK_IMPL(status_name, expr)                              \
  do {                                                                         \
    auto status_name = (expr);                                                 \
    if (!status_name.ok()) {                                                   \
      return status_name;                                                      \
    }                                                                          \
  } while (false)
#endif

#ifndef SAN_RETURN_NOT_OK
#define SAN_RETURN_NOT_OK(expr)                                                \
  SAN_RETURN_NOT_OK_IMPL(SAN_DETAIL_CONCAT(_san_status_, __LINE__), expr)
#endif

#ifndef SAN_ASSIGN_OR_RAISE_IMPL
#define SAN_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr)                      \
  auto result_name = (rexpr);                                                  \
  if (!result_name.ok())                                                       \
    return result_name.status();                                               \
  lhs = std::move(result_name).ValueOrDie()
#endif

#ifndef SAN_ASSIGN_OR_RAISE
#define SAN_ASSIGN_OR_RAISE(lhs, rexpr)                                        \
  SAN_ASSIGN_OR_RAISE_IMPL(SAN_DETAIL_CONCAT(_san_result_, __LINE__), lhs,     \
                           rexpr)
#endif
