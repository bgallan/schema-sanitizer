// Declares a move-only regular-file owner that never follows a final symbolic
// link or Windows reparse point. Native handle lifetime remains coupled to the
// process descriptor governor across reads, mappings, moves, and close errors.

#pragma once

#include "internal/runtime/process_fd_governor.hh"
#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace sanitize::internal {

class SecureReadOnlyFile final {
public:
  /// Creates an empty secure file owner with no descriptor reservation.
  SecureReadOnlyFile() noexcept = default;
  /// Prevents two owners from closing or accounting for the same native file.
  SecureReadOnlyFile(const SecureReadOnlyFile &) = delete;
  /// Prevents duplicated native-handle and descriptor-permit ownership.
  SecureReadOnlyFile &operator=(const SecureReadOnlyFile &) = delete;
  /// Transfers an opened native file and its descriptor permit from `other`.
  SecureReadOnlyFile(SecureReadOnlyFile &&other) noexcept;
  /// Closes the current file before adopting `other`'s native ownership.
  SecureReadOnlyFile &operator=(SecureReadOnlyFile &&other) noexcept;
  /// Closes the native file and commits or retains its descriptor accounting.
  ~SecureReadOnlyFile() noexcept;

  /// Opens one unchanged final-path regular file without following a link.
  [[nodiscard]] static sanitize::Result<SecureReadOnlyFile>
  Open(const std::string &path, std::string_view operation);

  /// Reads at most `capacity` sequential bytes from the validated native file.
  [[nodiscard]] sanitize::Result<std::size_t> Read(char *buffer,
                                                   std::size_t capacity);
  /// Closes the native file and reports whether physical closure was proven.
  [[nodiscard]] bool Close() noexcept;
  /// Reports whether this owner still has an open native file handle.
  [[nodiscard]] bool is_open() const noexcept;
  /// Returns the validated size captured from the opened filesystem object.
  [[nodiscard]] std::uint64_t size() const noexcept { return size_; }

#if defined(_WIN32)
  /// Returns the validated Windows handle for read-only mapping operations.
  [[nodiscard]] void *native_handle() const noexcept { return handle_; }
#else
  /// Returns the validated POSIX descriptor for read-only mapping operations.
  [[nodiscard]] int native_handle() const noexcept { return descriptor_; }
#endif

private:
  internal::ProcessFdPermitLease fd_lease_;
#if defined(_WIN32)
  void *handle_ = nullptr;
#else
  int descriptor_ = -1;
#endif
  std::uint64_t size_ = 0;
};

} // namespace sanitize::internal
