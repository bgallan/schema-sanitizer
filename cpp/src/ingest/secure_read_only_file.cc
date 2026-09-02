// Implements race-checked local input opening without following final links or
// accepting non-regular filesystem objects. Pre-open and handle-level checks,
// bounded reads, and fail-closed descriptor accounting share one RAII owner.

#include "ingest/secure_read_only_file.hh"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <utility>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <filesystem>
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace sanitize::internal {

SecureReadOnlyFile::SecureReadOnlyFile(SecureReadOnlyFile &&other) noexcept
    : fd_lease_(std::move(other.fd_lease_)),
#if defined(_WIN32)
      handle_(std::exchange(other.handle_, nullptr)),
#else
      descriptor_(std::exchange(other.descriptor_, -1)),
#endif
      size_(std::exchange(other.size_, 0U)) {
}

SecureReadOnlyFile &
SecureReadOnlyFile::operator=(SecureReadOnlyFile &&other) noexcept {
  if (this != &other) {
    (void)Close();
    fd_lease_ = std::move(other.fd_lease_);
#if defined(_WIN32)
    handle_ = std::exchange(other.handle_, nullptr);
#else
    descriptor_ = std::exchange(other.descriptor_, -1);
#endif
    size_ = std::exchange(other.size_, 0U);
  }
  return *this;
}

SecureReadOnlyFile::~SecureReadOnlyFile() noexcept { (void)Close(); }

sanitize::Result<SecureReadOnlyFile>
SecureReadOnlyFile::Open(const std::string &path, std::string_view operation) {
  SecureReadOnlyFile file;
  file.fd_lease_ = internal::ProcessFdPermitLease(1U);
  if (!file.fd_lease_) {
    return sanitize::Status::IOError(
        operation, ": file descriptor capacity exhausted for '", path, "'");
  }

#if defined(_WIN32)
  const auto utf8_path = std::u8string(path.begin(), path.end());
  const auto native_path = std::filesystem::path(utf8_path).wstring();
  const auto path_attributes = GetFileAttributesW(native_path.c_str());
  if (path_attributes == INVALID_FILE_ATTRIBUTES) {
    return sanitize::Status::IOError(
        operation, ": failed to inspect input file '", path, "'");
  }
  if ((path_attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0U) {
    return sanitize::Status::Invalid(operation,
                                     ": symbolic links and reparse points are "
                                     "not accepted: '",
                                     path, "'");
  }
  if ((path_attributes & FILE_ATTRIBUTE_DIRECTORY) != 0U) {
    return sanitize::Status::Invalid(
        operation, ": input must be a regular file: '", path, "'");
  }

  HANDLE handle =
      CreateFileW(native_path.c_str(), GENERIC_READ,
                  FILE_SHARE_READ | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
                  FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN |
                      FILE_FLAG_OPEN_REPARSE_POINT,
                  nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    return sanitize::Status::IOError(operation, ": failed to open input file '",
                                     path, "'");
  }
  file.handle_ = handle;
  file.fd_lease_.mark_opened();

  FILE_ATTRIBUTE_TAG_INFO tag_info{};
  if (!GetFileInformationByHandleEx(handle, FileAttributeTagInfo, &tag_info,
                                    sizeof(tag_info))) {
    (void)file.Close();
    return sanitize::Status::IOError(
        operation, ": failed to inspect opened input file '", path, "'");
  }
  if ((tag_info.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0U) {
    (void)file.Close();
    return sanitize::Status::Invalid(operation,
                                     ": symbolic links and reparse points are "
                                     "not accepted: '",
                                     path, "'");
  }
  if (GetFileType(handle) != FILE_TYPE_DISK) {
    (void)file.Close();
    return sanitize::Status::Invalid(
        operation, ": input must be a regular disk file: '", path, "'");
  }
  LARGE_INTEGER file_size{};
  if (!GetFileSizeEx(handle, &file_size) || file_size.QuadPart < 0) {
    (void)file.Close();
    return sanitize::Status::IOError(
        operation, ": failed to determine input file size: '", path, "'");
  }
  file.size_ = static_cast<std::uint64_t>(file_size.QuadPart);
#else
  struct stat path_metadata{};
  if (lstat(path.c_str(), &path_metadata) != 0) {
    return sanitize::Status::IOError(
        operation, ": failed to inspect input file '", path, "'");
  }
  if (S_ISLNK(path_metadata.st_mode)) {
    return sanitize::Status::Invalid(
        operation, ": symbolic links are not accepted: '", path, "'");
  }
  if (!S_ISREG(path_metadata.st_mode) || path_metadata.st_size < 0) {
    return sanitize::Status::Invalid(
        operation, ": input must be a regular file: '", path, "'");
  }

  int flags = O_RDONLY;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
  flags |= O_NOFOLLOW;
#endif
  const int descriptor = openat(AT_FDCWD, path.c_str(), flags);
  if (descriptor < 0) {
    return sanitize::Status::IOError(operation, ": failed to open input file '",
                                     path, "'");
  }
  file.descriptor_ = descriptor;
  file.fd_lease_.mark_opened();

  struct stat opened_metadata{};
  if (fstat(descriptor, &opened_metadata) != 0) {
    (void)file.Close();
    return sanitize::Status::IOError(
        operation, ": failed to inspect opened input file '", path, "'");
  }
  if (!S_ISREG(opened_metadata.st_mode) || opened_metadata.st_size < 0) {
    (void)file.Close();
    return sanitize::Status::Invalid(
        operation, ": opened input is not a regular file: '", path, "'");
  }
  if (path_metadata.st_dev != opened_metadata.st_dev ||
      path_metadata.st_ino != opened_metadata.st_ino) {
    (void)file.Close();
    return sanitize::Status::Invalid(
        operation, ": input path changed while it was being opened: '", path,
        "'");
  }
  file.size_ = static_cast<std::uint64_t>(opened_metadata.st_size);
#endif
  return file;
}

sanitize::Result<std::size_t> SecureReadOnlyFile::Read(char *buffer,
                                                       std::size_t capacity) {
  if (!is_open()) {
    return sanitize::Status::Invalid(
        "SecureReadOnlyFile: cannot read a closed input");
  }
  if (capacity == 0U) {
    return std::size_t{0};
  }
#if defined(_WIN32)
  const auto requested = static_cast<DWORD>(std::min<std::size_t>(
      capacity, static_cast<std::size_t>(std::numeric_limits<DWORD>::max())));
  DWORD read = 0;
  if (!ReadFile(static_cast<HANDLE>(handle_), buffer, requested, &read,
                nullptr)) {
    return sanitize::Status::IOError(
        "SecureReadOnlyFile: failed reading input");
  }
  return static_cast<std::size_t>(read);
#else
  const auto requested = std::min<std::size_t>(
      capacity, static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
  ssize_t read = -1;
  do {
    read = ::read(descriptor_, buffer, requested);
  } while (read < 0 && errno == EINTR);
  if (read < 0) {
    return sanitize::Status::IOError(
        "SecureReadOnlyFile: failed reading input");
  }
  return static_cast<std::size_t>(read);
#endif
}

bool SecureReadOnlyFile::Close() noexcept {
  if (!is_open()) {
    fd_lease_.reset();
    size_ = 0U;
    return true;
  }
#if defined(_WIN32)
  const bool closed = CloseHandle(static_cast<HANDLE>(handle_)) != 0;
  handle_ = nullptr;
#else
  const bool closed = close(descriptor_) == 0;
  descriptor_ = -1;
#endif
  fd_lease_.commit_physical_close(closed);
  fd_lease_.reset();
  size_ = 0U;
  return closed;
}

bool SecureReadOnlyFile::is_open() const noexcept {
#if defined(_WIN32)
  return handle_ != nullptr;
#else
  return descriptor_ >= 0;
#endif
}

} // namespace sanitize::internal
