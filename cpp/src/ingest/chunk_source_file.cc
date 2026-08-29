// Implements local-file chunk sources and bounded file checks. The helpers
// enforce memory and descriptor limits while preserving stable chunk-view
// lifetimes.

#include "ingest/chunk_source_detail.hh"
#include "ingest/secure_read_only_file.hh"
#include "internal/memory/memory_budget.hh"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <sys/mman.h>
#endif

namespace sanitize {
namespace {

enum class DetectedCompression : std::uint8_t { kNone = 0, kGzip };

/// Rejects mapped files whose size exceeds addressable or configured input
/// bounds.
sanitize::Status validate_mapped_file_size(const std::string &path,
                                           std::uint64_t size,
                                           std::uint64_t limit) {
  return internal::validate_materialized_input_growth(
      "FileChunkSource: mapped file", path, 0, size, limit);
}

/// Inspects compression without consuming bytes from the chunk source.
DetectedCompression sniff_compression(const std::uint8_t *data,
                                      std::size_t size) {
  if (!data || size < 2) {
    return DetectedCompression::kNone;
  }
  return data[0] == 0x1f && data[1] == 0x8b ? DetectedCompression::kGzip
                                            : DetectedCompression::kNone;
}

/// Inspects file compression without consuming bytes from the chunk source.
sanitize::Result<DetectedCompression>
sniff_file_compression(const std::string &path) {
  SAN_ASSIGN_OR_RAISE(auto input, internal::SecureReadOnlyFile::Open(
                                      path, "compression detection"));
  std::array<std::uint8_t, 2> magic{};
  SAN_ASSIGN_OR_RAISE(
      const auto read,
      input.Read(reinterpret_cast<char *>(magic.data()), magic.size()));
  return sniff_compression(magic.data(), read);
}

struct MappedFile {
  internal::SecureReadOnlyFile file;
#if defined(_WIN32)
  HANDLE mapping = nullptr;
#endif
  const char *data = nullptr;
  std::size_t size = 0;

  /// Releases resources retained by `MappedFile` without propagating cleanup
  /// failures.
  ~MappedFile() noexcept {
#if defined(_WIN32)
    if (data) {
      UnmapViewOfFile(data);
    }
    if (mapping) {
      CloseHandle(mapping);
    }
#else
    if (data && size > 0) {
      munmap(const_cast<char *>(data), size);
    }
#endif
  }
};

/// Maps file read only with read-only ownership and platform-specific cleanup.
sanitize::Result<std::shared_ptr<MappedFile>>
map_file_read_only(const std::string &path, std::uint64_t limit) {
  auto mapped = std::make_shared<MappedFile>();
  SAN_ASSIGN_OR_RAISE(mapped->file, internal::SecureReadOnlyFile::Open(
                                        path, "FileChunkSource"));
  const auto file_size = mapped->file.size();
  if (file_size >
      static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::OutOfMemory(
        "FileChunkSource: mapped file size is out of range: '", path, "'");
  }
  mapped->size = static_cast<std::size_t>(file_size);
  SAN_RETURN_NOT_OK(validate_mapped_file_size(path, file_size, limit));
  if (mapped->size == 0) {
    return mapped;
  }
#if defined(_WIN32)
  mapped->mapping =
      CreateFileMappingW(static_cast<HANDLE>(mapped->file.native_handle()),
                         nullptr, PAGE_READONLY, 0, 0, nullptr);
  if (!mapped->mapping) {
    return sanitize::Status::IOError(
        "FileChunkSource: CreateFileMappingW failed for '", path, "'");
  }
  mapped->data = static_cast<const char *>(
      MapViewOfFile(mapped->mapping, FILE_MAP_READ, 0, 0, 0));
  if (!mapped->data) {
    return sanitize::Status::IOError(
        "FileChunkSource: MapViewOfFile failed for '", path, "'");
  }
#else
  void *view = mmap(nullptr, mapped->size, PROT_READ, MAP_PRIVATE,
                    mapped->file.native_handle(), 0);
  if (view == MAP_FAILED) {
    return sanitize::Status::IOError("FileChunkSource: mmap failed for '", path,
                                     "'");
  }
  if (!view) {
    (void)munmap(view, mapped->size);
    return sanitize::Status::IOError(
        "FileChunkSource: mmap returned an unsupported null address for '",
        path, "'");
  }
  mapped->data = static_cast<const char *>(view);
#if defined(MADV_SEQUENTIAL)
  (void)madvise(view, mapped->size, MADV_SEQUENTIAL);
#endif
#endif
  return mapped;
}

class FileChunkSource final : public ChunkSource {
public:
  /// Initializes a bounded file source for mapped or transparently decompressed
  /// input.
  FileChunkSource(std::string path, std::int64_t memory_limit_bytes)
      : path_(std::move(path)),
        materialized_limit_(static_cast<std::uint64_t>(
            internal::memory_budget_from_limit(memory_limit_bytes)
                .materialized_input_bytes)) {}

  /// Releases the securely opened input through its non-throwing RAII owner.
  ~FileChunkSource() override = default;

  /// Rewinds the file source and clears its per-pass cursor state.
  sanitize::Status Reset() override {
    if (!input_.Close()) {
      return sanitize::Status::IOError("FileChunkSource: failed closing input");
    }
    pos_ = 0;
    eof_ = false;
    full_owner_.reset();
    full_data_ = {};
    return {};
  }

  /// Reads the next bounded file chunk and advances its absolute byte offset.
  sanitize::Result<Chunk> NextChunk(std::int64_t max_bytes) override {
    SAN_RETURN_NOT_OK(
        internal::validate_chunk_request(max_bytes, "FileChunkSource"));
    if (eof_) {
      Chunk chunk;
      chunk.base_offset = pos_;
      return chunk;
    }
    SAN_RETURN_NOT_OK(open_if_needed());

    const auto requested = static_cast<std::size_t>(max_bytes);
    auto bytes = std::make_shared<std::string>(requested, '\0');
    SAN_ASSIGN_OR_RAISE(const auto read, input_.Read(bytes->data(), requested));
    if (read == 0U) {
      eof_ = true;
      Chunk chunk;
      chunk.base_offset = pos_;
      return chunk;
    }

    bytes->resize(read);
    Chunk chunk;
    chunk.owner = bytes;
    chunk.data = std::string_view(*bytes);
    chunk.base_offset = pos_;
    pos_ += read;
    return chunk;
  }

  /// Exposes the complete file bytes without extending their documented
  /// lifetime.
  sanitize::Result<Chunk> View() override {
    if (!full_owner_) {
      SAN_ASSIGN_OR_RAISE(auto mapped,
                          map_file_read_only(path_, materialized_limit_));
      full_data_ = mapped->size == 0
                       ? std::string_view{}
                       : std::string_view(mapped->data, mapped->size);
      full_owner_ = std::move(mapped);
    }
    Chunk chunk;
    chunk.owner = full_owner_;
    chunk.data = full_data_;
    return chunk;
  }

private:
  /// Lazily acquires a descriptor permit and opens the source in binary mode.
  sanitize::Status open_if_needed() {
    if (input_.is_open()) {
      return {};
    }
    SAN_ASSIGN_OR_RAISE(
        input_, internal::SecureReadOnlyFile::Open(path_, "FileChunkSource"));
    return {};
  }

  std::string path_;
  std::uint64_t materialized_limit_ = 0;
  internal::SecureReadOnlyFile input_;
  std::size_t pos_ = 0;
  bool eof_ = false;
  std::shared_ptr<const void> full_owner_;
  std::string_view full_data_;
};

} // namespace

namespace internal {

sanitize::Status validate_chunk_request(std::int64_t max_bytes,
                                        std::string_view operation) {
  if (max_bytes <= 0) {
    return sanitize::Status::Invalid(operation, ": max_bytes must be > 0");
  }
  if (max_bytes > kMaxChunkRequestBytes) {
    return sanitize::Status::Invalid(
        operation, ": max_bytes exceeds safety limit: ", max_bytes, " bytes > ",
        kMaxChunkRequestBytes, " bytes");
  }
  return {};
}

sanitize::Status validate_materialized_input_growth(std::string_view operation,
                                                    std::string_view source,
                                                    std::uint64_t current,
                                                    std::uint64_t additional,
                                                    std::uint64_t limit) {
  if (current > limit || additional > limit - current) {
    return sanitize::Status::OutOfMemory(
        operation, " exceeds configured limit: ", current, " + ", additional,
        " bytes > ", limit, " bytes for '", source, "'");
  }
  return {};
}

/// Rejects compressed input on routes that require an uncompressed core file
/// source.
sanitize::Status ensure_uncompressed_file(const std::string &path,
                                          std::string_view operation) {
  SAN_ASSIGN_OR_RAISE(const auto compression, sniff_file_compression(path));
  if (compression == DetectedCompression::kNone) {
    return {};
  }
  return sanitize::Status::NotImplemented(
      operation, ": compressed input is not available in core runtime; provide "
                 "decompressed input or use an adapter path");
}

/// Creates a shared file-backed source using the configured materialized-input
/// limit.
ChunkSourcePtr make_file_chunk_source(std::string path,
                                      std::int64_t memory_limit_bytes) {
  return std::make_shared<FileChunkSource>(std::move(path), memory_limit_bytes);
}

} // namespace internal
} // namespace sanitize
