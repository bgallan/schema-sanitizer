// Implements local-file chunk sources and bounded file checks.

#include "ingest/chunk_source_detail.hh"
#include "internal/memory/memory_budget.hh"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
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
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace sanitize {
namespace {

enum class DetectedCompression : std::uint8_t { kNone = 0, kGzip };

sanitize::Status validate_mapped_file_size(const std::string &path,
                                           std::uint64_t size,
                                           std::uint64_t limit) {
  return internal::validate_materialized_input_growth(
      "FileChunkSource: mapped file", path, 0, size, limit);
}

DetectedCompression sniff_compression(const std::uint8_t *data,
                                      std::size_t size) {
  if (!data || size < 2) {
    return DetectedCompression::kNone;
  }
  return data[0] == 0x1f && data[1] == 0x8b ? DetectedCompression::kGzip
                                            : DetectedCompression::kNone;
}

sanitize::Result<DetectedCompression>
sniff_file_compression(const std::string &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.good()) {
    return sanitize::Status::Invalid("failed to open input file '", path, "'");
  }
  std::array<std::uint8_t, 2> magic{};
  input.read(reinterpret_cast<char *>(magic.data()),
             static_cast<std::streamsize>(magic.size()));
  return sniff_compression(magic.data(),
                           static_cast<std::size_t>(input.gcount()));
}

struct MappedFile {
#if defined(_WIN32)
  HANDLE file = INVALID_HANDLE_VALUE;
  HANDLE mapping = nullptr;
#else
  int file = -1;
#endif
  const char *data = nullptr;
  std::size_t size = 0;

  ~MappedFile() {
#if defined(_WIN32)
    if (data) {
      UnmapViewOfFile(data);
    }
    if (mapping) {
      CloseHandle(mapping);
    }
    if (file != INVALID_HANDLE_VALUE) {
      CloseHandle(file);
    }
#else
    if (data && size > 0) {
      munmap(const_cast<char *>(data), size);
    }
    if (file >= 0) {
      close(file);
    }
#endif
  }
};

sanitize::Result<std::shared_ptr<MappedFile>>
map_file_read_only(const std::string &path, std::uint64_t limit) {
  auto mapped = std::make_shared<MappedFile>();
#if defined(_WIN32)
  const auto utf8_path = std::u8string(path.begin(), path.end());
  const auto native_path = std::filesystem::path(utf8_path).wstring();
  mapped->file =
      CreateFileW(native_path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr,
                  OPEN_EXISTING,
                  FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
  if (mapped->file == INVALID_HANDLE_VALUE) {
    return sanitize::Status::IOError(
        "FileChunkSource: CreateFileW failed for '", path, "'");
  }
  if (GetFileType(mapped->file) != FILE_TYPE_DISK) {
    return sanitize::Status::Invalid(
        "FileChunkSource: memory mapping requires a regular disk file: '", path,
        "'");
  }
  LARGE_INTEGER file_size{};
  if (!GetFileSizeEx(mapped->file, &file_size) || file_size.QuadPart < 0 ||
      static_cast<std::uint64_t>(file_size.QuadPart) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::OutOfMemory(
        "FileChunkSource: mapped file size is out of range: '", path, "'");
  }
  mapped->size = static_cast<std::size_t>(file_size.QuadPart);
  SAN_RETURN_NOT_OK(validate_mapped_file_size(
      path, static_cast<std::uint64_t>(mapped->size), limit));
  if (mapped->size == 0) {
    return mapped;
  }
  mapped->mapping =
      CreateFileMappingW(mapped->file, nullptr, PAGE_READONLY, 0, 0, nullptr);
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
  int open_flags = O_RDONLY;
#ifdef O_CLOEXEC
  open_flags |= O_CLOEXEC;
#endif
  mapped->file = open(path.c_str(), open_flags);
  if (mapped->file < 0) {
    return sanitize::Status::IOError("FileChunkSource: open failed for '", path,
                                     "'");
  }
  struct stat metadata{};
  if (fstat(mapped->file, &metadata) != 0 || metadata.st_size < 0 ||
      !S_ISREG(metadata.st_mode) ||
      static_cast<std::uint64_t>(metadata.st_size) >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    return sanitize::Status::OutOfMemory(
        "FileChunkSource: mapped file size is out of range: '", path, "'");
  }
  mapped->size = static_cast<std::size_t>(metadata.st_size);
  SAN_RETURN_NOT_OK(validate_mapped_file_size(
      path, static_cast<std::uint64_t>(mapped->size), limit));
  if (mapped->size == 0) {
    return mapped;
  }
  void *view =
      mmap(nullptr, mapped->size, PROT_READ, MAP_PRIVATE, mapped->file, 0);
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
  FileChunkSource(std::string path, std::int64_t memory_limit_bytes)
      : path_(std::move(path)),
        materialized_limit_(static_cast<std::uint64_t>(
            internal::memory_budget_from_limit(memory_limit_bytes)
                .materialized_input_bytes)) {}

  sanitize::Status Reset() override {
    input_.close();
    input_.clear();
    pos_ = 0;
    eof_ = false;
    full_owner_.reset();
    full_data_ = {};
    return {};
  }

  sanitize::Result<Chunk> NextChunk(std::int64_t max_bytes) override {
    SAN_RETURN_NOT_OK(
        internal::validate_chunk_request(max_bytes, "FileChunkSource"));
    if (eof_) {
      Chunk chunk;
      chunk.base_offset = pos_;
      return chunk;
    }
    SAN_RETURN_NOT_OK(open_if_needed());

    const auto max_stream =
        static_cast<std::int64_t>(std::numeric_limits<std::streamsize>::max());
    const auto requested = static_cast<std::streamsize>(
        std::min<std::int64_t>(max_bytes, max_stream));
    auto bytes = std::make_shared<std::string>(
        static_cast<std::size_t>(requested), '\0');
    input_.read(bytes->data(), requested);
    const auto read = input_.gcount();
    if (read <= 0) {
      eof_ = true;
      Chunk chunk;
      chunk.base_offset = pos_;
      return chunk;
    }

    bytes->resize(static_cast<std::size_t>(read));
    Chunk chunk;
    chunk.owner = bytes;
    chunk.data = std::string_view(*bytes);
    chunk.base_offset = pos_;
    pos_ += static_cast<std::size_t>(read);
    if (read < requested || input_.eof()) {
      eof_ = true;
    }
    return chunk;
  }

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
  sanitize::Status open_if_needed() {
    if (input_.is_open()) {
      return {};
    }
    input_.open(path_, std::ios::binary);
    if (!input_.good()) {
      return sanitize::Status::Invalid("FileChunkSource: failed to open '",
                                       path_, "'");
    }
    return {};
  }

  std::string path_;
  std::uint64_t materialized_limit_ = 0;
  std::ifstream input_;
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

ChunkSourcePtr make_file_chunk_source(std::string path,
                                      std::int64_t memory_limit_bytes) {
  return std::make_shared<FileChunkSource>(std::move(path), memory_limit_bytes);
}

} // namespace internal
} // namespace sanitize
