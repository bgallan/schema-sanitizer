// Implements logical streams composed from multiple local paths.

#include "ingest/chunk_source_detail.hh"
#include "internal/memory/memory_budget.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize {
namespace {

constexpr std::int64_t kMaterializationReadBytes = 1024 * 1024;

class MultiPathChunkSource final : public ChunkSource {
public:
  MultiPathChunkSource(std::vector<std::string> paths,
                       std::vector<std::string> source_names,
                       std::string separator, internal::TextEncoding encoding,
                       std::int64_t memory_limit_bytes)
      : paths_(std::move(paths)),
        separator_(std::make_shared<std::string>(std::move(separator))),
        encoding_(encoding), memory_limit_bytes_(memory_limit_bytes),
        materialized_limit_(static_cast<std::uint64_t>(
            internal::memory_budget_from_limit(memory_limit_bytes)
                .materialized_input_bytes)) {
    source_names_.reserve(source_names.size());
    for (auto &name : source_names) {
      source_names_.push_back(
          std::make_shared<const std::string>(std::move(name)));
    }
  }

  sanitize::Status Reset() override {
    current_.reset();
    current_source_name_.reset();
    current_source_index_ = 0;
    index_ = 0;
    source_base_offset_ = 0;
    separator_pending_ = false;
    separator_pos_ = 0;
    full_view_.reset();
    return {};
  }

  sanitize::Result<Chunk> NextChunk(std::int64_t max_bytes) override {
    SAN_RETURN_NOT_OK(
        internal::validate_chunk_request(max_bytes, "MultiPathChunkSource"));
    for (;;) {
      if (separator_pending_) {
        return take_separator_chunk(max_bytes);
      }
      if (!current_) {
        if (index_ >= paths_.size()) {
          Chunk chunk;
          chunk.base_offset = source_base_offset_;
          return chunk;
        }
        current_source_name_.reset();
        if (index_ < source_names_.size()) {
          current_source_name_ = source_names_[index_];
          current_source_index_ = index_;
        }
        SAN_ASSIGN_OR_RAISE(current_, make_child_source(paths_[index_]));
        ++index_;
      }

      SAN_ASSIGN_OR_RAISE(auto chunk, current_->NextChunk(max_bytes));
      if (!chunk.data.empty()) {
        if (chunk.base_offset >
            std::numeric_limits<std::size_t>::max() - source_base_offset_) {
          return sanitize::Status::OutOfMemory(
              "MultiPathChunkSource: source offset overflow");
        }
        chunk.base_offset += source_base_offset_;
        if (current_source_name_) {
          chunk.source_name_owner = current_source_name_;
          chunk.source_name = std::string_view(*current_source_name_);
          chunk.source_index = current_source_index_;
          chunk.has_source_index = true;
        }
        return chunk;
      }
      if (chunk.base_offset >
          std::numeric_limits<std::size_t>::max() - source_base_offset_) {
        return sanitize::Status::OutOfMemory(
            "MultiPathChunkSource: source offset overflow");
      }
      source_base_offset_ += chunk.base_offset;
      current_.reset();
      if (index_ < paths_.size() && !separator_->empty()) {
        separator_pending_ = true;
        separator_pos_ = 0;
      }
    }
  }

  sanitize::Result<Chunk> View() override {
    if (!full_view_) {
      auto bytes = std::make_shared<std::string>();
      for (std::size_t index = 0; index < paths_.size(); ++index) {
        SAN_ASSIGN_OR_RAISE(auto source, make_child_source(paths_[index]));
        if (index > 0) {
          SAN_RETURN_NOT_OK(internal::validate_materialized_input_growth(
              "MultiPathChunkSource: separator materialization", paths_[index],
              static_cast<std::uint64_t>(bytes->size()),
              static_cast<std::uint64_t>(separator_->size()),
              materialized_limit_));
          bytes->append(*separator_);
        }
        for (;;) {
          SAN_ASSIGN_OR_RAISE(
              auto chunk, source->NextChunk(kMaterializationReadBytes));
          if (chunk.data.empty()) {
            break;
          }
          SAN_RETURN_NOT_OK(internal::validate_materialized_input_growth(
              "MultiPathChunkSource: full view", paths_[index],
              static_cast<std::uint64_t>(bytes->size()),
              static_cast<std::uint64_t>(chunk.data.size()),
              materialized_limit_));
          bytes->append(chunk.data);
        }
      }
      full_view_ = std::move(bytes);
    }
    Chunk chunk;
    chunk.owner = full_view_;
    chunk.data = std::string_view(*full_view_);
    return chunk;
  }

private:
  sanitize::Result<Chunk> take_separator_chunk(std::int64_t max_bytes) {
    const auto available = separator_->size() - separator_pos_;
    const auto take = std::min<std::size_t>(
        available, static_cast<std::size_t>(max_bytes));
    if (take > std::numeric_limits<std::size_t>::max() - source_base_offset_) {
      return sanitize::Status::OutOfMemory(
          "MultiPathChunkSource: source offset overflow");
    }
    Chunk chunk;
    chunk.owner = separator_;
    chunk.data = std::string_view(*separator_).substr(separator_pos_, take);
    chunk.base_offset = source_base_offset_;
    separator_pos_ += take;
    source_base_offset_ += take;
    if (separator_pos_ == separator_->size()) {
      separator_pending_ = false;
      separator_pos_ = 0;
    }
    return chunk;
  }

  sanitize::Result<ChunkSourcePtr>
  make_child_source(const std::string &path) const {
    if (encoding_ == internal::TextEncoding::kUtf8) {
      return chunk_source_from_path(path, memory_limit_bytes_);
    }
    return chunk_source_from_path_with_encoding(
        path, internal::text_encoding_name(encoding_), memory_limit_bytes_);
  }

  std::vector<std::string> paths_;
  std::vector<std::shared_ptr<const std::string>> source_names_;
  std::shared_ptr<std::string> separator_;
  internal::TextEncoding encoding_ = internal::TextEncoding::kUtf8;
  std::int64_t memory_limit_bytes_ = -1;
  std::uint64_t materialized_limit_ = 0;
  ChunkSourcePtr current_;
  std::shared_ptr<const std::string> current_source_name_;
  std::size_t current_source_index_ = 0;
  std::size_t index_ = 0;
  std::size_t source_base_offset_ = 0;
  bool separator_pending_ = false;
  std::size_t separator_pos_ = 0;
  std::shared_ptr<std::string> full_view_;
};

} // namespace

namespace internal {

ChunkSourcePtr
make_multi_path_chunk_source(std::vector<std::string> paths,
                             std::vector<std::string> source_names,
                             std::string separator, TextEncoding encoding,
                             std::int64_t memory_limit_bytes) {
  return std::make_shared<MultiPathChunkSource>(
      std::move(paths), std::move(source_names), std::move(separator), encoding,
      memory_limit_bytes);
}

} // namespace internal
} // namespace sanitize
