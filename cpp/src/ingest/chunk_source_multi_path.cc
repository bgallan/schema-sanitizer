// Implements logical streams composed from multiple local paths.

#include "ingest/chunk_source_detail.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize {
namespace {

class MultiPathChunkSource final : public ChunkSource {
public:
  MultiPathChunkSource(std::vector<std::string> paths,
                       std::vector<std::string> source_names,
                       std::string separator, internal::TextEncoding encoding)
      : paths_(std::move(paths)),
        separator_(std::make_shared<std::string>(std::move(separator))),
        encoding_(encoding) {
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
    full_view_.reset();
    return {};
  }

  sanitize::Result<Chunk> NextChunk(std::int64_t max_bytes) override {
    if (max_bytes <= 0) {
      return sanitize::Status::Invalid("NextChunk: max_bytes must be > 0");
    }
    if (separator_pending_) {
      separator_pending_ = false;
      Chunk chunk;
      chunk.owner = separator_;
      chunk.data = std::string_view(*separator_);
      chunk.base_offset = source_base_offset_;
      source_base_offset_ += separator_->size();
      return chunk;
    }

    for (;;) {
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
        chunk.base_offset += source_base_offset_;
        if (current_source_name_) {
          chunk.source_name_owner = current_source_name_;
          chunk.source_name = std::string_view(*current_source_name_);
          chunk.source_index = current_source_index_;
          chunk.has_source_index = true;
        }
        return chunk;
      }
      current_.reset();
      if (index_ < paths_.size() && !separator_->empty()) {
        separator_pending_ = true;
      }
    }
  }

  sanitize::Result<Chunk> View() override {
    if (!full_view_) {
      auto bytes = std::make_shared<std::string>();
      for (std::size_t index = 0; index < paths_.size(); ++index) {
        SAN_ASSIGN_OR_RAISE(auto source, make_child_source(paths_[index]));
        SAN_ASSIGN_OR_RAISE(auto chunk, source->View());
        if (index > 0) {
          bytes->append(*separator_);
        }
        bytes->append(chunk.data);
      }
      full_view_ = std::move(bytes);
    }
    Chunk chunk;
    chunk.owner = full_view_;
    chunk.data = std::string_view(*full_view_);
    return chunk;
  }

private:
  sanitize::Result<ChunkSourcePtr>
  make_child_source(const std::string &path) const {
    if (encoding_ == internal::TextEncoding::kUtf8) {
      return chunk_source_from_path(path);
    }
    return chunk_source_from_path_with_encoding(
        path, internal::text_encoding_name(encoding_));
  }

  std::vector<std::string> paths_;
  std::vector<std::shared_ptr<const std::string>> source_names_;
  std::shared_ptr<std::string> separator_;
  internal::TextEncoding encoding_ = internal::TextEncoding::kUtf8;
  ChunkSourcePtr current_;
  std::shared_ptr<const std::string> current_source_name_;
  std::size_t current_source_index_ = 0;
  std::size_t index_ = 0;
  std::size_t source_base_offset_ = 0;
  bool separator_pending_ = false;
  std::shared_ptr<std::string> full_view_;
};

} // namespace

namespace internal {

ChunkSourcePtr
make_multi_path_chunk_source(std::vector<std::string> paths,
                             std::vector<std::string> source_names,
                             std::string separator, TextEncoding encoding) {
  return std::make_shared<MultiPathChunkSource>(std::move(paths),
                                                std::move(source_names),
                                                std::move(separator), encoding);
}

} // namespace internal
} // namespace sanitize
