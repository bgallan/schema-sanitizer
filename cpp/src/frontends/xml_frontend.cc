// Connects XML parsing primitives to the frontend row-batch vtable.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "internal/frontends/builtin_frontends.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/parsing/streaming/xml_row_tag_scanner.hh"
#include "internal/parsing/xml_document.hh"
#include "sanitize/core/status.hh"
#include "sanitize/detail/hash.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

namespace {

struct BatchStorage {
  FlatRowBatch batch;
  std::vector<std::string> raw_rows;
  std::vector<std::unique_ptr<XmlNode>> nodes;
};

/// Frontend implementation for XML document and row-tag ingestion.
class XmlFrontend final {
public:
  /// Build an XML frontend from a chunk source and prepared options.
  XmlFrontend(ChunkSourcePtr src, const Options &options)
      : src_(std::move(src)), default_key_(options.default_key_name),
        row_tag_(options.xml_row_tag),
        chunk_bytes_((options.io_chunk_bytes > 0) ? options.io_chunk_bytes
                                                  : (int64_t{1} << 20)),
        memory_limit_bytes_(options.memory_limit_bytes) {
    if (row_tag_.empty()) {
      parse_status_ = parse_once();
    } else {
      scanner_ = std::make_unique<XmlRowTagScanner>(
          std::move(src_), row_tag_, chunk_bytes_, memory_limit_bytes_);
      parse_status_ = scanner_->Reset();
    }
  }

  /// Reset the frontend to emit rows from the beginning.
  void reset() noexcept {
    row_index_ = 0;
    done_ = false;
    if (scanner_) {
      parse_status_ = scanner_->Reset();
    }
  }

  /// Produce the next batch of XML rows.
  sanitize::Result<RowBatch> next_batch(int64_t capacity) {
    RowBatch out;
    if (capacity <= 0 || done_) {
      return out;
    }
    if (!parse_status_.ok()) {
      return parse_status_;
    }

    auto storage = std::make_shared<BatchStorage>();
    storage->batch.reset(capacity);
    storage->raw_rows.reserve(static_cast<std::size_t>(capacity));
    storage->nodes.reserve(static_cast<std::size_t>(capacity));

    if (scanner_) {
      int64_t produced = 0;
      while (produced < capacity) {
        SAN_ASSIGN_OR_RAISE(auto slice, scanner_->next_row());
        if (slice.text.empty()) {
          done_ = true;
          break;
        }

        storage->raw_rows.push_back(std::move(slice.text));
        std::string_view row_text(storage->raw_rows.back());
        XmlParser parser(row_text);
        SAN_ASSIGN_OR_RAISE(auto node, parser.parse_document());
        build_xml_node_model(node.get());
        const XmlNode *node_ptr = node.get();
        storage->nodes.push_back(std::move(node));
        append_row(storage.get(), node_ptr, row_text, slice.base_offset);
        produced++;
      }

      storage->batch.export_rows(&out.rows);
      out.owner = std::move(storage);
      return out;
    }

    int64_t produced = 0;
    while (produced < capacity && row_index_ < rows_.size()) {
      const XmlNode *node = rows_[row_index_++];
      std::string_view raw;
      std::size_t base_offset = 0;
      if (node && node->end_offset >= node->start_offset &&
          node->end_offset <= source_text_.size()) {
        raw = source_text_.substr(node->start_offset,
                                  node->end_offset - node->start_offset);
        base_offset = node->start_offset;
      }
      append_row(storage.get(), node, raw, base_offset);
      produced++;
    }
    if (row_index_ >= rows_.size()) {
      done_ = true;
    }

    storage->batch.export_rows(&out.rows);
    out.owner = std::move(storage);
    return out;
  }

private:
  sanitize::Result<std::string_view> read_source_text() {
    if (memory_limit_bytes_ <= 0) {
      SAN_ASSIGN_OR_RAISE(auto chunk, src_->View());
      source_owner_ = std::move(chunk.owner);
      return chunk.data;
    }

    SAN_RETURN_NOT_OK(src_->Reset());
    owned_text_.clear();
    for (;;) {
      SAN_ASSIGN_OR_RAISE(auto chunk, src_->NextChunk(chunk_bytes_));
      if (chunk.data.empty()) {
        break;
      }
      const auto limit = static_cast<std::uint64_t>(memory_limit_bytes_);
      const auto current = static_cast<std::uint64_t>(owned_text_.size());
      const auto incoming = static_cast<std::uint64_t>(chunk.data.size());
      if (incoming > limit || current > limit - incoming) {
        return sanitize::Status::OutOfMemory(
            "memory_limit_bytes limit exceeded during xml parsing: ",
            current + incoming, " bytes > ", memory_limit_bytes_, " bytes");
      }
      owned_text_.append(chunk.data);
    }
    return std::string_view(owned_text_);
  }

  sanitize::Status parse_once() {
    if (!src_) {
      return sanitize::Status::Invalid("XML frontend: source is null");
    }
    SAN_ASSIGN_OR_RAISE(source_text_, read_source_text());

    XmlParser parser(source_text_);
    SAN_ASSIGN_OR_RAISE(root_, parser.parse_document());
    build_xml_node_model(root_.get());
    select_rows();
    return sanitize::Status::OK();
  }

  void select_rows() {
    rows_.clear();
    if (!root_) {
      return;
    }
    rows_.push_back(root_.get());
  }

  void append_object_fields(BatchStorage *storage, const XmlNode *node) const {
    for (const XmlField &field : node->fields) {
      const std::string_view key(field.key);
      const uint64_t key_hash = sanitize::detail::hash_key64(key);
      storage->batch.push(FieldRef{
          .key = key,
          .key_hash = key_hash,
          .value = xml_field_to_value(field),
      });
    }
  }

  void append_row(BatchStorage *storage, const XmlNode *node,
                  std::string_view raw, std::size_t base_offset) const {
    storage->batch.start_row(raw, base_offset, 0, nullptr);
    const ValueView value = xml_node_to_value(node);
    if (value.is_object() && node) {
      append_object_fields(storage, node);
    } else {
      storage->batch.push(FieldRef{
          .key = default_key_,
          .key_hash = sanitize::detail::hash_key64(default_key_),
          .value = value,
      });
    }
    storage->batch.end_row();
  }

  ChunkSourcePtr src_;
  std::shared_ptr<const void> source_owner_;
  std::string owned_text_;
  std::string_view source_text_;
  sanitize::Status parse_status_ = sanitize::Status::OK();

  std::string default_key_;
  std::string row_tag_;
  int64_t chunk_bytes_ = int64_t{1} << 20;
  int64_t memory_limit_bytes_ = -1;
  std::unique_ptr<XmlRowTagScanner> scanner_;
  std::unique_ptr<XmlNode> root_;
  std::vector<const XmlNode *> rows_;

  std::size_t row_index_ = 0;
  bool done_ = false;
};

/// Reset an XML frontend through the C-style frontend vtable.
static void xml_reset(void *self) noexcept {
  static_cast<XmlFrontend *>(self)->reset();
}

/// Produce the next XML row batch through the frontend vtable.
static sanitize::Result<RowBatch> xml_next_batch(void *self, int64_t capacity) {
  return static_cast<XmlFrontend *>(self)->next_batch(capacity);
}

/// Ignore compiled-plan updates; XML ingestion has no plan-dependent setup.
static void xml_set_plan(void *, const CompiledPlan *) noexcept {}

/// Destroy an XML frontend through the frontend vtable.
static void xml_destroy(void *self) noexcept {
  delete static_cast<XmlFrontend *>(self);
}

static const FrontendVTable kXmlVTable{
    .reset = &xml_reset,
    .next_batch = &xml_next_batch,
    .set_plan = &xml_set_plan,
    .destroy = &xml_destroy,
};

} // namespace

FrontendHandle make_xml_frontend(ChunkSourcePtr xml, const Options &options) {
  auto *fe = new XmlFrontend(std::move(xml), options);
  return {fe, &kXmlVTable};
}

} // namespace sanitize::internal
