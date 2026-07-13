// Declares the XML frontend state shared by lifecycle and batching units.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "frontends/builtin_frontends.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/parsing/streaming/xml/row_scanner.hh"
#include "internal/parsing/xml/document.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal::xml_frontend_detail {

struct BatchStorage {
  FlatRowBatch batch;
  std::vector<std::string> raw_rows;
  std::vector<std::unique_ptr<XmlNode>> nodes;
};

class XmlFrontend final {
public:
  XmlFrontend(ChunkSourcePtr src, const Options &options);

  void reset() noexcept;
  sanitize::Result<RowBatch> next_batch(int64_t capacity);

private:
  sanitize::Result<std::string_view> read_source_text();
  sanitize::Status parse_once();
  void select_rows();
  void append_object_fields(BatchStorage *storage, const XmlNode *node) const;
  void append_row(BatchStorage *storage, const XmlNode *node,
                  std::string_view raw, std::size_t base_offset) const;

  ChunkSourcePtr src_;
  std::shared_ptr<const void> source_owner_;
  std::string owned_text_;
  std::string_view source_text_;
  sanitize::Status parse_status_ = sanitize::Status::OK();

  std::string default_key_;
  std::uint64_t default_key_hash_ = 0;
  std::string row_tag_;
  int64_t chunk_bytes_ = int64_t{1} << 20;
  int64_t memory_limit_bytes_ = -1;
  std::unique_ptr<XmlRowTagScanner> scanner_;
  std::unique_ptr<XmlNode> root_;
  std::vector<const XmlNode *> rows_;

  std::size_t row_index_ = 0;
  bool done_ = false;
};

} // namespace sanitize::internal::xml_frontend_detail
