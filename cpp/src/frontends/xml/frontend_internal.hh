// Declares the XML frontend state shared by lifecycle and batching units. It
// frames XML rows, records parser diagnostics, and returns budget-owned batches
// in source order.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "frontends/builtin_frontends.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/parsing/streaming/xml/row_scanner.hh"
#include "internal/parsing/xml/document.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal::xml_frontend_detail {

struct BatchStorage final : public sanitize::RowBatchReleaser {

  /// Creates pool-backed XML rows and retains parsed chunks until row release.
  BatchStorage(std::shared_ptr<void> pool,
               std::shared_ptr<PoolResource> xml_resource,
               std::size_t arena_block_bytes)
      : pool_keepalive(std::move(pool)),
        resource_keepalive(std::move(xml_resource)),
        raw_arena(pool_keepalive.get(), arena_block_bytes),
        batch(resource_keepalive.get()), raw_rows(resource_keepalive.get()),
        nodes(resource_keepalive.get()) {}

  std::shared_ptr<void> pool_keepalive;
  std::shared_ptr<PoolResource> resource_keepalive;
  BumpArena raw_arena;
  FlatRowBatch batch;
  std::pmr::vector<std::string_view> raw_rows;
  std::pmr::vector<XmlNodePtr> nodes;
  ReaderResourceDiagnostics reader_diagnostics;

  /// Releases a completed XML row range and reclaims its retained parse
  /// storage.
  void ReleaseRows(std::size_t begin, std::size_t count) noexcept override {
    if (begin >= nodes.size()) {
      return;
    }
    const auto releasable = std::min(count, nodes.size() - begin);
    const auto end = begin + releasable;
    for (std::size_t index = begin; index < end; ++index) {
      nodes[index].reset();
    }
  }
};

class XmlFrontend final {
public:
  XmlFrontend(ChunkSourcePtr src, const Options &options);

  void reset() noexcept;
  void set_plan(const CompiledPlan *plan) noexcept;
  void set_memory_pool(std::shared_ptr<void> pool) noexcept;
  void set_task_arena(std::shared_ptr<OperationTaskArena> task_arena) noexcept;
  sanitize::Result<RowBatch> next_batch(int64_t capacity);

private:
  /// Allocates parser state and either opens a row scanner or parses the full
  /// document.
  sanitize::Status ensure_initialized();

  /// Obtains the full XML input as a stable view or a bounded pooled copy.
  sanitize::Result<std::string_view> read_source_text();

  /// Parses the complete document, builds its value model, and selects output
  /// rows.
  sanitize::Status parse_once();

  /// Selects the parsed document root as the whole-document output row.
  void select_rows();

  /// Parses streamed row fragments concurrently and appends results in source
  /// order.
  sanitize::Status
  append_streamed_rows_parallel(BatchStorage *storage,
                                std::span<const std::string_view> row_texts,
                                std::span<const std::size_t> base_offsets);

  /// Appends an XML object's named fields to the current flat row.
  void append_object_fields(BatchStorage *storage, const XmlNode *node) const;

  /// Appends one XML node as object fields or a fallback-keyed scalar value.
  void append_row(BatchStorage *storage, const XmlNode *node,
                  std::string_view raw, std::size_t base_offset) const;

  // Declare allocator owners before every PMR-backed object. Members are
  // destroyed in reverse declaration order, so parsed trees and buffers must
  // disappear before their memory resource and operation pool.
  std::shared_ptr<void> memory_pool_;
  std::shared_ptr<PoolResource> xml_resource_;

  ChunkSourcePtr src_;
  std::shared_ptr<const void> source_owner_;
  std::unique_ptr<std::pmr::string> owned_text_;
  std::string_view source_text_;
  sanitize::Status parse_status_ = sanitize::Status::OK();

  std::string default_key_;
  std::uint64_t default_key_hash_ = 0;
  std::string row_tag_;
  int64_t chunk_bytes_ = int64_t{1} << 20;
  int64_t memory_limit_bytes_ = -1;
  std::unique_ptr<XmlRowTagScanner> scanner_;
  XmlNodePtr root_;
  std::unique_ptr<std::pmr::vector<const XmlNode *>> rows_;

  std::size_t row_index_ = 0;
  bool initialized_ = false;
  bool execution_mode_ = false;
  bool done_ = false;
  ReaderResourceDiagnostics document_diagnostics_;
  bool document_diagnostics_emitted_ = false;
  std::shared_ptr<OperationTaskArena> task_arena_;
};

} // namespace sanitize::internal::xml_frontend_detail
