// Owns one JSON frontend batch and its optional validated token index.

#pragma once

#include "frontends/json/text_row_materializer.hh"
#include "internal/memory/arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/json/validated_row.hh"
#include "internal/parsing/row_scanner.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <new>
#include <utility>
#include <vector>

namespace sanitize::internal {

struct JsonTextBatchStorage {
  JsonTextBatchStorage(std::shared_ptr<void> pool,
                       std::size_t arena_block_bytes)
      : pmr_pool(std::move(pool)), arena(pmr_pool.pool(), arena_block_bytes),
        doc(&pmr_pool), plan_ordered_scratch(&pmr_pool),
        validated_tokens(&pmr_pool), validated_rows(&pmr_pool) {}

  void configure_token_index(std::int64_t capacity,
                             std::size_t max_tokens) noexcept {
    max_validated_tokens = max_tokens;
    if (capacity <= 0 || max_tokens == 0) {
      return;
    }
    try {
      validated_rows.reserve(static_cast<std::size_t>(capacity));
    } catch (const std::bad_alloc &) {
      max_validated_tokens = 0;
    }
  }

  [[nodiscard]] const JsonValidatedRowTokens *
  retain_validated_row(std::uint32_t field_offset,
                       std::uint32_t field_count) noexcept {
    if (validated_rows.size() >= validated_rows.capacity()) {
      validated_tokens.resize(field_offset);
      max_validated_tokens = 0;
      return nullptr;
    }
    try {
      validated_rows.push_back(JsonValidatedRowTokens{
          .fields = nullptr,
          .field_offset = field_offset,
          .field_count = field_count,
      });
    } catch (const std::bad_alloc &) {
      validated_tokens.resize(field_offset);
      max_validated_tokens = 0;
      return nullptr;
    }
    return &validated_rows.back();
  }

  void finalize_validated_rows() noexcept {
    const auto *base = validated_tokens.data();
    for (auto &row : validated_rows) {
      row.fields = row.field_count == 0 ? nullptr : base + row.field_offset;
    }
  }

  void keep_data_owner(const std::shared_ptr<const void> &owner) {
    if (!owner || owner.get() == last_data_owner_ptr) {
      return;
    }
    last_data_owner_ptr = owner.get();
    keepalive.push_back(owner);
  }

  void keep_source_name(const std::shared_ptr<const std::string> &owner) {
    if (!owner || owner.get() == last_source_name_owner_ptr) {
      return;
    }
    last_source_name_owner_ptr = owner.get();
    keepalive.push_back(std::static_pointer_cast<const void>(owner));
  }

  void prepare_output_rows(std::int64_t capacity, std::size_t max_tokens,
                           bool direct_raw, std::vector<RowRef> *rows) {
    if (direct_raw) {
      rows->reserve(static_cast<std::size_t>(capacity));
    } else {
      batch.reset(capacity);
      configure_token_index(capacity, max_tokens);
    }
    arena.reset();
  }

  void append_deferred_raw(const TextSlice &slice, bool require_object_row,
                           std::vector<RowRef> *rows) {
    keep_data_owner(slice.owner);
    keep_source_name(slice.source_file_owner);
    rows->push_back(RowRef{
        .raw = slice.view,
        .base_offset = slice.base_offset,
        .source_file = slice.source_file,
        .flags = static_cast<std::uint8_t>(
            std::to_underlying(RowFlags::kRawOnly) |
            (require_object_row
                 ? std::to_underlying(RowFlags::kJsonObjectRequired)
                 : 0U)),
    });
  }

  void finish_output_rows(bool direct_raw, std::vector<RowRef> *rows) noexcept {
    if (!direct_raw) {
      finalize_validated_rows();
      batch.export_rows(rows);
    }
  }

  PoolResource pmr_pool;
  BumpArena arena;
  JsonOnDemandDoc doc;
  FlatRowBatch batch;
  PlanOrderedRowScratch plan_ordered_scratch;
  std::pmr::vector<JsonValidatedFieldToken> validated_tokens;
  std::pmr::vector<JsonValidatedRowTokens> validated_rows;
  std::vector<std::shared_ptr<const void>> keepalive;
  std::size_t max_validated_tokens = 0;
  const void *last_data_owner_ptr = nullptr;
  const void *last_source_name_owner_ptr = nullptr;
};

} // namespace sanitize::internal
