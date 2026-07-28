// Defines immutable top-level JSON field spans handed from validation to
// workers.

#pragma once

#include "sanitize/core/row_stream.hh"

#include <cstddef>
#include <cstdint>

namespace sanitize::internal {

// One validated object member. Offsets are relative to RowRef::raw. Key and
// value ends are reconstructed from the canonical object separators, halving
// the token footprint without rescanning either JSON value.
struct JsonValidatedFieldToken {
  std::uint32_t key_offset = 0;
  std::uint32_t value_offset = 0;
};

static_assert(sizeof(JsonValidatedFieldToken) == 8);

// Immutable view finalized after one frontend batch has been scanned. The
// frontend owner retained by RowBatch keeps both this descriptor and its token
// storage alive until all worker packets have completed.
struct JsonValidatedRowTokens {
  const JsonValidatedFieldToken *fields = nullptr;
  std::uint32_t field_offset = 0;
  std::uint32_t field_count = 0;
};

[[nodiscard]] inline const JsonValidatedRowTokens *
json_validated_row_tokens(const sanitize::RowRef &row) noexcept {
  constexpr auto token_flag =
      std::to_underlying(sanitize::RowFlags::kJsonValidatedTokens);
  if ((row.flags & token_flag) == 0 || !row.direct_ctx) {
    return nullptr;
  }
  return static_cast<const JsonValidatedRowTokens *>(row.direct_ctx);
}

[[nodiscard]] inline bool json_validated_row_tokens_are_plan_ordered(
    const sanitize::RowRef &row) noexcept {
  constexpr auto ordered_flag =
      std::to_underlying(sanitize::RowFlags::kJsonPlanOrderedTokens);
  return (row.flags & ordered_flag) != 0;
}

} // namespace sanitize::internal
