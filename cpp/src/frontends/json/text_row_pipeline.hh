// Resolves JSON text row representation and validates deferred raw rows. The
// pipeline preserves source offsets and ownership while enforcing plan order
// and memory bounds.

#pragma once

#include "internal/parsing/json/validated_row.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string_view>
#include <vector>

namespace sanitize {
struct CompiledPlan;
struct Options;
} // namespace sanitize

namespace sanitize::internal {
class JsonOnDemandDoc;

[[nodiscard]] bool
parallel_json_row_frontend_enabled(const sanitize::Options &options) noexcept;

/// Returns whether a JSON error represents an operation-fatal safety ceiling.
/// Such errors bypass row-level recovery to prevent resource amplification.
[[nodiscard]] bool
json_error_exceeds_hard_safety_limit(const sanitize::Status &status) noexcept;

/// Derives the operation-wide top-level JSON token allowance from memory
/// limits. The returned count is shared across validation packets.
[[nodiscard]] std::size_t
json_token_index_max_fields(std::int64_t memory_limit_bytes) noexcept;

struct JsonTextRowPolicy {
  bool plan_ordered = false;
  bool raw_only = false;
  bool validate_raw = false;
};

[[nodiscard]] JsonTextRowPolicy resolve_json_text_row_policy(
    const sanitize::CompiledPlan *plan, bool line_delimited, bool stop_on_error,
    bool direct_rows, bool parallel_rows,
    sanitize::FrontendMaterializationMode mode) noexcept;

struct JsonTextRowValidation {
  bool tokenized_object = false;
  bool plan_ordered_tokens = false;
  std::uint32_t field_offset = 0;
  std::uint32_t field_count = 0;
};

/// Validates one deferred JSON row and can index top-level object field spans.
/// Rolls back the token vector when its budget cannot hold the complete row.
[[nodiscard]] sanitize::Result<JsonTextRowValidation> validate_json_text_row(
    JsonOnDemandDoc *doc, std::string_view raw, std::size_t base_offset,
    std::pmr::vector<JsonValidatedFieldToken> *tokens, std::size_t max_tokens,
    const sanitize::CompiledPlan *plan = nullptr);

} // namespace sanitize::internal
