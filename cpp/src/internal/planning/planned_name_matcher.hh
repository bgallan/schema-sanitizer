// Declares helpers for matching dirty source keys to planned output fields.
//
// The matcher centralizes the common exact lookup, sanitized-base lookup, and
// fallback output-name comparison used by frontends and strict-schema checks.

#pragma once

#include <string_view>

#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

/// Finds the planned field addressable by a dirty source key.
[[nodiscard]] const sanitize::FieldIndex *
find_planned_field(const sanitize::StructLayout &layout, std::string_view key,
                   uint64_t key_hash,
                   const sanitize::PreparedOptions &opts) noexcept;

/// Finds the planned field addressable by a dirty source key and raw policy.
[[nodiscard]] const sanitize::FieldIndex *
find_planned_field(const sanitize::StructLayout &layout, std::string_view key,
                   uint64_t key_hash,
                   std::string_view field_name_policy) noexcept;

/// Returns whether any planned field can be addressed by a dirty source key.
[[nodiscard]] bool
matches_planned_field(const sanitize::StructLayout &layout,
                      std::string_view key, uint64_t key_hash,
                      const sanitize::PreparedOptions &opts) noexcept;

/// Returns whether any planned field can be addressed by a dirty source key.
[[nodiscard]] bool
matches_planned_field(const sanitize::StructLayout &layout,
                      std::string_view key, uint64_t key_hash,
                      std::string_view field_name_policy) noexcept;

} // namespace sanitize::internal
