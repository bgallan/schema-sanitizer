// Implements planned field matching for source keys.
// The helpers normalize private planning state without leaking wire or layout
// details into public APIs.

#include "internal/planning/planned_name_matcher.hh"

#include <string>

#include "internal/planning/field_name_sanitizer.hh"
#include "sanitize/detail/hash.hh"

namespace sanitize::internal {
namespace {

constexpr std::size_t kInitialSuffixLength = 6;
constexpr std::size_t kMaxSuffixLength = 16;
constexpr std::string_view kPreservePolicy = "preserve";
constexpr std::string_view kFlattenedSuffix = "_flattened";
constexpr std::string_view kSanitizedFlattenedSuffix = "flattened";

/// Finds a planned field with the exact sanitized base form of a dirty key.
const sanitize::FieldIndex *
find_sanitized_base(const sanitize::StructLayout &layout, std::string_view key,
                    std::string_view field_name_policy) noexcept {
  if (field_name_policy == kPreservePolicy) {
    return nullptr;
  }
  std::string base;
  try {
    base = clean_field_name_base(key, field_name_policy);
  } catch (...) {
    return nullptr;
  }
  if (base.empty() || base == key) {
    return nullptr;
  }
  return layout.find(base, sanitize::detail::hash_key64(base));
}

/// Probes one candidate output name directly in the compiled layout.
const sanitize::FieldIndex *
find_candidate(const sanitize::StructLayout &layout,
               std::string_view candidate) noexcept {
  return layout.find(candidate, sanitize::detail::hash_key64(candidate));
}

/// Probes flattened variants for one candidate unflattened output name.
const sanitize::FieldIndex *
find_flattened_candidate(const sanitize::StructLayout &layout,
                         std::string_view candidate) noexcept {
  std::string flattened;
  flattened.reserve(candidate.size() + kFlattenedSuffix.size());
  flattened.assign(candidate.data(), candidate.size());
  flattened += kFlattenedSuffix;
  if (const auto *field = find_candidate(layout, flattened)) {
    return field;
  }

  flattened.assign(candidate.data(), candidate.size());
  flattened += kSanitizedFlattenedSuffix;
  return find_candidate(layout, flattened);
}

/// Probes generated collision and flattening candidates without scanning the
/// full layout.
const sanitize::FieldIndex *
find_generated_candidates(const sanitize::StructLayout &layout,
                          std::string_view key,
                          std::string_view field_name_policy) noexcept {
  if (uses_preserve_policy(field_name_policy)) {
    return find_flattened_candidate(layout, key);
  }

  std::string base;
  try {
    base = clean_field_name_base(key, field_name_policy);
  } catch (...) {
    return nullptr;
  }
  if (base.empty()) {
    return nullptr;
  }

  if (const auto *field = find_flattened_candidate(layout, base)) {
    return field;
  }

  for (std::size_t len = kInitialSuffixLength; len <= kMaxSuffixLength; ++len) {
    const std::string suffixed = clean_with_suffix(key, base, len);
    if (const auto *field = find_candidate(layout, suffixed)) {
      return field;
    }
    if (const auto *field = find_flattened_candidate(layout, suffixed)) {
      return field;
    }
  }

  return nullptr;
}

} // namespace

const sanitize::FieldIndex *
find_planned_field(const sanitize::StructLayout &layout, std::string_view key,
                   uint64_t key_hash,
                   std::string_view field_name_policy) noexcept {
  if (const auto *field = layout.find(key, key_hash)) {
    return field;
  }
  if (const auto *field = layout.find_alias(key, key_hash)) {
    return field;
  }
  if (const auto *field = find_sanitized_base(layout, key, field_name_policy)) {
    return field;
  }
  return find_generated_candidates(layout, key, field_name_policy);
}

const sanitize::FieldIndex *
find_planned_field(const sanitize::StructLayout &layout, std::string_view key,
                   uint64_t key_hash,
                   const sanitize::PreparedOptions &opts) noexcept {
  if (key == opts.spec.default_key_name) {
    if (const auto *field = layout.find(key, key_hash)) {
      return field;
    }
  }
  return find_planned_field(layout, key, key_hash, opts.spec.field_name_policy);
}

bool matches_planned_field(const sanitize::StructLayout &layout,
                           std::string_view key, uint64_t key_hash,
                           std::string_view field_name_policy) noexcept {
  return find_planned_field(layout, key, key_hash, field_name_policy) !=
         nullptr;
}

bool matches_planned_field(const sanitize::StructLayout &layout,
                           std::string_view key, uint64_t key_hash,
                           const sanitize::PreparedOptions &opts) noexcept {
  return find_planned_field(layout, key, key_hash, opts) != nullptr;
}

} // namespace sanitize::internal
