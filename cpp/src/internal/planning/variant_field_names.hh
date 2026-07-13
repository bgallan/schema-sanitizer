// Declares helpers for hybrid schema-registry versioned field names.

#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace sanitize::internal {

/// Parsed components of a name such as field_v2_struct_array.
struct VersionedFieldName {
  std::string_view base;
  int version = 0;
  std::string_view semantic_type;
};

/// Parse <base>_v<version>_<semantic_type>, requiring version >= 2.
std::optional<VersionedFieldName>
parse_versioned_field_name(std::string_view name) noexcept;

/// Render <base>_v<version>_<semantic_type> with one exact allocation.
std::string make_versioned_field_name(std::string_view base, int version,
                                      std::string_view semantic_type);

/// Return the unversioned base for hybrid version names, or empty when absent.
std::string_view versioned_field_base(std::string_view name) noexcept;

/// Return whether two field names are different members of one version family.
bool same_variant_family(std::string_view lhs, std::string_view rhs) noexcept;

/// Return whether name belongs to the requested version-family base.
bool in_variant_family(std::string_view name,
                       std::string_view family_base) noexcept;

/// Return the family base, normalizing versioned names to their original base.
std::string_view variant_family_base(std::string_view name) noexcept;

} // namespace sanitize::internal
