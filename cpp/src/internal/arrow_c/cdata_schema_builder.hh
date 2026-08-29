// Declares logical-schema export to Arrow C Data schemas.
// The implementation preserves Arrow ownership and error contracts without
// depending on the Arrow C++ library.

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

struct ArrowSchema;

namespace sanitize::internal {

struct CDataFieldLayout {
  std::string name;
  bool nullable = true;
  sanitize::LogicalType logical_type;
  std::string format_override;
};

/// Exports fields as struct schema.
sanitize::Status export_fields_as_struct_schema(
    const std::vector<CDataFieldLayout> &fields, ArrowSchema *out,
    std::string_view timestamp_precision = "TIMESTAMP_MICROS");

} // namespace sanitize::internal
