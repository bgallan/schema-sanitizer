// Encodes Arrow direct logical schemas into the canonical logical-schema wire
// format.

#include "api/python_abi3/arrow_direct/schema/payload.hh"

#include "api/python_abi3/arrow_direct/schema/logical.hh"
#include "internal/planning/options_schema_serialization.hh"

#include <new>
#include <stdexcept>
#include <string>
#include <vector>

namespace core_abi3_internal {

sanitize::Result<std::string>
logical_schema_payload_from_arrow_schema(const ArrowSchema *schema,
                                         const ArrowDirectOptions &options) {
  std::vector<ArrowInputNode> fields;
  SAN_ASSIGN_OR_RAISE(
      auto logical, logical_schema_from_arrow_schema(schema, &fields, options));
  return sanitize::internal::options_io::serialize_logical_schema_bytes(
      logical);
}

} // namespace core_abi3_internal
