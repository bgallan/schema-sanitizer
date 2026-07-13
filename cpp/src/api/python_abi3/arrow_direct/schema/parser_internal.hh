// Internal contracts for Arrow C schema node parsing.

#pragma once

#include "api/python_abi3/arrow_direct/schema/logical.hh"

namespace core_abi3_internal::arrow_schema_internal {

sanitize::Result<sanitize::LogicalType>
parse_arrow_type(const ArrowSchema *schema, ArrowInputNode *node,
                 const ArrowDirectOptions &options);

sanitize::Result<sanitize::LogicalType>
parse_struct_type(const ArrowSchema *schema, ArrowInputNode *node,
                  const ArrowDirectOptions &options);

sanitize::Result<sanitize::LogicalType>
parse_list_type(const ArrowSchema *schema, ArrowInputNode *node,
                ArrowNodeKind kind, const ArrowDirectOptions &options);

sanitize::Result<sanitize::LogicalType>
parse_fixed_size_list_type(const ArrowSchema *schema, ArrowInputNode *node,
                           const ArrowDirectOptions &options);

sanitize::Result<sanitize::LogicalType>
parse_map_type(const ArrowSchema *schema, ArrowInputNode *node,
               const ArrowDirectOptions &options);

} // namespace core_abi3_internal::arrow_schema_internal
