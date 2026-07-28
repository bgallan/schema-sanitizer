// Implements bounded Parquet footer parsing for native reader dispatch.

#include "internal/parquet/footer_reader/api.hh"

#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/arrow_c/cdata_stream_runtime.hh"
#include "internal/json_encoding/token_writer.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/runtime/ordered_executor.hh"
#include "internal/string_lookup.hh"

#include "nanoarrow/nanoarrow.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <new>
#include <numeric>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
#include <zlib.h>
#endif

namespace sanitize::internal::parquet_footer_reader {
namespace {
// clang-format off: these implementation fragments have dependency order.
#include "pages/footer_reader_format_primitives.cc.inc"
#include "pages/footer_reader_levels.cc.inc"
#include "thrift/compact_reader.hh.inc"
#include "thrift/compact_reader.cc.inc"
#include "thrift/logical_type_reader.cc.inc"
#include "thrift/schema_elements.cc.inc"
#include "thrift/footer_metadata_column_reader.cc.inc"
#include "thrift/footer_metadata_row_group_reader.cc.inc"
#include "footer_reader_schema.cc.inc"
#include "pages/footer_reader_page_headers.cc.inc"
#include "pages/footer_reader_decompression.cc.inc"
#include "pages/footer_reader_fixed_width_formats.cc.inc"
#include "pages/footer_reader_fixed_width_copy.cc.inc"
#include "pages/footer_reader_plain_decode.cc.inc"
#include "pages/footer_reader_delta_binary.cc.inc"
#include "pages/footer_reader_byte_stream_split.cc.inc"
#include "pages/footer_reader_delta_length_pages.cc.inc"
#include "pages/footer_reader_dictionary_indices.cc.inc"
#include "pages/footer_reader_dictionary_page.cc.inc"
#include "pages/footer_reader_page_decode_values.cc.inc"
#include "pages/footer_reader_page_read.cc.inc"
#include "pages/footer_reader_page_indexes.cc.inc"
#include "runtime/native_buffer_limits.cc.inc"
#include "native_stream/schema/native_stream_recursive_model.cc.inc"
#include "native_stream/schema/native_stream_recursive_tree.cc.inc"
#include "native_stream/schema/native_stream_repeated_path_support.cc.inc"
#include "native_stream/schema/native_stream_repeated_level_layouts.cc.inc"
#include "pages/footer_reader_native_page_plans.cc.inc"
#include "reporting/footer_reader_diagnostics_json.cc.inc"

#include "native_stream/schema/native_stream_arrow_state.cc.inc"
#include "native_stream/schema/native_stream_output_layout.cc.inc"
#include "native_stream/diagnostics/native_stream_recursive_diagnostics.cc.inc"
#include "native_stream/diagnostics/native_stream_output_layout.cc.inc"
#include "native_stream/materialization/native_stream_page_layout.cc.inc"
#include "runtime/native_stream_readiness.cc.inc"

#include "native_stream/decode/native_stream_scalar_columns.cc.inc"
#include "native_stream/decode/native_stream_list_columns.cc.inc"
#include "native_stream/decode/native_stream_list_binary_columns.cc.inc"
#include "native_stream/decode/native_stream_binary_columns.cc.inc"
#include "native_stream/decode/native_stream_dictionary_binary_columns.cc.inc"
#include "native_stream/decode/native_stream_dictionary_fixed_columns.cc.inc"

#include "native_stream/schema/native_stream_arrow_schema_setup.cc.inc"
#include "native_stream/schema/native_stream_arrow_schema_builders.cc.inc"
#include "native_stream/schema/native_stream_arrow_schema_root.cc.inc"

#include "native_stream/materialization/layout/native_stream_array_shells.cc.inc"
#include "native_stream/materialization/native_stream_validity.cc.inc"
#include "native_stream/materialization/native_stream_recursive_containers.cc.inc"
#include "native_stream/materialization/native_stream_recursive_children.cc.inc"
#include "native_stream/materialization/row_group/native_stream_retained_budget.cc.inc"
#include "native_stream/materialization/row_group/native_stream_row_group.cc.inc"
#include "native_stream/materialization/row_group/native_stream_parallel_columns.cc.inc"
} // namespace

#include "reporting/footer_reader_json.cc.inc"
#include "reporting/footer_reader_public.cc.inc"
// clang-format on
} // namespace sanitize::internal::parquet_footer_reader
