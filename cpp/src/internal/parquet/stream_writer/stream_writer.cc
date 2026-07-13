// Implements a native Parquet writer for Arrow C streams.

#include "internal/parquet/parquet_stream_writer.hh"

#include "internal/json_output/schema/model.hh"
#include "internal/string_lookup.hh"
#include "sanitize/abi/cdata_types.hh"

#if defined(SCHEMA_SANITIZER_HAS_ZLIB)
#include <zlib.h>
#endif

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize::internal::parquet_stream_writer {
namespace {
// clang-format off
#include "stream_writer_types.cc.inc"
#include "stream_writer_configuration.cc.inc"
#include "stream_writer_arrow_values.cc.inc"
#include "stream_writer_schema_nodes.cc.inc"
#include "stream_writer_compact_thrift.cc.inc"
#include "stream_writer_page_headers.cc.inc"
#include "stream_writer_compression.cc.inc"
#include "stream_writer_statistics.cc.inc"
#include "stream_writer_value_encodings.cc.inc"
#include "stream_writer_collection.cc.inc"
#include "stream_writer_pages.cc.inc"
#include "stream_writer_page_indexes.cc.inc"
#include "stream_writer_schema_elements.cc.inc"
#include "stream_writer_footer.cc.inc"
// clang-format on
} // namespace

#include "stream_writer_api.cc.inc"
} // namespace sanitize::internal::parquet_stream_writer
