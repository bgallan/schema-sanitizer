// Declares CSV serialization for Arrow C streams.

#pragma once

#include "nanoarrow/nanoarrow.h"

#include <cstdint>
#include <string_view>

#include "sanitize/core/status.hh"

namespace sanitize::internal::csv_stream_writer {

struct WriteStats {
  int64_t materialized_rows = 0;
  int64_t batches = 0;
};

class Output {
public:
  // Destroys the output target.
  virtual ~Output() = default;
  // Writes encoded CSV bytes.
  virtual Status Write(std::string_view data) = 0;
  // Flushes buffered output.
  virtual Status Flush() = 0;
};

// Writes all batches from an Arrow C stream as CSV.
Result<WriteStats> write_stream(ArrowArrayStream *stream, Output &out_file,
                                std::int64_t memory_limit_bytes);

// Returns whether an Arrow C schema can be serialized by the native CSV writer.
bool schema_is_supported(const ArrowSchema &schema);

} // namespace sanitize::internal::csv_stream_writer
