// Declares JSON Lines serialization for Arrow C streams.

#pragma once

#include "nanoarrow/nanoarrow.h"

#include <cstdint>
#include <string_view>

#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal::jsonl_stream_writer {

struct WriteStats {
  int64_t materialized_rows = 0;
  int64_t batches = 0;
};

class Output {
public:
  // Destroys the output target.
  virtual ~Output() = default;
  // Writes one encoded JSONL batch.
  virtual Status Write(std::string_view data) = 0;
  // Flushes buffered output.
  virtual Status Flush() = 0;
};

// Writes all batches from an Arrow C stream as JSON Lines.
Result<WriteStats> write_stream(
    ArrowArrayStream *stream, Output &out_file, std::int64_t memory_limit_bytes,
    sanitize::ThreadingMode threading_mode = sanitize::ThreadingMode::kSingle);

// Writes one Arrow C record batch as JSON Lines.
Status write_batch(Output &out_file, const ArrowSchema &schema,
                   const ArrowArray &array, std::int64_t memory_limit_bytes);

} // namespace sanitize::internal::jsonl_stream_writer
