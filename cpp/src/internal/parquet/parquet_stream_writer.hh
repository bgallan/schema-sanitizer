// Declares a small native Parquet writer for supported Arrow C streams.

#pragma once

#include "nanoarrow/nanoarrow.h"

#include "sanitize/core/status.hh"

#include <cstdint>
#include <string>
#include <string_view>

namespace sanitize::internal::parquet_stream_writer {

class Output {
public:
  // Destroys the output abstraction.
  virtual ~Output() = default;
  // Writes bytes to the output target.
  virtual sanitize::Status Write(std::string_view data) = 0;
  // Flushes the output target.
  virtual sanitize::Status Flush() = 0;
};

struct WriterOptions {
  std::int64_t memory_limit_bytes = -1;
  std::string compression;
  int gzip_level = -1;
};

// Writes supported Arrow C streams as native Parquet.
sanitize::Status write_stream(ArrowArrayStream *stream, Output &out_file,
                              const WriterOptions &options = {});

} // namespace sanitize::internal::parquet_stream_writer
