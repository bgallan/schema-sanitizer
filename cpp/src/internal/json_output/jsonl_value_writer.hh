// Declares Arrow value serialization helpers for the JSONL stream writer.

#pragma once

#include "internal/json_output/schema/model.hh"

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

#include <cstdint>
#include <string>

namespace sanitize::internal::jsonl_stream_writer {

// Appends one Arrow value as JSON according to the parsed JSONL field schema.
sanitize::Status append_value(std::string &out, const JsonlField &field,
                              const ArrowArray &array, int64_t row);

} // namespace sanitize::internal::jsonl_stream_writer
