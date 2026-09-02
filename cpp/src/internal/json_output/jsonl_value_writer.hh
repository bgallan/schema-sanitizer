// Declares Arrow value serialization helpers for the JSONL stream writer.
// The code validates Arrow layouts and emits deterministic JSON with correct
// null and logical-type semantics.

#pragma once

#include "internal/json_output/schema/model.hh"
#include "internal/output/text_buffer.hh"

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

#include <cstdint>

namespace sanitize::internal::jsonl_stream_writer {

/// Appends one Arrow value as JSON according to the parsed JSONL field schema.
sanitize::Status append_value(TextBuffer &out, const JsonlField &field,
                              const ArrowArray &array, int64_t row);

} // namespace sanitize::internal::jsonl_stream_writer
