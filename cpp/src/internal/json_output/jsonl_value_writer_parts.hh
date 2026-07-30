// Declares private helper groups for Arrow value JSON serialization.

#pragma once

#include "internal/json_output/schema/model.hh"
#include "internal/output/text_buffer.hh"

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

#include <cstdint>

namespace sanitize::internal::jsonl_stream_writer {

// Appends primitive scalar Arrow values.
sanitize::Status append_int8_value(TextBuffer &out, const ArrowArray &array,
                                   int64_t row);
sanitize::Status append_uint8_value(TextBuffer &out, const ArrowArray &array,
                                    int64_t row);
sanitize::Status append_int16_value(TextBuffer &out, const ArrowArray &array,
                                    int64_t row);
sanitize::Status append_uint16_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row);
sanitize::Status append_int32_value(TextBuffer &out, const ArrowArray &array,
                                    int64_t row);
sanitize::Status append_uint32_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row);
sanitize::Status append_int64_value(TextBuffer &out, const ArrowArray &array,
                                    int64_t row);
sanitize::Status append_uint64_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row);
sanitize::Status append_float16_value(TextBuffer &out, const ArrowArray &array,
                                      int64_t row);
sanitize::Status append_float32_value(TextBuffer &out, const ArrowArray &array,
                                      int64_t row);
sanitize::Status append_float64_value(TextBuffer &out, const ArrowArray &array,
                                      int64_t row);

// Appends UTF-8 and binary-like Arrow values.
sanitize::Status append_string32_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row);
sanitize::Status append_string64_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row);
sanitize::Status append_binary32_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row, bool quote = true);
sanitize::Status append_binary64_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row, bool quote = true);
sanitize::Status append_fixed_size_binary_value(TextBuffer &out,
                                                const JsonlField &field,
                                                const ArrowArray &array,
                                                int64_t row, bool quote = true);

// Appends date, time, and timestamp Arrow values.
sanitize::Status append_timestamp_value(TextBuffer &out,
                                        const ArrowArray &array, int64_t row,
                                        int64_t units_per_second,
                                        bool quote = true);
sanitize::Status append_date32_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row, bool quote = true);
sanitize::Status append_date64_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row, bool quote = true);
sanitize::Status append_time32s_value(TextBuffer &out, const ArrowArray &array,
                                      int64_t row, bool quote = true);
sanitize::Status append_time32ms_value(TextBuffer &out, const ArrowArray &array,
                                       int64_t row, bool quote = true);
sanitize::Status append_time64_value(TextBuffer &out, const ArrowArray &array,
                                     int64_t row, int64_t units_per_second,
                                     bool quote = true);

// Appends Arrow logical values that are rendered as JSON strings.
sanitize::Status append_decimal_value(TextBuffer &out, const JsonlField &field,
                                      const ArrowArray &array, int64_t row,
                                      bool quote = true);
sanitize::Status append_duration_value(TextBuffer &out, const JsonlField &field,
                                       const ArrowArray &array, int64_t row,
                                       bool quote = true);
sanitize::Status append_interval_value(TextBuffer &out, const JsonlField &field,
                                       const ArrowArray &array, int64_t row);

// Appends nested Arrow values.
sanitize::Status append_struct_value(TextBuffer &out, const JsonlField &field,
                                     const ArrowArray &array, int64_t row);
sanitize::Status append_list32_value(TextBuffer &out, const JsonlField &field,
                                     const ArrowArray &array, int64_t row);
sanitize::Status append_list64_value(TextBuffer &out, const JsonlField &field,
                                     const ArrowArray &array, int64_t row);
sanitize::Status append_fixed_size_list_value(TextBuffer &out,
                                              const JsonlField &field,
                                              const ArrowArray &array,
                                              int64_t row);
sanitize::Status append_dictionary_value(TextBuffer &out,
                                         const JsonlField &field,
                                         const ArrowArray &array, int64_t row);

} // namespace sanitize::internal::jsonl_stream_writer
