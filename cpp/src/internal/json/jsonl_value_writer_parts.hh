// Declares private helper groups for Arrow value JSON serialization.

#pragma once

#include "internal/json/jsonl_stream_writer_schema.hh"

#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/status.hh"

#include <cstdint>
#include <string>

namespace sanitize::internal::jsonl_stream_writer {

// Appends primitive scalar Arrow values.
sanitize::Status append_int8_value(std::string &out, const ArrowArray &array,
                                   int64_t row);
sanitize::Status append_uint8_value(std::string &out, const ArrowArray &array,
                                    int64_t row);
sanitize::Status append_int16_value(std::string &out, const ArrowArray &array,
                                    int64_t row);
sanitize::Status append_uint16_value(std::string &out, const ArrowArray &array,
                                     int64_t row);
sanitize::Status append_int32_value(std::string &out, const ArrowArray &array,
                                    int64_t row);
sanitize::Status append_uint32_value(std::string &out, const ArrowArray &array,
                                     int64_t row);
sanitize::Status append_int64_value(std::string &out, const ArrowArray &array,
                                    int64_t row);
sanitize::Status append_uint64_value(std::string &out, const ArrowArray &array,
                                     int64_t row);
sanitize::Status append_float16_value(std::string &out, const ArrowArray &array,
                                      int64_t row);
sanitize::Status append_float32_value(std::string &out, const ArrowArray &array,
                                      int64_t row);
sanitize::Status append_float64_value(std::string &out, const ArrowArray &array,
                                      int64_t row);

// Appends UTF-8 and binary-like Arrow values.
sanitize::Status append_string32_value(std::string &out,
                                       const ArrowArray &array, int64_t row);
sanitize::Status append_string64_value(std::string &out,
                                       const ArrowArray &array, int64_t row);
sanitize::Status append_binary32_value(std::string &out,
                                       const ArrowArray &array, int64_t row);
sanitize::Status append_binary64_value(std::string &out,
                                       const ArrowArray &array, int64_t row);

// Appends date, time, and timestamp Arrow values.
sanitize::Status append_timestamp_value(std::string &out,
                                        const ArrowArray &array, int64_t row,
                                        int64_t units_per_second);
sanitize::Status append_date32_value(std::string &out, const ArrowArray &array,
                                     int64_t row);
sanitize::Status append_date64_value(std::string &out, const ArrowArray &array,
                                     int64_t row);
sanitize::Status append_time32s_value(std::string &out, const ArrowArray &array,
                                      int64_t row);
sanitize::Status append_time32ms_value(std::string &out,
                                       const ArrowArray &array, int64_t row);
sanitize::Status append_time64_value(std::string &out, const ArrowArray &array,
                                     int64_t row, int64_t units_per_second);

// Appends Arrow logical values that are rendered as JSON strings.
sanitize::Status append_decimal_value(std::string &out, const JsonlField &field,
                                      const ArrowArray &array, int64_t row);
sanitize::Status append_duration_value(std::string &out,
                                       const JsonlField &field,
                                       const ArrowArray &array, int64_t row);
sanitize::Status append_interval_value(std::string &out,
                                       const JsonlField &field,
                                       const ArrowArray &array, int64_t row);

// Appends nested Arrow values.
sanitize::Status append_struct_value(std::string &out, const JsonlField &field,
                                     const ArrowArray &array, int64_t row);
sanitize::Status append_list32_value(std::string &out, const JsonlField &field,
                                     const ArrowArray &array, int64_t row);
sanitize::Status append_list64_value(std::string &out, const JsonlField &field,
                                     const ArrowArray &array, int64_t row);
sanitize::Status append_fixed_size_list_value(std::string &out,
                                              const JsonlField &field,
                                              const ArrowArray &array,
                                              int64_t row);
sanitize::Status append_dictionary_value(std::string &out,
                                         const JsonlField &field,
                                         const ArrowArray &array, int64_t row);

} // namespace sanitize::internal::jsonl_stream_writer
