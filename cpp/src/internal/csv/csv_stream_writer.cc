// Implements CSV serialization for Arrow C streams.

#include "internal/csv/csv_stream_writer.hh"

#include "internal/json/jsonl_stream_writer_schema.hh"
#include "internal/json/jsonl_value_writer.hh"
#include "internal/parsing/json_string_decode.hh"
#include "sanitize/abi/cdata_types.hh"

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace sanitize::internal::csv_stream_writer {
namespace {

namespace jsonl = sanitize::internal::jsonl_stream_writer;

constexpr json_string_decode::DecodeErrors kCsvJsonDecodeErrors{
    .truncated_escape = "CSV writer: truncated JSON string escape",
    .incomplete_unicode_escape = "CSV writer: incomplete JSON unicode escape",
    .invalid_unicode_hex = "CSV writer: invalid JSON unicode escape",
    .missing_low_surrogate = "CSV writer: missing JSON low surrogate",
    .invalid_low_surrogate_hex =
        "CSV writer: invalid JSON low surrogate escape",
    .invalid_low_surrogate_range =
        "CSV writer: invalid JSON low surrogate range",
    .unexpected_low_surrogate = "CSV writer: unexpected JSON low surrogate",
    .invalid_escape = "CSV writer: invalid JSON string escape",
};

std::string stream_error_message(ArrowArrayStream *stream,
                                 std::string_view fallback) {
  if (stream && stream->get_last_error) {
    if (const char *message = stream->get_last_error(stream)) {
      if (*message) {
        return std::string(message);
      }
    }
  }
  return std::string(fallback);
}

sanitize::Status flush_buffer(Output &out_file, std::string &buffer) {
  if (buffer.empty()) {
    return sanitize::Status::OK();
  }
  auto status = out_file.Write(buffer);
  buffer.clear();
  return status;
}

sanitize::Status flush_buffer_if_large(Output &out_file, std::string &buffer) {
  constexpr std::size_t kFlushThresholdBytes = 1 << 20;
  if (buffer.size() < kFlushThresholdBytes) {
    return sanitize::Status::OK();
  }
  return flush_buffer(out_file, buffer);
}

bool needs_csv_quotes(std::string_view value) {
  return value.find_first_of(",\"\r\n") != std::string_view::npos;
}

void append_csv_escaped(std::string &out, std::string_view value) {
  if (!needs_csv_quotes(value)) {
    out.append(value);
    return;
  }
  out.push_back('"');
  for (const char ch : value) {
    if (ch == '"') {
      out += "\"\"";
    } else {
      out.push_back(ch);
    }
  }
  out.push_back('"');
}

bool is_json_string_literal(std::string_view value) {
  return value.size() >= 2 && value.front() == '"' && value.back() == '"';
}

sanitize::Status append_csv_cell_from_json(std::string &out,
                                           std::string_view json_value,
                                           std::vector<char> *decode_buffer) {
  if (json_value == "null") {
    return sanitize::Status::OK();
  }
  if (!is_json_string_literal(json_value)) {
    append_csv_escaped(out, json_value);
    return sanitize::Status::OK();
  }
  decode_buffer->assign(json_value.size(), '\0');
  SAN_ASSIGN_OR_RAISE(auto decoded,
                      json_string_decode::decode_json_string_slice(
                          decode_buffer->data(), json_value.data() + 1,
                          json_value.data() + json_value.size() - 1, json_value,
                          0, kCsvJsonDecodeErrors));
  append_csv_escaped(out, decoded);
  return sanitize::Status::OK();
}

sanitize::Status append_csv_cell(std::string &out,
                                 const jsonl::JsonlField &field,
                                 const ArrowArray &array, int64_t row,
                                 std::vector<char> *decode_buffer) {
  std::string json_value;
  SAN_RETURN_NOT_OK(jsonl::append_value(json_value, field, array, row));
  return append_csv_cell_from_json(out, json_value, decode_buffer);
}

sanitize::Status write_header(Output &out_file, const jsonl::JsonlField &root,
                              std::string &buffer) {
  for (std::size_t i = 0; i < root.children.size(); ++i) {
    if (i != 0) {
      buffer.push_back(',');
    }
    append_csv_escaped(buffer, root.children[i].name);
  }
  buffer.push_back('\n');
  return flush_buffer_if_large(out_file, buffer);
}

sanitize::Status write_batch_csv(Output &out_file,
                                 const jsonl::JsonlField &root,
                                 const ArrowArray &array, std::string &buffer) {
  SAN_RETURN_NOT_OK(jsonl::validate_batch(root, array));
  if (array.length > 0) {
    constexpr int64_t kDefaultRowReserve = 96;
    const auto reserve_size = static_cast<std::size_t>(
        std::min<int64_t>(array.length, 8192) * kDefaultRowReserve);
    buffer.reserve(std::max(buffer.capacity(), reserve_size));
  }

  std::vector<char> decode_buffer;
  for (int64_t row = 0; row < array.length; ++row) {
    for (std::size_t col = 0; col < root.children.size(); ++col) {
      if (col != 0) {
        buffer.push_back(',');
      }
      SAN_RETURN_NOT_OK(append_csv_cell(buffer, root.children[col],
                                        *array.children[col], row,
                                        &decode_buffer));
    }
    buffer.push_back('\n');
    SAN_RETURN_NOT_OK(flush_buffer_if_large(out_file, buffer));
  }
  return sanitize::Status::OK();
}

} // namespace

sanitize::Result<WriteStats> write_stream(ArrowArrayStream *stream,
                                          Output &out_file) {
  if (!stream) {
    return sanitize::Status::Invalid("CSV writer: Arrow C stream is null");
  }
  sanitize::CSchemaGuard schema;
  const int schema_rc = stream->get_schema(stream, schema.get());
  if (schema_rc != 0) {
    return sanitize::Status::IOError(
        stream_error_message(stream, "CSV writer: get_schema failed"));
  }
  SAN_ASSIGN_OR_RAISE(auto root, jsonl::parse_schema_field(schema.value()));

  std::string buffer;
  SAN_RETURN_NOT_OK(write_header(out_file, root, buffer));

  WriteStats stats;
  while (true) {
    sanitize::CArrayGuard batch;
    const int next_rc = stream->get_next(stream, batch.get());
    if (next_rc != 0) {
      return sanitize::Status::IOError(
          stream_error_message(stream, "CSV writer: get_next failed"));
    }
    if (!batch.value().release) {
      break;
    }
    ++stats.batches;
    stats.materialized_rows += batch.value().length;
    SAN_RETURN_NOT_OK(write_batch_csv(out_file, root, batch.value(), buffer));
  }
  SAN_RETURN_NOT_OK(flush_buffer(out_file, buffer));
  SAN_RETURN_NOT_OK(out_file.Flush());
  return stats;
}

bool schema_is_supported(const ArrowSchema &schema) {
  return jsonl::parse_schema_field(schema).ok();
}

} // namespace sanitize::internal::csv_stream_writer
