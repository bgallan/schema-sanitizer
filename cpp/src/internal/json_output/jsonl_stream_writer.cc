// Implements JSON Lines serialization for Arrow C streams.

#include "internal/json_output/jsonl_stream_writer.hh"

#include "internal/json_encoding/token_writer.hh"
#include "internal/json_output/jsonl_value_writer.hh"
#include "internal/json_output/schema/model.hh"
#include "sanitize/abi/cdata_types.hh"

#include <algorithm>
#include <cstdint>
#include <string>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

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

sanitize::Status write_batch_jsonl(Output &out_file, const JsonlField &root,
                                   const ArrowArray &array) {
  SAN_RETURN_NOT_OK(validate_batch(root, array));

  std::string batch_text;
  if (array.length > 0) {
    constexpr int64_t kDefaultRowReserve = 128;
    const auto reserve_size = static_cast<std::size_t>(
        std::min<int64_t>(array.length, 8192) * kDefaultRowReserve);
    batch_text.reserve(reserve_size);
  }

  for (int64_t row = 0; row < array.length; ++row) {
    batch_text.push_back('{');
    bool first = true;
    for (std::size_t col = 0; col < root.children.size(); ++col) {
      if (!first) {
        batch_text.push_back(',');
      }
      first = false;
      sanitize::internal::json_encoding::append_string(batch_text,
                                                       root.children[col].name);
      batch_text.push_back(':');
      SAN_RETURN_NOT_OK(append_value(batch_text, root.children[col],
                                     *array.children[col], row));
    }
    batch_text.push_back('}');
    batch_text.push_back('\n');
    SAN_RETURN_NOT_OK(flush_buffer_if_large(out_file, batch_text));
  }

  return flush_buffer(out_file, batch_text);
}

} // namespace

sanitize::Status write_batch(Output &out_file, const ArrowSchema &schema,
                             const ArrowArray &array) {
  SAN_ASSIGN_OR_RAISE(auto root, parse_schema_field(schema));
  SAN_RETURN_NOT_OK(write_batch_jsonl(out_file, root, array));
  return out_file.Flush();
}

sanitize::Result<WriteStats> write_stream(ArrowArrayStream *stream,
                                          Output &out_file) {
  if (!stream) {
    return sanitize::Status::Invalid("JSONL writer: Arrow C stream is null");
  }
  sanitize::CSchemaGuard schema;
  const int schema_rc = stream->get_schema(stream, schema.get());
  if (schema_rc != 0) {
    return sanitize::Status::IOError(
        stream_error_message(stream, "JSONL writer: get_schema failed"));
  }
  SAN_ASSIGN_OR_RAISE(auto root, parse_schema_field(schema.value()));

  WriteStats stats;
  while (true) {
    sanitize::CArrayGuard batch;
    const int next_rc = stream->get_next(stream, batch.get());
    if (next_rc != 0) {
      return sanitize::Status::IOError(
          stream_error_message(stream, "JSONL writer: get_next failed"));
    }
    if (!batch.value().release) {
      break;
    }
    ++stats.batches;
    stats.materialized_rows += batch.value().length;
    SAN_RETURN_NOT_OK(write_batch_jsonl(out_file, root, batch.value()));
  }
  SAN_RETURN_NOT_OK(out_file.Flush());
  return stats;
}

} // namespace sanitize::internal::jsonl_stream_writer
