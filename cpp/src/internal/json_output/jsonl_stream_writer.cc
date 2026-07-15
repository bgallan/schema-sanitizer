// Implements JSON Lines serialization for Arrow C streams.

#include "internal/json_output/jsonl_stream_writer.hh"

#include "internal/json_encoding/token_writer.hh"
#include "internal/json_output/jsonl_value_writer.hh"
#include "internal/json_output/schema/model.hh"
#include "internal/memory/memory_pool.hh"
#include "sanitize/abi/cdata_types.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>

namespace sanitize::internal::jsonl_stream_writer {
namespace {

constexpr std::size_t kFlushThresholdBytes = 1U << 20;
constexpr std::size_t kMaxRetainedOutputBytes = 4U << 20;

void clear_output_buffer(std::string &buffer) noexcept {
  if (secure_memory_cleanup_enabled() && !buffer.empty()) {
    secure_zero_memory(buffer.data(), buffer.size());
  }
  if (buffer.capacity() > kMaxRetainedOutputBytes) {
    std::string empty;
    buffer.swap(empty);
  } else {
    buffer.clear();
  }
}

class ScopedStringWipe final {
public:
  explicit ScopedStringWipe(std::string *value) noexcept : value_(value) {}
  ~ScopedStringWipe() {
    if (value_ && secure_memory_cleanup_enabled() && !value_->empty()) {
      secure_zero_memory(value_->data(), value_->size());
    }
  }

  ScopedStringWipe(const ScopedStringWipe &) = delete;
  ScopedStringWipe &operator=(const ScopedStringWipe &) = delete;

private:
  std::string *value_;
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
  clear_output_buffer(buffer);
  return status;
}

sanitize::Status flush_buffer_if_large(Output &out_file, std::string &buffer) {
  if (buffer.size() < kFlushThresholdBytes) {
    return sanitize::Status::OK();
  }
  return flush_buffer(out_file, buffer);
}

sanitize::Status write_batch_jsonl(Output &out_file, const JsonlField &root,
                                   const ArrowArray &array,
                                   const ArrayValidationLimits &limits) {
  SAN_RETURN_NOT_OK(validate_batch(root, array, limits));

  std::string batch_text;
  ScopedStringWipe batch_text_wipe(&batch_text);
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
                             const ArrowArray &array,
                             std::int64_t memory_limit_bytes) {
  SAN_ASSIGN_OR_RAISE(auto root, parse_schema_field(schema));
  const auto limits = array_validation_limits(memory_limit_bytes);
  SAN_RETURN_NOT_OK(write_batch_jsonl(out_file, root, array, limits));
  return out_file.Flush();
}

sanitize::Result<WriteStats> write_stream(ArrowArrayStream *stream,
                                          Output &out_file,
                                          std::int64_t memory_limit_bytes) {
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
  const auto limits = array_validation_limits(memory_limit_bytes);

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
    if (batch.value().length < 0 ||
        stats.batches == std::numeric_limits<std::int64_t>::max() ||
        batch.value().length > std::numeric_limits<std::int64_t>::max() -
                                   stats.materialized_rows) {
      return sanitize::Status::Invalid(
          "JSONL writer: write statistics overflow");
    }
    SAN_RETURN_NOT_OK(write_batch_jsonl(out_file, root, batch.value(), limits));
    ++stats.batches;
    stats.materialized_rows += batch.value().length;
  }
  SAN_RETURN_NOT_OK(out_file.Flush());
  return stats;
}

} // namespace sanitize::internal::jsonl_stream_writer
