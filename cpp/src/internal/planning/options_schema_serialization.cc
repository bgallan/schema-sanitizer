// Serializes and deserializes logical schemas inside portable wire payloads.
//
// Keeps nested logical-schema wire I/O separate from options envelopes and
// gives Python ABI3 probes and registry operations one canonical codec owner.

#include "internal/planning/options_schema_serialization.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "internal/planning/options_bytes_reader.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal::options_io {

namespace {

constexpr uint32_t kMaxFields = 1u << 20;
constexpr uint32_t kMaxDepth = 512;

void append_u8(std::string &out, std::uint8_t value) {
  out.push_back(static_cast<char>(value));
}

void append_u32(std::string &out, std::uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xFFu));
  }
}

void append_string(std::string &out, std::string_view value) {
  append_u32(out, static_cast<std::uint32_t>(value.size()));
  out.append(value.data(), value.size());
}

void append_logical_type(std::string &out, const sanitize::LogicalType &type);

void append_logical_field(std::string &out,
                          const sanitize::LogicalField &field) {
  append_string(out, field.name);
  append_u8(out, field.nullable ? 1u : 0u);
  if (field.type) {
    append_logical_type(out, *field.type);
  } else {
    append_u8(out, std::to_underlying(sanitize::LogicalKind::kNull));
  }
}

void append_logical_type(std::string &out, const sanitize::LogicalType &type) {
  append_u8(out, std::to_underlying(type.kind));
  if (type.kind == sanitize::LogicalKind::kStruct) {
    append_u32(out, static_cast<std::uint32_t>(type.fields.size()));
    for (const auto &field : type.fields) {
      append_logical_field(out, field);
    }
  } else if (type.kind == sanitize::LogicalKind::kList) {
    if (type.value) {
      append_logical_type(out, *type.value);
    } else {
      append_u8(out, std::to_underlying(sanitize::LogicalKind::kNull));
    }
  }
}

class LogicalSchemaReader {
public:
  // Creates a reader over one logical-schema payload.
  explicit LogicalSchemaReader(std::string_view input) : input_(input) {}

  // Reads the full logical schema payload.
  sanitize::Result<sanitize::LogicalSchema> read_schema() {
    uint32_t n_fields = 0;
    if (!read_u32(input_, &pos_, &n_fields)) {
      return sanitize::Status::Invalid(
          "deserialize_options: truncated logical schema field count");
    }
    if (n_fields > kMaxFields) {
      return sanitize::Status::Invalid(
          "deserialize_options: logical schema too large");
    }

    sanitize::LogicalSchema out;
    out.fields.reserve(n_fields);
    for (uint32_t i = 0; i < n_fields; ++i) {
      sanitize::LogicalField field;
      SAN_RETURN_NOT_OK(read_field(&field, 0));
      out.fields.push_back(std::move(field));
    }
    if (pos_ != input_.size()) {
      return sanitize::Status::Invalid(
          "deserialize_options: trailing bytes in logical schema");
    }

    return out;
  }

private:
  // Reads one logical field node.
  sanitize::Status read_field(sanitize::LogicalField *out, uint32_t depth) {
    if (!read_string(input_, &pos_, &out->name)) {
      return sanitize::Status::Invalid(
          "deserialize_options: truncated logical field name");
    }
    uint8_t nullable = 0;
    if (!read_u8(input_, &pos_, &nullable)) {
      return sanitize::Status::Invalid(
          "deserialize_options: truncated logical field nullable");
    }
    if (nullable > 1) {
      return sanitize::Status::Invalid(
          "deserialize_options: invalid logical field nullable");
    }
    out->nullable = (nullable != 0);
    SAN_ASSIGN_OR_RAISE(auto type, read_type(depth + 1));
    out->type = std::make_unique<sanitize::LogicalType>(std::move(type));
    return sanitize::Status::OK();
  }

  // Reads one logical type node.
  sanitize::Result<sanitize::LogicalType> read_type(uint32_t depth) {
    if (depth > kMaxDepth) {
      return sanitize::Status::Invalid(
          "deserialize_options: logical schema nesting too deep");
    }

    uint8_t kind_u8 = 0;
    if (!read_u8(input_, &pos_, &kind_u8)) {
      return sanitize::Status::Invalid(
          "deserialize_options: truncated logical type kind");
    }
    auto kind = static_cast<sanitize::LogicalKind>(kind_u8);
    sanitize::LogicalType t(kind);
    switch (kind) {
    case sanitize::LogicalKind::kStruct: {
      uint32_t n = 0;
      if (!read_u32(input_, &pos_, &n)) {
        return sanitize::Status::Invalid(
            "deserialize_options: truncated logical struct fields");
      }
      if (n > kMaxFields) {
        return sanitize::Status::Invalid(
            "deserialize_options: logical struct too large");
      }
      t.fields.reserve(n);
      for (uint32_t i = 0; i < n; ++i) {
        sanitize::LogicalField field;
        SAN_RETURN_NOT_OK(read_field(&field, depth));
        t.fields.push_back(std::move(field));
      }
      break;
    }
    case sanitize::LogicalKind::kList: {
      SAN_ASSIGN_OR_RAISE(auto element, read_type(depth + 1));
      t.value = std::make_unique<sanitize::LogicalType>(std::move(element));
      break;
    }
    case sanitize::LogicalKind::kNull:
    case sanitize::LogicalKind::kBool:
    case sanitize::LogicalKind::kInt64:
    case sanitize::LogicalKind::kFloat64:
    case sanitize::LogicalKind::kUtf8:
    case sanitize::LogicalKind::kTimestampNs:
    case sanitize::LogicalKind::kDate32:
    case sanitize::LogicalKind::kTime32s:
      break;
    default:
      return sanitize::Status::Invalid(
          "deserialize_options: unknown logical kind");
    }
    return t;
  }

  std::string_view input_;
  std::size_t pos_ = 0;
};

} // namespace

std::string
serialize_logical_schema_bytes(const sanitize::LogicalSchema &schema) {
  std::string out;
  append_u32(out, static_cast<std::uint32_t>(schema.fields.size()));
  for (const auto &field : schema.fields) {
    append_logical_field(out, field);
  }
  return out;
}

sanitize::Result<sanitize::LogicalSchema>
deserialize_logical_schema_bytes(std::string_view in) {
  LogicalSchemaReader reader(in);
  return reader.read_schema();
}

} // namespace sanitize::internal::options_io
