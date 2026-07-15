// Serializes and deserializes logical schemas inside portable wire payloads.
//
// Keeps nested logical-schema wire I/O separate from options envelopes and
// gives Python ABI3 probes and registry operations one canonical codec owner.

#include "internal/planning/options_schema_serialization.hh"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "internal/planning/options_bytes_reader.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"

namespace sanitize::internal::options_io {

namespace {

constexpr std::size_t kMinimumFieldBytes = 6U;

void require_output_growth(const std::string &out, std::size_t additional) {
  if (additional > kMaxLogicalSchemaPayloadBytes ||
      out.size() > kMaxLogicalSchemaPayloadBytes - additional) {
    throw std::length_error("logical schema payload exceeds safety limit");
  }
}

std::uint32_t checked_u32_size(std::size_t value,
                               std::string_view label) {
  if (value > std::numeric_limits<std::uint32_t>::max()) {
    throw std::length_error(std::string(label) + " exceeds uint32 range");
  }
  return static_cast<std::uint32_t>(value);
}

void append_u8(std::string &out, std::uint8_t value) {
  require_output_growth(out, 1);
  out.push_back(static_cast<char>(value));
}

void append_u32(std::string &out, std::uint32_t value) {
  require_output_growth(out, 4);
  for (int shift = 0; shift < 32; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xFFu));
  }
}

void append_string(std::string &out, std::string_view value) {
  append_u32(out, checked_u32_size(value.size(), "logical field name"));
  require_output_growth(out, value.size());
  out.append(value.data(), value.size());
}

struct LogicalSchemaWriteBudget {
  std::uint32_t remaining_nodes = kMaxLogicalSchemaNodes;

  void consume_node() {
    if (remaining_nodes == 0) {
      throw std::length_error("logical schema node count exceeds safety limit");
    }
    --remaining_nodes;
  }
};

void append_logical_type(std::string &out, const sanitize::LogicalType &type,
                         std::uint32_t depth,
                         LogicalSchemaWriteBudget *budget);

void append_logical_field(std::string &out,
                          const sanitize::LogicalField &field,
                          std::uint32_t depth,
                          LogicalSchemaWriteBudget *budget) {
  if (!budget) {
    throw std::invalid_argument("logical schema write budget is null");
  }
  budget->consume_node();
  append_string(out, field.name);
  append_u8(out, field.nullable ? 1u : 0u);
  if (field.type) {
    append_logical_type(out, *field.type, depth + 1U, budget);
  } else {
    sanitize::LogicalType null_type(sanitize::LogicalKind::kNull);
    append_logical_type(out, null_type, depth + 1U, budget);
  }
}

void append_logical_type(std::string &out, const sanitize::LogicalType &type,
                         std::uint32_t depth,
                         LogicalSchemaWriteBudget *budget) {
  if (!budget) {
    throw std::invalid_argument("logical schema write budget is null");
  }
  if (depth > kMaxLogicalSchemaDepth) {
    throw std::length_error("logical schema nesting exceeds safety limit");
  }
  budget->consume_node();
  append_u8(out, std::to_underlying(type.kind));
  if (type.kind == sanitize::LogicalKind::kStruct) {
    if (type.fields.size() > kMaxLogicalSchemaFieldsPerStruct) {
      throw std::length_error("logical struct field count exceeds safety limit");
    }
    append_u32(out, checked_u32_size(type.fields.size(),
                                     "logical struct field count"));
    for (const auto &field : type.fields) {
      append_logical_field(out, field, depth, budget);
    }
  } else if (type.kind == sanitize::LogicalKind::kList) {
    if (type.value) {
      append_logical_type(out, *type.value, depth + 1U, budget);
    } else {
      sanitize::LogicalType null_type(sanitize::LogicalKind::kNull);
      append_logical_type(out, null_type, depth + 1U, budget);
    }
  }
}

class LogicalSchemaReader {
public:
  explicit LogicalSchemaReader(std::string_view input) : input_(input) {}

  sanitize::Result<sanitize::LogicalSchema> read_schema() {
    if (input_.size() > kMaxLogicalSchemaPayloadBytes) {
      return sanitize::Status::Invalid(
          "deserialize_options: logical schema payload too large");
    }
    std::uint32_t n_fields = 0;
    if (!read_u32(input_, &pos_, &n_fields)) {
      return sanitize::Status::Invalid(
          "deserialize_options: truncated logical schema field count");
    }
    SAN_RETURN_NOT_OK(validate_field_collection(n_fields, "logical schema"));

    sanitize::LogicalSchema out;
    out.fields.reserve(n_fields);
    for (std::uint32_t i = 0; i < n_fields; ++i) {
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
  sanitize::Status consume_node() {
    if (remaining_nodes_ == 0) {
      return sanitize::Status::Invalid(
          "deserialize_options: logical schema node count exceeds safety limit");
    }
    --remaining_nodes_;
    return sanitize::Status::OK();
  }

  sanitize::Status validate_field_collection(std::uint32_t count,
                                             std::string_view context) const {
    if (count > kMaxLogicalSchemaFieldsPerStruct) {
      return sanitize::Status::Invalid("deserialize_options: ", context,
                                       " has too many fields");
    }
    if (pos_ > input_.size() ||
        static_cast<std::size_t>(count) >
            (input_.size() - pos_) / kMinimumFieldBytes) {
      return sanitize::Status::Invalid("deserialize_options: ", context,
                                       " field count exceeds remaining bytes");
    }
    const auto minimum_nodes = static_cast<std::uint64_t>(count) * 2U;
    if (minimum_nodes > remaining_nodes_) {
      return sanitize::Status::Invalid("deserialize_options: ", context,
                                       " exceeds logical schema node budget");
    }
    return sanitize::Status::OK();
  }

  sanitize::Status read_field(sanitize::LogicalField *out,
                              std::uint32_t depth) {
    SAN_RETURN_NOT_OK(consume_node());
    if (!read_string(input_, &pos_, &out->name)) {
      return sanitize::Status::Invalid(
          "deserialize_options: truncated logical field name");
    }
    std::uint8_t nullable = 0;
    if (!read_u8(input_, &pos_, &nullable)) {
      return sanitize::Status::Invalid(
          "deserialize_options: truncated logical field nullable");
    }
    if (nullable > 1) {
      return sanitize::Status::Invalid(
          "deserialize_options: invalid logical field nullable");
    }
    out->nullable = (nullable != 0);
    SAN_ASSIGN_OR_RAISE(auto type, read_type(depth + 1U));
    out->type = std::make_unique<sanitize::LogicalType>(std::move(type));
    return sanitize::Status::OK();
  }

  sanitize::Result<sanitize::LogicalType> read_type(std::uint32_t depth) {
    if (depth > kMaxLogicalSchemaDepth) {
      return sanitize::Status::Invalid(
          "deserialize_options: logical schema nesting too deep");
    }
    SAN_RETURN_NOT_OK(consume_node());

    std::uint8_t kind_u8 = 0;
    if (!read_u8(input_, &pos_, &kind_u8)) {
      return sanitize::Status::Invalid(
          "deserialize_options: truncated logical type kind");
    }
    auto kind = static_cast<sanitize::LogicalKind>(kind_u8);
    sanitize::LogicalType type(kind);
    switch (kind) {
    case sanitize::LogicalKind::kStruct: {
      std::uint32_t count = 0;
      if (!read_u32(input_, &pos_, &count)) {
        return sanitize::Status::Invalid(
            "deserialize_options: truncated logical struct fields");
      }
      SAN_RETURN_NOT_OK(validate_field_collection(count, "logical struct"));
      type.fields.reserve(count);
      for (std::uint32_t i = 0; i < count; ++i) {
        sanitize::LogicalField field;
        SAN_RETURN_NOT_OK(read_field(&field, depth));
        type.fields.push_back(std::move(field));
      }
      break;
    }
    case sanitize::LogicalKind::kList: {
      SAN_ASSIGN_OR_RAISE(auto element, read_type(depth + 1U));
      type.value =
          std::make_unique<sanitize::LogicalType>(std::move(element));
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
    return type;
  }

  std::string_view input_;
  std::size_t pos_ = 0;
  std::uint32_t remaining_nodes_ = kMaxLogicalSchemaNodes;
};

} // namespace

sanitize::Result<std::string>
serialize_logical_schema_bytes(const sanitize::LogicalSchema &schema) {
  try {
    if (schema.fields.size() > kMaxLogicalSchemaFieldsPerStruct) {
      return sanitize::Status::Invalid(
          "logical schema field count exceeds safety limit");
    }
    std::string out;
    LogicalSchemaWriteBudget budget;
    append_u32(out, checked_u32_size(schema.fields.size(),
                                     "logical schema fields"));
    for (const auto &field : schema.fields) {
      append_logical_field(out, field, 0, &budget);
    }
    return out;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "logical schema serialization ran out of memory");
  } catch (const std::length_error &error) {
    return sanitize::Status::Invalid(
        "logical schema serialization exceeds safety limits: ",
        error.what());
  } catch (const std::exception &error) {
    return sanitize::Status::Invalid(
        "logical schema serialization failed: ", error.what());
  }
}

sanitize::Result<sanitize::LogicalSchema>
deserialize_logical_schema_bytes(std::string_view in) {
  LogicalSchemaReader reader(in);
  return reader.read_schema();
}

} // namespace sanitize::internal::options_io
