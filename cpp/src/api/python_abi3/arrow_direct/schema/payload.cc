// Encodes Arrow direct logical schemas into the Python options payload format.

#include "api/python_abi3/arrow_direct/schema/payload.hh"
#include "api/python_abi3/arrow_direct/schema/logical.hh"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace core_abi3_internal {
namespace {

constexpr std::uint8_t kLogicalKindNull = 0;
constexpr std::uint8_t kLogicalKindBool = 1;
constexpr std::uint8_t kLogicalKindInt64 = 2;
constexpr std::uint8_t kLogicalKindFloat64 = 3;
constexpr std::uint8_t kLogicalKindUtf8 = 4;
constexpr std::uint8_t kLogicalKindTimestampNs = 5;
constexpr std::uint8_t kLogicalKindDate32 = 6;
constexpr std::uint8_t kLogicalKindTime32s = 7;
constexpr std::uint8_t kLogicalKindStruct = 8;
constexpr std::uint8_t kLogicalKindList = 9;

// Appends a little-endian u8 to a logical schema payload.
void append_u8(std::string &out, std::uint8_t value) {
  out.push_back(static_cast<char>(value));
}

// Appends a little-endian u32 to a logical schema payload.
void append_u32(std::string &out, std::uint32_t value) {
  out.push_back(static_cast<char>(value & 0xFFU));
  out.push_back(static_cast<char>((value >> 8U) & 0xFFU));
  out.push_back(static_cast<char>((value >> 16U) & 0xFFU));
  out.push_back(static_cast<char>((value >> 24U) & 0xFFU));
}

// Appends a length-prefixed UTF-8 string to a logical schema payload.
void append_string(std::string &out, std::string_view value) {
  append_u32(out, static_cast<std::uint32_t>(value.size()));
  out.append(value.data(), value.size());
}

// Converts a logical type to the compact options payload kind byte.
std::uint8_t logical_kind_byte(const sanitize::LogicalType &type) {
  using Kind = sanitize::LogicalKind;
  switch (type.kind) {
  case Kind::kNull:
    return kLogicalKindNull;
  case Kind::kBool:
    return kLogicalKindBool;
  case Kind::kInt64:
    return kLogicalKindInt64;
  case Kind::kFloat64:
    return kLogicalKindFloat64;
  case Kind::kUtf8:
    return kLogicalKindUtf8;
  case Kind::kTimestampNs:
    return kLogicalKindTimestampNs;
  case Kind::kDate32:
    return kLogicalKindDate32;
  case Kind::kTime32s:
    return kLogicalKindTime32s;
  case Kind::kStruct:
    return kLogicalKindStruct;
  case Kind::kList:
    return kLogicalKindList;
  }
  return kLogicalKindUtf8;
}

void encode_logical_type(std::string &out, const sanitize::LogicalType &type);

// Encodes one logical field into the Python options payload layout.
void encode_logical_field(std::string &out,
                          const sanitize::LogicalField &field) {
  append_string(out, field.name);
  append_u8(out, field.nullable ? 1U : 0U);
  encode_logical_type(out, *field.type);
}

void encode_logical_type(std::string &out, const sanitize::LogicalType &type) {
  const std::uint8_t kind = logical_kind_byte(type);
  append_u8(out, kind);
  if (kind == kLogicalKindStruct) {
    append_u32(out, static_cast<std::uint32_t>(type.fields.size()));
    for (const auto &field : type.fields) {
      encode_logical_field(out, field);
    }
    return;
  }
  if (kind == kLogicalKindList && type.value) {
    encode_logical_type(out, *type.value);
  }
}

} // namespace

sanitize::Result<std::string>
logical_schema_payload_from_arrow_schema(const ArrowSchema *schema,
                                         const ArrowDirectOptions &options) {
  std::vector<ArrowInputNode> fields;
  SAN_ASSIGN_OR_RAISE(
      auto logical, logical_schema_from_arrow_schema(schema, &fields, options));
  std::string payload;
  append_u32(payload, static_cast<std::uint32_t>(logical.fields.size()));
  for (const auto &field : logical.fields) {
    encode_logical_field(payload, field);
  }
  return payload;
}

} // namespace core_abi3_internal
