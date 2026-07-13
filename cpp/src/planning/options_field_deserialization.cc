// Typed option-field decoding for the stable SZOPT payload.
#include "internal/planning/options_deserialization.hh"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "internal/planning/options_bytes_reader.hh"
#include "internal/planning/options_schema_serialization.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"

namespace sanitize::internal::options_io {

namespace {

using internal::options_io::read_string;
using internal::options_io::read_u32;
using internal::options_io::read_u8;

struct OptionFieldReader {
  sanitize::Status (*read)(std::string_view bytes, std::size_t *pos,
                           Options *out);
};

// Reads an optional logical schema field from the options payload.
static sanitize::Result<std::optional<sanitize::LogicalSchema>>
read_schema(std::string_view in, std::size_t *pos) {
  uint8_t has = 0;
  if (!read_u8(in, pos, &has))
    return sanitize::Status::Invalid(
        "deserialize_options: truncated schema flag");
  if (has == 0)
    return std::nullopt;
  uint32_t n = 0;
  if (!read_u32(in, pos, &n))
    return sanitize::Status::Invalid(
        "deserialize_options: truncated schema size");
  if (*pos > in.size() || static_cast<std::size_t>(n) > in.size() - *pos)
    return sanitize::Status::Invalid("deserialize_options: truncated schema");
  const std::string_view payload = in.substr(*pos, n);
  *pos += n;
  SAN_ASSIGN_OR_RAISE(
      auto logical_schema,
      sanitize::internal::options_io::deserialize_logical_schema_bytes(
          payload));
  return logical_schema;
}

// Scalar reads
static bool read_value(std::string_view in, std::size_t *pos, int32_t *out) {
  uint32_t u = 0;
  if (!read_u32(in, pos, &u))
    return false;
  *out = static_cast<int32_t>(u);
  return true;
}

// Reads a little-endian int64 option value.
static bool read_value(std::string_view in, std::size_t *pos, int64_t *out) {
  if (!pos || !out || *pos > in.size() || in.size() - *pos < 8)
    return false;
  const auto *p = reinterpret_cast<const uint8_t *>(in.data() + *pos);
  uint64_t u = 0;
  for (int i = 0; i < 8; ++i)
    u |= (static_cast<uint64_t>(p[i]) << (8 * i));
  *pos += 8;
  *out = static_cast<int64_t>(u);
  return true;
}

// Reads a length-prefixed string option value.
static bool read_value(std::string_view in, std::size_t *pos,
                       std::string *out) {
  return read_string(in, pos, out);
}

// Reads a vector of length-prefixed string option values.
static bool read_value(std::string_view in, std::size_t *pos,
                       std::vector<std::string> *out) {
  uint32_t n = 0;
  if (!read_u32(in, pos, &n))
    return false;
  out->clear();
  out->reserve(n);
  for (uint32_t i = 0; i < n; ++i) {
    std::string s;
    if (!read_string(in, pos, &s))
      return false;
    out->push_back(std::move(s));
  }
  return true;
}

// Returns whether a decoded integer belongs to an enum's wire domain.
template <auto... Values> static bool enum_wire_value(int32_t value) {
  constexpr auto allowed = std::array{std::to_underlying(Values)...};
  return std::ranges::contains(allowed, value);
}

template <class Enum> static bool valid_enum_value(int32_t value) {
  if constexpr (std::is_same_v<Enum, SchemaEvolutionMode>) {
    return enum_wire_value<SchemaEvolutionMode::kStrict,
                           SchemaEvolutionMode::kAdditive>(value);
  } else if constexpr (std::is_same_v<Enum, FieldOrderPolicy>) {
    return enum_wire_value<FieldOrderPolicy::kAlphabetically,
                           FieldOrderPolicy::kSchemaContractFirst>(value);
  } else if constexpr (std::is_same_v<Enum, OnErrorPolicy>) {
    return enum_wire_value<OnErrorPolicy::kStop, OnErrorPolicy::kSkipRow,
                           OnErrorPolicy::kEmitNullRow>(value);
  } else {
    return true;
  }
}

// Reads one typed option field from the stable binary options payload.
template <class T>
static sanitize::Status read_option_field(std::string_view bytes,
                                          std::size_t *pos, T *out,
                                          const char *field_name) {
  if constexpr (std::is_same_v<T, std::optional<sanitize::LogicalSchema>>) {
    SAN_ASSIGN_OR_RAISE(auto schema, read_schema(bytes, pos));
    *out = std::move(schema);
    return sanitize::Status::OK();
  } else if constexpr (std::is_same_v<T, bool>) {
    uint8_t b = 0;
    if (!read_u8(bytes, pos, &b)) {
      return sanitize::Status::Invalid(
          std::string("deserialize_options: truncated field: ") + field_name);
    }
    if (b > 1) {
      return sanitize::Status::Invalid(
          std::string("deserialize_options: invalid bool field: ") +
          field_name);
    }
    *out = (b != 0);
    return sanitize::Status::OK();
  } else if constexpr (std::is_enum_v<T>) {
    int32_t v = 0;
    if (!read_value(bytes, pos, &v)) {
      return sanitize::Status::Invalid(
          std::string("deserialize_options: truncated field: ") + field_name);
    }
    if (!valid_enum_value<T>(v)) {
      return sanitize::Status::Invalid(
          std::string("deserialize_options: invalid enum field: ") +
          field_name);
    }
    *out = static_cast<T>(v);
    return sanitize::Status::OK();
  } else {
    if (!read_value(bytes, pos, out)) {
      return sanitize::Status::Invalid(
          std::string("deserialize_options: truncated field: ") + field_name);
    }
    return sanitize::Status::OK();
  }
}

static constexpr auto kOptionFieldReaders = std::to_array<OptionFieldReader>({
#define SCHEMA_SANITIZER_OPTION(type, name, default_expr, group, doc)          \
  OptionFieldReader{+[](std::string_view bytes, std::size_t *pos,              \
                        Options *out) -> sanitize::Status {                    \
    return read_option_field<type>(bytes, pos, &out->name, #name);             \
  }},
#define SCHEMA_SANITIZER_OPTION_DEFAULT(type, name, group, doc)                \
  OptionFieldReader{+[](std::string_view bytes, std::size_t *pos,              \
                        Options *out) -> sanitize::Status {                    \
    return read_option_field<type>(bytes, pos, &out->name, #name);             \
  }},
#include "sanitize/options/options_catalog.def"
#undef SCHEMA_SANITIZER_OPTION_DEFAULT
#undef SCHEMA_SANITIZER_OPTION
});

} // namespace

// Reads all catalog fields in stable wire-format order.
sanitize::Status read_option_fields(std::string_view bytes, std::size_t *pos,
                                    Options *out) {
  for (const OptionFieldReader &field : kOptionFieldReaders) {
    SAN_RETURN_NOT_OK(field.read(bytes, pos, out));
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal::options_io
