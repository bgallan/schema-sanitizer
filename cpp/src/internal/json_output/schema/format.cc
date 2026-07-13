// Maps Arrow C Data format strings to native writer field kinds.

#include "internal/json_output/schema/model.hh"

#include <string>
#include <string_view>

namespace sanitize::internal::jsonl_stream_writer {

sanitize::Result<JsonlKind> kind_from_format(std::string_view format) {
  if (format == "n") {
    return JsonlKind::kNull;
  }
  if (format == "b") {
    return JsonlKind::kBool;
  }
  if (format == "c") {
    return JsonlKind::kInt8;
  }
  if (format == "C") {
    return JsonlKind::kUInt8;
  }
  if (format == "s") {
    return JsonlKind::kInt16;
  }
  if (format == "S") {
    return JsonlKind::kUInt16;
  }
  if (format == "i") {
    return JsonlKind::kInt32;
  }
  if (format == "I") {
    return JsonlKind::kUInt32;
  }
  if (format == "l") {
    return JsonlKind::kInt64;
  }
  if (format == "L") {
    return JsonlKind::kUInt64;
  }
  if (format == "e") {
    return JsonlKind::kFloat16;
  }
  if (format == "f") {
    return JsonlKind::kFloat32;
  }
  if (format == "g") {
    return JsonlKind::kFloat64;
  }
  if (format == "u") {
    return JsonlKind::kString;
  }
  if (format == "U") {
    return JsonlKind::kLargeString;
  }
  if (format == "z") {
    return JsonlKind::kBinary;
  }
  if (format == "Z") {
    return JsonlKind::kLargeBinary;
  }
  if (format.starts_with("w:")) {
    return JsonlKind::kFixedSizeBinary;
  }
  if (format.starts_with("tsm:")) {
    return JsonlKind::kTimestampMillis;
  }
  if (format.starts_with("tsu:")) {
    return JsonlKind::kTimestampMicros;
  }
  if (format.starts_with("tsn:")) {
    return JsonlKind::kTimestampNanos;
  }
  if (format == "tdD") {
    return JsonlKind::kDate32;
  }
  if (format == "tdm") {
    return JsonlKind::kDate64;
  }
  if (format == "tts") {
    return JsonlKind::kTime32s;
  }
  if (format == "ttm") {
    return JsonlKind::kTime32ms;
  }
  if (format == "ttu") {
    return JsonlKind::kTime64us;
  }
  if (format == "ttn") {
    return JsonlKind::kTime64ns;
  }
  if (format == "tDs" || format == "tDm" || format == "tDu" ||
      format == "tDn") {
    return JsonlKind::kDuration;
  }
  if (format == "tiM" || format == "tiD" || format == "tin") {
    return JsonlKind::kInterval;
  }
  if (format.starts_with("d:")) {
    return JsonlKind::kDecimal;
  }
  if (format == "+s") {
    return JsonlKind::kStruct;
  }
  if (format == "+l") {
    return JsonlKind::kList;
  }
  if (format == "+L") {
    return JsonlKind::kLargeList;
  }
  if (format.starts_with("+w:")) {
    return JsonlKind::kFixedSizeList;
  }
  if (format == "+m") {
    return JsonlKind::kMap;
  }
  return sanitize::Status::Invalid("JSONL writer: unsupported Arrow format '",
                                   std::string(format), "'");
}

} // namespace sanitize::internal::jsonl_stream_writer
