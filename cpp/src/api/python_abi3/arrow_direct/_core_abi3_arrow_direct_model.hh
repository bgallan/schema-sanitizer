// Declares the native Arrow direct ingestion schema model.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "sanitize/core/logical_schema.hh"

namespace core_abi3_internal {

// Physical Arrow storage selected once while parsing the C schema.
enum class ArrowStorageKind : std::uint8_t {
  kNone,
  kInt8,
  kUInt8,
  kInt16,
  kUInt16,
  kInt32,
  kUInt32,
  kInt64,
  kUInt64,
  kFloat32,
  kFloat64,
  kOffset32,
  kOffset64,
  kTimeMilliseconds,
  kTimeMicroseconds,
  kTimeNanoseconds,
  kDurationSeconds,
  kDurationMilliseconds,
  kDurationMicroseconds,
  kDurationNanoseconds,
  kIntervalMonths,
  kIntervalDayTime,
  kIntervalMonthDayNano,
};

// Logical value families consumed from Arrow C Data arrays.
enum class ArrowNodeKind : std::uint8_t {
  kNull,
  kBool,
  kInt,
  kUInt64Text,
  kFloat,
  kUtf8,
  kBinaryBase64,
  kDecimalText,
  kTimestamp,
  kDate32,
  kDate64,
  kTime32s,
  kTimeText,
  kDurationText,
  kIntervalText,
  kStruct,
  kList,
  kLargeList,
  kFixedSizeList,
  kMap,
  kDictionary,
};

// Parsed Arrow C schema node used by the direct frontend at row materialization
// time.
struct ArrowInputNode {
  std::string name;
  ArrowNodeKind kind = ArrowNodeKind::kNull;
  ArrowStorageKind storage_kind = ArrowStorageKind::kNone;
  sanitize::LogicalType logical_type;
  std::vector<ArrowInputNode> children;
  int64_t timestamp_source_units_per_second = 1000000000LL;
  int64_t timestamp_target_units_per_second = 1000000LL;
  int32_t decimal_scale = 0;
  int32_t decimal_byte_width = 16;
  int32_t fixed_size_list_size = 0;
};

} // namespace core_abi3_internal
