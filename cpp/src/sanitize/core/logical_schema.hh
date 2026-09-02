// Defines owned logical schema fields, recursive types, and traversal helpers.
// Copy-safe models and depth calculations provide the common schema contract
// used by planning, registry evolution, and analytical output adapters.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace sanitize {

// A minimal logical type system used for inference + schema evolution.
//
// Design goals:
// - Keep Arrow types at the boundary (materialization + output), not in
// inference.
// - Provide a small, stable set of kinds that map cleanly to ValueView tags.
// - Remain cheap to traverse and stringify.

enum class LogicalKind : uint8_t {
  kNull = 0,
  kBool,
  kInt64,
  kFloat64,
  kUtf8,
  kTimestampNs,
  kDate32,
  kTime32s,
  kStruct,
  kList,
};

struct LogicalType;

struct LogicalField {
  std::string name;
  std::unique_ptr<LogicalType> type;
  bool nullable = true;

  /// Creates an empty logical field.
  LogicalField() = default;
  /// Creates a deep copy of another logical field.
  LogicalField(const LogicalField &o);
  /// Copies another logical field.
  LogicalField &operator=(const LogicalField &o);
  /// Moves a logical field.
  LogicalField(LogicalField &&) noexcept = default;
  /// Moves another logical field.
  LogicalField &operator=(LogicalField &&) noexcept = default;
};

struct LogicalType {
  LogicalKind kind = LogicalKind::kNull;

  // Struct children.
  std::vector<LogicalField> fields;

  // List element.
  std::unique_ptr<LogicalType> value;

  /// Creates a null logical type.
  LogicalType() = default;
  /// Creates a logical type for one scalar or container kind.
  explicit LogicalType(LogicalKind k) : kind(k) {}

  /// Creates a deep copy of another logical type.
  LogicalType(const LogicalType &o);
  /// Copies another logical type.
  LogicalType &operator=(const LogicalType &o);
  /// Moves a logical type.
  LogicalType(LogicalType &&) noexcept = default;
  /// Moves another logical type.
  LogicalType &operator=(LogicalType &&) noexcept = default;

  /// Creates a boolean logical type.
  static LogicalType Bool() { return LogicalType(LogicalKind::kBool); }
  /// Creates an int64 logical type.
  static LogicalType Int64() { return LogicalType(LogicalKind::kInt64); }
  /// Creates a float64 logical type.
  static LogicalType Float64() { return LogicalType(LogicalKind::kFloat64); }
  /// Creates a UTF-8 logical type.
  static LogicalType Utf8() { return LogicalType(LogicalKind::kUtf8); }
  /// Creates a nanosecond timestamp logical type.
  static LogicalType TimestampNs() {
    return LogicalType(LogicalKind::kTimestampNs);
  }
  /// Creates a date32 logical type.
  static LogicalType Date32() { return LogicalType(LogicalKind::kDate32); }
  /// Creates a time32 logical type.
  static LogicalType Time32s() { return LogicalType(LogicalKind::kTime32s); }

  /// Creates a list logical type.
  static LogicalType List(LogicalType elem);
  /// Creates a struct logical type.
  static LogicalType Struct(std::vector<LogicalField> f);
};

struct LogicalSchema {
  std::vector<LogicalField> fields;

  /// Creates an empty logical schema.
  LogicalSchema() = default;
  /// Creates a deep copy of another logical schema.
  LogicalSchema(const LogicalSchema &o);
  /// Copies another logical schema.
  LogicalSchema &operator=(const LogicalSchema &o);
  /// Moves a logical schema.
  LogicalSchema(LogicalSchema &&) noexcept = default;
  /// Moves another logical schema.
  LogicalSchema &operator=(LogicalSchema &&) noexcept = default;
};

/// Computes Arrow container depth. Scalar leaves and root field wrappers do not
/// count, while struct and list containers do.
int arrow_schema_depth(const LogicalSchema &s);

/// Computes Parquet/BigQuery RECORD depth. Scalar leaves, root field wrappers,
/// and list containers do not count, while struct containers do.
int parquet_schema_depth(const LogicalSchema &s);

} // namespace sanitize
