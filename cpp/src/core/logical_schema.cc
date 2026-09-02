// Implements logical schema construction, traversal, and copying helpers.
// It owns deep-copy semantics for recursive types and computes the nesting
// depths used by Arrow and Parquet contract enforcement.

#include "sanitize/core/logical_schema.hh"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace sanitize {

namespace {

/// Deep-copies a logical type tree.
static std::unique_ptr<LogicalType> clone_type(const LogicalType &t);

/// Deep-copies one logical field and its type.
static LogicalField clone_field(const LogicalField &f) {
  LogicalField out;
  out.name = f.name;
  out.nullable = f.nullable;
  if (f.type)
    out.type = clone_type(*f.type);
  return out;
}

static std::unique_ptr<LogicalType> clone_type(const LogicalType &t) {
  auto out = std::make_unique<LogicalType>();
  out->kind = t.kind;
  if (!t.fields.empty()) {
    out->fields.reserve(t.fields.size());
    for (const auto &f : t.fields)
      out->fields.push_back(clone_field(f));
  }
  if (t.value)
    out->value = clone_type(*t.value);
  return out;
}

/// Computes Arrow container depth for a logical type tree.
static int arrow_depth_type(const LogicalType &t) {
  switch (t.kind) {
  case LogicalKind::kStruct: {
    int maxd = 0;
    for (const auto &f : t.fields) {
      if (f.type)
        maxd = std::max(maxd, arrow_depth_type(*f.type));
    }
    return 1 + maxd;
  }
  case LogicalKind::kList:
    return 1 + (t.value ? arrow_depth_type(*t.value) : 0);
  default:
    return 0;
  }
}

/// Computes Parquet/BigQuery RECORD depth for a logical type tree.
static int parquet_depth_type(const LogicalType &t) {
  switch (t.kind) {
  case LogicalKind::kStruct: {
    int maxd = 0;
    for (const auto &f : t.fields) {
      if (f.type)
        maxd = std::max(maxd, parquet_depth_type(*f.type));
    }
    return 1 + maxd;
  }
  case LogicalKind::kList:
    return t.value ? parquet_depth_type(*t.value) : 0;
  default:
    return 0;
  }
}

} // namespace

LogicalField::LogicalField(const LogicalField &o) {
  name = o.name;
  nullable = o.nullable;
  if (o.type)
    type = clone_type(*o.type);
}

LogicalField &LogicalField::operator=(const LogicalField &o) {
  if (this == &o)
    return *this;
  name = o.name;
  nullable = o.nullable;
  type.reset();
  if (o.type)
    type = clone_type(*o.type);
  return *this;
}

LogicalSchema::LogicalSchema(const LogicalSchema &o) {
  fields.reserve(o.fields.size());
  for (const auto &f : o.fields)
    fields.push_back(clone_field(f));
}

LogicalSchema &LogicalSchema::operator=(const LogicalSchema &o) {
  if (this == &o)
    return *this;
  fields.clear();
  fields.reserve(o.fields.size());
  for (const auto &f : o.fields)
    fields.push_back(clone_field(f));
  return *this;
}

LogicalType::LogicalType(const LogicalType &o) {
  kind = o.kind;
  if (!o.fields.empty()) {
    fields.reserve(o.fields.size());
    for (const auto &f : o.fields)
      fields.push_back(clone_field(f));
  }
  if (o.value)
    value = clone_type(*o.value);
}

LogicalType &LogicalType::operator=(const LogicalType &o) {
  if (this == &o)
    return *this;
  kind = o.kind;
  fields.clear();
  value.reset();
  if (!o.fields.empty()) {
    fields.reserve(o.fields.size());
    for (const auto &f : o.fields)
      fields.push_back(clone_field(f));
  }
  if (o.value)
    value = clone_type(*o.value);
  return *this;
}

LogicalType LogicalType::List(LogicalType elem) {
  LogicalType out(LogicalKind::kList);
  out.value = std::make_unique<LogicalType>(std::move(elem));
  return out;
}

LogicalType LogicalType::Struct(std::vector<LogicalField> f) {
  LogicalType out(LogicalKind::kStruct);
  out.fields = std::move(f);
  return out;
}

int arrow_schema_depth(const LogicalSchema &s) {
  int maxd = 0;
  for (const auto &f : s.fields) {
    if (f.type)
      maxd = std::max(maxd, arrow_depth_type(*f.type));
  }
  return maxd;
}

int parquet_schema_depth(const LogicalSchema &s) {
  int maxd = 0;
  for (const auto &f : s.fields) {
    if (f.type)
      maxd = std::max(maxd, parquet_depth_type(*f.type));
  }
  return maxd;
}

} // namespace sanitize
