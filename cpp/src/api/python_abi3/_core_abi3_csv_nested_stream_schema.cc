/*
 * CSV nested-column stream schema rewriting.
 *
 * Builds the output Arrow schema where top-level nested columns become UTF-8.
 */
#include "api/python_abi3/_core_abi3_csv_nested_stream_parts.hh"

#include <cstddef>
#include <cstdint>
#include <utility>

namespace core_abi3_internal::csv_nested_stream {

namespace {

bool is_nested_kind(jsonl::JsonlKind kind) {
  return kind == jsonl::JsonlKind::kStruct || kind == jsonl::JsonlKind::kList ||
         kind == jsonl::JsonlKind::kLargeList || kind == jsonl::JsonlKind::kMap;
}

} // namespace

sanitize::Status load_csv_nested_schema(CsvNestedStreamState *stream_state,
                                        ArrowSchema *base_schema) {
  stream_state->columns.clear();
  if (base_schema->n_children < 0) {
    return sanitize::Status::Invalid("CSV nested stream: invalid schema");
  }
  stream_state->columns.reserve(
      static_cast<std::size_t>(base_schema->n_children));
  for (std::int64_t i = 0; i < base_schema->n_children; ++i) {
    if (!base_schema->children || !base_schema->children[i]) {
      return sanitize::Status::Invalid(
          "CSV nested stream: missing schema child");
    }
    SAN_ASSIGN_OR_RAISE(auto field,
                        jsonl::parse_schema_field(*base_schema->children[i]));
    CsvNestedColumnPlan plan;
    plan.nested = is_nested_kind(field.kind);
    plan.field = std::move(field);
    stream_state->columns.push_back(std::move(plan));
  }
  stream_state->schema_loaded = true;
  return sanitize::Status::OK();
}

sanitize::Status append_schema_children(CsvNestedStreamState *stream_state,
                                        CsvNestedSchemaState *schema_state) {
  ArrowSchema &base = schema_state->base.value();
  const std::int64_t base_children = base.n_children;
  schema_state->children.reserve(static_cast<std::size_t>(base_children));
  schema_state->nested_fields.reserve(stream_state->columns.size());
  for (std::int64_t i = 0; i < base_children; ++i) {
    const auto &column = stream_state->columns[static_cast<std::size_t>(i)];
    if (!column.nested) {
      schema_state->children.push_back(base.children[i]);
      continue;
    }
    CsvNestedSchemaChild child;
    child.name = column.field.name;
    schema_state->nested_fields.push_back(std::move(child));
    auto &stored = schema_state->nested_fields.back();
    clear_schema(&stored.schema);
    stored.schema.format = "u";
    stored.schema.name = stored.name.c_str();
    stored.schema.metadata = nullptr;
    stored.schema.flags = base.children[i]->flags;
    stored.schema.n_children = 0;
    stored.schema.children = nullptr;
    stored.schema.dictionary = nullptr;
    stored.schema.private_data = nullptr;
    stored.schema.release = &csv_nested_schema_child_release;
    schema_state->children.push_back(&stored.schema);
  }
  return sanitize::Status::OK();
}

} // namespace core_abi3_internal::csv_nested_stream
