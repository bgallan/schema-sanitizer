// Materializes eligible JSON objects directly in frozen plan order. The
// pipeline preserves source offsets and ownership while enforcing plan order
// and memory bounds.

#include "frontends/json/text_row_materializer.hh"

#include <new>
#include <string>

#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/row_scanner.hh"
#include "internal/planning/planned_name_matcher.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {
namespace {

struct EmitContext {
  PlanOrderedRowScratch *scratch = nullptr;
  FlatRowBatch *batch = nullptr;
  const CompiledPlan *plan = nullptr;
  std::string_view field_name_policy;
  std::size_t observed_fields = 0;
  bool requires_raw_fallback = false;
};

/// Emits plan ordered field into the destination while retaining canonical
/// field order.
sanitize::Status emit_plan_ordered_field(void *raw_context,
                                         std::string_view key,
                                         std::uint64_t key_hash,
                                         ValueView value) {
  auto *context = static_cast<EmitContext *>(raw_context);
  if (!context || !context->scratch || !context->batch || !context->plan) {
    return sanitize::Status::Invalid(
        "JSON plan-ordered materializer received invalid context");
  }
  if (context->observed_fields >= kMaxMaterializedFieldsPerRow) {
    return sanitize::Status::Invalid(
        "JSON object field count exceeds safety limit: ",
        context->observed_fields + 1U, " > ", kMaxMaterializedFieldsPerRow);
  }
  ++context->observed_fields;

  bool empty_container = false;
  SAN_RETURN_NOT_OK(value.container_is_empty(&empty_container));
  if (empty_container) {
    // Generic row lookup ignores empty containers entirely. Leaving the
    // prefilled null slot unseen also allows a later duplicate scalar field
    // to become the first materializable value, exactly as RowFieldSnapshot.
    return sanitize::Status::OK();
  }
  if (value.is_object() || value.is_array()) {
    context->requires_raw_fallback = true;
    return sanitize::Status::OK();
  }

  const auto *planned = find_planned_field(
      context->plan->root_layout, key, key_hash, context->field_name_policy);
  if (planned && planned->index >= 0) {
    const auto index = static_cast<std::size_t>(planned->index);
    if (index < context->scratch->planned_seen.size() &&
        context->scratch->planned_seen[index] == 0) {
      const auto &column = context->plan->columns[index];
      if (!context->batch->set_current_row_field(
              index, FieldRef{.key = column.name,
                              .key_hash = column.name_hash,
                              .value = value})) {
        return sanitize::Status::Invalid(
            "JSON plan-ordered field slot is out of range");
      }
      context->scratch->planned_seen[index] = 1;
    }
    return sanitize::Status::OK();
  }
  context->batch->push(
      FieldRef{.key = key, .key_hash = key_hash, .value = value});
  return sanitize::Status::OK();
}

/// Removes leading ASCII whitespace without allocating a replacement string.
std::string_view trim_leading_json_ws(std::string_view value) noexcept {
  while (!value.empty() && is_ws(static_cast<unsigned char>(value.front()))) {
    value.remove_prefix(1);
  }
  return value;
}

/// Creates an invalid status that prefixes the parser message with its absolute
/// byte offset.
sanitize::Status prefixed_parse_error(std::size_t base_offset,
                                      std::string_view message) {
  return sanitize::Status::Invalid(
      std::string("JSON parse error at byte ") +
      std::to_string(static_cast<int64_t>(base_offset)) + ": " +
      std::string(message));
}

} // namespace

/// Materializes one JSON object in compiled-plan order and fills absent fields
/// with nulls.
sanitize::Status
append_plan_ordered_json_row(JsonOnDemandDoc *document, FlatRowBatch *batch,
                             PlanOrderedRowScratch *scratch,
                             const CompiledPlan &plan,
                             std::string_view field_name_policy,
                             std::string_view raw, std::size_t base_offset) {
  if (!document || !batch || !scratch) {
    return sanitize::Status::Invalid(
        "JSON plan-ordered materializer received null state");
  }
  const std::string_view probe = trim_leading_json_ws(raw);
  if (probe.empty() || probe.front() != '{') {
    rewrite_current_row_as_raw(batch);
    return sanitize::Status::OK();
  }

  try {
    scratch->planned_seen.assign(plan.columns.size(), std::uint8_t{0});
    for (const auto &column : plan.columns) {
      batch->push(FieldRef{.key = column.name,
                           .key_hash = column.name_hash,
                           .value = ValueView::Null()});
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "JSON plan-ordered row scratch allocation failed");
  }

  EmitContext context{.scratch = scratch,
                      .batch = batch,
                      .plan = &plan,
                      .field_name_policy = field_name_policy};
  auto status = document->ForEachObjectFieldC(
      raw, &context, &emit_plan_ordered_field, base_offset);
  if (!status.ok()) {
    return prefixed_parse_error(base_offset, status.message());
  }
  if (context.requires_raw_fallback) {
    rewrite_current_row_as_raw(batch);
    return sanitize::Status::OK();
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
