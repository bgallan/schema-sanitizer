// Implements JSON row-mode selection and validated top-level token handoff.
#include "frontends/json/text_row_pipeline.hh"

#include "internal/materialization/ingest_stream/column_partition.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/json/ondemand/scan.hh"
#include "internal/parsing/row_scanner.hh"
#include "internal/runtime/execution_policy.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

#include <limits>
#include <new>
#include <string>

namespace sanitize::internal {
namespace {

struct ValidationContext {
  std::size_t fields = 0;
};

sanitize::Status count_field(void *raw_ctx, std::string_view, uint64_t,
                             ValueView) {
  auto *ctx = static_cast<ValidationContext *>(raw_ctx);
  if (ctx->fields >= kMaxMaterializedFieldsPerRow) {
    return sanitize::Status::Invalid(
        "JSON object field count exceeds safety limit: ", ctx->fields + 1U,
        " > ", kMaxMaterializedFieldsPerRow);
  }
  ++ctx->fields;
  return sanitize::Status::OK();
}

std::string_view trim_leading_ws(std::string_view value) noexcept {
  while (!value.empty() && is_ws(static_cast<unsigned char>(value.front()))) {
    value.remove_prefix(1);
  }
  return value;
}

sanitize::Status require_root_end(json_scan::Cursor &cursor) {
  json_scan::skip_ws(cursor);
  if (cursor.p != cursor.end) {
    return sanitize::Status::Invalid(
        "JSON parse error: trailing characters at byte ",
        std::to_string(cursor.offset()));
  }
  return sanitize::Status::OK();
}

sanitize::Status prefixed_error(std::size_t base_offset,
                                std::string_view message) {
  return sanitize::Status::Invalid(
      std::string("JSON parse error at byte ") +
      std::to_string(static_cast<int64_t>(base_offset)) + ": " +
      std::string(message));
}

sanitize::Status canonical_object_validation(JsonOnDemandDoc *doc,
                                             std::string_view raw,
                                             std::size_t base_offset) {
  doc->Reset();
  ValidationContext context;
  const auto status =
      doc->ForEachObjectFieldC(raw, &context, &count_field, base_offset);
  doc->Reset();
  return status;
}

sanitize::Status
scan_object_tokens(std::string_view raw, std::size_t base_offset,
                   std::pmr::vector<JsonValidatedFieldToken> *tokens,
                   std::size_t max_tokens, const sanitize::CompiledPlan *plan,
                   bool *captured, bool *has_escapes, bool *plan_ordered) {
  if (!captured || !has_escapes || !plan_ordered) {
    return sanitize::Status::Invalid("JSON token capture state is null");
  }
  *has_escapes = false;
  *plan_ordered = plan != nullptr;
  *captured = tokens &&
              raw.size() <= std::numeric_limits<std::uint32_t>::max() &&
              tokens->size() <= max_tokens &&
              tokens->size() <= std::numeric_limits<std::uint32_t>::max();

  json_scan::Cursor cursor{
      .p = raw.data(),
      .end = raw.data() + raw.size(),
      .base = base_offset,
      .text_begin = raw.data(),
  };
  json_scan::skip_ws(cursor);
  SAN_RETURN_NOT_OK(json_scan::expect(cursor, '{'));
  json_scan::skip_ws(cursor);
  if (cursor.p < cursor.end && *cursor.p == '}') {
    ++cursor.p;
    *has_escapes = cursor.saw_escape;
    *plan_ordered = *plan_ordered && plan->columns.empty();
    return require_root_end(cursor);
  }

  std::size_t fields = 0;
  while (true) {
    if (fields >= kMaxMaterializedFieldsPerRow) {
      return sanitize::Status::Invalid(
          "JSON object field count exceeds safety limit: ", fields + 1U, " > ",
          kMaxMaterializedFieldsPerRow);
    }
    if (cursor.p >= cursor.end || *cursor.p != '"') {
      return sanitize::Status::Invalid(
          "JSON parse error: expected string key at byte ",
          std::to_string(cursor.offset()));
    }

    const char *key_start = cursor.p;
    const char *key_content_begin = nullptr;
    const char *key_content_end = nullptr;
    bool key_has_escapes = false;
    SAN_RETURN_NOT_OK(json_scan::scan_string(cursor, key_content_begin,
                                             key_content_end, key_has_escapes));
    if (*plan_ordered) {
      if (key_has_escapes || fields >= plan->columns.size() ||
          std::string_view(
              key_content_begin,
              static_cast<std::size_t>(key_content_end - key_content_begin)) !=
              plan->columns[fields].name ||
          plan->columns[fields].has_variant_sibling) {
        *plan_ordered = false;
      }
    }
    json_scan::skip_ws(cursor);
    SAN_RETURN_NOT_OK(json_scan::expect(cursor, ':'));
    json_scan::skip_ws(cursor);

    const char *value_start = cursor.p;
    SAN_RETURN_NOT_OK(json_scan::skip_value(cursor));

    if (*captured) {
      if (tokens->size() >= max_tokens ||
          tokens->size() >= std::numeric_limits<std::uint32_t>::max()) {
        *captured = false;
      } else {
        try {
          tokens->push_back(JsonValidatedFieldToken{
              .key_offset = static_cast<std::uint32_t>(key_start - raw.data()),
              .value_offset =
                  static_cast<std::uint32_t>(value_start - raw.data()),
          });
        } catch (const std::bad_alloc &) {
          *captured = false;
        }
      }
    }
    ++fields;

    json_scan::skip_ws(cursor);
    if (cursor.p >= cursor.end) {
      return sanitize::Status::Invalid(
          "JSON parse error: unterminated object at byte ",
          std::to_string(cursor.offset()));
    }
    if (*cursor.p == ',') {
      ++cursor.p;
      json_scan::skip_ws(cursor);
      continue;
    }
    if (*cursor.p == '}') {
      ++cursor.p;
      *has_escapes = cursor.saw_escape;
      *plan_ordered = *plan_ordered && fields == plan->columns.size();
      return require_root_end(cursor);
    }
    return sanitize::Status::Invalid(
        "JSON parse error: expected ',' or '}' at byte ",
        std::to_string(cursor.offset()));
  }
}

} // namespace

std::size_t
json_token_index_max_fields(std::int64_t memory_limit_bytes) noexcept {
  const auto budget = memory_budget_from_limit(memory_limit_bytes);
  const auto token_bytes = bounded_fraction(
      budget.total_bytes, 8, 32LL * 1024LL * 1024LL, 64LL * 1024LL);
  return static_cast<std::size_t>(token_bytes) /
         sizeof(JsonValidatedFieldToken);
}

bool json_error_exceeds_hard_safety_limit(
    const sanitize::Status &status) noexcept {
  if (status.code() != sanitize::StatusCode::kInvalid) {
    return false;
  }
  const std::string &message = status.message();
  return message.find("exceeds safety limit") != std::string::npos ||
         message.find("exceeds internal safety limit") != std::string::npos;
}

bool parallel_json_row_frontend_enabled(
    const sanitize::Options &options) noexcept {
  return options.threading_mode == sanitize::ThreadingMode::kMulti &&
         execution_policy_from(options.threading_mode,
                               options.memory_limit_bytes)
                 .effective_workers > 1;
}

JsonTextRowPolicy resolve_json_text_row_policy(
    const sanitize::CompiledPlan *plan, bool line_delimited, bool stop_on_error,
    bool direct_rows, bool parallel_rows,
    sanitize::FrontendMaterializationMode mode) noexcept {
  JsonTextRowPolicy policy;
  const bool candidate = plan && line_delimited && stop_on_error &&
                         is_column_partition_candidate(*plan);
  policy.plan_ordered =
      mode == sanitize::FrontendMaterializationMode::kPlanOrdered && candidate;
  if (mode == sanitize::FrontendMaterializationMode::kDefault) {
    policy.plan_ordered = candidate;
  }
  policy.raw_only =
      plan && !policy.plan_ordered &&
      (direct_rows || parallel_rows || (line_delimited && stop_on_error));
  if (mode == sanitize::FrontendMaterializationMode::kValidatedRaw) {
    policy.raw_only = plan && line_delimited && stop_on_error;
    policy.validate_raw = policy.raw_only;
  } else if (mode ==
             sanitize::FrontendMaterializationMode::kDeferredValidationRaw) {
    policy.plan_ordered = false;
    policy.raw_only = plan && line_delimited && stop_on_error;
    policy.validate_raw = false;
  } else if (mode ==
             sanitize::FrontendMaterializationMode::kWorkerAuthoritativeRaw) {
    policy.plan_ordered = false;
    // JSON and JSON-array scanners already own complete value boundaries.
    // Worker-local materialization performs the only authoritative parse.
    policy.raw_only = plan != nullptr;
    policy.validate_raw = false;
  }
  return policy;
}

sanitize::Result<JsonTextRowValidation> validate_json_text_row(
    JsonOnDemandDoc *doc, std::string_view raw, std::size_t base_offset,
    std::pmr::vector<JsonValidatedFieldToken> *tokens, std::size_t max_tokens,
    const sanitize::CompiledPlan *plan) {
  if (!doc) {
    return sanitize::Status::Invalid("JSON row validator is null");
  }

  const auto probe = trim_leading_ws(raw);
  if (probe.empty() || probe.front() != '{') {
    doc->Reset();
    const auto parsed = doc->ParseValue(raw, base_offset);
    const auto status = parsed.ok() ? sanitize::Status::OK() : parsed.status();
    doc->Reset();
    if (!status.ok()) {
      return prefixed_error(base_offset, status.message());
    }
    return JsonTextRowValidation{};
  }

  const auto token_begin = tokens ? tokens->size() : 0;
  bool captured = false;
  bool has_escapes = false;
  bool plan_ordered = false;
  const auto scan_status =
      scan_object_tokens(raw, base_offset, tokens, max_tokens, plan, &captured,
                         &has_escapes, &plan_ordered);
  if (!scan_status.ok() || has_escapes) {
    const auto canonical = canonical_object_validation(doc, raw, base_offset);
    if (!canonical.ok()) {
      if (tokens) {
        tokens->resize(token_begin);
      }
      return prefixed_error(base_offset, canonical.message());
    }
    if (!scan_status.ok()) {
      if (tokens) {
        tokens->resize(token_begin);
      }
      return JsonTextRowValidation{};
    }
  }

  if (!captured || !tokens ||
      token_begin > std::numeric_limits<std::uint32_t>::max() ||
      tokens->size() - token_begin >
          std::numeric_limits<std::uint32_t>::max()) {
    if (tokens) {
      tokens->resize(token_begin);
    }
    return JsonTextRowValidation{};
  }

  return JsonTextRowValidation{
      .tokenized_object = true,
      .plan_ordered_tokens = plan_ordered,
      .field_offset = static_cast<std::uint32_t>(token_begin),
      .field_count = static_cast<std::uint32_t>(tokens->size() - token_begin),
  };
}

} // namespace sanitize::internal
