// Materializes syntax-validated JSON object fields without reparsing the root.

#include "internal/materialization/batch_appender_internal.hh"

#include "internal/materialization/conversion/detail.hh"
#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/json/ondemand/scan.hh"
#include "internal/parsing/json/validated_row.hh"
#include "sanitize/core/primitives.hh"
#include "sanitize/detail/hash.hh"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>
#include <system_error>
#include <vector>

namespace sanitize::internal {
namespace {

class JsonTokenScratchReset final {
public:
  explicit JsonTokenScratchReset(JsonOnDemandDoc *doc) noexcept : doc_(doc) {}
  ~JsonTokenScratchReset() noexcept {
    if (doc_) {
      doc_->Reset();
    }
  }

private:
  JsonOnDemandDoc *doc_ = nullptr;
};

[[nodiscard]] bool token_offset_is_valid(std::string_view raw,
                                         std::uint32_t offset) noexcept {
  return static_cast<std::size_t>(offset) <= raw.size();
}

void trim_json_ws_back(std::string_view raw, std::size_t *offset) noexcept {
  while (*offset > 0 && json_scan::is_ws(raw[*offset - 1])) {
    --*offset;
  }
}

sanitize::Result<std::string_view>
validated_key_token(std::string_view raw,
                    const JsonValidatedFieldToken &token) {
  if (!token_offset_is_valid(raw, token.key_offset) ||
      !token_offset_is_valid(raw, token.value_offset) ||
      token.key_offset >= token.value_offset) {
    return Status::Invalid("validated JSON key token is out of range");
  }
  auto key_end = static_cast<std::size_t>(token.value_offset);
  trim_json_ws_back(raw, &key_end);
  if (key_end == 0 || raw[key_end - 1] != ':') {
    return Status::Invalid("validated JSON key separator is missing");
  }
  --key_end;
  trim_json_ws_back(raw, &key_end);
  const auto key_begin = static_cast<std::size_t>(token.key_offset);
  if (key_end <= key_begin + 1 || raw[key_begin] != '"' ||
      raw[key_end - 1] != '"') {
    return Status::Invalid("validated JSON key token is not a string");
  }
  return raw.substr(key_begin, key_end - key_begin);
}

sanitize::Result<std::string_view>
validated_value_token(std::string_view raw,
                      const JsonValidatedFieldToken *tokens, std::size_t count,
                      std::size_t index) {
  const auto value_begin = static_cast<std::size_t>(tokens[index].value_offset);
  if (value_begin > raw.size()) {
    return Status::Invalid("validated JSON value token is out of range");
  }
  auto value_end = index + 1U < count
                       ? static_cast<std::size_t>(tokens[index + 1U].key_offset)
                       : raw.size();
  if (value_end > raw.size() || value_end < value_begin) {
    return Status::Invalid("validated JSON value boundary is out of range");
  }
  trim_json_ws_back(raw, &value_end);
  const char separator = index + 1U < count ? ',' : '}';
  if (value_end == 0 || raw[value_end - 1] != separator) {
    return Status::Invalid("validated JSON value separator is missing");
  }
  --value_end;
  trim_json_ws_back(raw, &value_end);
  if (value_end < value_begin) {
    return Status::Invalid("validated JSON value token is empty");
  }
  return raw.substr(value_begin, value_end - value_begin);
}

sanitize::Result<std::string_view>
materialize_validated_key(JsonOnDemandDoc *doc, std::string_view raw,
                          std::size_t base_offset,
                          const JsonValidatedFieldToken &token) {
  SAN_ASSIGN_OR_RAISE(auto key_token, validated_key_token(raw, token));
  if (key_token.find('\\') == std::string_view::npos) {
    return key_token.substr(1, key_token.size() - 2);
  }
  SAN_ASSIGN_OR_RAISE(
      auto parsed,
      doc->ParseValue(
          key_token, base_offset + static_cast<std::size_t>(token.key_offset)));
  if (!parsed.is_string()) {
    return Status::Invalid("validated JSON key token is not a string");
  }
  return parsed.as_string_view();
}

[[nodiscard]] bool parse_int64_token(std::string_view token,
                                     std::int64_t *out) noexcept {
  if (!out || token.empty()) {
    return false;
  }
  const auto result =
      std::from_chars(token.data(), token.data() + token.size(), *out);
  return result.ec == std::errc{} && result.ptr == token.data() + token.size();
}

[[nodiscard]] bool parse_float64_token(std::string_view token, double *out) {
  if (!out || token.empty()) {
    return false;
  }
  std::int64_t integer = 0;
  if (parse_int64_token(token, &integer)) {
    *out = static_cast<double>(integer);
    return true;
  }
  return sanitize::parse_ascii_float64_strict(token, out);
}

// Converts the exact lexical forms used by stable flat JSONL plans without
// constructing a generic ValueView. Mismatches return false so the canonical
// parser and conversion diagnostics remain the single compatibility fallback.
[[nodiscard]] bool
try_convert_plan_ordered_scalar_token(const sanitize::ColumnPlan &column,
                                      std::string_view token,
                                      DirectScalarValue *out) {
  if (!out || token.empty()) {
    return false;
  }
  out->reset(column.logical_type.kind);
  if (token == "null") {
    return true;
  }

  switch (column.logical_type.kind) {
  case sanitize::LogicalKind::kNull:
    return true;
  case sanitize::LogicalKind::kBool:
    if (token == "true" || token == "false") {
      out->is_null = false;
      out->b = token.front() == 't';
      return true;
    }
    return false;
  case sanitize::LogicalKind::kInt64: {
    std::int64_t value = 0;
    if (!parse_int64_token(token, &value)) {
      return false;
    }
    out->is_null = false;
    out->i64 = value;
    return true;
  }
  case sanitize::LogicalKind::kFloat64: {
    double value = 0.0;
    if (!parse_float64_token(token, &value)) {
      return false;
    }
    out->is_null = false;
    out->f64 = value;
    return true;
  }
  case sanitize::LogicalKind::kUtf8:
    if (token.size() >= 2 && token.front() == '"' && token.back() == '"' &&
        token.find('\\') == std::string_view::npos) {
      out->is_null = false;
      out->borrows_utf8 = true;
      out->borrowed_utf8 = token.substr(1, token.size() - 2);
      return true;
    }
    return false;
  case sanitize::LogicalKind::kTimestampNs: {
    std::int64_t value = 0;
    if (!parse_int64_token(token, &value)) {
      return false;
    }
    out->is_null = false;
    out->i64 = value;
    return true;
  }
  case sanitize::LogicalKind::kDate32:
  case sanitize::LogicalKind::kTime32s: {
    std::int64_t value = 0;
    if (!parse_int64_token(token, &value) ||
        value < std::numeric_limits<std::int32_t>::min() ||
        value > std::numeric_limits<std::int32_t>::max()) {
      return false;
    }
    out->is_null = false;
    out->i64 = value;
    return true;
  }
  case sanitize::LogicalKind::kStruct:
  case sanitize::LogicalKind::kList:
    return false;
  }
  return false;
}

sanitize::Result<std::optional<AppendRowResult>>
try_append_plan_ordered_json_tokens(BatchAppender *app, JsonOnDemandDoc *doc,
                                    std::string_view raw,
                                    std::size_t base_offset,
                                    std::string_view source_file,
                                    const JsonValidatedRowTokens &tokens,
                                    const PreparedOptions &opts,
                                    sanitize::IngestDiagnostics *diagnostics) {
  if (!app->supports_direct_scalar_rows()) {
    return std::optional<AppendRowResult>{};
  }
  if (tokens.field_count != app->plan().columns.size()) {
    return Status::Invalid(
        "plan-ordered JSON token count does not match compiled plan");
  }
  if (tokens.field_count > 0 && !tokens.fields) {
    return Status::Invalid("plan-ordered JSON token array is null");
  }

  auto &values = app->prepare_direct_scalars();
  CoerceError error;
  ConvertCtx ctx{
      .opts = opts,
      .diagnostics = diagnostics,
      .error = &error,
  };

  for (std::size_t index = 0; index < tokens.field_count; ++index) {
    const auto &column = app->plan().columns[index];
    sanitize::ValueView value = sanitize::ValueView::Null();
    if (column.name == "source_file" && !source_file.empty()) {
      value = sanitize::ValueView::String(source_file);
    } else {
      SAN_ASSIGN_OR_RAISE(
          auto value_text,
          validated_value_token(raw, tokens.fields, tokens.field_count, index));
      if (try_convert_plan_ordered_scalar_token(column, value_text,
                                                &values[index])) {
        continue;
      }
      SAN_ASSIGN_OR_RAISE(
          value, doc->ParseValue(value_text,
                                 base_offset +
                                     static_cast<std::size_t>(
                                         tokens.fields[index].value_offset)));
    }

    bool empty_container = false;
    SAN_RETURN_NOT_OK(value.container_is_empty(&empty_container));
    if (empty_container || value.is_null()) {
      continue;
    }

    const sanitize::Status status =
        convert_direct_scalar(column, value, ctx, &values[index]);
    if (status.ok()) {
      continue;
    }
    if (error.detail.empty()) {
      error.code = sanitize::DiagnosticCode::kTypeMismatch;
      error.path_id = static_cast<std::uint32_t>(column.path_id);
      error.detail = status.message();
    }
    return handle_direct_scalar_conversion_error(app, opts, diagnostics, error,
                                                 status);
  }

  SAN_RETURN_NOT_OK(app->append_direct_scalars());
  return std::optional<AppendRowResult>(AppendRowResult{});
}

sanitize::Status
materialize_validated_json_fields(JsonOnDemandDoc *doc,
                                  std::vector<sanitize::FieldRef> *fields,
                                  std::string_view raw, std::size_t base_offset,
                                  const JsonValidatedRowTokens &tokens) {
  if (!doc || !fields) {
    return Status::Invalid("validated JSON materializer received null state");
  }
  if (tokens.field_count > 0 && !tokens.fields) {
    return Status::Invalid("validated JSON token array is null");
  }
  fields->clear();
  if (fields->capacity() < tokens.field_count) {
    fields->reserve(tokens.field_count);
  }
  for (std::size_t index = 0; index < tokens.field_count; ++index) {
    const auto &token = tokens.fields[index];
    SAN_ASSIGN_OR_RAISE(
        auto key, materialize_validated_key(doc, raw, base_offset, token));
    SAN_ASSIGN_OR_RAISE(
        auto value_text,
        validated_value_token(raw, tokens.fields, tokens.field_count, index));
    SAN_ASSIGN_OR_RAISE(
        auto value,
        doc->ParseValue(value_text, base_offset + static_cast<std::size_t>(
                                                      token.value_offset)));
    fields->push_back(sanitize::FieldRef{
        .key = key,
        .key_hash = sanitize::detail::hash_key64(key),
        .value = value,
    });
  }
  return Status::OK();
}

} // namespace

sanitize::Result<PreparedRow> prepare_row_json_tokens(
    const sanitize::CompiledPlan &plan, JsonOnDemandDoc *doc,
    std::vector<sanitize::FieldRef> *fields, std::string_view raw,
    std::size_t base_offset, std::string_view source_file,
    const JsonValidatedRowTokens &tokens, const PreparedOptions &opts,
    sanitize::IngestDiagnostics *diagnostics) {
  if (!doc) {
    return Status::Invalid("prepare_row_json_tokens: doc is null");
  }
  if (!fields) {
    return Status::Invalid("prepare_row_json_tokens: fields is null");
  }
  doc->Reset();
  JsonTokenScratchReset scratch_reset(doc);
  SAN_RETURN_NOT_OK(
      materialize_validated_json_fields(doc, fields, raw, base_offset, tokens));
  const sanitize::RowRef row{
      .fields = fields->empty() ? nullptr : fields->data(),
      .size = fields->size(),
      .raw = raw,
      .base_offset = base_offset,
      .source_file = source_file,
  };
  return prepare_materialized_row(plan, row, opts, diagnostics);
}

sanitize::Result<AppendRowResult> append_row_json_tokens(
    BatchAppender *app, JsonOnDemandDoc *doc, std::string_view raw,
    std::size_t base_offset, std::string_view source_file,
    const JsonValidatedRowTokens &tokens, bool plan_ordered_tokens,
    const PreparedOptions &opts, sanitize::IngestDiagnostics *diagnostics) {
  if (!app) {
    return Status::Invalid("append_row_json_tokens: app is null");
  }
  if (!doc) {
    return Status::Invalid("append_row_json_tokens: doc is null");
  }
  doc->Reset();
  JsonTokenScratchReset scratch_reset(doc);
  if (plan_ordered_tokens) {
    SAN_ASSIGN_OR_RAISE(auto direct,
                        try_append_plan_ordered_json_tokens(
                            app, doc, raw, base_offset, source_file, tokens,
                            opts, diagnostics));
    if (direct.has_value()) {
      return std::move(*direct);
    }
    doc->Reset();
  }

  auto &fields = app->prepare_field_refs(tokens.field_count);
  SAN_RETURN_NOT_OK(materialize_validated_json_fields(doc, &fields, raw,
                                                      base_offset, tokens));
  const sanitize::RowRef row{
      .fields = fields.empty() ? nullptr : fields.data(),
      .size = fields.size(),
      .raw = raw,
      .base_offset = base_offset,
      .source_file = source_file,
  };
  return append_materialized_row_reuse(app, row, opts, diagnostics);
}

} // namespace sanitize::internal
