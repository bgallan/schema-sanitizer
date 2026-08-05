// Owns exact-mode projection state and immutable union-mode source mappings.

#include "frontends/csv/column_projection.hh"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <limits>
#include <ranges>
#include <string>
#include <unordered_set>
#include <utility>

#include "internal/memory/memory_budget.hh"
#include "internal/parsing/csv_parse.hh"
#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/planned_name_matcher.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/detail/hash.hh"

namespace sanitize::internal {

CsvColumnProjection::CsvColumnProjection(
    const sanitize::Options &opts, char delimiter,
    CsvSourceProjectionSetPtr source_projections)
    : source_projections_(std::move(source_projections)) {
  has_header_ = opts.csv_has_header;
  union_mode_ = has_header_ && opts.csv_header_mode == "union";
  strict_schema_ = opts.schema_evolution == SchemaEvolutionMode::kStrict &&
                   opts.arrow_schema_contract.has_value();
  raw_only_ = !union_mode_ && (static_cast<bool>(opts.arrow_schema_contract) ||
                               opts.threading_mode == ThreadingMode::kMulti);
  field_name_policy_ = opts.field_name_policy;
  direct_.delimiter = delimiter;
  direct_.escape_char =
      opts.csv_escape_char.empty() ? '\0' : opts.csv_escape_char[0];
  const auto budget = memory_budget_from_limit(opts.memory_limit_bytes);
  const auto total =
      static_cast<std::size_t>(std::max<std::int64_t>(1, budget.total_bytes));
  direct_.max_decoded_record_bytes =
      std::min<std::size_t>(kMaxCsvDecodedRecordBytes, total);
  direct_.max_field_bytes = std::min<std::size_t>(
      kMaxCsvFieldBytes,
      std::max<std::size_t>(1, direct_.max_decoded_record_bytes / 2U));
}

void CsvColumnProjection::set_plan(
    const sanitize::CompiledPlan *plan) noexcept {
  plan_ = plan;
  keep_mask_ready_ = false;
  keep_mask_.clear();
  resolved_fields_.clear();

  direct_ready_ = false;
  direct_.col_to_csv.clear();
  if (raw_only_ && plan_ && !has_header_) {
    direct_.col_to_csv.resize(plan_->columns.size(), -1);
    for (std::size_t i = 0; i < direct_.col_to_csv.size(); ++i) {
      direct_.col_to_csv[i] = static_cast<int32_t>(i);
    }
    direct_ready_ = true;
  } else if (raw_only_ && plan_ && header_ready_) {
    build_direct_from_headers(headers_.size());
  }
}

void CsvColumnProjection::reset_header() noexcept {
  header_ready_ = false;
  keep_mask_ready_ = false;
  resolved_fields_.clear();
  if (has_header_) {
    column_hashes_.clear();
  }
  if (union_mode_ && source_projections_) {
    headers_.clear();
    numeric_keys_.clear();
  }
}

bool CsvColumnProjection::has_header() const noexcept { return has_header_; }

bool CsvColumnProjection::header_ready() const noexcept {
  return header_ready_;
}

bool CsvColumnProjection::has_source_projections() const noexcept {
  return union_mode_ && static_cast<bool>(source_projections_);
}

bool CsvColumnProjection::can_use_raw_only() const noexcept {
  return raw_only_ && plan_ && (!has_header_ || direct_ready_);
}

const CsvDirectContext *CsvColumnProjection::direct_context() const noexcept {
  if (!raw_only_ || !plan_ || (has_header_ && !direct_ready_)) {
    return nullptr;
  }
  return &direct_;
}

std::size_t CsvColumnProjection::column_count_hint() const noexcept {
  if (union_mode_ && source_projections_) {
    return source_projections_->max_columns;
  }
  if (has_header_ && !headers_.empty()) {
    return headers_.size();
  }
  if (plan_ != nullptr) {
    return plan_->columns.size();
  }
  return numeric_keys_.size();
}

sanitize::Status CsvColumnProjection::validate_header_cells(
    std::span<const std::string_view> cells) const {
  std::unordered_set<std::string_view> observed;
  observed.reserve(cells.size());
  for (std::size_t index = 0; index < cells.size(); ++index) {
    const auto cell = cells[index];
    if (!cell.empty() && !observed.emplace(cell).second) {
      return sanitize::Status::Invalid(
          "CSV header contains a duplicate non-empty name at column ",
          index + 1U);
    }
  }
  if (!uses_preserve_policy(field_name_policy_)) {
    std::unordered_set<std::string> reconciled;
    reconciled.reserve(cells.size());
    for (std::size_t index = 0; index < cells.size(); ++index) {
      if (cells[index].empty()) {
        continue;
      }
      std::string normalized;
      try {
        normalized = clean_field_name_base(cells[index], field_name_policy_);
      } catch (const std::bad_alloc &) {
        return sanitize::Status::OutOfMemory(
            "CSV header reconciliation allocation failed");
      }
      if (!reconciled.emplace(std::move(normalized)).second) {
        return sanitize::Status::Invalid(
            "CSV header names collide after field-name reconciliation at "
            "column ",
            index + 1U, " under policy '", field_name_policy_, "'");
      }
    }
  }
  if (!strict_schema_ || !plan_) {
    return sanitize::Status::OK();
  }
  for (std::size_t index = 0; index < cells.size(); ++index) {
    std::string numeric;
    std::string_view key = cells[index];
    if (key.empty()) {
      numeric = std::to_string(index);
      key = numeric;
    }
    if (find_root_field_uncached(key) == nullptr) {
      return sanitize::Status::Invalid(
          "Strict schema evolution: observed extra field '", std::string(key),
          "'");
    }
  }
  return sanitize::Status::OK();
}

sanitize::Status CsvColumnProjection::validate_source_header(
    std::size_t source_index, std::span<const std::string_view> cells) const {
  if (!union_mode_ || !source_projections_) {
    return sanitize::Status::OK();
  }
  if (source_index >= source_projections_->headers.size()) {
    return sanitize::Status::Invalid(
        "CSV union source_index is outside the pre-read header set");
  }
  const auto &header = source_projections_->headers[source_index];
  if (header.source_index != source_index ||
      !std::ranges::equal(cells, header.fields)) {
    return sanitize::Status::Invalid(
        "CSV source header changed after union discovery for source_index ",
        source_index);
  }
  return sanitize::Status::OK();
}

void CsvColumnProjection::set_header_cells(
    std::span<const std::string_view> cells) {
  headers_.clear();
  resolved_fields_.clear();
  headers_.reserve(cells.size());
  for (const std::string_view cell : cells) {
    headers_.emplace_back(cell);
  }
  header_ready_ = true;
  ensure_numeric_keys(headers_.size());
  ensure_resolved_fields(headers_.size());
  column_hashes_.clear();
  ensure_column_hashes(headers_.size());
  ensure_keep_mask(headers_.size());

  if (raw_only_ && plan_) {
    build_direct_from_headers(cells.size());
  }
}

bool CsvColumnProjection::header_cells_equal(
    std::span<const std::string_view> cells) const {
  return std::ranges::equal(cells, headers_);
}

void CsvColumnProjection::build_direct_from_headers(std::size_t column_count) {
  if (!plan_) {
    return;
  }
  direct_.col_to_csv.assign(plan_->columns.size(), -1);
  ensure_resolved_fields(column_count);
  for (std::size_t i = 0; i < column_count; ++i) {
    const auto *field = resolved_fields_[i];
    if (!field) {
      continue;
    }
    const auto index = static_cast<std::size_t>(field->index);
    if (index < direct_.col_to_csv.size() && direct_.col_to_csv[index] < 0) {
      direct_.col_to_csv[index] = static_cast<int32_t>(i);
    }
  }
  direct_ready_ = true;
}

const sanitize::FieldIndex *CsvColumnProjection::find_root_field_uncached(
    std::string_view key) const noexcept {
  if (!plan_) {
    return nullptr;
  }
  return find_planned_field(plan_->root_layout, key,
                            sanitize::detail::hash_key64(key),
                            field_name_policy_);
}

const CsvSourceProjection *
CsvColumnProjection::source_projection(std::size_t source_index,
                                       bool has_source_index) const noexcept {
  if (!source_projections_) {
    return nullptr;
  }
  const auto resolved_index = has_source_index ? source_index : 0U;
  if (resolved_index >= source_projections_->projections.size()) {
    return nullptr;
  }
  const auto &projection = source_projections_->projections[resolved_index];
  if (projection.source_index != resolved_index) {
    return nullptr;
  }
  return &projection;
}

sanitize::Status CsvColumnProjection::append_union_cells(
    FlatRowBatch *batch, std::span<const std::string_view> cells,
    std::size_t source_index, bool has_source_index) const {
  const auto *projection = source_projection(source_index, has_source_index);
  const std::vector<std::string> *keys = nullptr;
  const std::vector<std::uint64_t> *hashes = nullptr;
  if (projection) {
    keys = &projection->column_keys;
    hashes = &projection->column_hashes;
  } else if (!headers_.empty()) {
    keys = &headers_;
    hashes = &column_hashes_;
  } else {
    return sanitize::Status::Invalid(
        "CSV union row has no immutable source projection");
  }
  if (cells.size() > keys->size()) {
    return sanitize::Status::Invalid(
        "CSV row contains more fields than its source header: expected at "
        "most ",
        keys->size(), ", observed ", cells.size());
  }

  const bool inference = plan_ == nullptr;
  for (std::size_t i = 0; i < keys->size(); ++i) {
    const std::string_view key = (*keys)[i];
    if (plan_ && find_root_field_uncached(key) == nullptr) {
      continue;
    }
    const bool has_value = i < cells.size() && !cells[i].empty();
    ValueView value = ValueView::Null();
    if (has_value) {
      value = ValueView::String(cells[i]);
    } else if (inference) {
      // Header presence is schema evidence even when every physical value is
      // empty or a short row omits the trailing field. Materialization still
      // emits null for the same cell once the canonical plan exists.
      value = ValueView::String(std::string_view{});
    }
    batch->push(FieldRef{
        .key = key,
        .key_hash = (*hashes)[i],
        .value = value,
    });
  }
  return sanitize::Status::OK();
}

sanitize::Status CsvColumnProjection::append_parsed_cells(
    FlatRowBatch *batch, std::span<const std::string_view> cells,
    std::size_t source_index, bool has_source_index) {
  if (union_mode_) {
    return append_union_cells(batch, cells, source_index, has_source_index);
  }

  if (has_header_ && cells.size() > headers_.size()) {
    headers_.resize(cells.size());
  }
  ensure_numeric_keys(cells.size());
  ensure_column_hashes(cells.size());
  ensure_keep_mask(cells.size());

  for (std::size_t i = 0; i < cells.size(); ++i) {
    if (keep_mask_ready_ && (i >= keep_mask_.size() || keep_mask_[i] == 0)) {
      continue;
    }
    const std::string_view key = column_key(i);
    const std::string_view cell = cells[i];
    batch->push(FieldRef{
        .key = key,
        .key_hash = column_hashes_[i],
        .value = cell.empty() ? ValueView::Null() : ValueView::String(cell),
    });
  }
  return sanitize::Status::OK();
}

std::string_view CsvColumnProjection::column_key(std::size_t index) {
  if (has_header_ && index < headers_.size() && !headers_[index].empty()) {
    return std::string_view(headers_[index]);
  }
  return std::string_view(numeric_keys_[index]);
}

void CsvColumnProjection::ensure_numeric_keys(std::size_t count) {
  if (numeric_keys_.size() >= count) {
    return;
  }
  const std::size_t old_size = numeric_keys_.size();
  numeric_keys_.resize(count);
  for (std::size_t i = old_size; i < count; ++i) {
    char buffer[std::numeric_limits<std::size_t>::digits10 + 2];
    const auto [end, error] =
        std::to_chars(std::begin(buffer), std::end(buffer), i);
    if (error != std::errc{}) [[unlikely]] {
      std::unreachable();
    }
    numeric_keys_[i].assign(buffer, end);
  }
}

void CsvColumnProjection::ensure_column_hashes(std::size_t column_count) {
  if (column_hashes_.size() >= column_count) {
    return;
  }
  const std::size_t first_new_column = column_hashes_.size();
  column_hashes_.resize(column_count);
  for (std::size_t i = first_new_column; i < column_count; ++i) {
    column_hashes_[i] = sanitize::detail::hash_key64(column_key(i));
  }
}

void CsvColumnProjection::ensure_resolved_fields(std::size_t column_count) {
  if (resolved_fields_.size() >= column_count) {
    return;
  }
  const std::size_t first_new_column = resolved_fields_.size();
  resolved_fields_.resize(column_count, nullptr);
  for (std::size_t i = first_new_column; i < column_count; ++i) {
    resolved_fields_[i] = find_root_field_uncached(column_key(i));
  }
}

void CsvColumnProjection::ensure_keep_mask(std::size_t column_count) {
  if (!plan_ || (keep_mask_ready_ && keep_mask_.size() >= column_count)) {
    return;
  }

  std::size_t first_new_column = 0;
  if (keep_mask_ready_) {
    first_new_column = keep_mask_.size();
    keep_mask_.resize(column_count, 0);
  } else {
    keep_mask_.assign(column_count, 0);
  }

  ensure_resolved_fields(column_count);
  for (std::size_t i = first_new_column; i < column_count; ++i) {
    keep_mask_[i] = resolved_fields_[i] != nullptr ? 1 : 0;
  }
  keep_mask_ready_ = true;
}

} // namespace sanitize::internal
