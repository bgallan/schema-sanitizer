// Owns CSV column projection state, header resolution, and row filtering.

#include "frontends/csv/column_projection.hh"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <limits>
#include <string>
#include <utility>

#include "internal/planning/planned_name_matcher.hh"
#include "sanitize/core/value_view.hh"
#include "sanitize/detail/hash.hh"

namespace sanitize::internal {

CsvColumnProjection::CsvColumnProjection(const sanitize::Options &opts,
                                         char delimiter) {
  has_header_ = opts.csv_has_header;
  strict_schema_ = opts.schema_evolution == SchemaEvolutionMode::kStrict &&
                   opts.arrow_schema_contract.has_value();
  raw_only_ = static_cast<bool>(opts.arrow_schema_contract);
  field_name_policy_ = opts.field_name_policy;
  direct_.delimiter = delimiter;
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
  }
}

void CsvColumnProjection::reset_header() noexcept {
  header_ready_ = false;
  keep_mask_ready_ = false;
  resolved_fields_.clear();
  if (has_header_) {
    column_hashes_.clear();
  }
}

bool CsvColumnProjection::has_header() const noexcept { return has_header_; }

bool CsvColumnProjection::header_ready() const noexcept {
  return header_ready_;
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
  if (has_header_ && !headers_.empty()) {
    return headers_.size();
  }
  if (plan_ != nullptr) {
    return plan_->columns.size();
  }
  return numeric_keys_.size();
}

sanitize::Status CsvColumnProjection::validate_header_cells(
    const std::vector<std::string_view> &cells) const {
  if (!strict_schema_ || !plan_) {
    return sanitize::Status::OK();
  }
  for (const std::string_view cell : cells) {
    if (find_root_field_uncached(cell) == nullptr) {
      return sanitize::Status::Invalid(
          "Strict schema evolution: observed extra field '", std::string(cell),
          "'");
    }
  }
  return sanitize::Status::OK();
}

void CsvColumnProjection::set_header_cells(
    const std::vector<std::string_view> &cells) {
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
    build_direct_from_headers(cells);
  }
}

bool CsvColumnProjection::header_cells_equal(
    const std::vector<std::string_view> &cells) const {
  return std::ranges::equal(cells, headers_);
}

void CsvColumnProjection::build_direct_from_headers(
    const std::vector<std::string_view> &cells) {
  if (!plan_) {
    return;
  }
  direct_.col_to_csv.assign(plan_->columns.size(), -1);
  for (std::size_t i = 0; i < cells.size(); ++i) {
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

void CsvColumnProjection::append_parsed_cells(
    FlatRowBatch *batch, const std::vector<std::string_view> &cells) {
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
