// Implements CSV column projection state for row batching.
//
// Converts parsed CSV cell positions into planned field keys, validates strict
// headers, and prepares direct materialization indexes for fixed schemas.

#include "internal/frontends/csv_column_projection.hh"

#include <string>

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
  root_field_cache_.clear();

  direct_ready_ = false;
  direct_.col_to_csv.clear();
  if (raw_only_ && plan_ && !has_header_) {
    // Column indices are used as keys when no header is present.
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
  root_field_cache_.clear();
}

void CsvColumnProjection::finish_empty_header() {
  header_ready_ = true;
  ensure_keep_mask(headers_.size());
}

bool CsvColumnProjection::has_header() const noexcept { return has_header_; }

bool CsvColumnProjection::header_ready() const noexcept {
  return header_ready_;
}

bool CsvColumnProjection::can_use_raw_only() const noexcept {
  return raw_only_ && plan_ && (!has_header_ || direct_ready_);
}

const CsvDirectContext *CsvColumnProjection::direct_context() const noexcept {
  if (!raw_only_ || !plan_) {
    return nullptr;
  }
  if (has_header_ && !direct_ready_) {
    return nullptr;
  }
  return &direct_;
}

sanitize::Status CsvColumnProjection::validate_header_cells(
    const std::vector<std::string_view> &cells) const {
  if (!strict_schema_ || !plan_) {
    return sanitize::Status::OK();
  }
  for (const std::string_view cell : cells) {
    if (find_root_field(cell) == nullptr) {
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
  headers_.reserve(cells.size());
  for (const std::string_view cell : cells) {
    headers_.emplace_back(cell);
  }
  header_ready_ = true;
  ensure_numeric_keys(headers_.size());
  ensure_keep_mask(headers_.size());

  if (raw_only_ && plan_) {
    build_direct_from_headers(cells);
  }
}

bool CsvColumnProjection::header_cells_equal(
    const std::vector<std::string_view> &cells) const {
  if (cells.size() != headers_.size()) {
    return false;
  }
  for (std::size_t i = 0; i < cells.size(); ++i) {
    if (cells[i] != headers_[i]) {
      return false;
    }
  }
  return true;
}

void CsvColumnProjection::append_parsed_cells(
    FlatRowBatch *batch, const std::vector<std::string_view> &cells) {
  if (cells.size() > headers_.size()) {
    // Expand header storage so missing header names fall back to numeric keys.
    headers_.resize(cells.size());
  }
  ensure_numeric_keys(cells.size());
  ensure_keep_mask(cells.size());

  for (std::size_t i = 0; i < cells.size(); ++i) {
    if (keep_mask_ready_ && (i >= keep_mask_.size() || keep_mask_[i] == 0)) {
      continue;
    }
    const std::string_view key = column_key(i);
    const uint64_t hash = sanitize::detail::hash_key64(key);
    const std::string_view cell = cells[i];
    batch->push(FieldRef{
        .key = key,
        .key_hash = hash,
        .value = cell.empty() ? ValueView::Null() : ValueView::String(cell),
    });
  }
}

void CsvColumnProjection::build_direct_from_headers(
    const std::vector<std::string_view> &cells) {
  if (!plan_) {
    return;
  }
  direct_.col_to_csv.assign(plan_->columns.size(), -1);
  for (std::size_t i = 0; i < cells.size(); ++i) {
    const std::string_view key = cells[i];
    const auto *field = find_root_field(key);
    if (!field) {
      continue;
    }
    const auto index = static_cast<std::size_t>(field->index);
    if (index >= direct_.col_to_csv.size()) {
      continue;
    }
    if (direct_.col_to_csv[index] < 0) {
      direct_.col_to_csv[index] = static_cast<int32_t>(i);
    }
  }
  direct_ready_ = true;
}

const sanitize::FieldIndex *
CsvColumnProjection::find_root_field(std::string_view key) const noexcept {
  if (const auto cached = root_field_cache_.find(key);
      cached != root_field_cache_.end()) {
    return cached->second;
  }
  const auto *field = find_root_field_uncached(key);
  try {
    root_field_cache_.emplace(std::string(key), field);
  } catch (...) {
    // Keep lookup noexcept; failing to cache only costs future scans.
  }
  return field;
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
    numeric_keys_[i] = std::to_string(i);
  }
}

void CsvColumnProjection::ensure_keep_mask(std::size_t column_count) {
  if (!plan_) {
    return;
  }

  if (keep_mask_.size() < column_count) {
    keep_mask_.resize(column_count, 0);
  }

  for (std::size_t i = 0; i < column_count; ++i) {
    const std::string_view key = column_key(i);
    keep_mask_[i] = (find_root_field(key) != nullptr) ? 1 : 0;
  }
  keep_mask_ready_ = true;
}

} // namespace sanitize::internal
