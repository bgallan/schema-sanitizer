// Maps Parquet schema elements to Arrow formats, leaf levels, and column
// projections. This fragment is included by the bounded footer reader inside
// its private implementation namespace.

/// Returns the Arrow C format string for a signed or unsigned Parquet integer
/// width.
std::string arrow_integer_format(std::int32_t bit_width, bool is_signed) {
  switch (bit_width) {
  case 8:
    return is_signed ? "c" : "C";
  case 16:
    return is_signed ? "s" : "S";
  case 32:
    return is_signed ? "i" : "I";
  case 64:
    return is_signed ? "l" : "L";
  default:
    return {};
  }
}

/// Maps a legacy Parquet converted integer type to its signed or unsigned Arrow
/// format.
std::string arrow_integer_format_from_converted_type(std::int32_t converted) {
  switch (converted) {
  case kConvertedUInt8:
    return "C";
  case kConvertedUInt16:
    return "S";
  case kConvertedUInt32:
    return "I";
  case kConvertedUInt64:
    return "L";
  case kConvertedInt8:
    return "c";
  case kConvertedInt16:
    return "s";
  case kConvertedInt32:
    return "i";
  case kConvertedInt64:
    return "l";
  default:
    return {};
  }
}

/// Maps Parquet date, time, and timestamp annotations to an Arrow C format
/// string.
std::string arrow_temporal_format(std::string_view logical_type,
                                  std::string_view unit,
                                  std::int32_t physical_type,
                                  bool is_adjusted_to_utc) {
  if (logical_type == "date") {
    return "tdD";
  }
  if (logical_type == "time") {
    if (unit == "millis") {
      return "ttm";
    }
    if (unit == "micros") {
      return "ttu";
    }
    if (unit == "nanos") {
      return "ttn";
    }
    if (physical_type == kPhysicalInt32) {
      return "ttm";
    }
    return {};
  }
  if (logical_type == "timestamp") {
    const auto timezone = is_adjusted_to_utc ? "UTC" : "";
    if (unit == "millis") {
      return std::string("tsm:") + timezone;
    }
    if (unit == "micros") {
      return std::string("tsu:") + timezone;
    }
    if (unit == "nanos") {
      return std::string("tsn:") + timezone;
    }
    return {};
  }
  return {};
}

/// Selects the Arrow C format string for one physical and logical Parquet leaf
/// type.
std::string arrow_format_for_leaf(const SchemaElementInfo &element) {
  if (!element.has_physical_type) {
    return {};
  }
  if (element.logical_type == "null") {
    return "n";
  }
  if (element.logical_type == "string" ||
      (element.has_converted_type &&
       element.converted_type == kConvertedUtf8)) {
    return "u";
  }
  if (element.logical_type == "date" || element.logical_type == "time" ||
      element.logical_type == "timestamp") {
    return arrow_temporal_format(
        element.logical_type, element.logical_type_time_unit,
        element.physical_type, element.logical_type_is_adjusted_to_utc);
  }
  if (element.logical_type == "integer" &&
      element.has_logical_type_integer_bit_width &&
      element.has_logical_type_integer_is_signed) {
    return arrow_integer_format(element.logical_type_integer_bit_width,
                                element.logical_type_integer_is_signed);
  }
  if (element.has_converted_type) {
    const auto integer_format =
        arrow_integer_format_from_converted_type(element.converted_type);
    if (!integer_format.empty()) {
      return integer_format;
    }
    if (element.converted_type == kConvertedDate) {
      return "tdD";
    }
    if (element.converted_type == kConvertedTimeMillis) {
      return "ttm";
    }
  }
  if ((element.logical_type == "decimal" ||
       (element.has_converted_type &&
        element.converted_type == kConvertedDecimal)) &&
      element.has_decimal_precision && element.has_decimal_scale &&
      element.has_type_length && element.type_length > 0) {
    return "d:" + std::to_string(element.decimal_precision) + "," +
           std::to_string(element.decimal_scale) + "," +
           std::to_string(element.type_length * 8);
  }
  switch (element.physical_type) {
  case kPhysicalBoolean:
    return "b";
  case kPhysicalInt32:
    return "i";
  case kPhysicalInt64:
    return "l";
  case kPhysicalFloat:
    return "f";
  case kPhysicalDouble:
    return "g";
  case kPhysicalByteArray:
    return "z";
  case kPhysicalFixedLenByteArray:
    if (element.has_type_length && element.type_length > 0) {
      return "w:" + std::to_string(element.type_length);
    }
    return {};
  default:
    return {};
  }
}

struct LeafLevelInfo {
  std::vector<std::string> path;
  std::vector<std::int16_t> path_definition_levels;
  std::vector<std::int16_t> path_repetition_levels;
  std::int16_t max_definition_level = 0;
  std::int16_t max_repetition_level = 0;
  bool top_level_required = true;
  std::int32_t fixed_type_length = 0;
  std::string native_arrow_format;
};

/// Computes maximum definition and repetition levels for every Parquet schema
/// leaf.
sanitize::Result<std::vector<LeafLevelInfo>>
schema_leaf_levels(const std::vector<SchemaElementInfo> &schema) {
  std::vector<LeafLevelInfo> out;
  if (schema.empty()) {
    return out;
  }

  struct SchemaTraversalFrame {
    std::int32_t remaining_children = 0;
    std::size_t path_size = 0;
    std::size_t definition_path_size = 0;
    std::size_t repetition_path_size = 0;
    std::int16_t definition_level = 0;
    std::int16_t repetition_level = 0;
    bool top_level_required = true;
  };

  const auto child_count = [](const SchemaElementInfo &element) {
    return element.has_num_children
               ? std::max<std::int32_t>(element.num_children, 0)
               : 0;
  };
  const auto append_leaf = [&out](const SchemaElementInfo &element,
                                  const std::vector<std::string> &path,
                                  const std::vector<std::int16_t> &definitions,
                                  const std::vector<std::int16_t> &repetitions,
                                  std::int16_t definition_level,
                                  std::int16_t repetition_level,
                                  bool top_level_required) {
    if (!element.has_physical_type) {
      return;
    }
    out.push_back(LeafLevelInfo{
        .path = path,
        .path_definition_levels = definitions,
        .path_repetition_levels = repetitions,
        .max_definition_level = definition_level,
        .max_repetition_level = repetition_level,
        .top_level_required = top_level_required,
        .fixed_type_length = element.has_type_length ? element.type_length : 0,
        .native_arrow_format = arrow_format_for_leaf(element),
    });
  };

  std::vector<std::string> path;
  std::vector<std::int16_t> path_definition_levels;
  std::vector<std::int16_t> path_repetition_levels;
  path.reserve(schema.size());
  path_definition_levels.reserve(schema.size());
  path_repetition_levels.reserve(schema.size());

  const auto &root = schema.front();
  const auto root_children = child_count(root);
  if (root_children == 0) {
    append_leaf(root, path, path_definition_levels, path_repetition_levels, 0,
                0, true);
    if (schema.size() != 1U) {
      return sanitize::Status::Invalid(
          "Parquet schema levels: trailing schema elements");
    }
    return out;
  }

  std::vector<SchemaTraversalFrame> pending;
  pending.reserve(schema.size());
  pending.push_back(SchemaTraversalFrame{
      .remaining_children = root_children,
  });

  std::size_t index = 1;
  while (!pending.empty()) {
    while (!pending.empty() && pending.back().remaining_children == 0) {
      pending.pop_back();
      if (!pending.empty()) {
        path.resize(pending.back().path_size);
        path_definition_levels.resize(pending.back().definition_path_size);
        path_repetition_levels.resize(pending.back().repetition_path_size);
      }
    }
    if (pending.empty()) {
      break;
    }
    if (index >= schema.size()) {
      return sanitize::Status::Invalid(
          "Parquet schema levels: truncated schema tree");
    }

    auto &parent = pending.back();
    --parent.remaining_children;
    const auto &element = schema[index++];
    const auto repetition =
        element.has_repetition_type ? element.repetition_type : 0;
    auto definition_level = parent.definition_level;
    auto repetition_level = parent.repetition_level;
    if (repetition == 1 || repetition == 2) {
      ++definition_level;
    }
    if (repetition == 2) {
      ++repetition_level;
    }

    path.push_back(element.name);
    path_definition_levels.push_back(definition_level);
    path_repetition_levels.push_back(repetition_level);
    const auto top_level_required =
        path.size() == 1U ? repetition == 0 : parent.top_level_required;
    const auto children = child_count(element);
    if (children == 0) {
      append_leaf(element, path, path_definition_levels, path_repetition_levels,
                  definition_level, repetition_level, top_level_required);
      path.resize(parent.path_size);
      path_definition_levels.resize(parent.definition_path_size);
      path_repetition_levels.resize(parent.repetition_path_size);
      continue;
    }

    pending.push_back(SchemaTraversalFrame{
        .remaining_children = children,
        .path_size = path.size(),
        .definition_path_size = path_definition_levels.size(),
        .repetition_path_size = path_repetition_levels.size(),
        .definition_level = definition_level,
        .repetition_level = repetition_level,
        .top_level_required = top_level_required,
    });
  }

  if (index != schema.size()) {
    return sanitize::Status::Invalid(
        "Parquet schema levels: trailing schema elements");
  }
  return out;
}

/// Expands a top-level column projection into source-ordered leaf indexes.
sanitize::Status
projected_top_level_leaf_indices(const std::vector<LeafLevelInfo> &leaves,
                                 const std::vector<std::string> &columns,
                                 std::vector<std::size_t> *out) {
  if (!out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: projection index output is null");
  }
  out->clear();
  if (columns.empty()) {
    out->resize(leaves.size());
    std::iota(out->begin(), out->end(), std::size_t{0});
    return {};
  }

  sanitize::internal::BorrowedStringLookupMap<std::vector<std::size_t>>
      indices_by_name;
  indices_by_name.reserve(leaves.size());
  for (std::size_t index = 0; index < leaves.size(); ++index) {
    const auto &path = leaves[index].path;
    if (!path.empty()) {
      indices_by_name[std::string_view(path.front())].push_back(index);
    }
  }

  out->reserve(leaves.size());
  for (const auto &name : columns) {
    const auto match = indices_by_name.find(std::string_view(name));
    if (match == indices_by_name.end()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: projection column not found: ", name);
    }
    out->insert(out->end(), match->second.begin(), match->second.end());
  }
  return {};
}

/// Copies the schema-level records selected by a top-level column projection.
sanitize::Status
project_leaf_levels_for_columns(const std::vector<LeafLevelInfo> &leaves,
                                const std::vector<std::string> &columns,
                                std::vector<LeafLevelInfo> *out) {
  if (!out) {
    return sanitize::Status::Invalid(
        "native Parquet reader: projection output is null");
  }
  out->clear();
  if (columns.empty()) {
    *out = leaves;
    return {};
  }
  std::vector<std::size_t> selected_indices;
  SAN_RETURN_NOT_OK(
      projected_top_level_leaf_indices(leaves, columns, &selected_indices));
  out->reserve(selected_indices.size());
  for (const auto index : selected_indices) {
    out->push_back(leaves[index]);
  }
  return {};
}

/// Assigns column levels from validated schema and page metadata for later
/// materialization.
sanitize::Result<std::vector<LeafLevelInfo>>
assign_column_levels(FooterInfo *info) {
  if (!info) {
    return sanitize::Status::Invalid("Parquet schema levels: internal error");
  }
  std::vector<LeafLevelInfo> leaves;
  SAN_ASSIGN_OR_RAISE(leaves, schema_leaf_levels(info->schema_elements));

  for (auto &row_group : info->row_groups) {
    if (row_group.columns.size() != leaves.size()) {
      return sanitize::Status::Invalid(
          "Parquet schema levels: row group column count mismatch");
    }
    for (std::size_t index = 0; index < leaves.size(); ++index) {
      auto &column = row_group.columns[index];
      const auto &leaf = leaves[index];
      if (column.path_in_schema != leaf.path) {
        return sanitize::Status::Invalid(
            "Parquet schema levels: row group column order mismatch");
      }
      column.max_definition_level = leaf.max_definition_level;
      column.max_repetition_level = leaf.max_repetition_level;
      column.path_definition_levels = leaf.path_definition_levels;
      column.path_repetition_levels = leaf.path_repetition_levels;
      column.top_level_required = leaf.top_level_required;
      column.fixed_type_length = leaf.fixed_type_length;
      column.native_arrow_format = leaf.native_arrow_format;
    }
  }
  return leaves;
}
