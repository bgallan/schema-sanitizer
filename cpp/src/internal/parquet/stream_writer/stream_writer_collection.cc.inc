// Collects Arrow values into one Parquet leaf-column staging buffer at a time.

bool leaf_belongs_to_subtree(const ParquetNode &node,
                             std::size_t target_leaf) noexcept {
  return target_leaf >= node.first_leaf_index &&
         target_leaf < node.first_leaf_index + node.leaf_count;
}

void emit_nulls_for_subtree(const ParquetNode &node, std::size_t target_leaf,
                            ColumnPageData *page, std::int16_t definition_level,
                            std::int16_t repetition_level) {
  if (!page) {
    return;
  }
  const auto leaf_end = node.first_leaf_index + node.leaf_count;
  for (std::size_t leaf = node.first_leaf_index; leaf < leaf_end; ++leaf) {
    if (leaf == target_leaf) {
      page->definition_levels.push_back(definition_level);
      page->repetition_levels.push_back(repetition_level);
      return;
    }
  }
}

sanitize::Status
collect_node_for_leaf(const ParquetNode &node, const ArrowArray &array,
                      std::int64_t row, std::size_t target_leaf,
                      ColumnPageData *page, std::int16_t definition_level,
                      std::int16_t repetition_level);

sanitize::Status collect_list_element_for_leaf(
    const ParquetNode &element, const ArrowArray &values, std::int64_t row,
    std::size_t target_leaf, ColumnPageData *page,
    std::int16_t definition_level, std::int16_t repetition_level) {
  return collect_node_for_leaf(element, values, row, target_leaf, page,
                               definition_level, repetition_level);
}

sanitize::Status
collect_node_for_leaf(const ParquetNode &node, const ArrowArray &array,
                      std::int64_t row, std::size_t target_leaf,
                      ColumnPageData *page, std::int16_t definition_level,
                      std::int16_t repetition_level) {
  if (!page || !leaf_belongs_to_subtree(node, target_leaf)) {
    return sanitize::Status::OK();
  }
  if (node.node_kind != NodeKind::kRoot && !node.required &&
      !bitmap_is_valid(array, row)) {
    emit_nulls_for_subtree(node, target_leaf, page, definition_level,
                           repetition_level);
    return sanitize::Status::OK();
  }
  if (node.node_kind != NodeKind::kRoot && node.required &&
      !bitmap_is_valid(array, row)) {
    return sanitize::Status::Invalid(
        "native Parquet writer: required field contains null");
  }

  switch (node.node_kind) {
  case NodeKind::kRoot:
    for (std::size_t i = 0; i < node.children.size(); ++i) {
      const auto &child_node = node.children[i];
      if (!leaf_belongs_to_subtree(child_node, target_leaf)) {
        continue;
      }
      if (!array.children || !array.children[i]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing root child array");
      }
      return collect_node_for_leaf(child_node, *array.children[i], row,
                                   target_leaf, page, definition_level,
                                   repetition_level);
    }
    break;
  case NodeKind::kStruct: {
    const auto present_definition =
        static_cast<std::int16_t>(definition_level + (node.required ? 0 : 1));
    const auto child_row = array.offset + row;
    for (std::size_t i = 0; i < node.children.size(); ++i) {
      const auto &child_node = node.children[i];
      if (!leaf_belongs_to_subtree(child_node, target_leaf)) {
        continue;
      }
      if (!array.children || !array.children[i]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing struct child array");
      }
      return collect_node_for_leaf(child_node, *array.children[i], child_row,
                                   target_leaf, page, present_definition,
                                   repetition_level);
    }
    break;
  }
  case NodeKind::kList: {
    if (!node.element || !array.children || !array.children[0]) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid list array");
    }
    const auto present_definition =
        static_cast<std::int16_t>(definition_level + (node.required ? 0 : 1));
    const auto repeated_definition =
        static_cast<std::int16_t>(present_definition + 1);
    const auto repeated_repetition = node.repeated_repetition_level;
    const auto index = array.offset + row;
    std::int64_t begin = 0;
    std::int64_t end = 0;
    if (node.arrow_kind == jsonl::JsonlKind::kFixedSizeList) {
      if (node.fixed_size_list_size < 0 ||
          index > std::numeric_limits<std::int64_t>::max() /
                      std::max<std::int64_t>(1, node.fixed_size_list_size)) {
        return sanitize::Status::Invalid(
            "native Parquet writer: invalid fixed-size list size");
      }
      begin = index * node.fixed_size_list_size;
      end = begin + node.fixed_size_list_size;
    } else if (node.arrow_kind == jsonl::JsonlKind::kLargeList) {
      if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing large-list offsets");
      }
      const auto *offsets = static_cast<const std::int64_t *>(array.buffers[1]);
      begin = offsets[index];
      end = offsets[index + 1];
    } else {
      if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing list offsets");
      }
      const auto *offsets = static_cast<const std::int32_t *>(array.buffers[1]);
      begin = offsets[index];
      end = offsets[index + 1];
    }
    if (begin < 0 || end < begin || end > array.children[0]->length) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid list offsets");
    }
    if (begin == end) {
      emit_nulls_for_subtree(node, target_leaf, page, present_definition,
                             repetition_level);
      return sanitize::Status::OK();
    }
    for (std::int64_t item = begin; item < end; ++item) {
      const auto item_repetition =
          item == begin ? repetition_level : repeated_repetition;
      SAN_RETURN_NOT_OK(collect_list_element_for_leaf(
          *node.element, *array.children[0], item, target_leaf, page,
          repeated_definition, item_repetition));
    }
    return sanitize::Status::OK();
  }
  case NodeKind::kMap: {
    if (!node.element || array.n_buffers <= 1 || !array.buffers ||
        !array.buffers[1] || !array.children || !array.children[0]) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid map array");
    }
    const auto present_definition =
        static_cast<std::int16_t>(definition_level + (node.required ? 0 : 1));
    const auto entry_definition =
        static_cast<std::int16_t>(present_definition + 1);
    const auto entry_repetition = node.repeated_repetition_level;
    const auto index = array.offset + row;
    const auto *offsets = static_cast<const std::int32_t *>(array.buffers[1]);
    const auto begin = offsets[index];
    const auto end = offsets[index + 1];
    if (begin < 0 || end < begin || end > array.children[0]->length) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid map offsets");
    }
    if (begin == end) {
      emit_nulls_for_subtree(node, target_leaf, page, present_definition,
                             repetition_level);
      return sanitize::Status::OK();
    }
    for (std::int64_t item = begin; item < end; ++item) {
      const auto item_repetition =
          item == begin ? repetition_level : entry_repetition;
      SAN_RETURN_NOT_OK(collect_node_for_leaf(
          *node.element, *array.children[0], item, target_leaf, page,
          entry_definition, item_repetition));
    }
    return sanitize::Status::OK();
  }
  case NodeKind::kPrimitive:
    if (node.leaf_index != target_leaf) {
      break;
    }
    if (node.arrow_kind == jsonl::JsonlKind::kNull) {
      page->definition_levels.push_back(definition_level);
      page->repetition_levels.push_back(repetition_level);
      return sanitize::Status::OK();
    }
    SAN_ASSIGN_OR_RAISE(const bool dictionary_null,
                        dictionary_value_is_null(node, array, row));
    if (dictionary_null) {
      page->definition_levels.push_back(definition_level);
      page->repetition_levels.push_back(repetition_level);
      return sanitize::Status::OK();
    }
    page->definition_levels.push_back(
        static_cast<std::int16_t>(definition_level + (node.required ? 0 : 1)));
    page->repetition_levels.push_back(repetition_level);
    return append_primitive_value(page->values, &page->bool_values, node, array,
                                  row);
  }
  return sanitize::Status::Invalid(
      "native Parquet writer: target leaf is not reachable from schema root");
}

void encode_bool_values_plain(std::string *out,
                              const std::vector<bool> &values) {
  std::uint8_t byte = 0;
  int bit = 0;
  for (const bool value : values) {
    if (value) {
      byte |= static_cast<std::uint8_t>(std::uint8_t{1} << bit);
    }
    ++bit;
    if (bit == 8) {
      out->push_back(static_cast<char>(byte));
      byte = 0;
      bit = 0;
    }
  }
  if (bit != 0) {
    out->push_back(static_cast<char>(byte));
  }
}

std::size_t
logical_column_page_data_bytes(const ColumnPageData &page_data) noexcept {
  const auto bool_bytes = (page_data.bool_values.size() + 7U) / 8U;
  const auto levels =
      page_data.definition_levels.size() + page_data.repetition_levels.size();
  if (levels > std::numeric_limits<std::size_t>::max() / sizeof(std::int16_t)) {
    return std::numeric_limits<std::size_t>::max();
  }
  const auto level_bytes = levels * sizeof(std::int16_t);
  if (page_data.values.size() >
      std::numeric_limits<std::size_t>::max() - bool_bytes - level_bytes) {
    return std::numeric_limits<std::size_t>::max();
  }
  return page_data.values.size() + bool_bytes + level_bytes;
}

sanitize::Result<std::int64_t> determine_bounded_row_group_count(
    const ParquetNode &root, const std::vector<LeafColumn> &leafs,
    const ArrowArray &array, std::int64_t row_offset, std::int64_t max_rows,
    std::int64_t max_retained_bytes) {
  if (row_offset < 0 || row_offset > array.length || max_rows <= 0) {
    return sanitize::Status::Invalid(
        "native Parquet writer: invalid bounded row-group range");
  }
  const auto available_rows = array.length - row_offset;
  auto candidate_rows = std::min(max_rows, available_rows);
  if (candidate_rows <= 0 || max_retained_bytes <= 0 || leafs.empty()) {
    return candidate_rows;
  }
  std::vector<std::size_t> per_row_bytes(
      static_cast<std::size_t>(candidate_rows), 0);
  const auto byte_limit = static_cast<std::size_t>(max_retained_bytes);
  for (std::size_t leaf_index = 0; leaf_index < leafs.size(); ++leaf_index) {
    ColumnPageData page;
    std::size_t previous_size = 0;
    std::size_t prefix = 0;
    for (std::int64_t row_index = 0; row_index < candidate_rows; ++row_index) {
      SAN_RETURN_NOT_OK(collect_node_for_leaf(
          root, array, row_offset + row_index, leaf_index, &page, 0, 0));
      const auto current_size = logical_column_page_data_bytes(page);
      const auto delta = current_size >= previous_size
                             ? current_size - previous_size
                             : std::numeric_limits<std::size_t>::max();
      auto &row_bytes = per_row_bytes[static_cast<std::size_t>(row_index)];
      row_bytes = saturating_size_add(row_bytes, delta);
      previous_size = current_size;
      prefix = saturating_size_add(prefix, row_bytes);
      if (prefix >= byte_limit) {
        candidate_rows = row_index + 1;
        per_row_bytes.resize(static_cast<std::size_t>(candidate_rows));
        break;
      }
    }
  }
  return std::max<std::int64_t>(1, candidate_rows);
}

sanitize::Result<ColumnPageData>
collect_column_page_data(const ParquetNode &root, const LeafColumn &leaf,
                         const ArrowArray &array, std::int64_t row_offset,
                         std::int64_t row_count, std::size_t leaf_index) {
  ColumnPageData page;
  for (std::int64_t row = 0; row < row_count; ++row) {
    SAN_RETURN_NOT_OK(collect_node_for_leaf(root, array, row_offset + row,
                                            leaf_index, &page, 0, 0));
  }
  if (leaf.node &&
      (leaf.node->arrow_kind == jsonl::JsonlKind::kBool ||
       (leaf.node->arrow_kind == jsonl::JsonlKind::kDictionary &&
        leaf.node->dictionary_value_kind == jsonl::JsonlKind::kBool))) {
    encode_bool_values_plain(&page.values, page.bool_values);
  }
  return page;
}
