// Appends a null/empty entry for every primitive leaf under a node.
void emit_nulls_for_subtree(const ParquetNode &node,
                            std::vector<ColumnPageData> *pages,
                            std::int16_t definition_level,
                            std::int16_t repetition_level) {
  const auto end = node.first_leaf_index + node.leaf_count;
  for (std::size_t leaf_index = node.first_leaf_index; leaf_index < end;
       ++leaf_index) {
    auto &page = (*pages)[leaf_index];
    page.definition_levels.push_back(definition_level);
    page.repetition_levels.push_back(repetition_level);
  }
}

// Collects Arrow values into Parquet column page buffers.

// Recursively collects Arrow nodes into Parquet page staging buffers.

// Recursively appends values and levels for one node.
sanitize::Status collect_node(const ParquetNode &node, const ArrowArray &array,
                              std::int64_t row,
                              std::vector<ColumnPageData> *pages,
                              std::int16_t definition_level,
                              std::int16_t repetition_level);

// Recursively appends values and levels for a present list element.
sanitize::Status collect_list_element(const ParquetNode &element,
                                      const ArrowArray &values,
                                      std::int64_t row,
                                      std::vector<ColumnPageData> *pages,
                                      std::int16_t definition_level,
                                      std::int16_t repetition_level) {
  return collect_node(element, values, row, pages, definition_level,
                      repetition_level);
}

sanitize::Status collect_node(const ParquetNode &node, const ArrowArray &array,
                              std::int64_t row,
                              std::vector<ColumnPageData> *pages,
                              std::int16_t definition_level,
                              std::int16_t repetition_level) {
  if (node.node_kind != NodeKind::kRoot && !node.required &&
      !bitmap_is_valid(array, row)) {
    emit_nulls_for_subtree(node, pages, definition_level, repetition_level);
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
      if (!array.children || !array.children[i]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing root child array");
      }
      SAN_RETURN_NOT_OK(collect_node(node.children[i], *array.children[i], row,
                                     pages, definition_level,
                                     repetition_level));
    }
    return sanitize::Status::OK();
  case NodeKind::kStruct: {
    const auto present_definition =
        static_cast<std::int16_t>(definition_level + (node.required ? 0 : 1));
    const auto child_row = array.offset + row;
    for (std::size_t i = 0; i < node.children.size(); ++i) {
      if (!array.children || !array.children[i]) {
        return sanitize::Status::Invalid(
            "native Parquet writer: missing struct child array");
      }
      SAN_RETURN_NOT_OK(collect_node(node.children[i], *array.children[i],
                                     child_row, pages, present_definition,
                                     repetition_level));
    }
    return sanitize::Status::OK();
  }
  case NodeKind::kList: {
    if (!array.children || !array.children[0]) {
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
      if (node.fixed_size_list_size < 0) {
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
    if (end < begin) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid list offsets");
    }
    if (begin == end) {
      emit_nulls_for_subtree(node, pages, present_definition, repetition_level);
      return sanitize::Status::OK();
    }
    for (std::int64_t item = begin; item < end; ++item) {
      const auto item_repetition =
          item == begin ? repetition_level : repeated_repetition;
      SAN_RETURN_NOT_OK(collect_list_element(*node.element, *array.children[0],
                                             item, pages, repeated_definition,
                                             item_repetition));
    }
    return sanitize::Status::OK();
  }
  case NodeKind::kMap: {
    if (array.n_buffers <= 1 || !array.buffers || !array.buffers[1] ||
        !array.children || !array.children[0]) {
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
    if (end < begin) {
      return sanitize::Status::Invalid(
          "native Parquet writer: invalid map offsets");
    }
    if (begin == end) {
      emit_nulls_for_subtree(node, pages, present_definition, repetition_level);
      return sanitize::Status::OK();
    }
    for (std::int64_t item = begin; item < end; ++item) {
      const auto item_repetition =
          item == begin ? repetition_level : entry_repetition;
      SAN_RETURN_NOT_OK(collect_node(*node.element, *array.children[0], item,
                                     pages, entry_definition, item_repetition));
    }
    return sanitize::Status::OK();
  }
  case NodeKind::kPrimitive: {
    auto &page = (*pages)[node.leaf_index];
    if (node.arrow_kind == jsonl::JsonlKind::kNull) {
      page.definition_levels.push_back(definition_level);
      page.repetition_levels.push_back(repetition_level);
      return sanitize::Status::OK();
    }
    SAN_ASSIGN_OR_RAISE(const bool dictionary_null,
                        dictionary_value_is_null(node, array, row));
    if (dictionary_null) {
      page.definition_levels.push_back(definition_level);
      page.repetition_levels.push_back(repetition_level);
      return sanitize::Status::OK();
    }
    page.definition_levels.push_back(
        static_cast<std::int16_t>(definition_level + (node.required ? 0 : 1)));
    page.repetition_levels.push_back(repetition_level);
    return append_primitive_value(page.values, &page.bool_values, node, array,
                                  row);
  }
  }
  return sanitize::Status::Invalid("native Parquet writer: invalid node kind");
}

// Encodes staged boolean values into Parquet PLAIN bytes.

// Encodes staged boolean values into Parquet PLAIN bytes.
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

// Collects a row range and finalizes per-column page staging data.

// Collects one batch into column page data.
sanitize::Result<std::vector<ColumnPageData>> collect_batch_pages(
    const ParquetNode &root, const std::vector<LeafColumn> &leafs,
    const ArrowArray &array, std::int64_t row_offset, std::int64_t row_count) {
  std::vector<ColumnPageData> pages(leafs.size());
  const auto row_end = row_offset + row_count;
  for (std::int64_t row = row_offset; row < row_end; ++row) {
    SAN_RETURN_NOT_OK(collect_node(root, array, row, &pages, 0, 0));
  }
  for (std::size_t i = 0; i < leafs.size(); ++i) {
    if (leafs[i].node &&
        (leafs[i].node->arrow_kind == jsonl::JsonlKind::kBool ||
         (leafs[i].node->arrow_kind == jsonl::JsonlKind::kDictionary &&
          leafs[i].node->dictionary_value_kind == jsonl::JsonlKind::kBool))) {
      encode_bool_values_plain(&pages[i].values, pages[i].bool_values);
    }
  }
  return pages;
}
