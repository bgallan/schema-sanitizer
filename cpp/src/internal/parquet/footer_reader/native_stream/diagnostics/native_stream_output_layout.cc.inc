void append_native_recursive_output_layout_diagnostics(
    std::string &out, const RowGroupInfo &row_group) {
  std::vector<NativeParquetOutputField> layout;
  auto status = build_native_output_layout(row_group.columns, &layout);
  out.push_back('{');
  bool first = true;
  json_encoding::append_int_field(out, first, "decoded", status.ok() ? 1 : 0);
  if (!status.ok()) {
    json_encoding::append_string_field(out, first, "error", status.message());
    json_encoding::append_key(out, first, "fields");
    out += "[]";
    out.push_back('}');
    return;
  }
  json_encoding::append_int_field(out, first, "field_count",
                                  static_cast<std::int64_t>(layout.size()));
  json_encoding::append_key(out, first, "fields");
  out.push_back('[');
  for (std::size_t i = 0; i < layout.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    const auto &field = layout[i];
    auto diagnostics =
        native_recursive_materialization_diagnostics(field.recursive_tree);
    out.push_back('{');
    bool field_first = true;
    json_encoding::append_string_field(out, field_first, "name", field.name);
    json_encoding::append_int_field(out, field_first, "top_level_required",
                                    field.top_level_required ? 1 : 0);
    json_encoding::append_int_field(out, field_first, "repetition_depth",
                                    field.recursive_tree.repetition_depth);
    json_encoding::append_int_field(
        out, field_first, "leaf_count",
        static_cast<std::int64_t>(field.recursive_counts.leaf_count));
    json_encoding::append_int_field(
        out, field_first, "struct_count",
        static_cast<std::int64_t>(field.recursive_counts.struct_count));
    json_encoding::append_int_field(
        out, field_first, "list_like_count",
        static_cast<std::int64_t>(field.recursive_counts.list_count));
    json_encoding::append_key(out, field_first, "column_indices");
    append_size_array(out, field.column_indices);

    std::vector<std::int32_t> leaf_max_definition_levels;
    std::vector<std::int32_t> leaf_max_repetition_levels;
    std::vector<std::vector<std::int16_t>> leaf_path_definition_levels;
    std::vector<std::vector<std::int16_t>> leaf_path_repetition_levels;
    leaf_max_definition_levels.reserve(field.column_indices.size());
    leaf_max_repetition_levels.reserve(field.column_indices.size());
    leaf_path_definition_levels.reserve(field.column_indices.size());
    leaf_path_repetition_levels.reserve(field.column_indices.size());
    for (const auto column_index : field.column_indices) {
      if (column_index >= row_group.columns.size()) {
        continue;
      }
      const auto &leaf_column = row_group.columns[column_index];
      leaf_max_definition_levels.push_back(leaf_column.max_definition_level);
      leaf_max_repetition_levels.push_back(leaf_column.max_repetition_level);
      leaf_path_definition_levels.push_back(leaf_column.path_definition_levels);
      leaf_path_repetition_levels.push_back(leaf_column.path_repetition_levels);
    }
    json_encoding::append_key(out, field_first, "leaf_max_definition_levels");
    append_int_array(out, leaf_max_definition_levels);
    json_encoding::append_key(out, field_first, "leaf_max_repetition_levels");
    append_int_array(out, leaf_max_repetition_levels);
    json_encoding::append_key(out, field_first, "leaf_path_definition_levels");
    append_int16_matrix(out, leaf_path_definition_levels);
    json_encoding::append_key(out, field_first, "leaf_path_repetition_levels");
    append_int16_matrix(out, leaf_path_repetition_levels);

    if (field.recursive_tree.valid &&
        field.recursive_tree.root < field.recursive_tree.nodes.size()) {
      const auto &root_node =
          field.recursive_tree.nodes[field.recursive_tree.root];
      json_encoding::append_string_field(
          out, field_first, "root_kind",
          native_recursive_materialization_node_kind_label(root_node.kind));
    }
    if (diagnostics.ok()) {
      json_encoding::append_int_field(
          out, field_first, "node_count",
          static_cast<std::int64_t>((*diagnostics).node_count));
      json_encoding::append_int_field(
          out, field_first, "map_count",
          static_cast<std::int64_t>((*diagnostics).map_count));
      json_encoding::append_int_field(
          out, field_first, "max_node_depth",
          static_cast<std::int64_t>((*diagnostics).max_node_depth));
      json_encoding::append_int_field(
          out, field_first, "max_child_count",
          static_cast<std::int64_t>((*diagnostics).max_child_count));
      json_encoding::append_string_field(out, field_first, "shape_signature",
                                         (*diagnostics).shape_signature);
      json_encoding::append_string_field(
          out, field_first, "structural_shape_signature",
          (*diagnostics).structural_shape_signature);
      json_encoding::append_key(out, field_first, "leaf_paths");
      append_string_array(out, (*diagnostics).leaf_paths);
      json_encoding::append_key(out, field_first, "leaf_path_components");
      append_string_matrix(out, (*diagnostics).leaf_path_components);
      json_encoding::append_key(out, field_first, "repeated_node_paths");
      append_string_array(out, (*diagnostics).repeated_node_paths);
      json_encoding::append_key(out, field_first,
                                "repeated_node_path_components");
      append_string_matrix(out, (*diagnostics).repeated_node_path_components);
    } else {
      json_encoding::append_string_field(out, field_first, "diagnostic_error",
                                         diagnostics.status().message());
    }
    out.push_back('}');
  }
  out.push_back(']');
  out.push_back('}');
}
