// Summarizes recursive schema nodes, repeated-level layouts, and
// materialization support for diagnostics. This fragment is included by the
// consolidated footer reader inside its private implementation namespace.

struct NativeRecursiveMaterializationDiagnostics {
  std::size_t node_count = 0;
  std::size_t leaf_count = 0;
  std::size_t struct_count = 0;
  std::size_t list_count = 0;
  std::size_t map_count = 0;
  std::size_t max_node_depth = 0;
  std::size_t max_child_count = 0;
  std::string shape_signature;
  std::string structural_shape_signature;
  std::vector<std::string> leaf_paths;
  std::vector<std::vector<std::string>> leaf_path_components;
  std::vector<std::string> repeated_node_paths;
  std::vector<std::vector<std::string>> repeated_node_path_components;
};

/// Joins recursive path components into a stable dotted diagnostic label.
std::string
native_recursive_path_label(const std::vector<std::string> &components) {
  if (components.empty()) {
    return "<root>";
  }
  std::string out;
  for (std::size_t i = 0; i < components.size(); ++i) {
    if (i > 0) {
      out.push_back('.');
    }
    out += components[i].empty() ? "<anon>" : components[i];
  }
  return out;
}

struct NativeRecursiveDiagnosticsFrame {
  std::size_t node_index = 0;
  std::size_t next_child = 0;
  std::size_t parent_path_size = 0;
  bool entered = false;
};

/// Traverses a recursive tree to collect shape signatures, path lists, and node
/// counts.
sanitize::Result<NativeRecursiveMaterializationDiagnostics>
native_recursive_materialization_diagnostics(
    const NativeRecursiveMaterializationTree &tree) {
  if (!tree.valid || tree.root >= tree.nodes.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: recursive diagnostics tree is invalid");
  }

  NativeRecursiveMaterializationDiagnostics diagnostics;
  diagnostics.shape_signature.reserve(tree.nodes.size() * 24);
  diagnostics.structural_shape_signature.reserve(tree.nodes.size() * 20);
  diagnostics.leaf_paths.reserve(tree.nodes.size());
  diagnostics.leaf_path_components.reserve(tree.nodes.size());
  diagnostics.repeated_node_paths.reserve(tree.nodes.size());
  diagnostics.repeated_node_path_components.reserve(tree.nodes.size());

  std::vector<std::string> path;
  path.reserve(tree.nodes.size());
  std::vector<NativeRecursiveDiagnosticsFrame> pending;
  pending.reserve(tree.nodes.size());
  pending.push_back({tree.root, 0, 0, false});
  std::vector<bool> visited(tree.nodes.size(), false);

  while (!pending.empty()) {
    auto &frame = pending.back();
    if (frame.node_index >= tree.nodes.size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: recursive diagnostics node is invalid");
    }
    const auto &node = tree.nodes[frame.node_index];

    if (!frame.entered) {
      if (visited[frame.node_index]) {
        return sanitize::Status::Invalid(
            "native Parquet reader: recursive diagnostics tree contains a "
            "repeated node");
      }
      visited[frame.node_index] = true;
      frame.entered = true;
      path.resize(frame.parent_path_size);
      path.push_back(node.name);

      ++diagnostics.node_count;
      diagnostics.max_node_depth =
          std::max(diagnostics.max_node_depth, path.size());
      diagnostics.max_child_count =
          std::max(diagnostics.max_child_count, node.children.size());

      const std::string_view kind =
          native_recursive_materialization_node_kind_label(node.kind);
      diagnostics.shape_signature.append(kind);
      diagnostics.shape_signature.push_back('(');
      diagnostics.shape_signature.append(node.name);
      diagnostics.shape_signature.push_back(')');
      diagnostics.structural_shape_signature.append(kind);
      diagnostics.structural_shape_signature.push_back('(');
      diagnostics.structural_shape_signature.append(node.name);
      diagnostics.structural_shape_signature.push_back(')');

      switch (node.kind) {
      case NativeRecursiveMaterializationNodeKind::Leaf:
        ++diagnostics.leaf_count;
        diagnostics.shape_signature.push_back('#');
        if (node.leaf_column_index.has_value()) {
          diagnostics.shape_signature.append(
              std::to_string(node.leaf_column_index.value()));
        } else {
          diagnostics.shape_signature.push_back('?');
        }
        diagnostics.leaf_paths.push_back(native_recursive_path_label(path));
        diagnostics.leaf_path_components.push_back(path);
        path.resize(frame.parent_path_size);
        pending.pop_back();
        continue;
      case NativeRecursiveMaterializationNodeKind::Struct:
        ++diagnostics.struct_count;
        break;
      case NativeRecursiveMaterializationNodeKind::List:
        ++diagnostics.list_count;
        diagnostics.repeated_node_paths.push_back(
            native_recursive_path_label(path));
        diagnostics.repeated_node_path_components.push_back(path);
        break;
      case NativeRecursiveMaterializationNodeKind::Map:
        ++diagnostics.map_count;
        diagnostics.repeated_node_paths.push_back(
            native_recursive_path_label(path));
        diagnostics.repeated_node_path_components.push_back(path);
        break;
      }
      diagnostics.shape_signature.push_back('<');
      diagnostics.structural_shape_signature.push_back('<');
    }

    if (frame.next_child < node.children.size()) {
      if (frame.next_child > 0) {
        diagnostics.shape_signature.push_back(',');
        diagnostics.structural_shape_signature.push_back(',');
      }
      const std::size_t child_index = node.children[frame.next_child++];
      pending.push_back({child_index, 0, path.size(), false});
      continue;
    }

    diagnostics.shape_signature.push_back('>');
    diagnostics.structural_shape_signature.push_back('>');
    path.resize(frame.parent_path_size);
    pending.pop_back();
  }
  return diagnostics;
}

/// Serializes a size vector as a compact JSON array for native-reader
/// diagnostics.
void append_size_array(std::string &out,
                       const std::vector<std::size_t> &items) {
  out.push_back('[');
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out.push_back(',');
    }
    out += std::to_string(items[i]);
  }
  out.push_back(']');
}
