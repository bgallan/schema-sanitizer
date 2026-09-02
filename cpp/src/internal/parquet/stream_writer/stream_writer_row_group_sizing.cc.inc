// Estimates encoded row cost and selects row-group boundaries within configured
// byte limits. This fragment is included by the consolidated writer to isolate
// format-specific details.

/// Returns the exact byte span for an eligible flat primitive Arrow row window.
std::optional<std::size_t>
flat_primitive_value_bytes(const ParquetNode &node, const ArrowArray &array,
                           std::int64_t row_offset,
                           std::int64_t row_count) noexcept {
  if (node.node_kind != NodeKind::kPrimitive ||
      node.arrow_kind == jsonl::JsonlKind::kDictionary ||
      array.null_count != 0 || row_offset < 0 || row_count < 0 ||
      row_offset > array.length - row_count) {
    return std::nullopt;
  }
  const auto rows = static_cast<std::size_t>(row_count);
  switch (node.physical_type) {
  case PhysicalType::kBoolean:
    return (rows + 7U) / 8U;
  case PhysicalType::kInt32:
  case PhysicalType::kFloat:
    return saturating_size_multiply(rows, 4U);
  case PhysicalType::kInt64:
  case PhysicalType::kDouble:
    return saturating_size_multiply(rows, 8U);
  case PhysicalType::kFixedLenByteArray: {
    const auto width = node.has_decimal_metadata ? node.decimal_byte_width
                                                 : node.fixed_binary_byte_width;
    if (width <= 0) {
      return std::nullopt;
    }
    return saturating_size_multiply(rows, static_cast<std::size_t>(width));
  }
  case PhysicalType::kByteArray:
    break;
  }
  if (array.n_buffers <= 2 || !array.buffers || !array.buffers[1]) {
    return std::nullopt;
  }
  const auto index = array.offset + row_offset;
  std::uint64_t begin = 0;
  std::uint64_t end = 0;
  if (node.arrow_kind == jsonl::JsonlKind::kLargeString ||
      node.arrow_kind == jsonl::JsonlKind::kLargeBinary) {
    const auto *offsets = static_cast<const std::int64_t *>(array.buffers[1]);
    if (offsets[index] < 0 || offsets[index + row_count] < offsets[index]) {
      return std::nullopt;
    }
    begin = static_cast<std::uint64_t>(offsets[index]);
    end = static_cast<std::uint64_t>(offsets[index + row_count]);
  } else if (node.arrow_kind == jsonl::JsonlKind::kString ||
             node.arrow_kind == jsonl::JsonlKind::kBinary) {
    const auto *offsets = static_cast<const std::int32_t *>(array.buffers[1]);
    if (offsets[index] < 0 || offsets[index + row_count] < offsets[index]) {
      return std::nullopt;
    }
    begin = static_cast<std::uint64_t>(offsets[index]);
    end = static_cast<std::uint64_t>(offsets[index + row_count]);
  } else {
    return std::nullopt;
  }
  const auto payload = end - begin;
  if (payload > std::numeric_limits<std::size_t>::max()) {
    return std::numeric_limits<std::size_t>::max();
  }
  return saturating_size_add(static_cast<std::size_t>(payload),
                             saturating_size_multiply(rows, 4U));
}

/// Estimates checked encoded bytes for a flat row-group interval.
std::optional<std::size_t>
flat_row_group_bytes(const ParquetNode &root,
                     const std::vector<LeafColumn> &leafs,
                     const ArrowArray &array, std::int64_t row_offset,
                     std::int64_t row_count) noexcept {
  if (root.node_kind != NodeKind::kRoot ||
      root.children.size() != leafs.size() ||
      array.n_children != static_cast<std::int64_t>(leafs.size()) ||
      !array.children) {
    return std::nullopt;
  }
  auto total = saturating_size_multiply(
      saturating_size_multiply(static_cast<std::size_t>(row_count),
                               leafs.size()),
      2U * sizeof(std::int16_t));
  for (std::size_t index = 0; index < leafs.size(); ++index) {
    const auto &node = root.children[index];
    if (leafs[index].node != &node || leafs[index].max_repetition_level != 0 ||
        !array.children[index]) {
      return std::nullopt;
    }
    const auto bytes = flat_primitive_value_bytes(node, *array.children[index],
                                                  row_offset, row_count);
    if (!bytes) {
      return std::nullopt;
    }
    total = saturating_size_add(total, *bytes);
  }
  return total;
}

/// Selects a fast-path flat row-group size from checked per-row byte estimates.
std::optional<std::int64_t> determine_flat_bounded_row_group_count(
    const ParquetNode &root, const std::vector<LeafColumn> &leafs,
    const ArrowArray &array, std::int64_t row_offset,
    std::int64_t candidate_rows, std::size_t byte_limit) noexcept {
  const auto total =
      flat_row_group_bytes(root, leafs, array, row_offset, candidate_rows);
  if (!total) {
    return std::nullopt;
  }
  if (*total < byte_limit) {
    return candidate_rows;
  }
  std::int64_t low = 1;
  std::int64_t high = candidate_rows;
  while (low < high) {
    const auto middle = low + (high - low) / 2;
    const auto bytes =
        flat_row_group_bytes(root, leafs, array, row_offset, middle);
    if (!bytes) {
      return std::nullopt;
    }
    if (*bytes >= byte_limit) {
      high = middle;
    } else {
      low = middle + 1;
    }
  }
  return low;
}
