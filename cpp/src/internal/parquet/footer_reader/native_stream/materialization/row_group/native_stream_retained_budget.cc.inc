// Enforces actual retained Arrow buffer capacity for native row windows.

sanitize::Status add_native_retained_capacity(
    std::int64_t *total, std::size_t capacity, std::size_t element_size,
    std::string_view label) {
  if (!total || element_size == 0 ||
      capacity > static_cast<std::size_t>(
                     std::numeric_limits<std::int64_t>::max()) /
                     element_size) {
    return sanitize::Status::Invalid(
        "native Parquet reader: retained ", label, " capacity overflows");
  }
  return add_i64_checked(total,
                         static_cast<std::int64_t>(capacity * element_size),
                         label);
}

sanitize::Result<std::int64_t> native_array_retained_capacity_bytes(
    const NativeParquetArrayState &state) {
  std::int64_t total = 0;
  SAN_RETURN_NOT_OK(add_native_retained_capacity(
      &total, state.columns.capacity(), sizeof(NativeParquetChildArray),
      "column shell bytes"));
  for (const auto &column : state.columns) {
    SAN_RETURN_NOT_OK(add_native_retained_capacity(
        &total, column.validity.capacity(), sizeof(std::uint8_t),
        "column validity bytes"));
    SAN_RETURN_NOT_OK(add_native_retained_capacity(
        &total, column.offsets.capacity(), sizeof(std::int32_t),
        "column offset bytes"));
    SAN_RETURN_NOT_OK(add_native_retained_capacity(
        &total, column.values.capacity(), sizeof(std::uint8_t),
        "column value bytes"));
  }
  SAN_RETURN_NOT_OK(add_native_retained_capacity(
      &total, state.structs.capacity(), sizeof(NativeParquetStructArray),
      "struct shell bytes"));
  for (const auto &node : state.structs) {
    SAN_RETURN_NOT_OK(add_native_retained_capacity(
        &total, node.validity.capacity(), sizeof(std::uint8_t),
        "struct validity bytes"));
    SAN_RETURN_NOT_OK(add_native_retained_capacity(
        &total, node.children.capacity(), sizeof(ArrowArray *),
        "struct child pointer bytes"));
  }
  SAN_RETURN_NOT_OK(add_native_retained_capacity(
      &total, state.lists.capacity(), sizeof(NativeParquetListArray),
      "list shell bytes"));
  for (const auto &node : state.lists) {
    SAN_RETURN_NOT_OK(add_native_retained_capacity(
        &total, node.validity.capacity(), sizeof(std::uint8_t),
        "list validity bytes"));
    SAN_RETURN_NOT_OK(add_native_retained_capacity(
        &total, node.offsets.capacity(), sizeof(std::int32_t),
        "list offset bytes"));
  }
  SAN_RETURN_NOT_OK(add_native_retained_capacity(
      &total, state.children.capacity(), sizeof(ArrowArray *),
      "root child pointer bytes"));
  return total;
}

sanitize::Status enforce_native_array_retained_budget(
    const NativeParquetArrayState &state, std::int64_t limit) {
  SAN_ASSIGN_OR_RAISE(const auto retained,
                      native_array_retained_capacity_bytes(state));
  if (retained > limit) {
    return sanitize::Status::OutOfMemory(
        "native Parquet reader: retained Arrow capacity ", retained,
        " exceeds configured limit ", limit);
  }
  return {};
}

