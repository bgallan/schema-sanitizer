// Accounts for retained Arrow and decoder capacity and divides scratch
// allowances among workers. This fragment is included by the consolidated
// footer reader inside its private implementation namespace.

/// Adds one retained allocation to a checked byte total.
sanitize::Status add_native_retained_capacity(std::int64_t *total,
                                              std::size_t capacity,
                                              std::size_t element_size,
                                              std::string_view label) {
  if (!total || element_size == 0 ||
      capacity >
          static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()) /
              element_size) {
    return sanitize::Status::Invalid("native Parquet reader: retained ", label,
                                     " capacity overflows");
  }
  return add_i64_checked(
      total, static_cast<std::int64_t>(capacity * element_size), label);
}

/// Measures recursively retained Arrow buffer capacity with checked arithmetic.
sanitize::Result<std::int64_t>
native_array_retained_capacity_bytes(const NativeParquetArrayState &state) {
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
  SAN_RETURN_NOT_OK(add_native_retained_capacity(&total, state.lists.capacity(),
                                                 sizeof(NativeParquetListArray),
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

/// Rejects a materialized Arrow array when recursively retained buffer capacity
/// exceeds its reservation.
sanitize::Status
enforce_native_array_retained_budget(const NativeParquetArrayState &state,
                                     std::int64_t limit) {
  SAN_ASSIGN_OR_RAISE(const auto retained,
                      native_array_retained_capacity_bytes(state));
  if (retained > limit) {
    return sanitize::Status::OutOfMemory(
        "native Parquet reader: retained Arrow capacity ", retained,
        " exceeds configured limit ", limit);
  }
  return {};
}

struct NativeParquetSharedReservationBudget {
  /// Initializes the shared atomic capacity available to parallel column
  /// decoders.
  NativeParquetSharedReservationBudget(std::int64_t capacity,
                                       std::int64_t initial)
      : limit(std::max<std::int64_t>(1, capacity)), used(initial) {}

  /// Atomically reserves shared scratch capacity when doing so remains within
  /// the configured limit.
  [[nodiscard]] bool try_reserve(std::int64_t bytes) noexcept {
    if (bytes < 0 || bytes > limit) {
      return false;
    }
    auto current = used.load(std::memory_order_relaxed);
    while (current <= limit - bytes) {
      if (used.compare_exchange_weak(current, current + bytes,
                                     std::memory_order_acq_rel,
                                     std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  /// Decrements shared reserved-byte usage by the supplied positive amount.
  void release(std::int64_t bytes) noexcept {
    if (bytes > 0) {
      used.fetch_sub(bytes, std::memory_order_acq_rel);
    }
  }

  std::int64_t limit = 1;
  std::atomic<std::int64_t> used{0};
};

struct NativeParquetColumnReservation {
  /// Binds one column decode to its shared scratch and retained-output
  /// reservations.
  NativeParquetColumnReservation(
      std::shared_ptr<NativeParquetSharedReservationBudget> owner,
      std::int64_t scratch_bytes, std::int64_t output_bytes)
      : budget(std::move(owner)), scratch(scratch_bytes), output(output_bytes) {
  }

  /// Returns uncommitted scratch and retained-output capacity to the shared
  /// decode budget.
  ~NativeParquetColumnReservation() {
    if (budget) {
      budget->release(scratch + output);
    }
  }

  /// Converts the reservation from scratch capacity to retained decoded-buffer
  /// capacity.
  void finish_decode() noexcept {
    if (budget && scratch > 0) {
      budget->release(scratch);
      scratch = 0;
    }
  }

  /// Commits output only after preparation succeeds, preserving physical output
  /// order.
  void commit_output() noexcept {
    finish_decode();
    // The committed Arrow buffers remain live in NativeParquetArrayState. Keep
    // their estimate charged in the shared coordinator until the whole
    // parallel materialization call has completed.
    output = 0;
  }

  std::shared_ptr<NativeParquetSharedReservationBudget> budget;
  std::int64_t scratch = 0;
  std::int64_t output = 0;
};

/// Estimates the largest temporary decode footprint among one column's page
/// spans.
std::int64_t
native_parquet_column_scratch_bytes(const ColumnChunkInfo &column) noexcept {
  std::int64_t maximum = 64LL * 1024LL;
  for (const auto &span : column.native_read_page_spans) {
    std::int64_t estimate = 0;
    const auto add_non_negative = [&estimate](std::int64_t value) noexcept {
      if (value <= 0 || estimate == std::numeric_limits<std::int64_t>::max()) {
        return;
      }
      if (estimate > std::numeric_limits<std::int64_t>::max() - value) {
        estimate = std::numeric_limits<std::int64_t>::max();
      } else {
        estimate += value;
      }
    };
    add_non_negative(span.payload_size);
    add_non_negative(span.value_payload_bytes);
    add_non_negative(span.materialized_value_bytes);
    add_non_negative(span.materialized_offset_bytes);
    if (span.non_null_count > 0) {
      add_non_negative(static_cast<std::int64_t>(span.non_null_count) *
                       static_cast<std::int64_t>(sizeof(std::int64_t)));
    }
    maximum = std::max(maximum, estimate);
  }
  return maximum;
}

/// Charges native Parquet column to the active memory budget before allocating
/// derived storage.
sanitize::Result<std::shared_ptr<NativeParquetColumnReservation>>
reserve_native_parquet_column(
    const RowGroupInfo &row_group, const ColumnChunkInfo &column,
    const NativeFlatRowWindow &window,
    const std::shared_ptr<NativeParquetSharedReservationBudget> &budget) {
  if (!budget) {
    return sanitize::Status::Invalid(
        "native Parquet reader: shared column reservation is unavailable");
  }
  const auto scratch = native_parquet_column_scratch_bytes(column);
  SAN_ASSIGN_OR_RAISE(
      const auto output,
      native_reader_flat_column_window_buffer_bytes(
          column, row_group.num_rows, window.first_row, window.row_count));
  std::int64_t total = scratch;
  SAN_RETURN_NOT_OK(
      add_i64_checked(&total, output, "parallel column reservation bytes"));
  if (!budget->try_reserve(total)) {
    return sanitize::Status::OutOfMemory(
        "native Parquet reader: parallel column reservation ", total,
        " exceeds remaining shared operation budget ", budget->limit);
  }
  try {
    return std::make_shared<NativeParquetColumnReservation>(budget, scratch,
                                                            output);
  } catch (const std::bad_alloc &) {
    budget->release(total);
    return sanitize::Status::OutOfMemory(
        "native Parquet reader: column reservation allocation failed");
  }
}

/// Returns the largest per-column scratch estimate in a row group.
std::int64_t native_parquet_max_column_scratch_bytes(
    const RowGroupInfo &row_group) noexcept {
  std::int64_t maximum = 64LL * 1024LL;
  for (const auto &column : row_group.columns) {
    maximum = std::max(maximum, native_parquet_column_scratch_bytes(column));
  }
  return maximum;
}
