// Implements bounded parallel native Parquet column materialization.

struct NativeParquetDecodedColumn {
  std::size_t index = 0;
  NativeParquetChildArray child;
};

struct NativeParquetColumnRange {
  std::size_t first = 0;
  std::size_t last = 0;
};

struct NativeParquetDecodedColumnRange {
  std::vector<NativeParquetDecodedColumn> columns;
};

void rebind_native_parquet_child_array(NativeParquetChildArray *child) {
  if (child) {
    child->array.buffers = child->buffers.data();
  }
}

std::int64_t native_parquet_max_column_scratch_bytes(
    const RowGroupInfo &row_group) noexcept {
  std::int64_t maximum = 64LL * 1024LL;
  for (const auto &column : row_group.columns) {
    for (const auto &span : column.native_read_page_spans) {
      std::int64_t estimate = 0;
      const auto add_non_negative = [&estimate](std::int64_t value) noexcept {
        if (value <= 0 ||
            estimate == std::numeric_limits<std::int64_t>::max()) {
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
      // non_null_count is int32, so widening before multiplying by an
      // eight-byte value index cannot overflow int64.
      if (span.non_null_count > 0) {
        add_non_negative(static_cast<std::int64_t>(span.non_null_count) *
                         static_cast<std::int64_t>(sizeof(std::int64_t)));
      }
      maximum = std::max(maximum, estimate);
    }
  }
  return maximum;
}

std::size_t native_parquet_column_worker_count(
    const NativeParquetStreamState &stream, const RowGroupInfo &row_group,
    const NativeFlatRowWindow &window,
    const std::shared_ptr<sanitize::internal::OperationTaskArena> &task_arena) {
  if (!task_arena || task_arena->inline_mode() ||
      row_group.columns.size() < 2 || window.row_count < 2048) {
    return 1;
  }
  const auto estimated_result = native_reader_flat_window_buffer_bytes(
      row_group, window.first_row, window.row_count);
  if (!estimated_result.ok()) {
    return 1;
  }
  constexpr std::int64_t kMinimumWorkBytesPerWorker = 256LL * 1024LL;
  const auto work_workers = std::max<std::int64_t>(
      1, estimated_result.ValueOrDie() / kMinimumWorkBytesPerWorker);
  auto workers = std::min<std::size_t>(
      {task_arena->worker_count(), row_group.columns.size(),
       static_cast<std::size_t>(work_workers)});
  if (workers < 2) {
    return 1;
  }

  // Retain the v64 aggregate scratch bound. Grouped tasks reuse one scratch
  // object sequentially for all columns assigned to a physical worker.
  const auto scratch_per_worker =
      native_parquet_max_column_scratch_bytes(row_group);
  if (scratch_per_worker <= 0) {
    return 1;
  }
  const auto scratch_budget =
      std::max<std::int64_t>(1, stream.max_buffer_bytes / 4);
  const auto scratch_workers =
      std::max<std::int64_t>(1, scratch_budget / scratch_per_worker);
  workers =
      std::min<std::size_t>(workers, static_cast<std::size_t>(scratch_workers));
  return workers < 2 ? 1 : workers;
}

sanitize::Status
ensure_native_parquet_worker_file(NativeParquetColumnWorkerState *worker_state,
                                  const std::string &path) {
  if (!worker_state) {
    return sanitize::Status::Invalid(
        "native Parquet reader: column worker state is null");
  }
  if (!worker_state->file.is_open()) {
    worker_state->file.open(path, std::ios::binary);
    if (!worker_state->file) {
      return sanitize::Status::IOError(
          "native Parquet reader: failed opening parallel input");
    }
  }
  worker_state->file.clear();
  return {};
}

bool native_parquet_fixed_cost_column(const ColumnChunkInfo &column) noexcept {
  if (!column.repeated_level_layouts.empty()) {
    return false;
  }
  switch (column.native_read_value_buffer_kind) {
  case NativeValueBufferKind::none:
  case NativeValueBufferKind::fixed_width:
  case NativeValueBufferKind::bit_packed_boolean:
  case NativeValueBufferKind::dictionary_fixed_width:
  case NativeValueBufferKind::delta_binary_packed:
  case NativeValueBufferKind::byte_stream_split:
    return true;
  case NativeValueBufferKind::plain_byte_array:
  case NativeValueBufferKind::dictionary_byte_array:
  case NativeValueBufferKind::delta_length_byte_array:
    return false;
  }
  return false;
}

bool native_parquet_grouped_column_decode_eligible(
    const RowGroupInfo &row_group, std::size_t worker_count) noexcept {
  if (worker_count < 2 || row_group.columns.size() < worker_count * 4) {
    return false;
  }
  return std::ranges::all_of(row_group.columns,
                             native_parquet_fixed_cost_column);
}

long double native_parquet_fixed_column_work_weight(
    const ColumnChunkInfo &column, const NativeFlatRowWindow &window) noexcept {
  long double weight = 1.0L;
  for (const auto &span : column.native_read_page_spans) {
    if (span.payload_size > 0) {
      weight += static_cast<long double>(span.payload_size);
    }
  }
  const auto values = std::max<std::int64_t>(0, window.row_count);
  switch (column.native_read_value_buffer_kind) {
  case NativeValueBufferKind::bit_packed_boolean:
    weight += static_cast<long double>((values + 7) / 8);
    break;
  case NativeValueBufferKind::fixed_width:
  case NativeValueBufferKind::dictionary_fixed_width:
  case NativeValueBufferKind::delta_binary_packed:
  case NativeValueBufferKind::byte_stream_split:
    weight += static_cast<long double>(values) *
              static_cast<long double>(std::max<std::int32_t>(
                  1, column.native_read_value_width_bytes));
    break;
  case NativeValueBufferKind::none:
  case NativeValueBufferKind::plain_byte_array:
  case NativeValueBufferKind::dictionary_byte_array:
  case NativeValueBufferKind::delta_length_byte_array:
    break;
  }
  return weight;
}

std::vector<NativeParquetColumnRange>
native_parquet_balanced_column_ranges(const RowGroupInfo &row_group,
                                      const NativeFlatRowWindow &window,
                                      std::size_t range_count) {
  const auto column_count = row_group.columns.size();
  range_count = std::max<std::size_t>(
      1, std::min<std::size_t>(range_count, column_count));
  std::vector<long double> weights;
  weights.reserve(column_count);
  long double remaining_weight = 0.0L;
  for (const auto &column : row_group.columns) {
    const auto weight = native_parquet_fixed_column_work_weight(column, window);
    weights.push_back(weight);
    remaining_weight += weight;
  }

  std::vector<NativeParquetColumnRange> ranges;
  ranges.reserve(range_count);
  std::size_t first = 0;
  for (std::size_t range_index = 0; range_index < range_count; ++range_index) {
    const auto ranges_left = range_count - range_index;
    if (ranges_left == 1) {
      ranges.push_back({first, column_count});
      break;
    }
    const auto target =
        remaining_weight / static_cast<long double>(ranges_left);
    const auto final_allowed = column_count - (ranges_left - 1);
    long double accumulated = 0.0L;
    auto last = first;
    while (last < final_allowed) {
      const auto next = weights[last];
      if (last > first && accumulated + next > target) {
        break;
      }
      accumulated += next;
      ++last;
      if (accumulated >= target) {
        break;
      }
    }
    if (last == first) {
      accumulated += weights[last];
      ++last;
    }
    ranges.push_back({first, last});
    remaining_weight =
        std::max<long double>(0.0L, remaining_weight - accumulated);
    first = last;
  }
  return ranges;
}

sanitize::Status
commit_native_parquet_decoded_column(NativeParquetDecodedColumn decoded,
                                     NativeParquetStreamState *stream,
                                     NativeParquetArrayState *state) {
  if (!stream || !state || decoded.index >= state->columns.size()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: column commit order mismatch");
  }
  state->columns[decoded.index] = std::move(decoded.child);
  rebind_native_parquet_child_array(&state->columns[decoded.index]);
  return enforce_native_array_retained_budget(*state, stream->max_buffer_bytes);
}

sanitize::Status materialize_native_row_group_columns_individual(
    NativeParquetStreamState *stream, const RowGroupInfo &row_group,
    const NativeFlatRowWindow &window, std::size_t worker_count,
    NativeParquetArrayState *state,
    std::shared_ptr<sanitize::internal::OperationTaskArena> task_arena) {
  using Executor =
      sanitize::internal::OrderedExecutor<std::size_t,
                                          NativeParquetDecodedColumn>;
  auto *worker_states = &stream->parallel_column_workers;
  auto worker = [path = stream->path, &row_group, window,
                 worker_states](std::size_t &&column_index,
                                std::size_t worker_index, std::stop_token stop)
      -> sanitize::Result<NativeParquetDecodedColumn> {
    if (stop.stop_requested()) {
      return sanitize::Status::Cancelled(
          "native Parquet reader: column decode cancelled");
    }
    if (column_index >= row_group.columns.size() ||
        worker_index >= worker_states->size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: column worker index is out of range");
    }
    auto &worker_state = (*worker_states)[worker_index];
    SAN_RETURN_NOT_OK(ensure_native_parquet_worker_file(&worker_state, path));
    NativeParquetDecodedColumn decoded;
    decoded.index = column_index;
    SAN_RETURN_NOT_OK(materialize_native_row_group_column(
        worker_state.file, row_group.columns[column_index], window,
        &worker_state.page_scratch, &decoded.child));
    return decoded;
  };

  SAN_ASSIGN_OR_RAISE(
      auto executor,
      Executor::Make(worker_count, worker_count, worker_count,
                     std::move(worker), std::move(task_arena),
                     sanitize::internal::TaskArenaLane::kUpstream,
                     sanitize::internal::TaskTelemetryKind::kInput));
  std::size_t submitted = 0;
  std::size_t committed = 0;
  const auto commit_next = [&]() -> sanitize::Status {
    SAN_ASSIGN_OR_RAISE(auto outcome, executor->TakeNext());
    if (!outcome.result.ok()) {
      executor->Cancel();
      return outcome.result.status();
    }
    auto decoded = std::move(outcome.result).ValueOrDie();
    if (decoded.index != outcome.ordinal) {
      executor->Cancel();
      return sanitize::Status::Invalid(
          "native Parquet reader: column commit order mismatch");
    }
    SAN_RETURN_NOT_OK(commit_native_parquet_decoded_column(std::move(decoded),
                                                           stream, state));
    ++committed;
    return {};
  };

  while (submitted < row_group.columns.size()) {
    if (executor->in_flight() >= executor->dispatch_window()) {
      SAN_RETURN_NOT_OK(commit_next());
    }
    SAN_RETURN_NOT_OK(executor->Submit(
        typename Executor::Packet{.ordinal = submitted, .payload = submitted}));
    ++submitted;
  }
  SAN_RETURN_NOT_OK(executor->FinishSubmission());
  while (committed < submitted) {
    SAN_RETURN_NOT_OK(commit_next());
  }
  return {};
}

sanitize::Status materialize_native_row_group_columns_grouped(
    NativeParquetStreamState *stream, const RowGroupInfo &row_group,
    const NativeFlatRowWindow &window, std::size_t worker_count,
    NativeParquetArrayState *state,
    std::shared_ptr<sanitize::internal::OperationTaskArena> task_arena) {
  const auto ranges =
      native_parquet_balanced_column_ranges(row_group, window, worker_count);
  using Executor =
      sanitize::internal::OrderedExecutor<NativeParquetColumnRange,
                                          NativeParquetDecodedColumnRange>;
  auto *worker_states = &stream->parallel_column_workers;
  auto worker = [path = stream->path, &row_group, window,
                 worker_states](NativeParquetColumnRange &&range,
                                std::size_t worker_index, std::stop_token stop)
      -> sanitize::Result<NativeParquetDecodedColumnRange> {
    if (range.first >= range.last || range.last > row_group.columns.size() ||
        worker_index >= worker_states->size()) {
      return sanitize::Status::Invalid(
          "native Parquet reader: column worker range is out of range");
    }
    auto &worker_state = (*worker_states)[worker_index];
    SAN_RETURN_NOT_OK(ensure_native_parquet_worker_file(&worker_state, path));
    NativeParquetDecodedColumnRange decoded;
    decoded.columns.reserve(range.last - range.first);
    for (auto index = range.first; index < range.last; ++index) {
      if (stop.stop_requested()) {
        return sanitize::Status::Cancelled(
            "native Parquet reader: column decode cancelled");
      }
      NativeParquetDecodedColumn column;
      column.index = index;
      SAN_RETURN_NOT_OK(materialize_native_row_group_column(
          worker_state.file, row_group.columns[index], window,
          &worker_state.page_scratch, &column.child));
      decoded.columns.push_back(std::move(column));
    }
    return decoded;
  };

  SAN_ASSIGN_OR_RAISE(
      auto executor,
      Executor::Make(worker_count, worker_count, worker_count,
                     std::move(worker), std::move(task_arena),
                     sanitize::internal::TaskArenaLane::kUpstream,
                     sanitize::internal::TaskTelemetryKind::kInput));
  std::size_t submitted = 0;
  std::size_t committed = 0;
  const auto commit_next = [&]() -> sanitize::Status {
    SAN_ASSIGN_OR_RAISE(auto outcome, executor->TakeNext());
    if (!outcome.result.ok()) {
      executor->Cancel();
      return outcome.result.status();
    }
    if (outcome.ordinal >= ranges.size()) {
      executor->Cancel();
      return sanitize::Status::Invalid(
          "native Parquet reader: column range commit order mismatch");
    }
    const auto expected = ranges[outcome.ordinal];
    auto decoded = std::move(outcome.result).ValueOrDie();
    if (decoded.columns.size() != expected.last - expected.first) {
      executor->Cancel();
      return sanitize::Status::Invalid(
          "native Parquet reader: column range result size mismatch");
    }
    for (std::size_t offset = 0; offset < decoded.columns.size(); ++offset) {
      auto &column = decoded.columns[offset];
      if (column.index != expected.first + offset) {
        executor->Cancel();
        return sanitize::Status::Invalid(
            "native Parquet reader: column range result order mismatch");
      }
      SAN_RETURN_NOT_OK(commit_native_parquet_decoded_column(std::move(column),
                                                             stream, state));
    }
    ++committed;
    return {};
  };

  while (submitted < ranges.size()) {
    SAN_RETURN_NOT_OK(executor->Submit(typename Executor::Packet{
        .ordinal = submitted, .payload = ranges[submitted]}));
    ++submitted;
  }
  SAN_RETURN_NOT_OK(executor->FinishSubmission());
  while (committed < submitted) {
    SAN_RETURN_NOT_OK(commit_next());
  }
  return {};
}

sanitize::Status materialize_native_row_group_columns_parallel(
    ArrowArrayStream *owner_stream, NativeParquetStreamState *stream,
    const RowGroupInfo &row_group, const NativeFlatRowWindow &window,
    std::size_t worker_count, NativeParquetArrayState *state) {
  if (!owner_stream || !stream || !state || worker_count < 2) {
    return sanitize::Status::Invalid(
        "native Parquet reader: invalid parallel column state");
  }
  auto task_arena = sanitize::internal::task_arena_for_stream(owner_stream);
  if (!task_arena || task_arena->inline_mode()) {
    return sanitize::Status::Invalid(
        "native Parquet reader: parallel column arena is unavailable");
  }
  if (stream->parallel_column_workers.size() < worker_count) {
    stream->parallel_column_workers.resize(worker_count);
  }
  if (native_parquet_grouped_column_decode_eligible(row_group, worker_count)) {
    return materialize_native_row_group_columns_grouped(
        stream, row_group, window, worker_count, state, std::move(task_arena));
  }
  return materialize_native_row_group_columns_individual(
      stream, row_group, window, worker_count, state, std::move(task_arena));
}
