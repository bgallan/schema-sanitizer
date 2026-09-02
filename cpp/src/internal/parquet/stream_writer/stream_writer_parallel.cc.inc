// Prepares Parquet column slices concurrently and commits encoded bytes in
// physical order. This fragment is included by the consolidated writer to
// isolate format-specific implementation details.

class StringParquetOutput final : public Output {
public:
  /// Wraps a string as a Parquet output target for worker-local preparation.
  explicit StringParquetOutput(std::string *target) : target_(target) {}

  /// Appends bytes to the in-memory Parquet staging target and reports
  /// allocation failures.
  sanitize::Status Write(std::string_view data) override {
    if (!target_) {
      return sanitize::Status::Invalid(
          "native Parquet writer: null in-memory output");
    }
    try {
      target_->append(data);
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "native Parquet writer: column artifact allocation failed");
    }
    return sanitize::Status::OK();
  }

  /// Writes the buffered Parquet bytes to the wrapped output and resets the
  /// staging buffer.
  sanitize::Status Flush() override { return sanitize::Status::OK(); }

private:
  std::string *target_ = nullptr;
};

struct PreparedColumnChunk {
  std::string bytes;
  ColumnChunkInfo info;
};

struct ParquetColumnTask {
  std::size_t column_index = 0;
  std::int64_t row_offset = 0;
  std::int64_t row_count = 0;
  bool allow_dictionary = true;
  bool final_slice = true;
};

/// Adds page base offset with checked offsets, counts, and retained-capacity
/// accounting.
void add_page_base_offset(PageInfo *page, std::int64_t base) noexcept {
  if (!page) {
    return;
  }
  page->first_page_offset += base;
  page->offset += base;
  if (page->dictionary_page_offset >= 0) {
    page->dictionary_page_offset += base;
  }
}

/// Reports whether one leaf column can be encoded as independent parallel page
/// slices.
bool parquet_column_can_slice(const LeafColumn &column) noexcept {
  if (!column.node ||
      column.node->arrow_kind == jsonl::JsonlKind::kDictionary) {
    return false;
  }
  const auto kind = leaf_value_kind(column);
  if (is_byte_array_kind(kind)) {
    return false;
  }
  return column.max_repetition_level > 0 || column.max_definition_level > 2;
}

/// Returns the row capacity assigned to each independently prepared column
/// slice.
std::size_t parquet_column_slice_capacity(const LeafColumn &column,
                                          std::int64_t row_count) noexcept {
  constexpr std::int64_t kRowsPerSlice = 4096;
  if (!parquet_column_can_slice(column) || row_count < 2 * kRowsPerSlice) {
    return 1;
  }
  return static_cast<std::size_t>(
      std::clamp<std::int64_t>(row_count / kRowsPerSlice, 1, 4));
}

/// Derives rows per parallel writer unit from row-group bounds and worker
/// count.
std::size_t
parquet_parallel_unit_capacity(const std::vector<LeafColumn> &columns,
                               std::int64_t row_count) noexcept {
  auto units = columns.size();
  for (const auto &column : columns) {
    units = saturating_size_add(
        units, parquet_column_slice_capacity(column, row_count) - 1U);
  }
  return units;
}

/// Returns stable slice counts whose aggregate work fits writer parallelism and
/// memory.
std::vector<std::size_t>
parquet_column_slice_counts(const std::vector<LeafColumn> &columns,
                            std::int64_t row_count, std::size_t worker_count) {
  std::vector<std::size_t> slices(columns.size(), 1U);
  const auto target_tasks = std::max(columns.size(), worker_count * 2U);
  if (columns.size() >= target_tasks) {
    return slices;
  }
  struct Candidate {
    std::size_t index = 0;
    std::size_t capacity = 1;
    int weight = 0;
  };
  std::vector<Candidate> candidates;
  candidates.reserve(columns.size());
  for (std::size_t index = 0; index < columns.size(); ++index) {
    const auto capacity =
        parquet_column_slice_capacity(columns[index], row_count);
    if (capacity <= 1) {
      continue;
    }
    candidates.push_back(Candidate{
        .index = index,
        .capacity = capacity,
        .weight = static_cast<int>(columns[index].max_repetition_level) * 4 +
                  static_cast<int>(columns[index].max_definition_level),
    });
  }
  std::ranges::sort(candidates,
                    [](const Candidate &left, const Candidate &right) {
                      if (left.weight != right.weight) {
                        return left.weight > right.weight;
                      }
                      return left.index < right.index;
                    });
  auto tasks = columns.size();
  while (tasks < target_tasks) {
    bool advanced = false;
    for (const auto &candidate : candidates) {
      if (tasks >= target_tasks) {
        break;
      }
      auto &count = slices[candidate.index];
      if (count >= candidate.capacity) {
        continue;
      }
      ++count;
      ++tasks;
      advanced = true;
    }
    if (!advanced) {
      break;
    }
  }
  return slices;
}

template <class T>
/// Orders fixed-width encoded values according to their Parquet logical
/// statistics semantics.
bool parquet_encoded_fixed_less(std::string_view left,
                                std::string_view right) noexcept {
  if (left.size() != sizeof(T) || right.size() != sizeof(T)) {
    return byte_array_less(left, right);
  }
  T left_value{};
  T right_value{};
  std::memcpy(&left_value, left.data(), sizeof(T));
  std::memcpy(&right_value, right.data(), sizeof(T));
  if constexpr (std::is_floating_point_v<T>) {
    return floating_less_for_stats(left_value, right_value);
  }
  return left_value < right_value;
}

/// Orders encoded Parquet statistics according to the leaf's physical and
/// logical type.
bool parquet_encoded_stat_less(const LeafColumn &column, std::string_view left,
                               std::string_view right) noexcept {
  const auto kind = leaf_value_kind(column);
  switch (kind) {
  case jsonl::JsonlKind::kBool:
    return !left.empty() && !right.empty() && left.front() < right.front();
  case jsonl::JsonlKind::kInt8:
  case jsonl::JsonlKind::kInt16:
  case jsonl::JsonlKind::kInt32:
  case jsonl::JsonlKind::kDate32:
  case jsonl::JsonlKind::kTime32s:
    return parquet_encoded_fixed_less<std::int32_t>(left, right);
  case jsonl::JsonlKind::kUInt8:
  case jsonl::JsonlKind::kUInt16:
  case jsonl::JsonlKind::kUInt32:
    return parquet_encoded_fixed_less<std::uint32_t>(left, right);
  case jsonl::JsonlKind::kInt64:
  case jsonl::JsonlKind::kTimestampMillis:
  case jsonl::JsonlKind::kTimestampMicros:
  case jsonl::JsonlKind::kTimestampNanos:
    return parquet_encoded_fixed_less<std::int64_t>(left, right);
  case jsonl::JsonlKind::kUInt64:
    return parquet_encoded_fixed_less<std::uint64_t>(left, right);
  case jsonl::JsonlKind::kFloat32:
    return parquet_encoded_fixed_less<float>(left, right);
  case jsonl::JsonlKind::kFloat64:
    return parquet_encoded_fixed_less<double>(left, right);
  case jsonl::JsonlKind::kDecimal:
    return !left.empty() && left.size() == right.size()
               ? signed_big_endian_bytes_less(left, right)
               : byte_array_less(left, right);
  default:
    return byte_array_less(left, right);
  }
}

/// Expands row-group statistics with one page's encoded minimum and maximum.
void merge_column_bounds(const LeafColumn &column, const PageInfo &source,
                         std::optional<std::string> *minimum,
                         std::optional<std::string> *maximum) {
  if (!minimum || !maximum || !source.min_value || !source.max_value) {
    return;
  }
  if (!*minimum ||
      parquet_encoded_stat_less(column, *source.min_value, **minimum)) {
    *minimum = *source.min_value;
  }
  if (!*maximum ||
      parquet_encoded_stat_less(column, **maximum, *source.max_value)) {
    *maximum = *source.max_value;
  }
}

/// Commits prepared slice only after preparation succeeds, preserving physical
/// output order.
sanitize::Status commit_prepared_slice(CountingOutput &out,
                                       PreparedColumnChunk *prepared,
                                       ColumnChunkInfo *column,
                                       std::optional<std::string> *minimum,
                                       std::optional<std::string> *maximum,
                                       const LeafColumn &leaf) {
  if (!prepared || !column) {
    return sanitize::Status::Invalid(
        "native Parquet writer: null prepared column commit");
  }
  merge_column_bounds(leaf, prepared->info.aggregate, minimum, maximum);
  const auto base = out.offset();
  for (auto &page : prepared->info.pages) {
    add_page_base_offset(&page, base);
    merge_page_info(column->aggregate, page);
    column->pages.push_back(std::move(page));
  }
  SAN_RETURN_NOT_OK(out.Write(prepared->bytes));
  std::string().swap(prepared->bytes);
  return sanitize::Status::OK();
}

/// Selects bounded column-worker parallelism from operation policy and
/// available work.
std::size_t
parquet_column_worker_count(const WriterOptions &options,
                            const sanitize::internal::MemoryBudget &budget,
                            const std::vector<LeafColumn> &columns,
                            std::size_t column_count, std::int64_t row_count,
                            std::int64_t row_group_target_bytes,
                            std::int64_t target_page_bytes) noexcept {
  const auto has_nested_levels = parquet_columns_have_nested_levels(columns);
  const auto minimum_rows = has_nested_levels   ? 16384
                            : column_count <= 4 ? 65536
                            : column_count <= 8 ? 16384
                                                : 4096;
  if (options.threading_mode == sanitize::ThreadingMode::kSingle ||
      column_count < 2 || row_count < minimum_rows ||
      budget.total_bytes < 96LL * 1024LL * 1024LL) {
    return 1;
  }
  const auto policy = sanitize::internal::execution_policy_from(
      options.threading_mode, options.memory_limit_bytes);
  if (policy.effective_workers <= 1) {
    return 1;
  }

  const auto retained_pair = std::min<std::int64_t>(
      budget.total_bytes,
      std::max<std::int64_t>(1, row_group_target_bytes) * 2);
  const auto candidate_pool =
      std::max<std::int64_t>(1, budget.total_bytes - retained_pair);
  const auto per_worker_candidate = std::max<std::int64_t>(
      1024LL * 1024LL,
      std::min<std::int64_t>(target_page_bytes, 16LL * 1024LL * 1024LL) * 3);
  const auto memory_workers =
      std::max<std::int64_t>(1, candidate_pool / per_worker_candidate);
  const auto rows_per_worker = has_nested_levels ? 4096 : minimum_rows;
  const auto row_workers =
      1 + (std::max<std::int64_t>(1, row_count) - 1) / rows_per_worker;
  const auto parallel_units =
      parquet_parallel_unit_capacity(columns, row_count);
  return static_cast<std::size_t>(std::max<std::int64_t>(
      1, std::min<std::int64_t>({policy.effective_workers,
                                 static_cast<std::int64_t>(parallel_units),
                                 memory_workers, row_workers})));
}

/// Writes row group columns in physical Parquet order after all offsets and
/// metadata sizes are validated.
sanitize::Status write_row_group_columns(
    CountingOutput &out, const ParquetNode &root,
    const std::vector<LeafColumn> &columns, const ArrowArray &batch,
    std::int64_t row_offset, std::int64_t row_count,
    CompressionCodec compression_codec, int gzip_level,
    std::int64_t target_page_bytes, const WriterOptions &options,
    const sanitize::internal::MemoryBudget &budget,
    std::shared_ptr<sanitize::internal::OperationTaskArena> task_arena,
    RowGroupInfo *row_group) {
  if (!row_group) {
    return sanitize::Status::Invalid("native Parquet writer: null row group");
  }
  const auto row_group_target_bytes =
      parallel_safe_max_bytes_per_row_group(options, budget, columns);
  const auto worker_count = parquet_column_worker_count(
      options, budget, columns, columns.size(), row_count,
      row_group_target_bytes, target_page_bytes);
  if (worker_count == 1) {
    for (std::size_t i = 0; i < columns.size(); ++i) {
      SAN_ASSIGN_OR_RAISE(auto page_data,
                          collect_column_page_data(root, columns[i], batch,
                                                   row_offset, row_count, i));
      SAN_ASSIGN_OR_RAISE(auto page,
                          write_column_pages(out, columns[i], page_data,
                                             compression_codec, gzip_level,
                                             target_page_bytes));
      row_group->columns.push_back(std::move(page));
    }
    return sanitize::Status::OK();
  }

  const auto slice_counts =
      parquet_column_slice_counts(columns, row_count, worker_count);
  std::vector<ParquetColumnTask> tasks;
  tasks.reserve(std::accumulate(slice_counts.begin(), slice_counts.end(),
                                std::size_t{0}));
  for (std::size_t column = 0; column < columns.size(); ++column) {
    const auto slices = slice_counts[column];
    for (std::size_t slice = 0; slice < slices; ++slice) {
      const auto begin = static_cast<std::int64_t>(
          (static_cast<std::uint64_t>(row_count) * slice) / slices);
      const auto end = static_cast<std::int64_t>(
          (static_cast<std::uint64_t>(row_count) * (slice + 1U)) / slices);
      tasks.push_back(ParquetColumnTask{
          .column_index = column,
          .row_offset = row_offset + begin,
          .row_count = end - begin,
          .allow_dictionary = slices == 1,
          .final_slice = slice + 1U == slices,
      });
    }
  }

  using Executor = sanitize::internal::OrderedExecutor<ParquetColumnTask,
                                                       PreparedColumnChunk>;
  auto worker = [&](ParquetColumnTask &&task, std::size_t,
                    sanitize::internal::StopToken stop)
      -> sanitize::Result<PreparedColumnChunk> {
    if (stop.stop_requested()) {
      return sanitize::Status::Cancelled(
          "native Parquet writer: column preparation cancelled");
    }
    SAN_ASSIGN_OR_RAISE(auto page_data, collect_column_page_data(
                                            root, columns[task.column_index],
                                            batch, task.row_offset,
                                            task.row_count, task.column_index));
    PreparedColumnChunk prepared;
    StringParquetOutput memory_output(&prepared.bytes);
    CountingOutput counted(memory_output);
    SAN_ASSIGN_OR_RAISE(prepared.info,
                        write_column_pages(counted, columns[task.column_index],
                                           page_data, compression_codec,
                                           gzip_level, target_page_bytes,
                                           task.allow_dictionary));
    return prepared;
  };

  auto output_lane = sanitize::internal::TaskArenaLane::kOutput;
  if (task_arena && task_arena->worker_count() > worker_count) {
    const auto has_nested_levels = parquet_columns_have_nested_levels(columns);
    const auto dedicated_rows = static_cast<std::int64_t>(std::min<std::size_t>(
        static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()),
        std::max<std::size_t>(1, worker_count) * 16384U));
    if (has_nested_levels || row_count < dedicated_rows) {
      output_lane = sanitize::internal::TaskArenaLane::kOutputCompact;
    }
  }
  SAN_ASSIGN_OR_RAISE(
      auto executor,
      Executor::Make(worker_count, std::max<std::size_t>(1, worker_count * 2),
                     worker_count, std::move(worker), std::move(task_arena),
                     output_lane,
                     sanitize::internal::TaskTelemetryKind::kOutput));

  std::size_t submitted = 0;
  std::size_t committed = 0;
  std::size_t current_column = 0;
  ColumnChunkInfo current_chunk;
  std::optional<std::string> current_minimum;
  std::optional<std::string> current_maximum;
  auto take_and_commit = [&]() -> sanitize::Status {
    SAN_ASSIGN_OR_RAISE(auto outcome, executor->TakeNext());
    if (!outcome.result.ok()) {
      executor->Cancel();
      return outcome.result.status();
    }
    const auto &task = tasks[outcome.ordinal];
    if (task.column_index != current_column) {
      executor->Cancel();
      return sanitize::Status::Invalid(
          "native Parquet writer: column slice commit order mismatch");
    }
    auto prepared = std::move(outcome.result).ValueOrDie();
    SAN_RETURN_NOT_OK(commit_prepared_slice(out, &prepared, &current_chunk,
                                            &current_minimum, &current_maximum,
                                            columns[current_column]));
    if (task.final_slice) {
      current_chunk.aggregate.min_value = std::move(current_minimum);
      current_chunk.aggregate.max_value = std::move(current_maximum);
      row_group->columns.push_back(std::move(current_chunk));
      current_chunk = ColumnChunkInfo{};
      current_minimum.reset();
      current_maximum.reset();
      ++current_column;
    }
    ++committed;
    return sanitize::Status::OK();
  };

  while (submitted < tasks.size()) {
    if (executor->in_flight() >= executor->dispatch_window()) {
      SAN_RETURN_NOT_OK(take_and_commit());
    }
    SAN_RETURN_NOT_OK(executor->Submit(typename Executor::Packet{
        .ordinal = submitted, .payload = tasks[submitted]}));
    ++submitted;
  }
  SAN_RETURN_NOT_OK(executor->FinishSubmission());
  while (committed < submitted) {
    SAN_RETURN_NOT_OK(take_and_commit());
  }
  return sanitize::Status::OK();
}
