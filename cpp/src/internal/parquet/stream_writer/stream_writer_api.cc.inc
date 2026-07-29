namespace {
// Returns the Arrow stream error string when available.
std::string stream_error_message(ArrowArrayStream *stream,
                                 std::string_view fallback) {
  if (stream && stream->get_last_error) {
    if (const char *message = stream->get_last_error(stream)) {
      if (*message) {
        return std::string(message);
      }
    }
  }
  return std::string(fallback);
}

std::size_t retained_optional_string_bytes(
    const std::optional<std::string> &value) noexcept {
  return value ? value->capacity() : 0;
}

std::size_t retained_page_metadata_bytes(const PageInfo &page) noexcept {
  auto total = retained_optional_string_bytes(page.min_value);
  return saturating_size_add(total,
                             retained_optional_string_bytes(page.max_value));
}

std::size_t
retained_row_group_metadata_bytes(const RowGroupInfo &row_group) noexcept {
  auto total =
      saturating_size_add(sizeof(RowGroupInfo),
                          saturating_size_multiply(row_group.columns.capacity(),
                                                   sizeof(ColumnChunkInfo)));
  for (const auto &column : row_group.columns) {
    total = saturating_size_add(total,
                                retained_page_metadata_bytes(column.aggregate));
    total = saturating_size_add(
        total,
        saturating_size_multiply(column.pages.capacity(), sizeof(PageInfo)));
    for (const auto &page : column.pages) {
      total = saturating_size_add(total, retained_page_metadata_bytes(page));
    }
  }
  return total;
}

bool metadata_budget_would_be_exceeded(std::size_t retained,
                                       std::size_t additional,
                                       std::size_t limit) noexcept {
  return additional > limit || retained > limit - additional;
}
} // namespace

sanitize::Status write_stream(ArrowArrayStream *stream, Output &out_file,
                              const WriterOptions &raw_options) {
  if (!stream) {
    return sanitize::Status::Invalid(
        "native Parquet writer: Arrow C stream is null");
  }

  sanitize::CSchemaGuard schema;
  const int schema_rc = stream->get_schema(stream, schema.get());
  if (schema_rc != 0) {
    return sanitize::Status::IOError(stream_error_message(
        stream, "native Parquet writer: get_schema failed"));
  }
  SAN_ASSIGN_OR_RAISE(auto root, parse_supported_root_schema(schema.value()));
  auto columns = analyze_schema(&root);

  CountingOutput out(out_file);
  SAN_RETURN_NOT_OK(out.Write(kMagic));

  auto task_arena = sanitize::internal::task_arena_for_stream(stream);
  auto telemetry = task_arena ? task_arena->telemetry() : nullptr;
  sanitize::internal::PerformancePhaseScope output_scope(
      telemetry, sanitize::internal::PerformancePhase::kOutput);
  sanitize::internal::PerformanceCompletionScope completion_scope(telemetry);
  const auto options = normalized_writer_options(raw_options);
  const auto budget =
      sanitize::internal::memory_budget_from_limit(options.memory_limit_bytes);
  std::vector<RowGroupInfo> row_groups;
  std::int64_t total_rows = 0;
  std::int64_t accepted_rows = 0;
  std::size_t accepted_row_groups = 0;
  const auto max_rows_per_row_group = budget.parquet_row_group_rows;
  const auto max_row_groups = std::min<std::int64_t>(
      kHardMaxRowGroups,
      std::max<std::int64_t>(1, budget.total_bytes / (64 * 1024)));
  const auto max_footer_metadata_bytes =
      static_cast<std::size_t>(budget.parquet_footer_bytes);
  std::size_t retained_footer_metadata_bytes = 0;
  const auto max_bytes_per_row_group =
      parallel_safe_max_bytes_per_row_group(options, budget, columns);
  const auto target_page_bytes = budget.parquet_page_bytes;
  SAN_ASSIGN_OR_RAISE(const auto compression_codec,
                      compression_codec_from_options(options));
  SAN_ASSIGN_OR_RAISE(const auto gzip_level, gzip_level_from_options(options));

  const auto arena_workers = task_arena ? task_arena->worker_count() : 1U;
  // Small Arrow batches are cheapest to encode serially per row group, but
  // independent row groups can overlap with upstream materialization. Reserve
  // a conservative fraction of the arena for ordered row-group preparation.
  // The fraction continues growing on hosts wider than 32 workers.
  const auto row_group_workers =
      arena_workers >= 4U
          ? std::min(arena_workers,
                     std::max<std::size_t>(2U, arena_workers / 8U))
          : 1U;
  const auto row_group_retained_limit = static_cast<std::size_t>(
      std::max<std::int64_t>(1, budget.total_bytes / 4));
  using RowGroupExecutor =
      sanitize::internal::OrderedExecutor<ParquetRowGroupTask,
                                          PreparedParquetRowGroup>;
  std::unique_ptr<RowGroupExecutor> row_group_executor;
  std::optional<ParquetRowGroupTask> pending_row_group;
  std::deque<std::size_t> in_flight_estimates;
  std::size_t in_flight_estimated_bytes = 0;
  std::size_t row_group_ordinal = 0;

  const auto register_row_group =
      [&](RowGroupInfo row_group) -> sanitize::Status {
    const auto row_group_metadata_bytes =
        retained_row_group_metadata_bytes(row_group);
    if (metadata_budget_would_be_exceeded(retained_footer_metadata_bytes,
                                          row_group_metadata_bytes,
                                          max_footer_metadata_bytes)) {
      return sanitize::Status::Invalid(
          "native Parquet writer: footer metadata exceeds safety limit");
    }
    if (total_rows >
        std::numeric_limits<std::int64_t>::max() - row_group.num_rows) {
      return sanitize::Status::Invalid(
          "native Parquet writer: total row count exceeds int64 range");
    }
    retained_footer_metadata_bytes += row_group_metadata_bytes;
    total_rows += row_group.num_rows;
    row_groups.push_back(std::move(row_group));
    return sanitize::Status::OK();
  };

  const auto write_inline_row_group =
      [&](const ParquetRowGroupTask &task) -> sanitize::Status {
    RowGroupInfo row_group;
    row_group.num_rows = task.row_count;
    row_group.columns.reserve(columns.size());
    SAN_RETURN_NOT_OK(write_row_group_columns(
        out, root, columns, task.owner->value(), task.row_offset,
        task.row_count, compression_codec, gzip_level, target_page_bytes,
        options, budget, task_arena, &row_group));
    SAN_RETURN_NOT_OK(write_row_group_page_indexes(out, columns, &row_group));
    return register_row_group(std::move(row_group));
  };

  const auto commit_next_prepared = [&]() -> sanitize::Status {
    if (!row_group_executor || in_flight_estimates.empty()) {
      return sanitize::Status::Invalid(
          "native Parquet writer: missing asynchronous row group");
    }
    SAN_ASSIGN_OR_RAISE(auto outcome, row_group_executor->TakeNext());
    const auto estimate = in_flight_estimates.front();
    in_flight_estimates.pop_front();
    in_flight_estimated_bytes = estimate <= in_flight_estimated_bytes
                                    ? in_flight_estimated_bytes - estimate
                                    : 0;
    if (!outcome.result.ok()) {
      row_group_executor->Cancel();
      return outcome.result.status();
    }
    auto prepared = std::move(outcome.result).ValueOrDie();
    SAN_RETURN_NOT_OK(commit_prepared_row_group(out, columns, &prepared));
    return register_row_group(std::move(prepared.info));
  };

  const auto finish_parallel_row_groups = [&]() -> sanitize::Status {
    if (!row_group_executor) {
      return sanitize::Status::OK();
    }
    SAN_RETURN_NOT_OK(row_group_executor->FinishSubmission());
    while (row_group_executor->in_flight() > 0) {
      SAN_RETURN_NOT_OK(commit_next_prepared());
    }
    row_group_executor.reset();
    in_flight_estimates.clear();
    in_flight_estimated_bytes = 0;
    return sanitize::Status::OK();
  };

  const auto create_row_group_executor = [&]() -> sanitize::Status {
    if (row_group_executor || row_group_workers <= 1) {
      return sanitize::Status::OK();
    }
    auto worker = [&](ParquetRowGroupTask &&task, std::size_t,
                      sanitize::internal::StopToken stop)
        -> sanitize::Result<PreparedParquetRowGroup> {
      if (stop.stop_requested()) {
        return sanitize::Status::Cancelled(
            "native Parquet writer: row-group preparation cancelled");
      }
      return prepare_serial_row_group(root, columns, std::move(task),
                                      compression_codec, gzip_level,
                                      target_page_bytes, options, budget);
    };
    SAN_ASSIGN_OR_RAISE(
        row_group_executor,
        RowGroupExecutor::Make(row_group_workers, row_group_workers,
                               row_group_workers, std::move(worker), task_arena,
                               sanitize::internal::TaskArenaLane::kOutput,
                               sanitize::internal::TaskTelemetryKind::kOutput));
    return sanitize::Status::OK();
  };

  const auto submit_parallel_row_group =
      [&](ParquetRowGroupTask task) -> sanitize::Status {
    SAN_RETURN_NOT_OK(create_row_group_executor());
    while (
        row_group_executor->in_flight() >=
            row_group_executor->dispatch_window() ||
        (!in_flight_estimates.empty() &&
         task.estimated_retained_bytes >
             row_group_retained_limit - std::min(row_group_retained_limit,
                                                 in_flight_estimated_bytes))) {
      SAN_RETURN_NOT_OK(commit_next_prepared());
    }
    const auto estimate = task.estimated_retained_bytes;
    SAN_RETURN_NOT_OK(
        row_group_executor->Submit(typename RowGroupExecutor::Packet{
            .ordinal = row_group_ordinal++, .payload = std::move(task)}));
    in_flight_estimates.push_back(estimate);
    in_flight_estimated_bytes =
        saturating_size_add(in_flight_estimated_bytes, estimate);
    return sanitize::Status::OK();
  };

  const auto flush_pending_inline = [&]() -> sanitize::Status {
    if (!pending_row_group) {
      return sanitize::Status::OK();
    }
    auto task = std::move(*pending_row_group);
    pending_row_group.reset();
    return write_inline_row_group(task);
  };

  const auto schedule_row_group =
      [&](ParquetRowGroupTask task) -> sanitize::Status {
    const auto inner_workers = parquet_column_worker_count(
        options, budget, columns, columns.size(), task.row_count,
        max_bytes_per_row_group, target_page_bytes);
    const bool eligible =
        row_group_workers > 1 && inner_workers == 1 &&
        task.estimated_retained_bytes > 0 &&
        task.estimated_retained_bytes <= row_group_retained_limit / 2U;
    if (!eligible) {
      SAN_RETURN_NOT_OK(finish_parallel_row_groups());
      SAN_RETURN_NOT_OK(flush_pending_inline());
      return write_inline_row_group(task);
    }
    if (row_group_executor) {
      return submit_parallel_row_group(std::move(task));
    }
    if (!pending_row_group) {
      pending_row_group = std::move(task);
      return sanitize::Status::OK();
    }
    auto first = std::move(*pending_row_group);
    pending_row_group.reset();
    if (first.estimated_retained_bytes >
        row_group_retained_limit - task.estimated_retained_bytes) {
      SAN_RETURN_NOT_OK(write_inline_row_group(first));
      pending_row_group = std::move(task);
      return sanitize::Status::OK();
    }
    SAN_RETURN_NOT_OK(submit_parallel_row_group(std::move(first)));
    return submit_parallel_row_group(std::move(task));
  };

  while (true) {
    std::shared_ptr<sanitize::CArrayGuard> batch;
    try {
      batch = std::make_shared<sanitize::CArrayGuard>();
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "native Parquet writer: batch owner allocation failed");
    }
    const int next_rc = stream->get_next(stream, batch->get());
    if (next_rc != 0) {
      SAN_RETURN_NOT_OK(finish_parallel_row_groups());
      SAN_RETURN_NOT_OK(flush_pending_inline());
      return sanitize::Status::IOError(stream_error_message(
          stream, "native Parquet writer: get_next failed"));
    }
    if (!batch->value().release) {
      break;
    }
    SAN_RETURN_NOT_OK(validate_batch(root, batch->value()));
    if (batch->value().length == 0) {
      continue;
    }
    for (std::int64_t row_offset = 0; row_offset < batch->value().length;) {
      if (accepted_row_groups >= static_cast<std::size_t>(max_row_groups)) {
        return sanitize::Status::Invalid(
            "native Parquet writer: row-group count exceeds safety limit");
      }
      std::size_t estimated_retained_bytes = 0;
      SAN_ASSIGN_OR_RAISE(const auto row_count,
                          determine_bounded_row_group_count(
                              root, columns, batch->value(), row_offset,
                              max_rows_per_row_group, max_bytes_per_row_group,
                              &estimated_retained_bytes));
      if (row_count <= 0) {
        return sanitize::Status::Invalid(
            "native Parquet writer: bounded row-group collector made no "
            "progress");
      }
      if (accepted_rows >
          std::numeric_limits<std::int64_t>::max() - row_count) {
        return sanitize::Status::Invalid(
            "native Parquet writer: total row count exceeds int64 range");
      }
      const auto fixed_row_group_metadata_bytes = saturating_size_add(
          sizeof(RowGroupInfo),
          saturating_size_multiply(columns.size(), sizeof(ColumnChunkInfo)));
      if (metadata_budget_would_be_exceeded(retained_footer_metadata_bytes,
                                            fixed_row_group_metadata_bytes,
                                            max_footer_metadata_bytes)) {
        return sanitize::Status::Invalid(
            "native Parquet writer: footer metadata exceeds safety limit");
      }
      ++accepted_row_groups;
      accepted_rows += row_count;
      SAN_RETURN_NOT_OK(schedule_row_group(ParquetRowGroupTask{
          .owner = batch,
          .row_offset = row_offset,
          .row_count = row_count,
          .estimated_retained_bytes = estimated_retained_bytes,
      }));
      row_offset += row_count;
    }
  }

  SAN_RETURN_NOT_OK(finish_parallel_row_groups());
  SAN_RETURN_NOT_OK(flush_pending_inline());
  if (total_rows != accepted_rows || row_groups.size() != accepted_row_groups) {
    return sanitize::Status::Invalid(
        "native Parquet writer: row-group completion mismatch");
  }

  std::string footer =
      encode_footer(root, columns, row_groups, total_rows, compression_codec);
  if (footer.size() >
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return sanitize::Status::Invalid(
        "native Parquet writer: footer is too large");
  }
  SAN_RETURN_NOT_OK(out.Write(footer));
  std::string trailer;
  append_u32_le(trailer, static_cast<std::uint32_t>(footer.size()));
  trailer.append(kMagic);
  SAN_RETURN_NOT_OK(out.Write(trailer));
  return out.Flush();
}
