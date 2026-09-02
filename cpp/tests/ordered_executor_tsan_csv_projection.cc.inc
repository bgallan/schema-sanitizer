// Exercises immutable per-source CSV projections under concurrent switching.
// Deterministic checksums validate header selection and column metadata while
// ordered workers repeatedly alternate among independent source layouts.

/// Computes the deterministic projection checksum for one task ordinal.
std::uint64_t csv_projection_checksum(
    const sanitize::internal::CsvSourceProjectionSet &set,
    std::uint64_t ordinal) {
  const std::size_t source_index =
      static_cast<std::size_t>((ordinal * 17U) % set.projections.size());
  const auto &projection = set.projections[source_index];
  std::uint64_t checksum = static_cast<std::uint64_t>(projection.source_index);
  for (std::size_t column = 0; column < projection.column_keys.size(); ++column) {
    checksum ^= projection.column_hashes[column] +
                static_cast<std::uint64_t>(projection.column_keys[column].size()) +
                static_cast<std::uint64_t>(column * 131U);
  }
  return checksum;
}

/// Verifies concurrent source switching preserves immutable CSV projections.
bool run_csv_projection_switch_round() {
  auto projections =
      std::make_shared<sanitize::internal::CsvSourceProjectionSet>();
  constexpr std::size_t source_count = 32U;
  constexpr std::size_t columns_per_source = 12U;
  projections->headers.reserve(source_count);
  projections->projections.reserve(source_count);
  for (std::size_t source = 0; source < source_count; ++source) {
    sanitize::internal::CsvSourceHeader header;
    sanitize::internal::CsvSourceProjection projection;
    header.source_index = source;
    projection.source_index = source;
    for (std::size_t column = 0; column < columns_per_source; ++column) {
      const std::string key = "source_" + std::to_string(source) + "_column_" +
                              std::to_string(column);
      header.fields.push_back(key);
      projection.column_keys.push_back(key);
      projection.column_hashes.push_back(
          static_cast<std::uint64_t>((source + 1U) * 1'000'003U + column));
    }
    projections->headers.push_back(std::move(header));
    projections->projections.push_back(std::move(projection));
  }
  projections->max_columns = columns_per_source;
  sanitize::internal::CsvSourceProjectionSetPtr immutable = projections;

  auto made = Executor::Make(
      8, 32, 32,
      [immutable](std::uint64_t &&ordinal, std::size_t,
                  sanitize::internal::StopToken stop)
          -> sanitize::Result<std::uint64_t> {
        if (stop.stop_requested()) {
          return sanitize::Status::Cancelled(
              "CSV projection switch probe cancelled");
        }
        return csv_projection_checksum(*immutable, ordinal);
      });
  if (!made.ok()) {
    return false;
  }
  auto executor = std::move(made).ValueOrDie();
  constexpr std::uint64_t task_count = 1'024U;
  std::uint64_t submitted = 0U;
  std::uint64_t consumed = 0U;
  while (submitted < task_count) {
    while (executor->in_flight() >= executor->dispatch_window()) {
      auto next = executor->TakeNext();
      if (!next.ok()) {
        return false;
      }
      auto outcome = std::move(next).ValueOrDie();
      if (!outcome.result.ok() || outcome.ordinal != consumed ||
          std::move(outcome.result).ValueOrDie() !=
              csv_projection_checksum(*immutable, consumed)) {
        return false;
      }
      ++consumed;
    }
    if (!executor->Submit({submitted, submitted}).ok()) {
      return false;
    }
    ++submitted;
  }
  if (!executor->FinishSubmission().ok()) {
    return false;
  }
  while (consumed < task_count) {
    auto next = executor->TakeNext();
    if (!next.ok()) {
      return false;
    }
    auto outcome = std::move(next).ValueOrDie();
    if (!outcome.result.ok() || outcome.ordinal != consumed ||
        std::move(outcome.result).ValueOrDie() !=
            csv_projection_checksum(*immutable, consumed)) {
      return false;
    }
    ++consumed;
  }
  return true;
}
