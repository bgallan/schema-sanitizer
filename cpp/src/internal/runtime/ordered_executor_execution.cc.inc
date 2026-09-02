// Executes one ordered packet under cooperative cancellation. Exceptions
// become status-bearing outcomes without disrupting the packet's
// ordinal position.

/// Executes one packet and converts cancellation or exceptions into its
/// outcome.
Outcome execute_packet(Packet &&packet, std::size_t worker_index,
                       sanitize::internal::StopToken stop) noexcept {
  const auto ordinal = packet.ordinal;
  try {
    if (stop.stop_requested()) {
      return Outcome{.ordinal = ordinal,
                     .result = sanitize::Status::Cancelled(
                         "ordered worker stop requested")};
    }
    return Outcome{.ordinal = ordinal,
                   .result =
                       worker_(std::move(packet.payload), worker_index, stop)};
  } catch (const std::bad_alloc &) {
    return Outcome{.ordinal = ordinal,
                   .result = sanitize::Status::OutOfMemory(
                       "ordered worker allocation failed")};
  } catch (const std::exception &error) {
    return Outcome{.ordinal = ordinal,
                   .result = sanitize::Status::Invalid(
                       "ordered worker raised: ", error.what())};
  } catch (...) {
    return Outcome{.ordinal = ordinal,
                   .result = sanitize::Status::Invalid(
                       "ordered worker raised an unknown exception")};
  }
}
