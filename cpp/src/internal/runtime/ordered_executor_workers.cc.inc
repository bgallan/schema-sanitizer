sanitize::Status start_workers() {
  if (inline_mode() || arena_) {
    return sanitize::Status::OK();
  }
  try {
    workers_.reserve(worker_count_);
    for (std::size_t index = 0; index < worker_count_; ++index) {
      workers_.emplace_back(
          [this, index](std::stop_token stop) { worker_loop(index, stop); });
    }
  } catch (const std::bad_alloc &) {
    Cancel();
    workers_.clear();
    return sanitize::Status::OutOfMemory(
        "OrderedExecutor::Make: worker-pool allocation failed");
  } catch (const std::system_error &error) {
    Cancel();
    workers_.clear();
    return sanitize::Status::Invalid(
        "OrderedExecutor::Make: unable to create worker thread: ",
        error.what());
  } catch (const std::exception &error) {
    Cancel();
    workers_.clear();
    return sanitize::Status::Invalid(
        "OrderedExecutor::Make: worker-pool startup failed: ", error.what());
  } catch (...) {
    Cancel();
    workers_.clear();
    return sanitize::Status::Invalid(
        "OrderedExecutor::Make: worker-pool startup failed");
  }
  return sanitize::Status::OK();
}
