// Implements dedicated ordered-executor worker startup and shutdown.
// Physical thread permits, stop requests, and joins follow
// process-wide governance.

/// Starts governed dedicated workers unless execution is inline or
/// arena-backed.
sanitize::Status start_workers() {
  if (inline_mode() || arena_) {
    return sanitize::Status::OK();
  }
  try {
    workers_.reserve(worker_count_);
    for (std::size_t index = 0; index < worker_count_; ++index) {
      ProcessPhysicalThreadPermitLease permit(1U);
      if (!permit) {
        throw std::system_error(
            std::make_error_code(std::errc::resource_unavailable_try_again),
            "native physical thread capacity exhausted");
      }
      workers_.emplace_back([this, index, permit = std::move(permit)](
                                sanitize::internal::StopToken stop) mutable {
        mark_process_physical_thread_running();
        struct PhysicalThreadRunningGuard final {
          /// Records that this governed physical worker has stopped.
          ~PhysicalThreadRunningGuard() noexcept {
            mark_process_physical_thread_stopped();
          }
        } running_guard;
        worker_loop(index, stop);
        // permit retires after the running marker, including exception-free
        // early exits. If thread construction fails, the closure destructor
        // retires it before emplace_back propagates the error.
      });
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
