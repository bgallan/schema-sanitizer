// Provides portable cooperative cancellation, joining threads, and
// move-only callbacks. Compatibility paths preserve stop-aware waits on
// platforms lacking newer primitives.

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <version>

#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L
#include <stop_token>
#endif

namespace sanitize::internal {

#if defined(SCHEMA_SANITIZER_FORCE_PORTABLE_THREAD_COMPAT) ||                  \
    (defined(_MSC_VER) && defined(__SANITIZE_ADDRESS__))
#define SCHEMA_SANITIZER_PORTABLE_THREAD_COMPAT_ACTIVE 1
#endif

/// Blocks until an atomic value changes, using the platform
/// compatibility implementation.
template <class T>
void WaitOnAtomic(
    const std::atomic<T> &value, T old,
    std::memory_order order = std::memory_order_seq_cst) noexcept {
#if (defined(_MSC_VER) && defined(__SANITIZE_ADDRESS__)) ||                    \
    defined(SCHEMA_SANITIZER_FORCE_ATOMIC_WAIT_POLLING)
  // Windows MSVC ASan CI has observed indefinite stalls in native atomic
  // waits. Keep the same value-based contract while yielding to publishers
  // through a bounded polling interval. Normal Windows builds and every other
  // sanitizer retain the native atomic wait.
  while (value.load(order) == old) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
#else
  value.wait(old, order);
#endif
}

#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L &&              \
    !defined(SCHEMA_SANITIZER_PORTABLE_THREAD_COMPAT_ACTIVE)

using StopSource = std::stop_source;
using StopToken = std::stop_token;
using JThread = std::jthread;

template <class Callback> using StopCallback = std::stop_callback<Callback>;

#else

namespace thread_compat_detail {

class CallbackState {
public:
  /// Creates an enabled callback synchronization state.
  CallbackState() = default;
  /// Disables copying callback synchronization state.
  CallbackState(const CallbackState &) = delete;
  /// Disables copy assignment for callback synchronization state.
  CallbackState &operator=(const CallbackState &) = delete;
  /// Enables polymorphic cleanup of callback implementations.
  virtual ~CallbackState() = default;

  /// Runs the callback once unless it has been disabled or is already active.
  virtual void Invoke() noexcept = 0;
  /// Prevents future invocation and waits for another thread's callback.
  virtual void Disable() noexcept = 0;
};

struct StopState final {
  std::atomic<bool> requested{false};
  std::mutex callbacks_mutex;
  std::vector<std::weak_ptr<CallbackState>> callbacks;
};

template <class Callback> class CallbackStateImpl final : public CallbackState {
public:
  /// Stores the callback invoked for cooperative cancellation.
  explicit CallbackStateImpl(Callback callback)
      : callback_(std::move(callback)) {}

  /// Serializes callback execution and terminates if the callback throws.
  void Invoke() noexcept override {
    {
      std::lock_guard lock(mutex_);
      if (!enabled_ || running_) {
        return;
      }
      running_ = true;
      running_thread_ = std::this_thread::get_id();
    }
    try {
      std::invoke(callback_);
    } catch (...) {
      std::terminate();
    }
    {
      std::lock_guard lock(mutex_);
      running_ = false;
      running_thread_ = {};
    }
    finished_.notify_all();
  }

  /// Disables future calls and waits for any remote invocation to finish.
  void Disable() noexcept override {
    std::unique_lock lock(mutex_);
    enabled_ = false;
    if (running_thread_ == std::this_thread::get_id()) {
      return;
    }
    finished_.wait(lock, [this] { return !running_; });
  }

private:
  Callback callback_;
  std::mutex mutex_;
  std::condition_variable finished_;
  std::thread::id running_thread_;
  bool enabled_ = true;
  bool running_ = false;
};

} // namespace thread_compat_detail

class StopToken final {
public:
  /// Creates a token with no associated stop state.
  StopToken() noexcept = default;

  /// Reports whether the associated source requested cancellation.
  [[nodiscard]] bool stop_requested() const noexcept {
    return state_ && state_->requested.load(std::memory_order_acquire);
  }

  /// Reports whether this token is associated with a stop source.
  [[nodiscard]] bool stop_possible() const noexcept {
    return static_cast<bool>(state_);
  }

private:
  /// Shares the cancellation state owned by a stop source.
  explicit StopToken(
      std::shared_ptr<thread_compat_detail::StopState> state) noexcept
      : state_(std::move(state)) {}

  std::shared_ptr<thread_compat_detail::StopState> state_;

  friend class StopSource;
  template <class Callback> friend class StopCallback;
};

class StopSource final {
public:
  /// Creates a fresh shared cooperative-cancellation state.
  StopSource() : state_(std::make_shared<thread_compat_detail::StopState>()) {}

  /// Returns a token observing this source's cancellation state.
  [[nodiscard]] StopToken get_token() const noexcept {
    return StopToken(state_);
  }

  /// Latches cancellation and invokes each registered callback once.
  bool request_stop() noexcept {
    if (!state_ ||
        state_->requested.exchange(true, std::memory_order_acq_rel)) {
      return false;
    }

    std::unique_lock lock(state_->callbacks_mutex);
    while (!state_->callbacks.empty()) {
      auto callback = state_->callbacks.back().lock();
      state_->callbacks.pop_back();
      lock.unlock();
      if (callback) {
        callback->Invoke();
      }
      lock.lock();
    }
    return true;
  }

private:
  std::shared_ptr<thread_compat_detail::StopState> state_;
};

template <class Callback> class StopCallback final {
public:
  /// Registers a callback or invokes it immediately after prior cancellation.
  StopCallback(const StopToken &token, Callback callback)
      : state_(token.state_),
        callback_(
            std::make_shared<thread_compat_detail::CallbackStateImpl<Callback>>(
                std::move(callback))) {
    if (!state_) {
      return;
    }

    bool invoke_now = false;
    {
      std::lock_guard lock(state_->callbacks_mutex);
      if (state_->requested.load(std::memory_order_acquire)) {
        invoke_now = true;
      } else {
        state_->callbacks.emplace_back(callback_);
      }
    }
    if (invoke_now) {
      callback_->Invoke();
    }
  }

  /// Disables copying a registered stop callback.
  StopCallback(const StopCallback &) = delete;
  /// Disables copy assignment for a registered stop callback.
  StopCallback &operator=(const StopCallback &) = delete;

  /// Disables and unregisters the callback before releasing its state.
  ~StopCallback() {
    if (callback_) {
      callback_->Disable();
    }
    if (state_) {
      std::lock_guard lock(state_->callbacks_mutex);
      std::erase_if(state_->callbacks, [this](const auto &weak_callback) {
        const auto callback = weak_callback.lock();
        return !callback || callback == callback_;
      });
    }
  }

private:
  std::shared_ptr<thread_compat_detail::StopState> state_;
  std::shared_ptr<thread_compat_detail::CallbackState> callback_;
};

class JThread final {
public:
  /// Creates a non-joinable compatibility thread.
  JThread() = default;

  /// Starts a thread that receives this object's cooperative stop token.
  template <class Function>
  explicit JThread(Function &&function)
      : stop_source_(),
        thread_([token = stop_source_.get_token(),
                 function = std::forward<Function>(function)]() mutable {
          std::invoke(function, token);
        }) {}

  /// Disables copying a compatibility joining thread.
  JThread(const JThread &) = delete;
  /// Disables copy assignment for a compatibility joining thread.
  JThread &operator=(const JThread &) = delete;

  /// Transfers the stop source and native thread from another owner.
  JThread(JThread &&other) noexcept
      : stop_source_(std::move(other.stop_source_)),
        thread_(std::move(other.thread_)) {}

  /// Stops and joins current work before adopting another thread.
  JThread &operator=(JThread &&other) noexcept {
    if (this != &other) {
      StopAndJoin();
      stop_source_ = std::move(other.stop_source_);
      thread_ = std::move(other.thread_);
    }
    return *this;
  }

  /// Requests cancellation and joins any owned native thread.
  ~JThread() { StopAndJoin(); }

  /// Requests cooperative cancellation of the owned thread.
  bool request_stop() noexcept { return stop_source_.request_stop(); }

  /// Reports whether this object owns a joinable native thread.
  [[nodiscard]] bool joinable() const noexcept { return thread_.joinable(); }

  /// Blocks until the owned native thread completes.
  void join() { thread_.join(); }

private:
  /// Requests cancellation and joins, terminating if joining fails.
  void StopAndJoin() noexcept {
    if (!thread_.joinable()) {
      return;
    }
    stop_source_.request_stop();
    try {
      thread_.join();
    } catch (...) {
      std::terminate();
    }
  }

  StopSource stop_source_;
  std::thread thread_;
};

#endif

/// Waits for a predicate while honoring cooperative
/// cancellation notifications.
template <class ConditionVariable, class Lock, class Predicate>
bool WaitWithStop(ConditionVariable &condition, Lock &lock,
                  const StopToken &stop, Predicate predicate) {
#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L &&              \
    !defined(SCHEMA_SANITIZER_PORTABLE_THREAD_COMPAT_ACTIVE)
  return condition.wait(lock, stop, std::move(predicate));
#else
  auto wake_waiter = [&condition] { condition.notify_all(); };
  StopCallback<decltype(wake_waiter)> stop_callback(stop,
                                                    std::move(wake_waiter));
  condition.wait(lock, [&] { return stop.stop_requested() || predicate(); });
  return predicate();
#endif
}

#if defined(__cpp_lib_move_only_function) &&                                   \
    __cpp_lib_move_only_function >= 202110L &&                                 \
    !defined(SCHEMA_SANITIZER_PORTABLE_THREAD_COMPAT_ACTIVE)

template <class Signature>
using MoveOnlyFunction = std::move_only_function<Signature>;

#else

template <class Signature> class MoveOnlyFunction;

template <class Result, class... Args>
class MoveOnlyFunction<Result(Args...)> final {
public:
  /// Creates an empty move-only callable wrapper.
  MoveOnlyFunction() noexcept = default;

  /// Type-erases an invocable compatible with the requested signature.
  template <class Function>
    requires(!std::is_same_v<std::remove_cvref_t<Function>, MoveOnlyFunction> &&
             std::is_invocable_r_v<Result, Function &, Args...>)
  MoveOnlyFunction(Function &&function)
      : function_(std::make_unique<Model<std::remove_cvref_t<Function>>>(
            std::forward<Function>(function))) {}

  /// Disables copying a move-only callable wrapper.
  MoveOnlyFunction(const MoveOnlyFunction &) = delete;
  /// Disables copy assignment for a move-only callable wrapper.
  MoveOnlyFunction &operator=(const MoveOnlyFunction &) = delete;
  /// Transfers the stored callable from another wrapper.
  MoveOnlyFunction(MoveOnlyFunction &&) noexcept = default;
  /// Transfers callable ownership from another wrapper.
  MoveOnlyFunction &operator=(MoveOnlyFunction &&) noexcept = default;

  /// Reports whether the wrapper currently contains a callable.
  explicit operator bool() const noexcept {
    return static_cast<bool>(function_);
  }

  /// Invokes the stored callable with forwarded arguments.
  Result operator()(Args... args) {
    return function_->Invoke(std::forward<Args>(args)...);
  }

private:
  class Interface {
  public:
    /// Enables polymorphic destruction of type-erased callable models.
    virtual ~Interface() = default;
    /// Invokes the erased callable with forwarded signature arguments.
    virtual Result Invoke(Args... args) = 0;
  };

  template <class Function> class Model final : public Interface {
  public:
    /// Stores one concrete callable in the type-erased model.
    template <class Forwarded>
    explicit Model(Forwarded &&function)
        : function_(std::forward<Forwarded>(function)) {}

    /// Invokes the stored concrete callable with forwarded arguments.
    Result Invoke(Args... args) override {
      return std::invoke(function_, std::forward<Args>(args)...);
    }

  private:
    Function function_;
  };

  std::unique_ptr<Interface> function_;
};

#endif

#if defined(SCHEMA_SANITIZER_PORTABLE_THREAD_COMPAT_ACTIVE)
#undef SCHEMA_SANITIZER_PORTABLE_THREAD_COMPAT_ACTIVE
#endif

} // namespace sanitize::internal
