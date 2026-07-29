// Portable cooperative cancellation, joining threads, and move-only callbacks.
#pragma once

#include <algorithm>
#include <atomic>
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

#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L &&              \
    !defined(SCHEMA_SANITIZER_FORCE_PORTABLE_THREAD_COMPAT)

using StopSource = std::stop_source;
using StopToken = std::stop_token;
using JThread = std::jthread;

template <class Callback> using StopCallback = std::stop_callback<Callback>;

#else

namespace thread_compat_detail {

class CallbackState {
public:
  CallbackState() = default;
  CallbackState(const CallbackState &) = delete;
  CallbackState &operator=(const CallbackState &) = delete;
  virtual ~CallbackState() = default;

  virtual void Invoke() noexcept = 0;
  virtual void Disable() noexcept = 0;
};

struct StopState final {
  std::atomic<bool> requested{false};
  std::mutex callbacks_mutex;
  std::vector<std::weak_ptr<CallbackState>> callbacks;
};

template <class Callback> class CallbackStateImpl final : public CallbackState {
public:
  explicit CallbackStateImpl(Callback callback)
      : callback_(std::move(callback)) {}

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
  StopToken() noexcept = default;

  [[nodiscard]] bool stop_requested() const noexcept {
    return state_ && state_->requested.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool stop_possible() const noexcept {
    return static_cast<bool>(state_);
  }

private:
  explicit StopToken(
      std::shared_ptr<thread_compat_detail::StopState> state) noexcept
      : state_(std::move(state)) {}

  std::shared_ptr<thread_compat_detail::StopState> state_;

  friend class StopSource;
  template <class Callback> friend class StopCallback;
};

class StopSource final {
public:
  StopSource() : state_(std::make_shared<thread_compat_detail::StopState>()) {}

  [[nodiscard]] StopToken get_token() const noexcept {
    return StopToken(state_);
  }

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

  StopCallback(const StopCallback &) = delete;
  StopCallback &operator=(const StopCallback &) = delete;

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
  JThread() = default;

  template <class Function>
  explicit JThread(Function &&function)
      : stop_source_(),
        thread_([token = stop_source_.get_token(),
                 function = std::forward<Function>(function)]() mutable {
          std::invoke(function, token);
        }) {}

  JThread(const JThread &) = delete;
  JThread &operator=(const JThread &) = delete;

  JThread(JThread &&other) noexcept
      : stop_source_(std::move(other.stop_source_)),
        thread_(std::move(other.thread_)) {}

  JThread &operator=(JThread &&other) noexcept {
    if (this != &other) {
      StopAndJoin();
      stop_source_ = std::move(other.stop_source_);
      thread_ = std::move(other.thread_);
    }
    return *this;
  }

  ~JThread() { StopAndJoin(); }

  bool request_stop() noexcept { return stop_source_.request_stop(); }

  [[nodiscard]] bool joinable() const noexcept { return thread_.joinable(); }

  void join() { thread_.join(); }

private:
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

template <class ConditionVariable, class Lock, class Predicate>
bool WaitWithStop(ConditionVariable &condition, Lock &lock,
                  const StopToken &stop, Predicate predicate) {
#if defined(__cpp_lib_jthread) && __cpp_lib_jthread >= 201911L &&              \
    !defined(SCHEMA_SANITIZER_FORCE_PORTABLE_THREAD_COMPAT)
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
    !defined(SCHEMA_SANITIZER_FORCE_PORTABLE_THREAD_COMPAT)

template <class Signature>
using MoveOnlyFunction = std::move_only_function<Signature>;

#else

template <class Signature> class MoveOnlyFunction;

template <class Result, class... Args>
class MoveOnlyFunction<Result(Args...)> final {
public:
  MoveOnlyFunction() noexcept = default;

  template <class Function>
    requires(!std::is_same_v<std::remove_cvref_t<Function>, MoveOnlyFunction> &&
             std::is_invocable_r_v<Result, Function &, Args...>)
  MoveOnlyFunction(Function &&function)
      : function_(std::make_unique<Model<std::remove_cvref_t<Function>>>(
            std::forward<Function>(function))) {}

  MoveOnlyFunction(const MoveOnlyFunction &) = delete;
  MoveOnlyFunction &operator=(const MoveOnlyFunction &) = delete;
  MoveOnlyFunction(MoveOnlyFunction &&) noexcept = default;
  MoveOnlyFunction &operator=(MoveOnlyFunction &&) noexcept = default;

  explicit operator bool() const noexcept {
    return static_cast<bool>(function_);
  }

  Result operator()(Args... args) {
    return function_->Invoke(std::forward<Args>(args)...);
  }

private:
  class Interface {
  public:
    virtual ~Interface() = default;
    virtual Result Invoke(Args... args) = 0;
  };

  template <class Function> class Model final : public Interface {
  public:
    template <class Forwarded>
    explicit Model(Forwarded &&function)
        : function_(std::forward<Forwarded>(function)) {}

    Result Invoke(Args... args) override {
      return std::invoke(function_, std::forward<Args>(args)...);
    }

  private:
    Function function_;
  };

  std::unique_ptr<Interface> function_;
};

#endif

} // namespace sanitize::internal
