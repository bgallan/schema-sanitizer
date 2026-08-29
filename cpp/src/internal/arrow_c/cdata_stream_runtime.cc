// Implements Arrow C stream runtime sidecars without changing the Arrow ABI.
// The implementation preserves Arrow ownership and error contracts without
// depending on the Arrow C++ library.

#include "internal/arrow_c/cdata_stream_runtime.hh"

#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/process_identity.hh"

#include <mutex>
#include <unordered_map>
#include <utility>

namespace sanitize::internal {
namespace {

/// Returns the mutex serializing access to the Arrow stream sidecar registry.
std::mutex &registry_mutex() {
  static std::mutex value;
  return value;
}

/// Returns the process-wide map from Arrow streams to operation task arenas.
std::unordered_map<const ArrowArrayStream *,
                   std::weak_ptr<OperationTaskArena>> &
registry() {
  static std::unordered_map<const ArrowArrayStream *,
                            std::weak_ptr<OperationTaskArena>>
      value;
  return value;
}

} // namespace

/// Associates an Arrow stream with its operation task arena for callback-time
/// lookup.
void attach_task_arena(ArrowArrayStream *stream,
                       std::shared_ptr<OperationTaskArena> arena) {
  if (!stream || !arena || !runtime_owner_process()) {
    return;
  }
  std::lock_guard lock(registry_mutex());
  registry()[stream] = std::move(arena);
}

/// Looks up the operation task arena attached to an Arrow stream without
/// transferring ownership.
std::shared_ptr<OperationTaskArena>
task_arena_for_stream(const ArrowArrayStream *stream) noexcept {
  if (!stream || !runtime_owner_process()) {
    return {};
  }
  try {
    std::lock_guard lock(registry_mutex());
    const auto found = registry().find(stream);
    if (found == registry().end()) {
      return {};
    }
    auto arena = found->second.lock();
    if (!arena) {
      registry().erase(found);
    }
    return arena;
  } catch (...) {
    return {};
  }
}

/// Removes and returns the task arena associated with an Arrow stream during
/// release.
void detach_task_arena(const ArrowArrayStream *stream) noexcept {
  if (!stream || !runtime_owner_process()) {
    return;
  }
  try {
    std::lock_guard lock(registry_mutex());
    registry().erase(stream);
  } catch (...) {
  }
}

/// Associates a derived Arrow stream with the same operation arena as its
/// parent stream.
void inherit_task_arena(ArrowArrayStream *outer,
                        const ArrowArrayStream *inner) {
  attach_task_arena(outer, task_arena_for_stream(inner));
}

} // namespace sanitize::internal
