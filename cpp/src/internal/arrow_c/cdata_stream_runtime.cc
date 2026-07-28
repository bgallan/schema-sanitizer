// Implements Arrow C stream runtime sidecars without changing the Arrow ABI.

#include "internal/arrow_c/cdata_stream_runtime.hh"

#include "internal/runtime/operation_task_arena.hh"

#include <mutex>
#include <unordered_map>
#include <utility>

namespace sanitize::internal {
namespace {

std::mutex &registry_mutex() {
  static std::mutex value;
  return value;
}

std::unordered_map<const ArrowArrayStream *,
                   std::weak_ptr<OperationTaskArena>> &
registry() {
  static std::unordered_map<const ArrowArrayStream *,
                            std::weak_ptr<OperationTaskArena>>
      value;
  return value;
}

} // namespace

void attach_task_arena(ArrowArrayStream *stream,
                       std::shared_ptr<OperationTaskArena> arena) {
  if (!stream || !arena) {
    return;
  }
  std::lock_guard lock(registry_mutex());
  registry()[stream] = std::move(arena);
}

std::shared_ptr<OperationTaskArena>
task_arena_for_stream(const ArrowArrayStream *stream) noexcept {
  if (!stream) {
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

void detach_task_arena(const ArrowArrayStream *stream) noexcept {
  if (!stream) {
    return;
  }
  try {
    std::lock_guard lock(registry_mutex());
    registry().erase(stream);
  } catch (...) {
  }
}

void inherit_task_arena(ArrowArrayStream *outer,
                        const ArrowArrayStream *inner) {
  attach_task_arena(outer, task_arena_for_stream(inner));
}

} // namespace sanitize::internal
