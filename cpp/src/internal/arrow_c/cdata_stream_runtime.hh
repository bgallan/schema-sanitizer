// Associates Arrow C streams with operation-scoped native runtime resources.
// The implementation preserves Arrow ownership and error contracts without
// depending on the Arrow C++ library.

#pragma once

#include <memory>

struct ArrowArrayStream;

namespace sanitize::internal {
class OperationTaskArena;

void attach_task_arena(ArrowArrayStream *stream,
                       std::shared_ptr<OperationTaskArena> arena);
[[nodiscard]] std::shared_ptr<OperationTaskArena>
task_arena_for_stream(const ArrowArrayStream *stream) noexcept;
void detach_task_arena(const ArrowArrayStream *stream) noexcept;
void inherit_task_arena(ArrowArrayStream *outer, const ArrowArrayStream *inner);

} // namespace sanitize::internal
