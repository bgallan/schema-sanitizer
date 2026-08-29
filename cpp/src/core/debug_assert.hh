// Defines debug-only assertion hooks for internal invariants.
// The macros preserve diagnostic checks in development builds and compile away
// without evaluating their conditions in release builds.

#pragma once

#include <cassert>

// Debug-only assertions for internal invariants. These are intentionally
// non-throwing and compile out in release builds.

#ifndef NDEBUG
#define SANITIZE_DCHECK(cond) assert(cond)
#else
#define SANITIZE_DCHECK(cond) ((void)0)
#endif
