// Defines debug-only assertion hooks for internal invariants.

#pragma once

#include <cassert>

// Debug-only assertions for internal invariants. These are intentionally
// non-throwing and compile out in release builds.

#ifndef NDEBUG
#define SCHEMA_SANITIZER_DCHECK(cond) assert(cond)
#else
#define SCHEMA_SANITIZER_DCHECK(cond) ((void)0)
#endif
