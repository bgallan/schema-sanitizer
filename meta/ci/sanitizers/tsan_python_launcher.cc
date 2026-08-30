// Starts CPython with the CI ThreadSanitizer runtime before extension modules.
// The minimal launcher preserves Python argument handling while ensuring race
// instrumentation initializes before any tested extension code.

#include <Python.h>

/// Returns fail-fast ThreadSanitizer options for the CPython test launcher.
extern "C" const char *__tsan_default_options() {
  return "halt_on_error=1:history_size=7:second_deadlock_stack=1";
}

/// Delegates process arguments to CPython after ThreadSanitizer startup.
int main(int argc, char **argv) { return Py_BytesMain(argc, argv); }
