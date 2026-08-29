// Starts CPython with ASan and UBSan linked before extension modules.
// The minimal launcher preserves Python argument handling while ensuring native
// sanitizer runtimes initialize before any tested extension code.

#include <Python.h>

/// Returns fail-fast AddressSanitizer options for the CPython test launcher.
extern "C" const char *__asan_default_options() {
  return "detect_leaks=0:halt_on_error=1:strict_string_checks=1";
}

/// Returns stack-reporting UndefinedBehaviorSanitizer options for the CPython launcher.
extern "C" const char *__ubsan_default_options() {
  return "halt_on_error=1:print_stacktrace=1";
}

/// Delegates process arguments to CPython after sanitizer startup.
int main(int argc, char **argv) { return Py_BytesMain(argc, argv); }
