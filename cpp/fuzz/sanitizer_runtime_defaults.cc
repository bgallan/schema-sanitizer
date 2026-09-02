// Supplies deterministic sanitizer defaults to native fuzz executables.
// Address, undefined-behavior, and thread sanitizer hooks fail fast while
// disabling only leak detection that belongs to separate CI coverage.

/// Returns the default AddressSanitizer options for fuzz processes.
extern "C" const char *__asan_default_options() {
  return "detect_leaks=0:halt_on_error=1:strict_string_checks=1";
}

/// Returns the default UndefinedBehaviorSanitizer options for fuzz processes.
extern "C" const char *__ubsan_default_options() {
  return "halt_on_error=1:print_stacktrace=1";
}

/// Returns the default ThreadSanitizer options for fuzz processes.
extern "C" const char *__tsan_default_options() {
  return "halt_on_error=1:history_size=7";
}
