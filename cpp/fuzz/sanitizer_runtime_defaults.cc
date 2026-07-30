// Supplies deterministic sanitizer defaults to standalone fuzz executables.

extern "C" const char *__asan_default_options() {
  return "detect_leaks=0:halt_on_error=1:strict_string_checks=1";
}

extern "C" const char *__ubsan_default_options() {
  return "halt_on_error=1:print_stacktrace=1";
}

extern "C" const char *__tsan_default_options() {
  return "halt_on_error=1:history_size=7";
}
