// Starts CPython with sanitizer runtimes linked before extension modules.

#include <Python.h>

extern "C" const char *__asan_default_options() {
  return "detect_leaks=0:halt_on_error=1:strict_string_checks=1";
}

extern "C" const char *__ubsan_default_options() {
  return "halt_on_error=1:print_stacktrace=1";
}

int main(int argc, char **argv) { return Py_BytesMain(argc, argv); }
