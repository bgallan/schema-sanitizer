// Starts CPython with ThreadSanitizer linked before extension modules.

#include <Python.h>

extern "C" const char *__tsan_default_options() {
  return "halt_on_error=1:history_size=7:second_deadlock_stack=1:"
         "ignore_noninstrumented_modules=1";
}

int main(int argc, char **argv) { return Py_BytesMain(argc, argv); }
