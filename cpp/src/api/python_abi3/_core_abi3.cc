/*
 * ABI3 Python module definition.
 *
 * This file creates the limited-API extension module from the shared module
 * definition owned by _core_abi3_module.cc.
 */
#include "api/python_abi3/_core_abi3_module.hh"

// Initializes the limited-API Python extension module.
PyMODINIT_FUNC PyInit__core_abi3(void) {
  PyObject *m = PyModule_Create(core_abi3_internal::module_definition());
  if (!m)
    return nullptr;
  return m;
}
