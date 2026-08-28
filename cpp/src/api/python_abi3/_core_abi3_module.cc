/* ABI3 module initializer, definition, and generated method table. */
#include "internal/abi/python_abi3/methods.hh"

#include <array>

namespace core_abi3_internal {
namespace {

auto kMethods = std::to_array<PyMethodDef>({
#define SCHEMA_SANITIZER_ABI3_METHOD(name, flags, doc)                         \
  {.ml_name = #name,                                                           \
   .ml_meth = _PyCFunction_CAST(py_##name),                                    \
   .ml_flags = flags,                                                          \
   .ml_doc = doc},
#include "internal/abi/python_abi3/method_catalog.inc"
#undef SCHEMA_SANITIZER_ABI3_METHOD
    {.ml_name = nullptr, .ml_meth = nullptr, .ml_flags = 0, .ml_doc = nullptr},
});

PyModuleDef kModule = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "schema_sanitizer._core_abi3",
    .m_doc = "schema-sanitizer minimal ABI3 bindings (limited API)",
    .m_size = -1,
    .m_methods = kMethods.data(),
    .m_slots = nullptr,
    .m_traverse = nullptr,
    .m_clear = nullptr,
    .m_free = nullptr,
};

PyObject *create_module() noexcept { return PyModule_Create(&kModule); }

} // namespace
} // namespace core_abi3_internal

PyMODINIT_FUNC PyInit__core_abi3(void) {
  return core_abi3_internal::create_module();
}
