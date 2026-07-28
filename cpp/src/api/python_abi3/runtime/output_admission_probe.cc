// Exercises bounded text-output worker admission for high-core regressions.
#include "internal/abi/python_abi3/methods.hh"

#include "internal/output/output_worker_admission.hh"
#include "internal/runtime/execution_policy.hh"
#include "sanitize/options/options.hh"

#include <cstdint>

namespace core_abi3_internal {

PyObject *py_output_worker_admission_probe(PyObject *, PyObject *args) {
  int full_admission = 0;
  if (!PyArg_ParseTuple(args, "p:output_worker_admission_probe",
                        &full_admission)) {
    return nullptr;
  }

  constexpr std::int64_t kProbeMemoryBytes = 256LL * 1024LL * 1024LL;
  constexpr auto operation_policy = sanitize::internal::execution_policy_from(
      sanitize::ThreadingMode::kMulti, kProbeMemoryBytes, 16);
  constexpr auto output_policy =
      sanitize::internal::execution_policy_with_worker_ceiling(operation_policy,
                                                               8, 1);
  static_assert(operation_policy.effective_workers == 16);
  static_assert(output_policy.effective_workers == 8);

  sanitize::internal::ordered_text_output::OutputAdmissionState state;
  const auto first =
      sanitize::internal::ordered_text_output::select_output_admission(
          output_policy, 3, 1, true, full_admission != 0, &state);
  const auto second =
      sanitize::internal::ordered_text_output::select_output_admission(
          output_policy, 3, 1, true, full_admission != 0, &state);
  const auto generations =
      first.effective_workers == second.effective_workers ? 1 : 2;

  PyObject *result = PyTuple_New(7);
  if (!result) {
    return nullptr;
  }
  if (!tuple_set_item_steal(result, 0,
                            PyLong_FromLongLong(first.effective_workers)) ||
      !tuple_set_item_steal(result, 1,
                            PyLong_FromLongLong(second.effective_workers)) ||
      !tuple_set_item_steal(result, 2, PyLong_FromLong(generations)) ||
      !tuple_set_item_steal(
          result, 3,
          PyBool_FromLong(
              sanitize::internal::ordered_text_output::
                      output_admission_requires_sampling(full_admission != 0)
                  ? 1
                  : 0)) ||
      !tuple_set_item_steal(
          result, 4, PyLong_FromLongLong(output_policy.task_queue_capacity)) ||
      !tuple_set_item_steal(
          result, 5, PyLong_FromLongLong(output_policy.reorder_capacity)) ||
      !tuple_set_item_steal(
          result, 6, PyLong_FromLongLong(state.accumulated_work_items))) {
    Py_DECREF(result);
    return nullptr;
  }
  return result;
}

} // namespace core_abi3_internal
