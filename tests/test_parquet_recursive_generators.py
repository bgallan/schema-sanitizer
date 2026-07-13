"""Core tests for recursive Parquet nested-layout contracts.

These tests exercise pure diagnostics and recursive corpus generators without
requiring PyArrow. Runtime materialization lives in focused native modules.
"""

from __future__ import annotations

from parquet_recursive_fuzz_helpers import (
    _recursive_fuzz_cartesian_specs,
    _recursive_fuzz_null_empty_matrix_specs,
    _recursive_fuzz_profile_labels,
    _recursive_fuzz_projection_permutation_specs,
    _recursive_fuzz_row_group_phase_labels,
    _recursive_fuzz_row_group_phase_matrix_specs,
    _recursive_fuzz_seeded_specs,
    _recursive_fuzz_signature,
)


def test_recursive_fuzz_cartesian_generator_covers_bounded_shape_space() -> None:
    """Verify the recursive nested shape corpus covers a bounded Cartesian surface."""
    cases = _recursive_fuzz_cartesian_specs()
    signatures = {_recursive_fuzz_signature(spec) for _, spec, _ in cases}
    root_kinds = {spec[0] for _, spec, _ in cases}
    max_repetition_depth = max(metrics["repetition_depth"] for _, _, metrics in cases)
    max_child_count = max(metrics["max_child_count"] for _, _, metrics in cases)
    multi_leaf_cases = [metrics for _, _, metrics in cases if metrics["leaf_count"] > 1]

    assert len(cases) == 47
    assert len(signatures) == len(cases)
    assert root_kinds == {"list", "map", "struct"}
    assert max_repetition_depth >= 7
    assert max_child_count >= 3
    assert len(multi_leaf_cases) >= 10
    assert any(metrics["map_count"] >= 2 for _, _, metrics in cases)
    assert any(metrics["list_count"] >= 2 for _, _, metrics in cases)
    assert any(metrics["struct_count"] >= 2 for _, _, metrics in cases)


def test_recursive_fuzz_null_empty_matrix_covers_container_profiles() -> None:
    """Verify generated recursive shapes cover null/empty/full profiles at each layer."""
    cases = _recursive_fuzz_null_empty_matrix_specs()
    signatures = {_recursive_fuzz_signature(spec) for _, spec, _ in cases}
    root_kinds = {spec[0] for _, spec, _ in cases}
    all_profiles: set[str] = set()
    for _, spec, _ in cases:
        all_profiles |= _recursive_fuzz_profile_labels(spec)

    assert len(cases) == 9
    assert len(signatures) == len(cases)
    assert root_kinds == {"list", "map", "struct"}
    assert all_profiles >= {
        "list-null",
        "list-empty",
        "list-with-null-element",
        "map-null",
        "map-empty",
        "map-with-null-value",
        "struct-null",
        "struct-sparse",
        "scalar-null",
    }
    assert max(metrics["repetition_depth"] for _, _, metrics in cases) >= 6
    assert min(metrics["leaf_count"] for _, _, metrics in cases) >= 4
    assert any(metrics["max_child_count"] >= 3 for _, _, metrics in cases)


def test_recursive_fuzz_row_group_phase_matrix_covers_distinct_profiles() -> None:
    """Verify the row-group phase corpus stresses deep nullable containers."""
    cases = _recursive_fuzz_row_group_phase_matrix_specs()
    signatures = {_recursive_fuzz_signature(spec) for _, spec, _ in cases}
    root_kinds = {spec[0] for _, spec, _ in cases}
    all_profiles: set[str] = set()
    for _, spec, _ in cases:
        all_profiles |= _recursive_fuzz_profile_labels(spec)

    assert _recursive_fuzz_row_group_phase_labels() == (
        "all-null",
        "empty-only",
        "sparse",
        "full",
    )
    assert len(cases) == 6
    assert len(signatures) == len(cases)
    assert root_kinds == {"list", "map", "struct"}
    assert all_profiles >= {
        "list-null",
        "list-empty",
        "list-with-null-element",
        "map-null",
        "map-empty",
        "map-with-null-value",
        "struct-null",
        "struct-sparse",
        "scalar-null",
    }
    assert max(metrics["repetition_depth"] for _, _, metrics in cases) >= 6
    assert any(metrics["map_count"] >= 3 for _, _, metrics in cases)
    assert any(metrics["list_count"] >= 3 for _, _, metrics in cases)
    assert any(metrics["struct_count"] >= 3 for _, _, metrics in cases)


def test_recursive_fuzz_projection_noise_corpus_covers_deep_noise_roots() -> None:
    """Verify the projected-root noise corpus contains deep unprojected shapes."""
    target = _recursive_fuzz_row_group_phase_matrix_specs()[0]
    noise_specs = _recursive_fuzz_cartesian_specs()[:12]
    target_signature = _recursive_fuzz_signature(target[1])
    noise_signatures = {_recursive_fuzz_signature(spec) for _, spec, _ in noise_specs}
    noise_metrics = [metrics for _, _, metrics in noise_specs]

    assert target_signature not in noise_signatures
    assert len(noise_specs) == 12
    assert len(noise_signatures) == len(noise_specs)
    assert {spec[0] for _, spec, _ in noise_specs} == {"list", "map", "struct"}
    assert max(metrics["repetition_depth"] for metrics in noise_metrics) >= 3
    assert any(metrics["map_count"] >= 2 for metrics in noise_metrics)
    assert any(metrics["list_count"] >= 2 for metrics in noise_metrics)
    assert any(metrics["struct_count"] >= 2 for metrics in noise_metrics)
    assert any(metrics["leaf_count"] > 1 for metrics in noise_metrics)


def test_recursive_fuzz_seeded_generator_covers_irregular_shape_space() -> None:
    """Verify seeded recursive fuzzing covers irregular bounded tree shapes."""
    cases = _recursive_fuzz_seeded_specs()
    signatures = {_recursive_fuzz_signature(spec) for _, spec, _ in cases}
    metrics = [metrics for _, _, metrics in cases]

    assert len(cases) == 30
    assert len(signatures) == len(cases)
    assert {spec[0] for _, spec, _ in cases} == {"list", "map", "struct"}
    assert max(item["repetition_depth"] for item in metrics) >= 6
    assert max(item["max_child_count"] for item in metrics) >= 4
    assert any(item["leaf_count"] >= 8 for item in metrics)
    assert any(item["list_count"] >= 3 for item in metrics)
    assert any(item["map_count"] >= 3 for item in metrics)
    assert any(item["struct_count"] >= 3 for item in metrics)
    assert any(
        item["list_count"] >= 2 and item["map_count"] >= 2 and item["struct_count"] >= 2
        for item in metrics
    )


def test_recursive_fuzz_projection_permutation_corpus_covers_irregular_roots() -> None:
    """Verify projection-permutation corpus spans independent deep root shapes."""
    cases = _recursive_fuzz_projection_permutation_specs()
    signatures = {_recursive_fuzz_signature(spec) for _, spec, _ in cases}
    metrics = [metrics for _, _, metrics in cases]

    assert [name for name, _, _ in cases] == [
        "alpha",
        "beta",
        "gamma",
        "delta",
        "epsilon",
        "zeta",
    ]
    assert len(cases) == 6
    assert len(signatures) == len(cases)
    assert {spec[0] for _, spec, _ in cases} == {"list", "map", "struct"}
    assert max(item["repetition_depth"] for item in metrics) >= 6
    assert max(item["max_child_count"] for item in metrics) >= 4
    assert any(item["leaf_count"] >= 8 for item in metrics)
    assert any(
        item["list_count"] >= 2 and item["map_count"] >= 2 and item["struct_count"] >= 2
        for item in metrics
    )
