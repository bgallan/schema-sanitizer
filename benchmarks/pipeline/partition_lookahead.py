#!/usr/bin/env python3
"""Measure bounded partition lookahead against an ordered local HTTP source."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from schema_sanitizer.pipeline.advanced import PartitionRunPlan, run_partitioned_to_parquet


def _payload(partition: int, rows: int) -> bytes:
    """Return deterministic nested JSONL bytes for one partition."""
    return (
        "\n".join(
            json.dumps(
                {
                    "partition": partition,
                    "ordinal": ordinal,
                    "payload": {
                        "label": f"row-{partition}-{ordinal}",
                        "values": [ordinal, ordinal + 1, ordinal + 2],
                    },
                },
                separators=(",", ":"),
            )
            for ordinal in range(rows)
        )
        + "\n"
    ).encode()


class _SourceServer:
    """Serve immutable partition bytes with deterministic GET latency."""

    def __init__(self, payloads: Mapping[str, bytes], delay_seconds: float) -> None:
        """Start one loopback server for the benchmark lifetime."""
        payload_by_path = dict(payloads)
        delay = max(0.0, delay_seconds)

        class Handler(BaseHTTPRequestHandler):
            """Serve HEAD/GET without logging benchmark requests."""

            def _body(self) -> bytes | None:
                return payload_by_path.get(self.path)

            def do_HEAD(self) -> None:  # noqa: N802
                body = self._body()
                if body is None:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()

            def do_GET(self) -> None:  # noqa: N802
                body = self._body()
                if body is None:
                    self.send_error(404)
                    return
                time.sleep(delay)
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args: Any) -> None:
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def base_url(self) -> str:
        """Return the bound loopback origin."""
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def close(self) -> None:
        """Stop the server and join its host thread."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()


def _canonical(value: Any) -> Any:
    """Remove generated UTC metadata recursively from benchmark output."""
    if isinstance(value, dict):
        return {
            key: _canonical(item)
            for key, item in value.items()
            if key not in {"ingestion_timestamp", "detected_at"}
        }
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    if isinstance(value, str) and value[:1] in {"[", "{"}:
        try:
            return _canonical(json.loads(value))
        except json.JSONDecodeError:
            pass
    return value


def _digest_outputs(paths: list[Path]) -> str:
    """Hash ordered logical Parquet rows across every partition."""
    rows = [_canonical(pq.read_table(path).to_pylist()) for path in paths]
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _run_case(
    label: str,
    plans: list[PartitionRunPlan],
    output_root: Path,
    kwargs: Mapping[str, Any] | Callable[[PartitionRunPlan], Mapping[str, Any]],
) -> tuple[float, str]:
    """Run one pipeline shape and return wall time plus logical digest."""
    case_plans = [
        PartitionRunPlan(
            plan.logical_date,
            plan.source_uri,
            str(output_root / label / f"part-{index}.parquet"),
        )
        for index, plan in enumerate(plans)
    ]
    for plan in case_plans:
        Path(plan.output_uri).parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    run_partitioned_to_parquet(
        case_plans,
        initial_schema_registry={},
        to_parquet_kwargs=kwargs,
    )
    elapsed = time.perf_counter() - started
    return elapsed, _digest_outputs([Path(plan.output_uri) for plan in case_plans])


def main() -> None:
    """Run repeated single/sequential-multi/lookahead-multi measurements."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--rows-per-partition", type=int, default=50_000)
    parser.add_argument("--delay-ms", type=float, default=75.0)
    parser.add_argument("--memory-mib", type=int, default=256)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if min(args.partitions, args.rows_per_partition, args.memory_mib, args.repeats) <= 0:
        parser.error("partitions, rows, memory, and repeats must be positive")

    payloads = {
        f"/partition-{index}.jsonl": _payload(index, args.rows_per_partition)
        for index in range(args.partitions)
    }
    server = _SourceServer(payloads, args.delay_ms / 1_000.0)
    common = {
        "input_format": "jsonl",
        "input_mode": "single_file",
        "memory_limit_bytes": args.memory_mib << 20,
    }
    plans = [PartitionRunPlan(None, f"{server.base_url}{path}", "unused") for path in payloads]
    cases: dict[str, Mapping[str, Any] | Callable[[PartitionRunPlan], Mapping[str, Any]]] = {
        "single": {**common, "multi_threading": False},
        "multi_sequential": lambda _plan: {**common, "multi_threading": True},
        "multi_lookahead": {**common, "multi_threading": True},
    }
    samples = {name: [] for name in cases}
    digests: dict[str, str] = {}
    try:
        with tempfile.TemporaryDirectory(prefix="schema-sanitizer-lookahead-") as raw:
            root = Path(raw)
            for iteration in range(args.warmups + args.repeats):
                for name, kwargs in cases.items():
                    elapsed, digest = _run_case(f"{name}-{iteration}", plans, root, kwargs)
                    if iteration >= args.warmups:
                        samples[name].append(elapsed)
                    digests[name] = digest
    finally:
        server.close()

    if len(set(digests.values())) != 1:
        raise RuntimeError("partition benchmark modes produced different logical rows")
    medians = {name: statistics.median(values) for name, values in samples.items()}
    report = {
        "partitions": args.partitions,
        "rows_per_partition": args.rows_per_partition,
        "delay_ms": args.delay_ms,
        "memory_mib": args.memory_mib,
        "samples": samples,
        "medians": medians,
        "lookahead_speedup_vs_sequential_multi": (
            medians["multi_sequential"] / medians["multi_lookahead"]
        ),
        "lookahead_speedup_vs_single": medians["single"] / medians["multi_lookahead"],
        "logical_outputs_equivalent": True,
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
