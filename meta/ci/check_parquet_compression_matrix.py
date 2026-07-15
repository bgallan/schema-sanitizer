"""Validate installed-wheel Parquet compression parity across all codecs."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow.parquet as pq

import schema_sanitizer as ss


def _write_source(path: Path, rows: int = 1024) -> list[str]:
    """Write a compressible CSV fixture and return its expected text values."""
    values = [f"shared-prefix-{'abc123' * 32}-{index:08d}" for index in range(rows)]
    path.write_text(
        "id,text\n" + "".join(f"{index},{value}\n" for index, value in enumerate(values)),
        encoding="utf-8",
    )
    return values


def _text_column_metadata(path: Path):
    """Return the Parquet file and metadata chunks for its text column."""
    parquet_file = pq.ParquetFile(path)
    text_index = parquet_file.schema_arrow.names.index("text")
    chunks = [
        parquet_file.metadata.row_group(index).column(text_index)
        for index in range(parquet_file.metadata.num_row_groups)
    ]
    return parquet_file, chunks


def main() -> None:
    """Generate and verify one Parquet output for every supported codec."""
    expected_names = {
        "gzip": "GZIP",
        "snappy": "SNAPPY",
        "uncompressed": "UNCOMPRESSED",
    }
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-codecs-") as raw_dir:
        root = Path(raw_dir)
        source = root / "input.csv"
        expected_values = _write_source(source)

        for codec, expected_name in expected_names.items():
            output = root / f"{codec}.parquet"
            kwargs: dict[str, object] = {"parquet_compression": codec}
            if codec == "gzip":
                kwargs["parquet_gzip_level"] = 6
            ss.to_parquet(source, output, input_format="csv", **kwargs)

            parquet_file, text_chunks = _text_column_metadata(output)
            actual_names = {
                parquet_file.metadata.row_group(row_group).column(column).compression
                for row_group in range(parquet_file.metadata.num_row_groups)
                for column in range(parquet_file.metadata.row_group(row_group).num_columns)
            }
            if actual_names != {expected_name}:
                raise AssertionError(
                    f"{codec}: expected {expected_name}, got {sorted(actual_names)}"
                )

            values = pq.read_table(output, columns=["text"]).column("text").to_pylist()
            if values != expected_values:
                raise AssertionError(f"{codec}: round-trip values differ")

            compressed = sum(chunk.total_compressed_size for chunk in text_chunks)
            uncompressed = sum(chunk.total_uncompressed_size for chunk in text_chunks)
            if codec in {"gzip", "snappy"} and compressed >= uncompressed:
                raise AssertionError(
                    f"{codec}: payload was not reduced ({compressed} >= {uncompressed})"
                )
            print(
                f"{codec}: codec={expected_name} compressed={compressed} "
                f"uncompressed={uncompressed}"
            )


if __name__ == "__main__":
    main()
