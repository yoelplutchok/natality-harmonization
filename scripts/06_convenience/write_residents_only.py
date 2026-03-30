#!/usr/bin/env python3
"""
Write residents-only subsets of V2 and V3 linked derived Parquet files.

Filters out foreign residents (is_foreign_resident == True) and the
is_foreign_resident column itself (redundant in the output).

Outputs:
  output/convenience/natality_v2_residents_only.parquet
  output/convenience/natality_v3_linked_residents_only.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def filter_residents(in_path: Path, out_path: Path, batch_size: int = 500_000) -> None:
    """Stream Parquet, keep only residents, write to new file."""
    if not in_path.is_file():
        raise FileNotFoundError(in_path)

    pf = pq.ParquetFile(in_path)
    in_schema = pf.schema_arrow
    print(f"Reading {in_path.name} ({pf.metadata.num_rows:,} rows, {len(in_schema.names)} cols)")

    # Drop is_foreign_resident and restatus from output (redundant for residents-only)
    drop_cols = {"is_foreign_resident", "restatus"}
    out_fields = [f for f in in_schema if f.name not in drop_cols]
    out_schema = pa.schema(out_fields)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(out_path, out_schema, compression="zstd")
    total_in = 0
    total_out = 0

    try:
        for batch in pf.iter_batches(batch_size=batch_size):
            total_in += batch.num_rows
            foreign = batch.column(batch.schema.get_field_index("is_foreign_resident"))
            # Keep rows where is_foreign_resident is explicitly False
            keep = pc.equal(foreign, pa.scalar(False))
            filtered = batch.filter(keep)

            if filtered.num_rows > 0:
                # Drop the redundant columns
                cols = [filtered.column(f.name) for f in out_fields]
                out_batch = pa.RecordBatch.from_arrays(cols, schema=out_schema)
                writer.write_batch(out_batch)
                total_out += out_batch.num_rows
    finally:
        writer.close()

    excluded = total_in - total_out
    print(f"  Wrote {total_out:,} resident rows to {out_path.name}")
    print(f"  Excluded {excluded:,} foreign-resident rows ({excluded / total_in * 100:.2f}%)")


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--v2-in", type=Path,
                    default=repo_root / "output" / "harmonized" / "natality_v2_harmonized_derived.parquet")
    p.add_argument("--v3-in", type=Path,
                    default=repo_root / "output" / "harmonized" / "natality_v3_linked_harmonized_derived.parquet")
    p.add_argument("--out-dir", type=Path,
                    default=repo_root / "output" / "convenience")
    p.add_argument("--batch-size", type=int, default=500_000)
    p.add_argument("--skip-v2", action="store_true", help="Skip V2 natality")
    p.add_argument("--skip-v3", action="store_true", help="Skip V3 linked")
    args = p.parse_args()

    if not args.skip_v2:
        filter_residents(args.v2_in, args.out_dir / "natality_v2_residents_only.parquet", args.batch_size)

    if not args.skip_v3:
        filter_residents(args.v3_in, args.out_dir / "natality_v3_linked_residents_only.parquet", args.batch_size)

    print("\nDone.")


if __name__ == "__main__":
    main()
