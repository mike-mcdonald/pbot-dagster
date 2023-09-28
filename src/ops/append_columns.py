import csv
import shutil

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from dagster import (
    Field,
    In,
    OpExecutionContext,
    Out,
    Permissive,
    String,
    op,
)
from dagster._utils import safe_tempfile_path

from ops.template import apply_substitutions

OP_CONFIG = dict(
    description="Adds constant value columns to a dataset",
    config_schema={
        "map": Field(
            Permissive(),
            description="Dictionary of columns to add to the CSV. Will have substitutions applied to values",
            is_required=True,
        ),
        "substitutions": Field(
            Permissive(),
            description="Subsitution mapping for substituting in `path`",
            is_required=False,
        ),
    },
    ins={"path": In(String)},
    out=Out(String, "The path to the file modified by this operation"),
)


def __bootstrap(context: OpExecutionContext, path: str):
    local_path = Path(path)

    substitutions = dict()

    for attr in {"anchor", "drive", "name", "parent", "root", "stem", "suffix"}:
        substitutions[attr] = local_path.__getattribute__(attr)

    if "substitutions" in context.op_config:
        substitutions = dict(**substitutions, **context.op_config["substitutions"])

    map: dict[str, str] = context.op_config["map"]

    map = {k: apply_substitutions(v, substitutions, context) for k, v in map.items()}

    return local_path, map


@op(**OP_CONFIG)
def append_columns_to_csv(context: OpExecutionContext, path: str) -> str:
    local_path, map = __bootstrap(context, path)

    with safe_tempfile_path() as tmp:
        context.log.info(f"Copying '{local_path}' to temporary location '{tmp}'...")

        shutil.copy(local_path, tmp)

        context.log.info(f"Appending columns: {map}...")

        with open(tmp, "r") as tf:
            reader = csv.reader(tf)

            with local_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                for row in reader:
                    if reader.line_num == 1:
                        # This is the header
                        row.extend([key for key in map.keys()])
                    else:
                        for value in map.values():
                            row.append(value)

                    writer.writerow(row)

        context.log.info(f"Removing temporary file '{tmp}'.")

    return str(local_path.resolve())


@op(**OP_CONFIG)
def append_columns_to_parquet(context: OpExecutionContext, path: str) -> str:
    local_path, map = __bootstrap(context, path)

    with safe_tempfile_path() as tmp:
        context.log.info(f"Copying file to temporary location '{tmp}'...")

        shutil.copy(local_path, tmp)

        table = pq.read_table(tmp, memory_map=True)

        for key, value in map.items():
            table = table.append_column(key, pa.array([value] * table.num_rows))

        with pq.ParquetWriter(str(local_path.resolve()), table.schema) as writer:
            writer.write_table(table)

        context.log.info(f"Removing temporary file '{tmp}'.")

    return str(local_path.resolve())
