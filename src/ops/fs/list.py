import hashlib

from pathlib import Path
from typing import Generator

from dagster import (
    Bool,
    DynamicOut,
    DynamicOutput,
    Field,
    List,
    OpExecutionContext,
    Out,
    String,
    op,
)


def traverse(p: Path, recurse: bool) -> Generator[Path]:
    for x in p.iterdir():
        if x.is_dir():
            if recurse:
                yield from traverse(p / x.name, recurse)
        else:
            yield x


@op(
    config_schema={
        "recursive": Field(
            Bool,
            description="Whether to traverse sub directories",
            default_value=False,
            is_required=False,
        ),
        "path": Field(String),
    },
    out=Out(List[String], description="The files in the directory"),
)
def list_dir(context: OpExecutionContext):
    path = Path(context.op_config["path"]).resolve()

    if path.is_file():
        context.log.warning(f"'{path}' is a file!")
        return path

    return [str(x.resolve()) for x in traverse(path, context.op_config["recursive"])]


@op(
    config_schema={
        "recursive": Field(
            Bool, description="Whether to traverse sub directories", default_value=False
        ),
        "path": Field(String),
    },
    out=DynamicOut(String, description="The files in the directory"),
)
def list_dir_dynamic(context: OpExecutionContext):
    path = Path(context.op_config["path"]).resolve()

    if path.is_file():
        context.log.warning(f"'{path}' is a file!")
        yield DynamicOutput(
            value=str(path),
            mapping_key=hashlib.sha256(str(path).encode("utf8")).hexdigest(),
        )
        return

    for x in traverse(path, context.op_config["recursive"]):
        yield DynamicOutput(
            value=str(x),
            mapping_key=hashlib.sha256(str(x).encode("utf8")).hexdigest(),
        )


@op(
    config_schema={
        "base_dir": Field(
            String, description="Where to start the subtree search", default_value="."
        ),
        "glob": Field(String),
    },
    out=Out(List[String], description="The files in the directory"),
)
def list_glob(context: OpExecutionContext):
    paths = (
        Path(context.op_config["base_dir"]).resolve().glob(context.op_config["glob"])
    )

    return [str(path.resolve()) for path in paths if path.is_file()]
