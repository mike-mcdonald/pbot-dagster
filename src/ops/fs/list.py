import hashlib

from pathlib import Path

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

    files: list(Path) = []

    def recursive(p: Path):
        for x in p.iterdir():
            if x.is_dir():
                if "recursive" in context.op_config and context.op_config["recursive"]:
                    recursive(p / x.name)
            else:
                files.append(x)

    recursive(path)

    return [str(x.resolve()) for x in files]


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

    def recursive(p: Path):
        for x in p.iterdir():
            if x.is_dir():
                if context.op_config["recursive"]:
                    recursive(p / x.name)
            else:
                yield DynamicOutput(
                    value=str(x),
                    mapping_key=hashlib.sha256(str(x).encode("utf8")).hexdigest(),
                )

    recursive(path)


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

    files: list(Path) = []

    for path in paths:
        if path.is_dir():
            context.log.warning(f"'{path}' is a directory! Skipping...")
        else:
            files.append(path)

    return [str(f.resolve()) for f in files]
