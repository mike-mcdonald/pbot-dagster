from pathlib import Path

from dagster import (
    Bool,
    Failure,
    Field,
    In,
    Nothing,
    OpExecutionContext,
    String,
    op,
)


def __remove(path: str) -> str:
    path: Path = Path(path).resolve()

    if path.is_dir():
        raise ValueError(f"'{path}' is a directory! Use `remove_dir` instead.")

    path.unlink(missing_ok=True)

    return str(path)


@op
def remove_file(_, path: str) -> str:
    return __remove(path)


@op(
    config_schema={"path": Field(String, "The path to remove")},
    ins={"nonce": In(Nothing, "Dummy input to allow scheduling")},
)
def remove_file_config(
    context: OpExecutionContext,
) -> str:
    return __remove(context.op_config["path"])


@op
def remove_files(context: OpExecutionContext, paths: list[str]) -> list[str]:
    removed = []

    for path in paths:
        try:
            removed.append(__remove(path))
        except Exception as err:
            context.log.warning(f"Received error deleting '{path}': {err}")
            continue

    return removed


def __remove_dir(path: Path, recursive: bool):
    path = Path(path).resolve()

    if path.is_file():
        raise Failure(f"'{path}' is a file! Use `remove_file` instead.")

    files: list[Path] = []

    def recursive(p: Path):
        for x in p.iterdir():
            if x.is_dir():
                if recursive:
                    recursive(p / x.name)
                    x.rmdir()
            else:
                x.unlink(missing_ok=True)
                files.append(x)

    recursive(path)

    return [str(f.resolve()) for f in files]


@op(
    config_schema={
        "recursive": Field(
            Bool, description="Whether to traverse sub directories", default_value=False
        )
    },
)
def remove_dir(context: OpExecutionContext, path: str) -> list[str]:
    return __remove_dir(path, context.op_config["recursive"])


@op(
    config_schema={
        "recursive": Field(
            Bool, description="Whether to traverse sub directories", default_value=False
        )
    },
)
def remove_dirs(context: OpExecutionContext, paths: list[str]) -> list[str]:
    files = []

    for path in paths:
        files.extend(__remove_dir(path, context.op_config["recursive"]))

    return files
