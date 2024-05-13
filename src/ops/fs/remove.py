from pathlib import Path

from dagster import Bool, Field, List, In, Nothing, OpExecutionContext, Out, String, op


def __remove(path: str):
    path: Path = Path(path).resolve()

    if path.is_dir():
        raise ValueError(f"'{path}' is a directory! Use `remove_dir` instead.")

    path.unlink(missing_ok=True)

    return str(path)


@op(
    ins={"path": In(String, "The path to remove")},
    out=Out(String, "The path removed even if nothing was found"),
)
def remove_file(_, path: str):
    return __remove(path)


@op(
    config_schema={"path": Field(String, "The path to remove")},
    ins={"nonce": In(Nothing, "Dummy input to allow scheduling")},
    out=Out(String, "The path removed even if nothing was found"),
)
def remove_file_config(
    context: OpExecutionContext,
):
    return __remove(context.op_config["path"])


@op(
    ins={"paths": In(List[String], "The paths to remove")},
    out=Out(List[String], "The paths removed"),
)
def remove_files(context: OpExecutionContext, paths: list[str]):
    removed = []

    for path in paths:
        try:
            removed.append(__remove(path))
        except Exception as err:
            context.log.warning(f"Received error deleting '{path}': {err}")
            continue

    return removed


@op(
    config_schema={
        "recursive": Field(
            Bool, description="Whether to traverse sub directories", default_value=False
        )
    },
    ins={"path": In(String)},
    out=Out(List[String], "The paths removed or an empty array"),
)
def remove_dir(context: OpExecutionContext, path: str):
    import shutil

    path = Path(path).resolve()

    if path.is_file():
        raise ValueError(f"'{path}' is a file! Use `remove_file` instead.")

    files = []

    def recursive(p: Path):
        for x in p.iterdir():
            if x.is_dir():
                if context.op_config["recursive"]:
                    recursive(p / x.name)
            else:
                x.unlink(missing_ok=True)
                files.append(x)

    recursive(path)
    shutil.rmtree(path)

    return [str(f) for f in files]