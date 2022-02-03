from pathlib import Path

from dagster import OpExecutionContext, fs_io_manager, job, op

from ops.fs import list_dir_dynamic


@op
def show_file(context: OpExecutionContext, path: str):
    path = Path(path).resolve()

    context.log.info(f"Path: {path}...")


@job(
    resource_defs={
        "io_manager": fs_io_manager,
    }
)
def test_list_dir_dynamic():
    list_dir_dynamic().map(show_file)
