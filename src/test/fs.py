from pathlib import Path

from dagster import OpExecutionContext, fs_io_manager, job, op

from ops.fs import list_dir_dynamic
from resources.fs import FileShareResource, fileshare_resource


@op
def show_file(context: OpExecutionContext, path: str):
    path = Path(path).resolve()

    context.log.info(f"Path: {path}...")


@op(required_resource_keys=["fileshare"])
def upload_file(context: OpExecutionContext, path: str):
    fs: FileShareResource = context.resources.fileshare
    p = Path(path).resolve()

    fs.upload(p, f"{p.stem}.{p.suffix}")


@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "fileshare": fileshare_resource,
    },
)
def test_list_dir_dynamic():
    paths = list_dir_dynamic()
    paths.map(show_file)
    paths.map(upload_file)
