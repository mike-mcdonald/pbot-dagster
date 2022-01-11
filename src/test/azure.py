from pathlib import Path

from dagster import List, String, OpExecutionContext, Out, fs_io_manager, job, op

from ops.azure import upload_files
from ops.fs import remove_files

from resources.azure_data_lake_gen2 import adls2_resource


@op(config_schema={"dir": str, "number": int}, out=Out(List[String]))
def create_files(context: OpExecutionContext):
    path = Path(context.op_config["dir"]).resolve()

    context.log.info(f"Creating directory {path}...")
    path.mkdir(parents=True, exist_ok=True)

    files = []

    for x in range(context.op_config["number"]):
        p = path.joinpath(f"{context.run_id}-{x}")
        with p.open("wb") as f:
            f.write(context.run_id.encode("utf8"))
        files.append(str(p.resolve()))

    return files


@job(
    resource_defs={
        "adls2_resource": adls2_resource,
        "io_manager": fs_io_manager,
    }
)
def test_azure_upload_files():
    remove_files(upload_files(create_files()))
