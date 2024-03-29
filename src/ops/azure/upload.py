from datetime import datetime
from pathlib import Path
from string import Template
from textwrap import dedent

from dagster import (
    Bool,
    Field,
    In,
    List,
    OpExecutionContext,
    Out,
    Permissive,
    String,
    op,
)

from resources.azure_data_lake_gen2 import AzureDataLakeGen2Resource
from ops.template import apply_substitutions


def __template_path(local_path: Path, remote_path: str, context: OpExecutionContext):
    substitutions = dict()

    for attr in {"anchor", "drive", "name", "parent", "root", "stem", "suffix"}:
        substitutions[attr] = local_path.__getattribute__(attr)

    if "substitutions" in context.op_config:
        substitutions.update(context.op_config["substitutions"])

    return apply_substitutions(
        template_string=remote_path,
        substitutions=substitutions,
        context=context,
    )


def __upload_file(context: OpExecutionContext, path: Path):
    if path.is_dir():
        raise ValueError(f"{path} is a directory. Use `upload_directory` instead.")

    client: AzureDataLakeGen2Resource = context.resources.adls2_resource

    remote_path: str = context.op_config["remote_path"]
    container = context.op_config["container"]

    remote_path = __template_path(
        local_path=(
            path.relative_to(Path(context.op_config["base_dir"]))
            if "base_dir" in context.op_config
            else path
        ),
        remote_path=context.op_config["remote_path"],
        context=context,
    )

    context.log.info(f"🚀 Uploading {path} to {remote_path} in {container}...")
    trace = datetime.now()

    try:
        client.upload_file(
            file_system=container,
            local_path=path,
            remote_path=remote_path,
        )
    except Exception as err:
        context.log.error(f"Failed to write {path} to {remote_path}.")
        raise err

    context.log.info(
        f"⌚ Uploading {path} to {remote_path} took {datetime.now() - trace}"
    )

    return str(path)


@op(
    config_schema={
        "base_dir": Field(
            String,
            description="The directory which all paths should be relative to. Used to template the remote path with the `parent` field.",
            is_required=False,
        ),
        "remote_path": Field(
            String,
            description=dedent(
                """Template string to generate the path.
                Will replace properties wrapped by {} with most `pathlib.Path` properties, run_id from OpContext."""
            ).strip(),
        ),
        "container": Field(str),
        "substitutions": Field(
            Permissive(),
            description="Extra mappings dict to apply to remote_path template",
            is_required=False,
        ),
    },
    required_resource_keys={"adls2_resource"},
    ins={"path": In(String)},
    out=Out(String, "The path uploaded or None if there was no file"),
)
def upload_file(context: OpExecutionContext, path):
    return __upload_file(context, Path(path))


@op(
    config_schema={
        "base_dir": Field(
            String,
            description="The directory which all paths should be relative to. Used to template the remote path with the `parent` field.",
            is_required=False,
        ),
        "remote_path": Field(
            String,
            description=dedent(
                """Template string to generate the path.
                Will replace properties wrapped by {} with most `pathlib.Path` properties, run_id from OpContext."""
            ).strip(),
        ),
        "container": Field(str),
        "substitutions": Field(
            Permissive(),
            description="Extra mappings dict to apply to remote_path template",
            is_required=False,
        ),
    },
    required_resource_keys={"adls2_resource"},
    ins={"paths": In(List[String])},
    out=Out(
        List[String], "The paths uploaded or an empty array if there were no files"
    ),
)
def upload_files(context: OpExecutionContext, paths):
    uploaded = []

    for path in paths:
        uploaded.append(__upload_file(context, path))

    return uploaded
