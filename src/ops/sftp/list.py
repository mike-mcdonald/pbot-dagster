import hashlib
import os
import re
import stat

from typing import Optional


from dagster import (
    DynamicOut,
    DynamicOutput,
    Field,
    OpExecutionContext,
    op,
    String,
)

from resources.ssh import SSHClientResource

COMMON_CONFIG = dict(
    config_schema={
        "path": Field(String, description="The base path to list"),
        "pattern": Field(
            String,
            description="The regex pattern to filter files with",
            is_required=False,
        ),
    },
    required_resource_keys=["sftp"],
)


def _list(resource: SSHClientResource, path: str, pattern: Optional[str]):
    if pattern is None:
        return [
            os.path.join(path, f.filename)
            for f in resource.list(path)
            if not stat.S_ISDIR(f.st_mode)
        ]

    regex = re.compile(pattern, re.IGNORECASE)

    return [
        os.path.join(path, f.filename)
        for f in resource.list(path)
        if not stat.S_ISDIR(f.st_mode) and regex.search(f.filename)
    ]


@op(**COMMON_CONFIG)
def list(
    context: OpExecutionContext,
):
    return _list(
        context.resources.sftp,
        context.op_config["path"],
        context.op_config["pattern"],
    )


@op(**COMMON_CONFIG, out=DynamicOut(String))
def list_dynamic(context: OpExecutionContext):
    for f in _list(
        context.resources.sftp,
        context.op_config["path"],
        context.op_config["pattern"],
    ):
        yield DynamicOutput(
            f,
            mapping_key=hashlib.sha256(str(f).encode("utf8")).hexdigest(),
        )
