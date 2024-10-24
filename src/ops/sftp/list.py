import hashlib
import os
import re
import stat

from typing import Optional


from dagster import (
    Bool,
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
        "recurse": Field(
            Bool,
            description="Whether to traverse subfolders",
            is_required=False,
            default_value=False,
        ),
    },
    required_resource_keys=["sftp"],
)


def _list(
    resource: SSHClientResource,
    path: str,
    pattern: Optional[str] = None,
    recurse: bool = False,
):
    for attr in resource.list_iter(path):
        # if attr is dir
        if stat.S_ISDIR(attr.st_mode) and recurse:
            yield from _list(
                resource, os.path.join(path, attr.filename), pattern, recurse
            )
        else:
            if pattern is None:
                yield os.path.join(path, attr.filename)
            else:
                regex = re.compile(pattern, re.IGNORECASE)
                if regex.search(attr.filename):
                    yield os.path.join(path, attr.filename)


@op(**COMMON_CONFIG)
def list(
    context: OpExecutionContext,
):
    return [
        f
        for f in _list(
            context.resources.sftp,
            context.op_config["path"],
            context.op_config["pattern"],
            context.op_config["recurse"],
        )
    ]


@op(**COMMON_CONFIG, out=DynamicOut(String))
def list_dynamic(context: OpExecutionContext):
    for f in _list(
        context.resources.sftp,
        context.op_config["path"],
        context.op_config["pattern"],
        context.op_config["recurse"],
    ):
        yield DynamicOutput(
            f,
            mapping_key=hashlib.sha256(str(f).encode("utf8")).hexdigest(),
        )
