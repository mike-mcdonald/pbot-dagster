import os
import re
import stat

import pandas as pd

from dagster import (
    Field,
    OpExecutionContext,
    String,
    op,
)

from resources.ssh import SSHClientResource


@op(
    config_schema={
        "remote_path": Field(
            String, description="Remote base path to search for files"
        ),
        "local_path": Field(
            String, description="Base path to store local copies of files"
        ),
        "company": Field(String, description="Name of the company these files are for"),
    },
    required_resource_keys=["sftp"],
)
def get_file_dataframe(context: OpExecutionContext) -> list[dict]:
    sftp: SSHClientResource = context.resources.sftp

    files = []

    try:
        for file in sftp.list_iter(context.op_config["remote_path"]):
            # Ignore directories
            if stat.S_ISDIR(file.st_mode):
                continue

            files.append(file.filename)
    except Exception as err:
        context.log.error(f"Error listing SFTP files!")
        raise err

    if not len(files):
        context.log.warning(f"No files found!")
        return files

    df = pd.DataFrame(data=files, columns=["filename"])

    df["company"] = context.op_config["company"]

    def parse(filename: str):
        pattern = re.compile(r"^([a-zA-Z0-9]+)_?[\w|\s]*_([a-zA-Z]+)\.([a-z]*)$")
        match = re.search(pattern, filename)

        if not match:
            return None

        return {"id": match[1], "doc_type": match[2], "file_type": match[3]}

    df["remote_path"] = df["filename"].map(
        lambda x: os.path.join(context.op_config["remote_path"], x)
    )
    df["local_path"] = df["filename"].map(
        lambda x: os.path.join(context.op_config["local_path"], x)
    )
    df["info"] = df["filename"].map(lambda x: parse(x))

    # This will be files that are not associated with any driver
    df = df[~df["info"].isnull()]

    df["id"] = df["info"].map(lambda x: x.get("id"))
    df["doc_type"] = df["info"].map(lambda x: x.get("doc_type"))

    context.log.info(f"{context.op_config['remote_path']} has {len(df)} driver files")

    return df.drop(columns=["info"]).to_dict(orient="records")
