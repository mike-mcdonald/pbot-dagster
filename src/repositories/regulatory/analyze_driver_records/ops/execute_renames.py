from datetime import datetime

import pandas as pd

from paramiko import SFTPClient

from dagster import op, OpExecutionContext


@op(required_resource_keys={"sftp"})
def execute_renames(context: OpExecutionContext, renames: list[dict]) -> list[dict]:
    df = pd.DataFrame.from_records(renames)

    if not len(df):
        context.log.warning("No files to rename!")
        return []

    client: SFTPClient = context.resources.sftp.connect()

    trace = datetime.now()

    for row in df[["remote_path", "rename_path"]].itertuples(index=False):
        context.log.debug(f"Renaming {row.remote_path} to {row.rename_path}...")

        try:
            client.rename(row.remote_path, row.rename_path)
        except FileNotFoundError as err:
            context.log.warning(f"File '{row.remote_path}' was not found. Skipping...")
        except OSError as err:
            # Rename failed because file already exists
            # Delete file instead
            context.log.warning(
                f"File '{row.rename_path}' already exists. Removing '{row.remote_path}' instead..."
            )
            client.remove(row.remote_path)

    client.close()

    context.log.info(f"Renaming {len(df)} files took {datetime.now() - trace}.")

    return renames
