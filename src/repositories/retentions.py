from pathlib import Path
from typing import Callable, Tuple

from dagster import (
    Failure,
    Field,
    Int,
    List,
    OpExecutionContext,
    Out,
    RunRequest,
    ScheduleEvaluationContext,
    fs_io_manager,
    job,
    op,
    repository,
    schedule,
)

from ops.fs.remove import remove_dirs, remove_files
from resources.fs import FileShareResource, fileshare_resource
from resources.mssql import MSSqlServerResource, mssql_resource
from pathlib import Path


@op(
    config_schema={
        "interval": Field(
            Int,
            description="Number of years to return date. Should be NEGATIVE number",
        ),
    },
    required_resource_keys={"sql_server"},
)
def get_removal_list(context: OpExecutionContext) -> list[dict]:
    interval = context.op_config["interval"]

    if int(interval) > 0:
        raise Failure("Value for interval config must be a negative number")

    conn: MSSqlServerResource = context.resources.sql_server

    query = f"""
    select
        s.AffidavitId as id,
        a.AffidavitUID as uid
    from
        AffidavitStatus s
    left join
        Affidavit a on s.AffidavitID = a.AffidavitID
    where
        s.AffidavitStatus = 'RepairsComplete'
    and
        s.StatusDate <= DATEADD(year, ?, GETDATE())"""

    result = []

    for row in conn.execute(context, query, interval).fetchall():
        result.append({"id": row.id, "uid": row.uid})

    if len(result) == 0:
        raise Failure("No records found for deletion")

    context.log.info(f"🚀 {len(result)} record ids found for removal.")

    return result


@op(
    required_resource_keys={"sql_server"},
    out={"successes": Out(List), "failures": Out(List)},
)
def remove_database_data(
    context: OpExecutionContext,
    records: list[dict],
) -> Tuple[list[dict], list[dict]]:
    conn: MSSqlServerResource = context.resources.sql_server

    successes = []
    failures = []

    for row in records:
        cursor = conn.execute(context, "exec sp_retention ?", row["id"])
        results = cursor.fetchone()

        if results is None:
            raise Failure("No records found that exceed retention period.")

        if results[0] == "Success":
            successes.append(row)
        else:
            failures.append(dict(**row, message=results[0]))

        cursor.close()

    context.log.info(
        f"🗑️ {len(successes)} AffidavitIDs were deleted and will have their files removed."
    )
    context.log.warning(f"⚠️ {len(failures)} AffidavitIDs failed deletion.")

    return successes, failures


def find_files_op_factory(
    name: str,
    resolver: Callable,
    **kwargs,
):

    @op(
        name=name,
        required_resource_keys={"fs_sidewalk_posting"},
    )
    def _inner_op(context: OpExecutionContext, affidavits: list[dict], **kwargs):
        fs: FileShareResource = context.resources.fs_sidewalk_posting

        files = []

        for affidavit in affidavits:
            resolver(fs, affidavit, files)

        context.log.info(f"Found {len(files)} for deletion")

        return files

    return _inner_op


def find_diagrams_inner(fs: FileShareResource, affidavit: dict, files: list):
    path = Path(fs.client.host) / "Diagrams" / f"{affidavit['uid']}.vsdx"

    if path.exists():
        files.append(str(path.resolve()))


find_diagrams = find_files_op_factory("find_diagrams", find_diagrams_inner)


def find_pdfs_inner(fs: FileShareResource, affidavit: dict, files: list):
    path = Path(fs.client.host) / "PDF"

    for p in Path(path).glob(f"{affidavit['id']}*.pdf"):
        if p.exists():
            files.append(str(p.resolve()))


find_pdfs = find_files_op_factory("find_pdfs", find_pdfs_inner)


def find_folders_inner(fs: FileShareResource, affidavit: dict, files: list):
    path: Path = Path(fs.client.host) / "AffidavitFolders" / str(affidavit["id"])

    if path.exists() and path.is_dir():
        files.append(str(path.resolve()))


find_folders = find_files_op_factory("find_folders", find_folders_inner)


@op
def log_failed_deletions(context: OpExecutionContext, records: list[dict]):
    if not len(records):
        return

    context.log.error("Printing Affidavit IDs for records that failed deletion...")

    for record in records:
        context.log.error(
            f"AffidavitID: {record['id']}, AffidavitUID: {record['uid']}, Error: {record['message']}"
        )

    raise Failure("There were affidavits that failed to be deleted.")


@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "sql_server": mssql_resource,
        "fs_sidewalk_posting": fileshare_resource,
    }
)
def process_retention():
    affidavits = get_removal_list()

    successes, failures = remove_database_data(affidavits)

    remove_files.alias("remove_diagrams")(find_diagrams(successes))
    remove_files.alias("remove_pdfs")(find_pdfs(successes))
    remove_dirs.alias("remove_folders")(find_folders(successes))

    log_failed_deletions(failures)


@schedule(
    job=process_retention,
    cron_schedule="59 23 * * 0",
    execution_timezone="US/Pacific",
)
def sidewalk_retention_schedule(context: ScheduleEvaluationContext):
    return RunRequest(
        run_key=context.scheduled_execution_time.isoformat(),
        run_config={
            "resources": {
                "sql_server": {"config": {"conn_id": "mssql_server_sidewalk"}},
                "fs_sidewalk_posting": {"config": {"conn_id": "fs_sidewalk_posting"}},
            },
            "ops": {
                "get_removal_list": {
                    "config": {
                        "interval": -2,
                    },
                },
                "remove_folders": {
                    "config": {"recursive": True},
                },
            },
        },
    )


@repository
def retention_repository():
    return [process_retention, sidewalk_retention_schedule]
