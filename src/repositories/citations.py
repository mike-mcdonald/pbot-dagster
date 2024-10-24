from datetime import datetime
from pathlib import Path
from dagster import (
    EnvVar,
    Field,
    List,
    OpExecutionContext,
    Out,
    RunRequest,
    ScheduleEvaluationContext,
    String,
    fs_io_manager,
    job,
    op,
    repository,
    schedule
)
from resources.ssh import SSHClientResource, ssh_resource


@op(
    out=Out(List[String]),
    required_resource_keys=["ssh_client"],
)
def get_citations(context: OpExecutionContext):
    import stat

    sftp = context.resources.ssh_client.connect()
    file_list = []
    try:
        for filename in sftp.listdir():
            current_stat = sftp.stat(filename)
            if stat.S_ISREG(current_stat.st_mode):
                if Path(filename).suffix == ".CSV":
                    file_list.append(str(filename))
    except Exception as err:
        raise err
    finally:
        sftp.close()

    context.log.info(f" 👮 {len(file_list)} citations files - {file_list}")
    return file_list


@op(
    config_schema={
        "exec_path": Field(String, description="Download files to this location")
    },
    out=Out(List[String]),
    required_resource_keys=["ssh_client"],
)

def download_citations(context: OpExecutionContext, file_list: list[str]):
    trace = datetime.now()
    try:
        sftp = context.resources.ssh_client.connect()
        for file in file_list:
            download_path = Path(context.op_config["exec_path"])
            download_path.mkdir(parents=True, exist_ok=True)
            download_path = download_path / Path(file).name
            sftp.get(file, download_path)
    except Exception as err:
        raise err
    finally:
        sftp.close()
    context.log.info(f"👮 Downloaded {len(file_list)} files took {datetime.now() - trace}")

    return file_list

@op(
    config_schema={
        "exec_path": Field(String, description="Download files to this location")
    },
)
def write_csv(context: OpExecutionContext, file_list: list[str]):
    import csv

    try:
        csv_filename = Path(context.op_config["exec_path"]) / Path("citations.csv")
        with open(csv_filename, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            for file in file_list:
                fullname =  str(Path(context.op_config["exec_path"]) / Path(file).name)
                writer.writerow([fullname])
    except Exception as err:
        raise err

    return file_list

@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "ssh_client": ssh_resource,
    },
)
def process_citations():
    write_csv(download_citations(get_citations()))

@schedule(
    job=process_citations,
    cron_schedule="0 12 * * *",
    execution_timezone="US/Pacific",
)
def citations_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time
    execution_date = execution_date.isoformat()
    execution_date_path = f"{EnvVar('CITATION_PATH').get_value()}{execution_date.strftime('%Y%m%dT%H%M%S')}"

    return RunRequest(
        run_key=execution_date,
        run_config={
            "resources": {
                "ssh_client": {
                    "config": {
                        "conn_id": "sftp_citation",
                    }
                }
            },
            "ops": {
                "download_citations": {
                    "config": {
                        "exec_path": f"{execution_date_path}",
                    }
                },
                "write_csv": {
                    "config": {
                        "exec_path": f"{execution_date_path}",
                    }
                },
            },
        },
    )


@repository
def citations_repository():
    return [process_citations, citations_schedule]
