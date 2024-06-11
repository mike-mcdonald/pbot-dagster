import os
from dagster import (
    EnvVar,
    Field,
    In,
    OpExecutionContext,
    Out,
    RunRequest,
    ScheduleEvaluationContext,
    String,
    fs_io_manager,
    job,
    op,
    repository,
    schedule,
)

from datetime import datetime, timedelta, timezone
from ftplib import FTP
from ops.template import apply_substitutions
from pathlib import Path

@op(
    config_schema={
        "ftp_hostname": Field(
            String,
            description = "ftp hostname"
        ),
        "ftp_username": Field(
            String,
            description = "ftp username"
        ),
        "ftp_password": Field(
            String,
            description = "ftp password"
        ),
        "ftp_uber_path": Field(
            String,
            description = "ftp uber path"
        ),
        "ftp_lyft_path": Field(
            String,
            description = "ftp lyft path"
        ),
        "download_path": Field(
            String,
            description = "Download files to this location"
        )
    }
)
def download_files(context: OpExecutionContext):


    def download_from_folder(ftp: FTP, folder_path: str):
        ftp.cwd(folder_path)
        filenames = ftp.nlst()  # Get list of files in the current directory
        for filename in filenames:
            context.log.info(f"🪪 {folder_path} - files {filenames}")
            if filename.endswith('.pdf'):
                with open(os.path.join(download_path, filename), 'wb') as local_file:
                    ftp.retrbinary(f'RETR {filename}', local_file.write)

    # Connect to FTP Server
    hostname = context.op_config["ftp_hostname"]
    username = context.op_config["ftp_username"]
    password= context.op_config["ftp_password"]
    uber_path= context.op_config["ftp_uber_path"]
    lyft_path= context.op_config["ftp_lyft_path"]

    download_path= context.op_config["download_path"]
    #ftp = FTP(host=hostname,user=username, passwd=password,encoding='utf-8')

    ftp = FTP(host=hostname)
    ftp.login(username, password)
    ftp.set_pasv(False)
    context.log.info(f"🪪 server {ftp}")
    download_from_folder(uber_path)
    download_from_folder(lyft_path)



# @op
#     config_schema={

#      "pdf_path": Field(
#         String,
#         description = "Path of pdf to analyze"
#      ),
#      "download_path": Field(
#         String,
#         description = "Download files to this location"
#      )
#     }

# )
# def analyze_files(context: OpExecutionContext):


@job(
    resource_defs={
        "io_manager": fs_io_manager,
    }
)
def process_pdfs():
    download_files()
    #analyze_files()

@schedule(
    job=process_pdfs,
    cron_schedule="* */1 * * *",
    execution_timezone="US/Pacific",
)
def pdfanalyzer_schedule(context: ScheduleEvaluationContext):
    execution_date = context.scheduled_execution_time
    execution_date = execution_date.isoformat()
    execution_date_path = f"{EnvVar('PDF_ANALYZER_PATH').get_value()}{execution_date.strftime('%Y%m%dT%H%M%S')}"

    return RunRequest(
        run_key=execution_date,
        run_config={
            "ops": {
                 "download_files": {
                    "config": {
                        "ftp_hostname": EnvVar("FTP_HOSTNAME").get_value(),
                        "ftp_username": EnvVar("FTP_USERNAME").get_value(),
                        "ftp_password": EnvVar("FTP_PASSWORD").get_value(),
                        "ftp_uber_path": EnvVar("FTP_UBER_PATH").get_value(),
                        "ftp_lyft_path": EnvVar("FTP_LYFT_PATH").get_value(),
                        "download_path": f"{execution_date_path}",
                    },
                },
                "analyze_files": {
                    "config": {
                         "download_path": EnvVar("PDF_ANALYZER_PATH").get_value(),
                    },
                },
            },
        }
    )

@repository
def pdfanalyzer_repository():
    return [process_pdfs,pdfanalyzer_schedule]
