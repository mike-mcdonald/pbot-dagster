import csv
import shutil
import pdfquery
import re

from concurrent.futures import ThreadPoolExecutor

from dagster import (
    EnvVar,
    Field,
    In,
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
    schedule,
)

from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from ops.fs.remove import remove_files
from ops.template import apply_substitutions
from pathlib import Path
from resources.ssh import ssh_resource


def is_date_within_a_year(report_date: str):
    prev_year = datetime.now() - relativedelta(years=1)
    report_date = datetime.strptime(report_date, "%m/%d/%Y")
    if report_date > prev_year:
        return True
    else:
        return False


def date_fixer(in_date, format="%m/%d/%Y"):
    return parse(str(in_date)).strftime(format)


def get_date_mmddyyyy(label: pdfquery):
    m = re.search(r"\d\d/\d\d/\d\d\d\d", label)
    try:
        if m:
            return date_fixer(m[0])
    except:
        raise Exception


def report_orderdate(pdf: pdfquery):
    datelabel = pdf.pq('LTTextLineHorizontal:contains("Order date")')
    if (datelabel.attr("x0")) is not None:
        left_corner = float(datelabel.attr("x0"))
        bottom_corner = float(datelabel.attr("y0"))
        x1_corner = float(datelabel.attr("x1"))
        y1_corner = float(datelabel.attr("y1"))
        ldate = pdf.pq(
            'LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")'
            % (left_corner, bottom_corner, x1_corner, y1_corner)
        ).text()
        report_date = get_date_mmddyyyy(ldate)
        return report_date
    else:
        return None


def get_date_bddyyyy(label: pdfquery):

    m = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)?[\s]\d{1,2}?[,\s]?[\s]\d{4}",
        label,
    )
    try:
        if m is not None:
            # strdate =  datetime.strptime(m[0], '%b %d, %Y').strftime('%m/%d/%Y')
            sdate = datetime.strptime(m[0], "%b %d, %Y")
        else:
            # Check for this date format  dd mon yyyy
            m = re.search(
                r"\d{1,2}?[\s](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)?[\s]\d{4}",
                label,
            )
            # strdate =  datetime.strptime(m[0], '%d %b %Y').strftime('%m/%d/%Y')
            sdate = datetime.strptime(m[0], "%d %b %Y")
    except:
        raise Exception

    return date_fixer(sdate)


def report_completeddate(filepath: Path):
    pdf = pdfquery.PDFQuery(filepath)
    pdf.load()
    datelabel = pdf.pq('LTTextLineHorizontal:contains("Report completed on")')
    pdf.file.close()
    if (datelabel.attr("x0")) is not None:
        left_corner = float(datelabel.attr("x0"))
        bottom_corner = float(datelabel.attr("y0"))
        x1_corner = float(datelabel.attr("x1"))
        y1_corner = float(datelabel.attr("y1"))
        ldate = pdf.pq(
            'LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")'
            % (left_corner, bottom_corner, x1_corner, y1_corner)
        ).text()
        report_date = get_date_bddyyyy(ldate)
        return report_date
    else:
        return None


def report_orderdate(filepath: Path):
    pdf = pdfquery.PDFQuery(filepath)
    pdf.load()
    datelabel = pdf.pq('LTTextLineHorizontal:contains("Order date")')
    pdf.file.close()
    if (datelabel.attr("x0")) is not None:
        left_corner = float(datelabel.attr("x0"))
        bottom_corner = float(datelabel.attr("y0"))
        x1_corner = float(datelabel.attr("x1"))
        y1_corner = float(datelabel.attr("y1"))
        ldate = pdf.pq(
            'LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")'
            % (left_corner, bottom_corner, x1_corner, y1_corner)
        ).text()
        report_date = get_date_mmddyyyy(
            ldate
        )  # different format than in report_completeddate
        return report_date
    else:
        return None


def find_status(filepath: Path):
    pdf = pdfquery.PDFQuery(filepath)
    pdf.load()
    label = pdf.pq('LTTextLineHorizontal:contains("Status")')
    pdf.file.close()
    if (label.attr("x0")) is not None:
        left_corner = float(label.attr("x0"))
        bottom_corner = float(label.attr("y0"))
        status_label = pdf.pq(
            'LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")'
            % (left_corner, bottom_corner - 30, left_corner + 150, bottom_corner)
        ).text()
        if re.search("Clear", status_label):
            return True, "Clear"
        else:
            return False, 'Status is not "Clear"'
    else:
        return False, "Unable to read Status"


def find_violations(filepath: Path):
    pdf = pdfquery.PDFQuery(filepath)
    pdf.load()
    label = pdf.pq(
        'LTTextLineHorizontal:contains("There are no violations in the report.")'
    )
    pdf.file.close()
    if (label.attr("x0")) is not None:
        return True, "No violations"
    else:
        return False, "Found violations in report"


def find_pattern(filename: str):
    m = re.search("_(BGC|MVR|DMV).pdf$", filename)
    if m is not None:
        return m.group()
    return None


def is_other_file(filename: str, ids: list[str]):
    id = filename.split("_")[0]
    if id in ids and find_pattern(filename) is None:
        return True
    else:
        return False


@op(
    config_schema={"driver_co": Field(String, description="Uber or Lyft")},
    out=Out(
        List[String],
    ),
    required_resource_keys=["ssh_client"],
)
def get_other_files(context: OpExecutionContext, allfiles: list[str]):
    other = []
    driver_co = context.op_config["driver_co"]
    trace = datetime.now()
    process_files = ["_BGC.pdf$", "_MVR.pdf$", "_DMV.pdf$"]
    for filename in allfiles:
        if find_pattern(filename) is None:
            if driver_co in filename:
                other.append(filename)

    context.log.info(
        f" 🚗 Get other files  for {driver_co}: {len(other)} in {datetime.now() - trace}."
    )
    return other


@op(
    config_schema={"driver_co": Field(String, description="Uber or Lyft")},
    out=Out(
        List[dict],
    ),
    required_resource_keys=["ssh_client"],
)
def get_files(context: OpExecutionContext, files: list[dict]):
    driver_co = context.op_config["driver_co"]
    records = []
    for row in files:
        if driver_co in row["Ftpfile"]:
            records.append(row)
    return records


@op(
    config_schema={
        "base_path": Field(String, description="Uber or Lyft"),
        "local_csv_file": Field(String, description="Name of Uber or Lyft csv file "),
    },
    out=Out(List[String]),
    required_resource_keys=["ssh_client"],
)
def upload_files(context: OpExecutionContext, results: list[dict]):
    files = []
    base_path = Path(context.op_config["base_path"])
    sftp = context.resources.ssh_client.connect()
    trace = datetime.now()
    try:
        for row in results:
            sourcefile = Path(row["Renamedfile"])
            targetfile = str(
                Path(base_path)
                / Path("ValidationFolder")
                / Path(row["Renamedfile"]).name
            )
            context.log.info(f" 🚗 Upload {sourcefile} to {targetfile}")
            sftp.put(sourcefile, targetfile)
            files.append(str(sourcefile))

        # upload csv file
        sourcefile = Path(context.op_config["local_csv_file"])
        targetfile = str(Path(base_path) / Path("ResultFolder") / Path(sourcefile).name)
        # targetfile = Path(context.op_config["base_path"]) / Path("ValidationFolder") / Path(sourcefile).name
        context.log.info(f" 🚗 Analysis file uploaded: {sourcefile} to {targetfile}")
        sftp.put(sourcefile, targetfile)
        files.append(str(sourcefile))

    except Exception as err:
        raise err
    finally:
        sftp.close()

    context.log.info(f" 🚗 Uploaded {len(results)} files in {datetime.now() - trace}.")

    return files


@op(
    config_schema={
        "csv_filename": Field(String, description="Name of Uber or Lyft csv file ")
    },
)
def write_csv(context: OpExecutionContext, records: list[dict]):
    csv_filename = Path(context.op_config["csv_filename"])
    with open(csv_filename, "w", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "Ftpfile",
                "Localfile",
                "Status",
                "Renamedfile",
                "Reportdate",
                "Analysis",
            ],
        )
        writer.writeheader()
        for row in records:
            writer.writerow(row)
    return records


@op(
    out=Out(
        List[dict],
    ),
)
def rename_files(context: OpExecutionContext, list_of_list: list[list[dict]]):

    files = []
    for l in list_of_list:
        files.extend(l)

    if len(files) > 0:
        # context.log.info(f" 🚗 Renaming: {results}")
        try:
            for row in files:
                sourcefile = Path(row["Localfile"])
                targetfile = Path(row["Renamedfile"])
                shutil.move(sourcefile, targetfile)
        except:
            raise Exception
    return files


@op(
    out=Out(List),
)
def analyze_bgcfiles(context: OpExecutionContext, list_of_list: list[list[str]]):
    results = []
    futures = []
    bgc_list = []

    for l in list_of_list:
        bgc_list.extend(l)

    with ThreadPoolExecutor(max_workers=3) as executor:
        trace = datetime.now()
        context.log.info(f" 🚗 Count bgc files: {len(bgc_list)}")
        if len(bgc_list) > 0:
            for localfile in bgc_list:

                def analyze(localfile):
                    match_filename = find_pattern(localfile)
                    context.log.info(f" 🚗 Analyzing bgc {localfile}")
                    dir = Path(localfile).parent.name
                    # split Path(dir) because using Path(localfile).parent.name / Path(localfile).name will fail
                    ftpfile = Path(dir) / Path(localfile).name
                    localfile = Path(localfile)
                    if match_filename is None:
                        context.log.info(
                            f" 🚗 filename does not match _BGC, MVR or DMV.pdf"
                        )
                    status = False
                    analysis = []
                    reportdate = None
                    status, reason = find_status(localfile)
                    analysis.append(reason)
                    reportdate = report_completeddate(localfile)
                    if reportdate is None:
                        reason = "Unable to process date"
                        status = False
                        analysis.append(reason)
                    elif not is_date_within_a_year(reportdate):
                        reason = " Report date is older than 1 year"
                        status = False
                        analysis.append(reason)
                    if status:
                        pass_filename = match_filename.split(".")[0] + "_Pass" + ".pdf"
                        renamedfile = re.sub(
                            match_filename,
                            pass_filename,
                            str(localfile),
                            flags=re.IGNORECASE,
                        )
                        result = {
                            "Ftpfile": str(ftpfile),
                            "Localfile": str(localfile),
                            "Status": "Pass",
                            "Renamedfile": renamedfile,
                            "Reportdate": reportdate,
                            "Analysis": "; ".join(analysis),
                        }
                        return result
                    else:
                        fail_filename = match_filename.split(".")[0] + "_Fail" + ".pdf"
                        renamedfile = re.sub(
                            match_filename,
                            fail_filename,
                            str(localfile),
                            flags=re.IGNORECASE,
                        )
                        result = {
                            "Ftpfile": str(ftpfile),
                            "Localfile": str(localfile),
                            "Status": "Fail",
                            "Renamedfile": renamedfile,
                            "Reportdate": reportdate,
                            "Analysis": "; ".join(analysis),
                        }
                        return result

                futures.append(executor.submit(analyze, localfile))

            for future in futures:
                try:
                    results.append(future.result())
                except Exception as err:
                    executor.shutdown(cancel_futures=True, wait=True)
                    raise err

    context.log.info(f" 🚗 Analyzed BGC-MVR: {results}")
    context.log.info(f"Checked {len(results)} new files in {datetime.now() - trace}.")
    return results


@op(
    ins={
        "download_files": In(List, description="List of files downloaded"),
    },
    out=Out(List),
)
def analyze_dmvfiles(context: OpExecutionContext, download_files: list[str]):
    futures = []
    results = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        trace = datetime.now()
        if len(download_files) > 0:

            for localfile in download_files:

                def analyze(localfile):
                    # input for find_pattern is type string
                    match_filename = find_pattern(localfile)
                    context.log.info(f" 🚗 Analyzing {localfile}")
                    dir = Path(localfile).parent.name
                    # split Path(dir) because using Path(localfile).parent.name / Path(localfile).name will fail
                    ftpfile = Path(dir) / Path(localfile).name
                    localfile = Path(localfile)
                    status = False
                    analysis = []
                    reportdate = None
                    status, reason = find_violations(localfile)
                    analysis.append(reason)
                    reportdate = report_orderdate(localfile)
                    if reportdate is None:
                        reason = "Unable to process date"
                        status = False
                        analysis.append(reason)
                    elif not is_date_within_a_year(reportdate):
                        reason = " Report date is older than 1 year"
                        status = False
                        analysis.append(reason)
                    if status:
                        pass_filename = match_filename.split(".")[0] + "_Pass" + ".pdf"
                        renamedfile = re.sub(
                            match_filename,
                            pass_filename,
                            str(localfile),
                            flags=re.IGNORECASE,
                        )
                        result = {
                            "Ftpfile": str(ftpfile),
                            "Localfile": str(localfile),
                            "Status": "Pass",
                            "Renamedfile": renamedfile,
                            "Reportdate": reportdate,
                            "Analysis": "; ".join(analysis),
                        }
                        return result
                    else:
                        fail_filename = match_filename.split(".")[0] + "_Fail" + ".pdf"
                        renamedfile = re.sub(
                            match_filename,
                            fail_filename,
                            str(localfile),
                            flags=re.IGNORECASE,
                        )
                        result = {
                            "Ftpfile": str(ftpfile),
                            "Localfile": str(localfile),
                            "Status": "Fail",
                            "Renamedfile": renamedfile,
                            "Reportdate": reportdate,
                            "Analysis": "; ".join(analysis),
                        }
                        return result

                futures.append(executor.submit(analyze, localfile))

            for future in futures:
                try:
                    results.append(future.result())
                except Exception as err:
                    executor.shutdown(cancel_futures=True, wait=True)
                    raise err

    context.log.info(f" 🚗 Analyzed DMV: {results}")
    context.log.info(f"Checked {len(results)} new files in {datetime.now() - trace}.")
    return results


@op(
    config_schema={
        "match_filename": Field(
            String, description="match this filename for *bgc.pdf or *mvr.pdf files"
        ),
    },
    out=Out(
        List[String],
    ),
)
def get_list(context: OpExecutionContext, download: list[str]):

    files = []

    try:
        for file in download:
            if re.search(context.op_config["match_filename"], file):
                files.append(file)
        context.log.info(
            f" 🚗  {len(files)} files matching {context.op_config['match_filename']} "
        )
    except Exception as err:
        raise err

    return files


@op(
    config_schema={
        "base_path": Field(String, description="Uber / Lyft files location"),
    },
    out=Out(List[String]),
    required_resource_keys=["ssh_client"],
)
def get_all(context: OpExecutionContext):
    import stat

    sftp = context.resources.ssh_client.connect()

    file_list = []

    try:
        sftp.chdir(context.op_config["base_path"])
        for filename in sftp.listdir():
            current_stat = sftp.stat(filename)
            if stat.S_ISREG(current_stat.st_mode):
                file = Path(context.op_config["base_path"]) / Path(filename)
                file_list.append(str(file))
        context.log.info(
            f" 🚗 {context.op_config['base_path']} has {len(file_list)} files. "
        )
    except Exception as err:
        raise err
    finally:
        sftp.close()

    context.log.info(f" 🚗 {file_list} ")
    return file_list


@op(
    config_schema={
        "exec_path": Field(String, description="Download files to this location")
    },
    out=Out(List),
    required_resource_keys=["ssh_client"],
)
def download_all(context: OpExecutionContext, list_of_list: list[list[str]]):
    futures = []
    files = []

    downloads = []
    for l in list_of_list:
        downloads.extend(l)

    with ThreadPoolExecutor(max_workers=3) as executor:
        trace = datetime.now()

        for file in downloads:
            download_path = Path(context.op_config["exec_path"]) / Path(file).parent
            download_path.mkdir(parents=True, exist_ok=True)
            download_filename = download_path / Path(file).name

            def download(remote_path, local_path):
                context.log.info(f"Downloading {remote_path}")
                context.resources.ssh_client.download(remote_path, local_path)
                return local_path

            futures.append(executor.submit(download, file, download_filename))

        for future in futures:
            try:
                files.append(str(future.result()))
            except Exception as err:
                executor.shutdown(cancel_futures=True, wait=True)
                raise err

        context.log.info(
            f"Downloading {len(downloads)} files took {datetime.now() - trace}"
        )
        context.log.info(f" 🚗 Local files: {files} ")
    return files


@op(
    config_schema={"base": Field(String, description="Upload files to base")},
    out=Out(List[String]),
    required_resource_keys=["ssh_client"],
)
def upload_all(context: OpExecutionContext, local_files: list[str]):
    backup_files = []
    base = context.op_config["base"]
    sftp = context.resources.ssh_client.connect()
    trace = datetime.now()
    try:

        for f in local_files:
            remote_path = str(Path(base) / Path(f).parent.name / Path(f).name)
            context.log.info(f"Upload {f} to {remote_path}")
            backup_files.append(remote_path)
            sftp.put(f, remote_path)
    except Exception as err:
        raise err
    finally:
        sftp.close()

    context.log.info(
        f"Uploading {len(local_files)} files took {datetime.now() - trace}"
    )

    return backup_files


@op(
    config_schema={
        "remote_folder": Field(String, description="Upload files to this folder")
    },
    out=Out(List[String]),
    required_resource_keys=["ssh_client"],
)
def upload_to_folder(context: OpExecutionContext, local_files: list[str]):
    futures = []
    files = []
    remote_folder = context.op_config["remote_folder"]

    with ThreadPoolExecutor(max_workers=2) as executor:
        trace = datetime.now()

        for f in local_files:
            remote_path = str(Path(remote_folder) / Path(f).name)
            context.log.info(f"Upload {f} to {remote_path}")

            def upload(local_path, remote_path):
                context.resources.ssh_client.upload(local_path, remote_path)
                return local_path

            futures.append(executor.submit(upload, f, remote_path))

        for future in futures:
            try:
                files.append(str(future.result()))
            except Exception as err:
                executor.shutdown(cancel_futures=True, wait=True)
                raise err

        context.log.info(
            f"Uploading {len(local_files)} files took {datetime.now() - trace}"
        )

    return files


@op(
    out=Out(
        List[dict],
    ),
    required_resource_keys=["ssh_client"],
)
def delete_ftp_files(context: OpExecutionContext, files: list[dict]):
    trace = datetime.now()
    try:
        sftp = context.resources.ssh_client.connect()
        for row in files:
            sftp.remove(row["Ftpfile"])
        context.log.info(
            f" 🚗 Deleted {len(files)} FTP files in {datetime.now() - trace}."
        )
    except Exception as err:
        context.log.info(f"Fail to delete {row['Ftpfile']}: {err}")
        raise err
    finally:
        sftp.close()

    return files


@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "ssh_client": ssh_resource,
    },
)
def process_pdfs():

    files = []
    download_files = []

    files.append(get_all.alias("get_uber_docs")())
    files.append(get_all.alias("get_lyft_docs")())
    download_files = download_all(files)

    bgc_files = []
    bgc_files.append(get_list.alias("get_bgc")(download_files))
    bgc_files.append(get_list.alias("get_mvr")(download_files))

    results = []
    results.append(analyze_bgcfiles(bgc_files))
    results.append(analyze_dmvfiles(get_list.alias("get_dmv")(download_files)))

    processed_files = rename_files(results)

    remove_files.alias("remove_uber")(
        upload_files.alias("upload_uber")(
            write_csv.alias("write_uber")(
                get_files.alias("get_uber_files")(processed_files)
            )
        )
    )

    remove_files.alias("remove_lyft")(
        upload_files.alias("upload_lyft")(
            write_csv.alias("write_lyft")(
                get_files.alias("get_lyft_files")(processed_files)
            )
        )
    )

    remove_files.alias("remove_other_uber")(
        upload_to_folder.alias("upload_other_uber_files")(
            get_other_files.alias("other_uber_files")(download_files)
        )
    )
    remove_files.alias("remove_other_lyft")(
        upload_to_folder.alias("upload_other_lyft_files")(
            get_other_files.alias("other_lyft_files")(download_files)
        )
    )


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
            "resources": {
                "ssh_client": {
                    "config": {
                        "conn_id": "sftp_pdfanalyzer",
                    }
                }
            },
            "ops": {
                "get_uber_docs": {
                    "config": {
                        "base_path": "Uber_Background_Docs",
                    },
                },
                "get_lyft_docs": {
                    "config": {
                        "base_path": "Lyft_Background_Docs",
                    },
                },
                "download_all": {
                    "config": {
                        "exec_path": f"{execution_date_path}",
                    }
                },
                "upload_all": {
                    "config": {
                        "backup_ftp_base": f"Vertical_Apps",
                    }
                },
                "get_bgc": {
                    "config": {
                        "match_filename": "_BGC.pdf$",
                    },
                },
                "get_mvr": {
                    "config": {
                        "match_filename": "_MVR.pdf$",
                    },
                },
                "get_dmv": {
                    "config": {
                        "match_filename": "_DMV.pdf$",
                    },
                },
                "get_uber_files": {
                    "config": {
                        "driver_co": "Uber",
                    },
                },
                "get_lyft_files": {
                    "config": {
                        "driver_co": "Lyft",
                    },
                },
                "write_uber": {
                    "config": {
                        "csv_filename": f"{execution_date_path}/Uber_Background_Docs/Uber_drivers_{execution_date}.csv",
                    }
                },
                "write_lyft": {
                    "config": {
                        "csv_filename": f"{execution_date_path}/Lyft_Background_Docs/Lyft_drivers_{execution_date}.csv",
                    }
                },
                "upload_uber": {
                    "config": {
                        "base_path": "Vertical_Apps/Uber_Background_Docs",
                        "local_csv_file": f"{execution_date_path}/Uber_Background_Docs/Uber_drivers_{execution_date}.csv",
                    },
                },
                "upload_lyft": {
                    "config": {
                        "base_path": "Vertical_Apps/Lyft_Background_Docs",
                        "local_csv_file": f"{execution_date_path}/Lyft_Background_Docs/Lyft_drivers_{execution_date}.csv",
                    },
                },
                "other_uber_files": {
                    "config": {
                        "driver_co": "Uber",
                    },
                },
                "other_lyft_files": {
                    "config": {
                        "driver_co": "Lyft",
                    },
                },
                "upload_other_uber_files": {
                    "config": {
                        "base_path": "Vertical_Apps/Uber_Background_Docs/ValidationFolder",
                    },
                },
                "upload_other_lyft_files": {
                    "config": {
                        "base_path": "Vertical_Apps/Lyft_Background_Docs/ValidationFolder",
                    },
                },
            },
        },
    )


@repository
def pdfanalyzer_repository():
    return [process_pdfs, pdfanalyzer_schedule]
