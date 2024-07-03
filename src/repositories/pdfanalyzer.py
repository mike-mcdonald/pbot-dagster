import csv
import shutil
import pdfquery
import os
import re

from dagster import (
    EnvVar,
    Failure,
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

from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from ops.template import apply_substitutions
from paramiko import SSHClient, SFTPClient
from pathlib import Path
from resources.ssh import SSHClientResource, ssh_resource

def is_date_within_a_year(report_date : str):
    prev_year = datetime.now() - relativedelta(years = 1)
    report_date =datetime.strptime(report_date, "%m/%d/%Y")
    if (report_date > prev_year):   
        return True
    else:
        return False

def date_fixer(in_date, format="%m/%d/%Y"):    
    return parse(str(in_date)).strftime(format)

def get_date_mmddyyyy(label : pdfquery):
    m = re.search(r"\d\d/\d\d/\d\d\d\d",label)
    try:
        if m:
            return date_fixer(m[0])
    except:
        raise Exception

def report_orderdate(pdf: pdfquery):
    datelabel = pdf.pq('LTTextLineHorizontal:contains("Order date")')
    if (datelabel.attr('x0')) is not None:
        left_corner = float(datelabel.attr('x0'))
        bottom_corner = float(datelabel.attr('y0'))
        x1_corner = float(datelabel.attr('x1'))
        y1_corner = float(datelabel.attr('y1'))
        ldate = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (left_corner, bottom_corner, x1_corner, y1_corner)).text()
        report_date = get_date_mmddyyyy(ldate)
        return report_date
    else:
        return None

def get_date_bddyyyy(label : pdfquery):                                            

    m = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)?[\s]\d{1,2}?[,\s]?[\s]\d{4}', label)
    try:
        if m is not None:
            #strdate =  datetime.strptime(m[0], '%b %d, %Y').strftime('%m/%d/%Y')
            sdate =  datetime.strptime(m[0], '%b %d, %Y')
        else:
            # Check for this date format  dd mon yyyy
            m = re.search(r'\d{1,2}?[\s](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)?[\s]\d{4}', label)
            #strdate =  datetime.strptime(m[0], '%d %b %Y').strftime('%m/%d/%Y') 
            sdate =  datetime.strptime(m[0], '%d %b %Y')
    except:
        raise Exception

    return date_fixer(sdate)


def report_completeddate(filepath: Path):
    pdf = pdfquery.PDFQuery(filepath)
    pdf.load()
    datelabel = pdf.pq('LTTextLineHorizontal:contains("Report completed on")')
    pdf.file.close()
    if (datelabel.attr('x0')) is not None:
        left_corner = float(datelabel.attr('x0'))
        bottom_corner = float(datelabel.attr('y0'))
        x1_corner = float(datelabel.attr('x1'))
        y1_corner = float(datelabel.attr('y1'))
        ldate = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (left_corner, bottom_corner, x1_corner, y1_corner)).text()
        report_date = get_date_bddyyyy(ldate)
        return report_date
    else:
        return None
    
def report_orderdate(filepath: Path):
    pdf = pdfquery.PDFQuery(filepath)
    pdf.load()
    datelabel = pdf.pq('LTTextLineHorizontal:contains("Order date")')
    pdf.file.close()
    if (datelabel.attr('x0')) is not None:
        left_corner = float(datelabel.attr('x0'))
        bottom_corner = float(datelabel.attr('y0'))
        x1_corner = float(datelabel.attr('x1'))
        y1_corner = float(datelabel.attr('y1'))
        ldate = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (left_corner, bottom_corner, x1_corner, y1_corner)).text()
        report_date = get_date_mmddyyyy(ldate)   #different format than in report_completeddate
        return report_date
    else:
        return None
    
def find_status(filepath: Path):
    pdf = pdfquery.PDFQuery(filepath)
    pdf.load()
    label = pdf.pq('LTTextLineHorizontal:contains("Status")') 
    pdf.file.close()
    if (label.attr('x0')) is not None:
        left_corner = float(label.attr('x0'))
        bottom_corner = float(label.attr('y0'))
        status_label = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (left_corner, bottom_corner-30, left_corner+150, bottom_corner)).text()
        if (re.search('Clear', status_label)):
            return True, 'Clear'    
        else:
            return False, 'Status is not "Clear"'
    else:
        return False, 'Unable to read Status'

def find_violations(filepath: Path):
    pdf = pdfquery.PDFQuery(filepath)
    pdf.load()
    label = pdf.pq('LTTextLineHorizontal:contains("There are no violations in the report.")')
    pdf.file.close()
    if (label.attr('x0')) is not None:
        return  True, 'No violations'
    else:
        return  False, 'Found violations in report'
    
def find_pattern(filename: str):
    m = re.search('_(BGC|MVR|DMV).pdf$', filename)
    if (m is not None):
        return m.group()
    return None


@op(
    config_schema={
        "driver_co": Field(
            String,
            description= "Uber or Lyft"
        )
    },
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
        "base_path": Field(
            String,
            description = "Uber / Lyft files location"
        ),
    },
    out=Out(
        List[String],
    ),
    required_resource_keys=["ssh_client"],
)   
def delete_file(context: OpExecutionContext):
    ssh_client: SSHClientResource = context.resources.ssh_client
    sftp = ssh_client.get_sftpClient()
    sftp.chdir(None)
    sftp.chdir(context.op_config["base_path"])
    filenames = sftp.listdir()  # Get list of files in the current directory
    if len(filenames) > 0:
        for filename in filenames:   
            sftp.remove(filename)
        context.log.info(f" 🚗 Deleting {len(filenames)} files from {context.op_config['base_path']} . ")
    return filenames
    

@op(
    config_schema={
        "base_path": Field(
            String,
            description = "Uber / Lyft files location"
        ),
    },
    out=Out(
        List[String],
    ),
    required_resource_keys=["ssh_client"],
)   
def move_file(context: OpExecutionContext):
    ssh_client: SSHClientResource = context.resources.ssh_client
    sftp = ssh_client.get_sftpClient()
    sftp.chdir(None)
    sftp.chdir(context.op_config["base_path"])
    filenames = sftp.listdir()  # Get list of files in the current directory
    if len(filenames) > 0:
        file_list = []
        for filename in filenames: 
            if not find_pattern(filename):  
                targetfile = Path(filename).parent / Path("ValidationFolder") / Path(filename).name
                sftp.rename(filename,str(targetfile))
                context.log.info(f" 🚗 Moving {filename} to {targetfile}")
                file_list.append(filename) 
        context.log.info(f" 🚗 Moving {len(file_list)} files from {context.op_config['base_path']} to ValidationFolder")
    return file_list
    
@op(
    config_schema={
        "base_path": Field(
            String,
            description= "Uber or Lyft"
        ),
        "local_csv_file": Field(
            String,
            description= "Name of Uber or Lyft csv file "
        ),
    },
    out={"results": Out(List)},
    required_resource_keys=["ssh_client"],
)   
def upload_file(context: OpExecutionContext, results: list[dict]):
    trace = datetime.now()
    try:
        ssh_client: SSHClientResource = context.resources.ssh_client
        sftp = ssh_client.get_sftpClient()
        sftp.chdir(None)
        for row in results:
            sourcefile = Path(row["Renamedfile"])
            targetfile = Path(row["Ftpfile"]).parent / Path("ValidationFolder") / Path(row["Renamedfile"]).name
            context.log.info(f" 🚗 Move {sourcefile} to {targetfile}.")
            sftp.put(sourcefile, str(targetfile))

        context.log.info(f" 🚗 Uploaded {len(results)} files in {datetime.now() - trace}.")
        
        local_csv_file = Path(context.op_config["local_csv_file"])   
        #targetfile = Path(context.op_config["base_path"]) / Path("ResultFolder") / Path(local_csv_file).name
        targetfile = Path(context.op_config["base_path"]) / Path("ValidationFolder") / Path(local_csv_file).name
        context.log.info(f" 🚗 Analysis file: {local_csv_file} to {targetfile}.")
        sftp.put(local_csv_file,targetfile)
    except:
          raise Exception
    return results

@op(
    config_schema={
        "csv_filename": Field(
            String,
            description= "Name of Uber or Lyft csv file "
        )
    },
)   
def write_csv(context: OpExecutionContext, records: list[dict]):
    csv_filename  = Path(context.op_config["csv_filename"]) 
    with open(csv_filename, 'w', newline ='') as csv_file: 
        writer = csv.DictWriter(csv_file, fieldnames = ['Ftpfile', 'Localfile', 'Status', 'Renamedfile', 'Reportdate', 'Analysis'] ) 
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
    file_list = []
    if len(list_of_list) > 0 :
        for results in list_of_list:
            if len(results) > 0:
                try:
                    for row in results:
                        sourcefile = Path(row["Localfile"])
                        targetfile = Path(row["Renamedfile"])
                        shutil.move(sourcefile, targetfile)
                        file_list.append(row)
                except:
                    raise Exception
    return file_list
    
@op(
    config_schema={
        "download_path": Field(
            String,
            description = "Location of downloaded files"
        ),
    },    
    ins={
        "download_files": In(List, description="List of files downloaded"),
    },
    out=Out(List),
)   
def analyze_bgcfiles(context: OpExecutionContext, download_files: list[str]):
    results = []
    trace = datetime.now()
    if len(download_files) > 0:
        filecount = 0
        download_path = Path(context.op_config["download_path"]) 
        for ftpfile in download_files:
            localfile = download_path / Path(ftpfile).name
            match_filename = find_pattern(ftpfile)
            status = False
            analysis = []
            reportdate = None       
            status, reason = find_status(localfile)
            analysis.append(reason)
            reportdate = report_completeddate(localfile)
            if reportdate is None:
                reason = 'Unable to process date'
                status = False
                analysis.append(reason)
            elif not is_date_within_a_year(reportdate):
                reason = ' Report date is older than 1 year'
                status = False
                analysis.append(reason)
            if status:
                pass_filename  = match_filename.split('.')[0] + "_Pass" + ".pdf"
                renamedfile = re.sub(match_filename,pass_filename,str(localfile),flags=re.IGNORECASE)  
                results.append({"Ftpfile": ftpfile, "Localfile": str(localfile),"Status": "Pass", "Renamedfile": renamedfile, "Reportdate": reportdate, "Analysis": '; '.join(analysis)})
            else:
                fail_filename  = match_filename.split('.')[0] + "_Fail" + ".pdf"
                renamedfile = re.sub(match_filename,fail_filename,str(localfile),flags=re.IGNORECASE)  
                results.append({"Ftpfile": ftpfile, "Localfile": str(localfile), "Status": "Fail", "Renamedfile": renamedfile, "Reportdate": reportdate, "Analysis": '; '.join(analysis)})
            filecount += 1          
    context.log.info(f"Checked {len(results)} new files in {datetime.now() - trace}.")
    return  results


@op(
    config_schema={
        "download_path": Field(
            String,
            description = "Location of downloaded files"
        ),
    },    
    ins={
        "download_files": In(List, description="List of files downloaded"),
    },
    out=Out(List),
)   
def analyze_dmvfiles(context: OpExecutionContext, download_files: list[str]):
    results = []
    trace = datetime.now()
    if len(download_files) > 0:
        filecount = 0
        download_path = Path(context.op_config["download_path"]) 
        for ftpfile in download_files:
            localfile = download_path / Path(ftpfile).name
            match_filename = find_pattern(ftpfile)
            status = False
            analysis = []
            reportdate = None    
            status, reason = find_violations(localfile)
            analysis.append(reason)
            reportdate = report_orderdate(localfile)
            if reportdate is None:
                reason = 'Unable to process date'
                status = False
                analysis.append(reason)
            elif not is_date_within_a_year(reportdate):
                reason = ' Report date is older than 1 year'
                status = False
                analysis.append(reason)
            if status:
                pass_filename  = match_filename.split('.')[0] + "_Pass" + ".pdf"
                renamedfile = re.sub(match_filename,pass_filename,str(localfile),flags=re.IGNORECASE)  
                results.append({"Ftpfile": ftpfile, "Localfile": str(localfile),"Status": "Pass", "Renamedfile": renamedfile, "Reportdate": reportdate, "Analysis": '; '.join(analysis)})
            else:
                fail_filename  = match_filename.split('.')[0] + "_Fail" + ".pdf"
                renamedfile = re.sub(match_filename,fail_filename,str(localfile),flags=re.IGNORECASE)  
                results.append({"Ftpfile": ftpfile, "Localfile": str(localfile), "Status": "Fail", "Renamedfile": renamedfile, "Reportdate": reportdate, "Analysis": '; '.join(analysis)})
            filecount += 1          
    context.log.info(f"Checked {len(results)} new files in {datetime.now() - trace}.")
    return  results


@op(
    config_schema={
        "download_path": Field(
            String,
            description = "Download files to this location"
        )
    },  
    out=Out(
        List[String],
    ),
    required_resource_keys=["ssh_client"],
)
def download_files(context: OpExecutionContext, list_of_list: list[list[str]]):
    file_list = []
    if len(list_of_list) > 0 :
        for download_list in list_of_list: 
            if len(download_list) > 0:
                try:
                    ssh_client: SSHClientResource = context.resources.ssh_client
                    sftp = ssh_client.get_sftpClient()
                    download_path = Path(context.op_config["download_path"])
                    download_path.mkdir(parents=True, exist_ok=True) 
                    trace = datetime.now()
                    for filename in download_list: 
                        sftp.get(filename, download_path / Path(filename).name)
                        file_list.append(filename) 
                    context.log.info(f" 🚗 Downloaded {len(download_list)} files in {datetime.now() - trace}.")
                except:
                    raise Exception
    return file_list


@op(
    config_schema={
        "base_path": Field(
            String,
            description = "Uber / Lyft files location"
        ),
        "match_filename": Field(
            String,
            description = "match this filename for *bgc.pdf or *mvr.pdf files"
        ),
    },
    out=Out(
        List[String],
    ),
    required_resource_keys=["ssh_client"],
)   
def get_list(context: OpExecutionContext):
    ssh_client: SSHClientResource = context.resources.ssh_client
    sftp = ssh_client.get_sftpClient()
    sftp.chdir(None)
    sftp.chdir(context.op_config["base_path"])
    filenames = sftp.listdir()  # Get list of files in the current directory
    if len(filenames) > 0:
        file_list = []
        for filename in filenames:   
            if (re.search(context.op_config["match_filename"], filename)):
                context.log.info(f" 🚗 {filename} - {context.op_config['match_filename']}")
                file = os.path.join(context.op_config["base_path"],filename)
                file_list.append(file)
                if len(file_list) == 5:
                    context.log.info(f" 🚗 {file_list}")
                    return file_list
        context.log.info(f" 🚗 {context.op_config['base_path']} has {len(file_list)} files matching {context.op_config['match_filename']} ")
    return file_list
    

@job(
    resource_defs={
        "io_manager": fs_io_manager,
        "ssh_client": ssh_resource,
    }
)
def process_pdfs():
    
    files = []
    files.append(get_list.alias("get_uber_bgc")())
    files.append(get_list.alias("get_lyft_bgc")())
    files.append(get_list.alias("get_uber_mvr")())
    
    dmv_files  = []
    dmv_files.append(get_list.alias("get_lyft_dmv")())
  
    results = []
    results.append(analyze_bgcfiles(download_files.alias("download_bgc")(files)))
    results.append(analyze_dmvfiles(download_files.alias("download_dmv")(dmv_files)))
    
    all_files = rename_files(results)
    
    upload_file.alias("upload_uber")(write_csv.alias("write_uber")(get_files.alias("get_uber_files")(all_files)))
    upload_file.alias("upload_lyft")(write_csv.alias("write_lyft")(get_files.alias("get_lyft_files")(all_files)))
    #upload_file(write_csv(rename_files(results)))
    # move_file.alias("move_uber")
    # move_file.alias("move_lyft")
    # delete_file.alias("delete_uber")
    # delete_file.alias("delete_lyft")


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
            "resources":{
                "ssh_client":{
                    "config":{
                        "conn_id": "sftp_pdfanalyzer",
                    }
                }
            },
            "ops": {
                "get_uber_bgc": {
                    "config":{
                         "base_path": "Uber_Background_Docs",
                         "match_filename": "_BGC.pdf$",
                    },
                },
                "get_lyft_bgc": {
                    "config":{
                         "base_path": "Lyft_Background_Docs",
                         "match_filename": "_BGC.pdf$",
                    },
                },
                "download_bgc": {
                    "config":{
                         "download_path": f"{execution_date_path}",
                    }
                },
                "get_uber_mvr": {
                    "config":{
                         "base_path": "Uber_Background_Docs",
                         "match_filename": "_MVR.pdf$",
                    },
                },
                "get_lyft_dmv": {
                    "config":{
                         "base_path": "Lyft_Background_Docs",
                         "match_filename": "_DMV.pdf$",
                    },
                },
                "download_dmv": {
                    "config":{
                         "download_path": f"{execution_date_path}",
                    }
                },
                "analyze_bgc": {
                    "config": {
                         "download_path": f"{execution_date_path}",
                    },
                },
                "analyze_mvr": {
                    "config": {
                         "download_path": f"{execution_date_path}",
                    },
                },
                "get_uber_files": {
                    "config":{
                         "driver_co": "Uber",
                    },
                },
                "get_lyft_files": {
                    "config":{
                          "driver_co": "Lyft",
                    },
                },
                "write_uber": {
                    "config":{
                         "csv_filename": f"{execution_date_path}/{execution_date}Uber_drivers.csv",
                    }
                },
                "write_lyft": {
                    "config":{
                         "csv_filename": f"{execution_date_path}/{execution_date}Lyft_drivers.csv",
                    }
                },
                "upload_uber": {
                    "config":{
                         "base_path": "Uber_Background_Docs",
                         "local_csv_file": f"{execution_date_path}/{execution_date}Uber_drivers.csv",
                    },
                },
                "upload_lyft": {
                    "config":{
                         "base_path": "Lyft_Background_Docs",
                         "local_csv_file": f"{execution_date_path}/{execution_date}Lyft_drivers.csv",
                    },
                },
                "move_uber": {
                    "config":{
                         "base_path": "Uber_Background_Docs",
                    },
                },
                "move_lyft": {
                    "config":{
                         "base_path": "Lyft_Background_Docs",
                    },
                },
                "delete_uber": {
                    "config":{
                         "base_path": "Uber_Background_Docs",
                    },
                },
                "delete_lyft": {
                    "config":{
                         "base_path": "Lyft_Background_Docs",
                    },
                },
            },
        }
    )

@repository
def pdfanalyzer_repository():
    return [process_pdfs,pdfanalyzer_schedule]