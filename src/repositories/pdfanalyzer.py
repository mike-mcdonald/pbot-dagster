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
        raise Exception(
                f"🔥 Error getting report date: '{ValueError}' - '{ValueError.text}'"
        )

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
        return 'Fail'

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
        raise Exception(
                f"🔥 Error getting report date: '{ValueError}' - '{ValueError.text}'"
        )  

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
        return ''
    
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
        return ''
    
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
            return 'Pass'    
        else:
            return 'Fail'
    else:
        return 'Fail'

def find_violations(filepath: Path):
    pdf = pdfquery.PDFQuery(filepath)
    pdf.load()
    label = pdf.pq('LTTextLineHorizontal:contains("There are no violations in the report.")')
    pdf.file.close()
    if (label.attr('x0')) is not None:
        return  'Pass'
    else:
        return 'Fail'
    
def find_pattern(filename: str):
    m = re.search('_(BGC|MVR|DMV).pdf$', filename)
    if (m is not None):
        return m.group()
    return None

@op(
    ins={
        "results": In(List, description=""),
    },
    out={"results": Out(List)},
    required_resource_keys=["ssh_client"],
)   
def upload_file(context: OpExecutionContext, results: list[dict]):
    trace = datetime.now()
    ssh_client: SSHClientResource = context.resources.ssh_client
    sftp = ssh_client.get_sftpClient()
    sftp.chdir(None)
    for row in results:
        sourcefile = Path(row["Renamedfile"])
        targetfile = Path(row["Ftpfile"]).parent / Path("ValidationFolder") / Path(row["Renamedfile"]).name
        context.op_config(f"🚗 {str(sourcefile)} to {str(targetfile)}")
        sftp.put(sourcefile, targetfile)
        row.update(Action='Uploaded')
        context.op_config(f"🚗 {row}")
    context.log.info(f" 🚗 Uploaded {len(results)} files in {datetime.now() - trace}.")
    return results

@op(
    config_schema={
        "download_path": Field(
            String,
            description = "Location of downloaded files"
        ),
    },
)   
def write_csv(context: OpExecutionContext, records: list[dict]):
    download_path = Path(context.op_config["download_path"]) 
    with open(download_path / 'drivers.csv', 'w', newline ='') as csv_file: 
        writer = csv.DictWriter(csv_file, fieldnames = ['Ftpfile', 'Localfile', 'Status', 'Renamedfile', 'Reportdate', 'Action'] ) 
        writer.writeheader()
        for row in records:
            writer.writerow(row)
    
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
                        context.op_config(f"🚗 {sourcefile} to {targetfile}")
                        shutil.move(sourcefile, targetfile)
                        row.update(Action='Renamed')
                        context.op_config(f"🚗 {row}")
                        file_list.append(row)
                except:
                    raise Exception(
                        f"🔥 Error renaming files: '{ValueError}' - '{ValueError.text}'"
                    )
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
def analyze_files(context: OpExecutionContext, download_files: list[str]):
    results = []
    trace = datetime.now()
    if len(download_files) > 0:
        filecount = 0
        download_path = Path(context.op_config["download_path"]) 
        for ftpfile in download_files:
            localfile = download_path / Path(ftpfile).name
            match_filename = find_pattern(ftpfile)
            status = ''
            reportdate = ''    
            if match_filename in ['_BGC.pdf','_MVR.pdf']:
                status = find_status(localfile)
                reportdate = report_completeddate(localfile)
            else:
                if match_filename in ['_DMV.pdf']:
                    status = find_violations(localfile)
                    reportdate = report_orderdate(localfile)
            if status in 'Pass':
                pass_filename  = match_filename.split('.')[0] + "_Pass" + ".pdf"
                renamedfile = re.sub(match_filename,pass_filename,str(localfile),flags=re.IGNORECASE)  
                results.append({"Ftpfile": ftpfile, "Localfile": str(localfile),"Status": "Pass", "Renamedfile": renamedfile, "Reportdate": reportdate, "Action": 'Validated'})
            else:
                fail_filename  = match_filename.split('.')[0] + "_Fail" + ".pdf"
                renamedfile = re.sub(match_filename,fail_filename,str(localfile),flags=re.IGNORECASE)  
                results.append({"Ftpfile": ftpfile, "Localfile": str(localfile), "Status": "Fail", "Renamedfile": renamedfile, "Reportdate": reportdate, "Action": 'Validated'})
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
                    raise Exception(
                        f"🔥 Error downloading {filename}: '{ValueError}' - '{ValueError.text}'"
                    )  
    return file_list


@op(
    config_schema={
        "base_path": Field(
            String,
            description = "uber files location"
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
                file = os.path.join(context.op_config["base_path"],filename)
                file_list.append(file)
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
    
    mvr_files  = []
    mvr_files.append(get_list.alias("get_uber_mvr")())
    mvr_files.append(get_list.alias("get_lyft_mvr")())
  
    results = []
    results.append(analyze_files.alias("analyze_bgc")(download_files.alias("download_bgc")(files)))
    results.append(analyze_files.alias("analyze_mvr")(download_files.alias("download_mvr")(mvr_files)))
    
    write_csv(rename_files(results))


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
                "get_lyft_mvr": {
                    "config":{
                         "base_path": "Lyft_Background_Docs",
                         "match_filename": "_DMV.pdf$",
                    },
                },
                "download_mvr": {
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
                "write_csv": {
                    "config":{
                         "download_path": f"{execution_date_path}",
                    }
                },
            },
        }
    )

@repository
def pdfanalyzer_repository():
    return [process_pdfs,pdfanalyzer_schedule]