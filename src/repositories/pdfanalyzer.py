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

def is_other_file(filename: str, ids : list[str]):
    id = filename.split('_')[0]  
    if (id in ids  and find_pattern(filename) is None) :
        return True
    else:
        return False

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
def get_other_files(context: OpExecutionContext, ids: list[str]):
    trace = datetime.now()
    try:
        ssh_client: SSHClientResource = context.resources.ssh_client
        sftp = ssh_client.get_sftpClient()
        sftp.chdir(None)
        sftp.chdir(context.op_config["base_path"])
        filenames = sftp.listdir()  # Get list of files in the current directory
        file_list = []
        if len(filenames) > 0:
            for filename in filenames:   
                if is_other_file(filename, ids):
                    file = os.path.join(context.op_config["base_path"],filename)
                    file_list.append(file)
        context.log.info(f" 🚗 Get other files: {len(file_list)} in {datetime.now() - trace}.")
    except Exception as err:
        context.log.info(f"Fail to get other files {filename}: {err}")
        raise err
    return file_list

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
    out=Out(
        List,
    ),
    required_resource_keys=["ssh_client"],
)   
def delete_files(context: OpExecutionContext, results: list[dict]):
    trace = datetime.now()
    try:
        ssh_client: SSHClientResource = context.resources.ssh_client
        sftp = ssh_client.get_sftpClient()
        sftp.chdir(None)
        for row in results:
            sftp.remove(row["Ftpfile"])
        context.log.info(f" 🚗 Deleted {len(results)} files in {datetime.now() - trace}.")
    except Exception as err:
        context.log.info(f"Fail to delete {row['Ftpfile']}: {err}")
        raise err
    return results

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
def move_files(context: OpExecutionContext, filenames: list[str]):
    trace = datetime.now()
    try:
        ssh_client: SSHClientResource = context.resources.ssh_client
        sftp = ssh_client.get_sftpClient()
        sftp.chdir(None)
        sftp.chdir(context.op_config["base_path"])
        for filename in filenames: 
            targetfile = Path(context.op_config["base_path"]) / Path("ValidationFolder") / Path(filename).name
            sftp.rename(str(filename),str(targetfile))
        context.log.info(f" 🚗 Moving {len(filenames)} other files from {context.op_config['base_path']} to ValidationFolder in {datetime.now() - trace}.")
    except Exception as err:
        context.log.info(f"Fail to move other file: {filename}: {err}")
        raise err
    return filenames    

@op(
    out={
         "unique_ids": Out(List),
    },
    required_resource_keys=["ssh_client"],
)  
def get_unique_ids(context: OpExecutionContext, files: list[dict]):
    id_list = []
    for row in files: 
        id_list.append(str(Path(row["Ftpfile"]).name).split('_')[0])
    unique_list = list(set(id_list))
    context.log.info(f" 🚗 {unique_list}")
    return unique_list

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
    out={"results": Out(List),
    },
    required_resource_keys=["ssh_client"],
)   
def upload_files(context: OpExecutionContext, results: list[dict]):
    trace = datetime.now()
    try:
        ssh_client: SSHClientResource = context.resources.ssh_client
        sftp = ssh_client.get_sftpClient()
        sftp.chdir(None)
        for row in results:
            sourcefile = Path(row["Renamedfile"])
            targetfile = Path(row["Ftpfile"]).parent / Path("ValidationFolder") / Path(row["Renamedfile"]).name
            context.log.info(f" 🚗 Upload {sourcefile} to {targetfile}")
            sftp.put(sourcefile, str(targetfile))
        context.log.info(f" 🚗 Uploaded {len(results)} files in {datetime.now() - trace}.")
        
        sourcefile = Path(context.op_config["local_csv_file"])   
        targetfile = Path(context.op_config["base_path"]) / Path("ResultFolder") / Path(sourcefile).name
        #targetfile = Path(context.op_config["base_path"]) / Path("ValidationFolder") / Path(sourcefile).name
        context.log.info(f" 🚗 Analysis file: {sourcefile} to {targetfile}")
        sftp.put(sourcefile,str(targetfile))
    except Exception as err:
        context.log.info(f"Fail to upload {sourcefile} to {targetfile}: {err}")
        raise err
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
                context.log.info(f" 🚗 Renaming: {results}")   
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
            description = "Download files to this location"
        )
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
        for ftpfile in download_files:
            localfile = Path(context.op_config["download_path"]) / Path(ftpfile)
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
    context.log.info(f" 🚗 Analyzed BGC-MVR: {results}")        
    context.log.info(f"Checked {len(results)} new files in {datetime.now() - trace}.")
    return  results


@op(
    config_schema={
        "download_path": Field(
            String,
            description = "Download files to this location"
        )
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
        for ftpfile in download_files:
            localfile = Path(context.op_config["download_path"]) / Path(ftpfile)
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
    context.log.info(f" 🚗 Analyzed DMV: {results}")      
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
                    trace = datetime.now()
                    for filename in download_list: 
                        download_path = Path(context.op_config["download_path"])  / Path(filename).parent 
                        download_path.mkdir(parents=True, exist_ok=True) 
                        download_filename = download_path / Path(filename).name
                        sftp.get(filename, download_filename )
                        file_list.append(filename) 
                        # sftp.get(filename, download_path / Path(filename) )
                        #file_list.append(filename) 
                    context.log.info(f" 🚗 Downloaded {file_list}")
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
                file = os.path.join(context.op_config["base_path"],filename)
                file_list.append(file)
                #for testing purpose - get 10 records
                #if len(file_list) == 10 :
                #     return file_list
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
    
    uber_files = upload_files.alias("upload_uber")(write_csv.alias("write_uber")(get_files.alias("get_uber_files")(all_files)))
    get_other_files.alias("get_uber_other_file")(get_unique_ids.alias("get_unique_uber_id")(uber_files))
    # move_files.alias("move_uber_other")( get_other_file.alias("get_uber_other_file")(get_unique_id.alias("get_unique_uber_id")(uber_files)))
    lyft_files = upload_files.alias("upload_lyft")(write_csv.alias("write_lyft")(get_files.alias("get_lyft_files")(all_files)))
    get_other_files.alias("get_lyft_other_file")(get_unique_ids.alias("get_unique_lyft_id")(lyft_files))
    # move_files.alias("move_lyft_other")(get_other_file.alias("get_lyft_other_file")(get_unique_id.alias("get_unique_lyft_id")(lyft_files)))
 

    # delete_files.alias("delete_uber")(uber_files)
    # delete_files.alias("delete_lyft")(lyft_files)


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
                "download_bgc": {
                    "config":{
                         "download_path": f"{execution_date_path}",
                    }
                },
                "download_dmv": {
                    "config":{
                         "download_path": f"{execution_date_path}",
                    }
                },
                "analyze_bgcfiles": {
                    "config":{
                         "download_path": f"{execution_date_path}",
                    }
                },
                "analyze_dmvfiles": {
                    "config":{
                         "download_path": f"{execution_date_path}",
                    }
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
                         "csv_filename": f"{execution_date_path}/Uber_drivers_{execution_date}.csv",
                    }
                },
                "write_lyft": {
                    "config":{
                         "csv_filename": f"{execution_date_path}/Lyft_drivers_{execution_date}.csv",
                    }
                },
                "upload_uber": {
                    "config":{
                         "base_path": "Uber_Background_Docs",
                         "local_csv_file": f"{execution_date_path}/Uber_drivers_{execution_date}.csv",
                    },
                },
                "upload_lyft": {
                    "config":{
                         "base_path": "Lyft_Background_Docs",
                         "local_csv_file": f"{execution_date_path}/Lyft_drivers_{execution_date}.csv",
                    },
                },
                "get_uber_other_file": {
                    "config":{
                         "base_path": "Uber_Background_Docs",
                    },
                },
                "get_lyft_other_file": {
                    "config":{
                         "base_path": "Lyft_Background_Docs",
                    },
                },
                "move_uber_other": {
                    "config":{
                         "base_path": "Uber_Background_Docs",
                    },
                },
                "move_lyft_other": {
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