from concurrent.futures import as_completed, Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from re import search
from typing import MutableMapping


from dateutil.parser import parse
from pdfquery import PDFQuery
from pyquery import PyQuery

from dagster import (
    Field,
    OpExecutionContext,
    String,
    op,
)


def load_pdf(path: str):
    pdf = PDFQuery(Path(path).resolve())
    pdf.load()
    pdf.file.close()

    return pdf


def find_text(pdf: PDFQuery, txt: str):
    return pdf.pq(f'LTTextLineHorizontal:contains("{txt}")')


def absolute_query(
    pdf: PDFQuery, pq: PyQuery, x0: float, y0: float, x1: float, y1: float
) -> PyQuery:
    return pdf.pq(f'LTTextLineHorizontal:in_bbox("{x0}, {y0}, {x1}, {y1}")')


def relative_query(
    pdf: PDFQuery,
    pq: PyQuery,
    rel_x0: float,
    rel_y0: float,
    rel_x1: float,
    rel_y1: float,
) -> PyQuery:
    return absolute_query(
        pdf,
        pq,
        float(pq.attr("x0")) + rel_x0,
        float(pq.attr("y0")) + rel_y0,
        float(pq.attr("x1")) + rel_x1,
        float(pq.attr("y1")) + rel_y1,
    )


def get_report_date(pdf: PDFQuery, date_label: str):
    label: PyQuery = find_text(pdf, date_label)

    if not label.length:
        return None

    try:
        return parse(
            absolute_query(
                pdf,
                label,
                float(label.attr("x0")),
                float(label.attr("y0")),
                float(label.attr("x1")),
                float(label.attr("y1")),
            )
            .text()
            .replace(date_label, "")
            .strip()
        )
    except:
        return None


def analyze_report_date(dttm: datetime):
    if dttm is None:
        return False, "Unable to determine report date"

    if dttm.date() < (datetime.now(tz=timezone.utc) - timedelta(days=365)).date():
        return False, "Report is more than a year old"

    return True, "Report is less than a year old"


def analyze_checkr_file(file: str) -> tuple[bool, str]:
    pdf = load_pdf(file)

    result = True
    analysis = []

    status_label = find_text(pdf, "Status")

    if status_label.length:
        status_value = relative_query(pdf, status_label, 0, -30, 50, -10)

        if search("Clear", status_value.text()):
            analysis.append('Status is "Clear"')
        else:
            result = False
            analysis.append('Status is not "Clear"')

    else:
        result = False
        analysis.append('Unable to find "Status" label')

    report_date = get_report_date(pdf, "Report completed on")

    date_analysis = analyze_report_date(report_date)
    analysis.append(date_analysis[1])
    if not date_analysis[0]:
        result = False

    return result, report_date, analysis


def analyze_safety_holdings_file(file: str) -> tuple[bool, str]:
    pdf = load_pdf(file)

    result = True
    analysis = []

    status_label = find_text(pdf, "There are no violations in the report.")

    if status_label.length:
        analysis.append("No violations found in the report")
    else:
        result = False
        analysis.append("Found violations in the report")

    report_date = get_report_date(pdf, "Order date:")

    date_analysis = analyze_report_date(report_date)
    analysis.append(date_analysis[1])
    if not date_analysis[0]:
        result = False

    return result, report_date, analysis


def analyze_sambasystems_file(file: str) -> tuple[bool, str]:
    pdf = load_pdf(file)

    result = True
    analysis = []

    status_label = find_text(pdf, "DISPOSITION: CLEAN")

    if status_label.length:
        analysis.append("No violations found in the report")
    else:
        result = False
        analysis.append("Found violations in the report")

    date_analysis = analyze_report_date(get_report_date(pdf, "Report completed on"))
    analysis.append(date_analysis[1])
    if not date_analysis[0]:
        result = False

    return result, analysis


@op(
    config_schema={
        "method": Field(
            String,
            description="Which method of analysis to use. Either 'status' or 'violations'",
        )
    },
    required_resource_keys=["ssh_client"],
)
def analyze_files(context: OpExecutionContext, data: list[dict]) -> list[dict]:
    analysis_callable = (
        analyze_safety_holdings_file
        if context.op_config["method"] == "violations"
        else analyze_checkr_file
    )

    jobs: MutableMapping[Future, MutableMapping] = {}

    trace = datetime.now()
    with ThreadPoolExecutor(max_workers=4) as executor:

        for file in data:
            jobs[executor.submit(analysis_callable, file["local_path"])] = file

    results = []

    for job in as_completed(jobs):
        file = jobs[job]

        context.log.debug(f"Analysis for {file['remote_path']} completed...")

        try:
            analysis = job.result()

            file["result"] = "Pass" if analysis[0] else "Fail"
            file["report_date"] = analysis[1]
            file["analysis"] = "; ".join(analysis[2])

            results.append(file)
        except Exception as err:
            file["result"] = "Error"
            file["report_date"] = None
            file["analysis"] = str(err)

            results.append(file)
        finally:
            del jobs[job]
            del file

    context.log.info(f"Analyzing {len(data)} files took {datetime.now() - trace}.")

    return results
