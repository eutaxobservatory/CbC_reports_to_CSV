import csv
import os
import shutil
from concurrent import futures
from os.path import exists

import pandas as pd

from .cbc_report import CbCReport
from .exceptions import ExtractionError, IncompatibleTables, NoCbCReportFound, StandardizationError
from .log import logger
from .pdf_to_dataframe import get_DataFrames
from .rules import Rules
from .standardize_dataframe import standardize_dataframe, unify_CbCR_tables

__all__ = ["extract_all_reports"]


def extract_all_reports(
    reports: list[CbCReport],
    rules: Rules,
    input_pdf_directory,
    intervened_dir,
    intermediate_files_dir,
    write_tables_to_dir,
    default_max_reports=100,
    force_rewrite=False,
    operator_wont_intervene=False,
    quiet=False,
    key=None,
):
    """Attempts to create a unique and standardized CSV file for each reports from the metadata file, using the rules file, the pdf repository and the CSV files that have been manually edited. Extracted files will be named '<mnc_id>_<end_of_year>.csv' and be on the specified directory <write_tables_to_dir>. May update the rules during execution (Rules object gets updated in-place).

    Temporary files will be inside the respective '<intermediate_files_dir>/<mnc_id>_<end_of_year>/' folder.
    ExtractTable.com's extractions will be named '<mnc_id>_<end_of_year>_<table_number>.csv'. Camelot-py's extractions have the same naming convention but  will be in '<intermediate_files_dir>/<mnc_id>_<end_of_year>/camelot/'."""

    def extract_one(
        key,
        executor,
        report,
        rules,
        input_pdf_directory,
        intervened_dir,
        intermediate_files_dir,
        write_tables_to_dir,
        operator_wont_intervene,
    ) -> tuple[bool, bool, pd.DataFrame]:
        """Extracts the tables from the pdf file of the report, standardizes the column names and jurisdiction codes, and returns a pandas.DataFrame conformant to the tidy data format. It also returns two flags: one indicating whether the operator will (not) continue to intervene, another stating whether the extraction was successful."""
        if exists(
            os.path.join(
                write_tables_to_dir, f"{report.group_name}_{report.end_of_year}.csv"
            )
        ):
            return operator_wont_intervene, True, None
        try:
            # 2. get a CSV version of the Tables
            logger.info("\nExtracting %s\n", report, exc_info=True)
            # 2a. check if there is a extraction (from pdf to csv) that underwent manual editing.
            manual_path = os.path.join(
                intervened_dir, f"{report.group_name}_{report.end_of_year}.csv"
            )
            if exists(manual_path):
                dfs = [pd.read_csv(manual_path, header=None).astype(str)]
            # 2b. otherwise, get the result from the 3rd party software that transforms the pdf tables into CSV (ExtractTable.com).
            # each file is ran by the 3rd party software only once. the results are cached in root/extraction/ExtractTable.com/
            elif exists(os.path.join(input_pdf_directory, report.filename_of_source)):
                try:
                    dfs = get_DataFrames(
                    key,
                    report,
                    input_pdf_directory,
                    executor=executor,
                    intermediate_files_dir=intermediate_files_dir,
                )
                except ExtractionError as e:
                    logger.error(
                        "Extraction error on %s :\n%s\n\n", report, e, exc_info=True
                    )
                    return operator_wont_intervene, False, None
            else:
                raise FileNotFoundError(
                    f"Source file not found at {os.path.join(input_pdf_directory, report.filename_of_source)}."
                )
            # 3. as reports may span across multiple tables, create a dataframe with all the data
            unified_df = unify_CbCR_tables(dfs, report)
            # 4. use the rules from `rules.json` (or another specified file!) to make column names and jurisdiction codes standard.
            # For jurisdiction/column names that cannot be resolved with the current rules, get input from the operator is human_bored == False.
            # As the operator can become bored during a report, update the value of human_bored for the remaining documents (only goes from not bored to bored.)
            operator_wont_intervene = standardize_dataframe(
                operator_wont_intervene, unified_df, report, rules
            )
            return operator_wont_intervene, True, unified_df
        except (
            IncompatibleTables,
            NoCbCReportFound,
            FileNotFoundError,
            StandardizationError,
        ) as exception:
            logger.error(
                "Fatal error on %s :\n%s\n\n", report, exception, exc_info=True
            )
            return operator_wont_intervene, False, None

    for directory in [intermediate_files_dir, write_tables_to_dir]:
        os.makedirs(directory, exist_ok=True)
    if force_rewrite:
        shutil.rmtree(write_tables_to_dir)
        os.makedirs(write_tables_to_dir)
    not_extracted = set(reports)
    with futures.ProcessPoolExecutor(max_workers=4) as executor:
        for report in [r for r in reports if r.to_extract][:default_max_reports]:
            operator_wont_intervene, success, df = extract_one(
                key,
                executor,
                report,
                rules,
                input_pdf_directory,
                intervened_dir,
                intermediate_files_dir,
                write_tables_to_dir,
                operator_wont_intervene,
            )
            if (
                success
            ):  # either because the reports has just been extracted, or because it was already extracted.
                # 5. export the final, standardized dataframe to CSV.
                if df is not None:
                    df.to_csv(
                        os.path.join(
                            write_tables_to_dir,
                            f"{report.group_name}_{report.end_of_year}.csv",
                        ),
                        index=False,
                        quoting=csv.QUOTE_NONNUMERIC,
                    )
                msg = f"{report} successfully extracted.\n"
                not_extracted.remove(report)
            else:
                msg = f"{report} failed to extract.\n"
            if not quiet:
                print(msg)

    return not_extracted
