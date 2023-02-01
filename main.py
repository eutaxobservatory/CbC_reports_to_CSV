# to be ran from the extraction directory
import argparse
import csv
import os
import shutil
from concurrent import futures
from datetime import datetime
from os.path import exists

import pandas as pd
from cbc_report import CbCReport, get_reports_from_metadata
from log import logger
from rules import Rules
from table_extraction import get_DataFrames
from table_standardize import standardize_dataframe, unify_CbCR_tables


def extract_one(
    executor,
    report,
    rules,
    input_pdf_directory,
    intervened_dir,
    intermediate_files_dir,
    write_tables_to_dir,
    human_bored,
) -> pd.DataFrame:
    if exists(
        os.path.join(
            write_tables_to_dir, f"{report.group_name}_{report.end_of_year}.csv"
        )
    ):
        return report, True, None
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
            dfs = get_DataFrames(
                report,
                input_pdf_directory,
                executor=executor,
                cache=True,
                intermediate_files_dir=intermediate_files_dir,
            )
        else:
            raise FileNotFoundError(
                f"Source file not found at {os.path.join(input_pdf_directory, report.filename_of_source)}."
            )
        # 3. as reports may span across multiple tables, create a dataframe with all the data
        unified_df = unify_CbCR_tables(dfs, report)
        # 4. use the rules from `rules.json` (or another specified file!) to make column names and jurisdiction codes standard.
        # For jurisdiction/column names that cannot be resolved with the current rules, get input from the operator is human_bored == False.
        # As the operator can become bored during a report, update the value of human_bored for the remaining documents (only goes from not bored to bored.)
        human_bored = standardize_dataframe(human_bored, unified_df, report, rules)
        return report, True, unified_df
    except Exception as exception:
        logger.error("Fatal error on %s :\n%s\n\n", report, exception, exc_info=True)
        return report, False, None


def extract_all_reports(
    reports: list[CbCReport],
    rules: Rules,
    input_pdf_directory,
    intervened_dir,
    intermediate_files_dir,
    write_tables_to_dir,
    default_max_reports=100,
    force_rewrite=False,
    quiet_mode=False,
):
    """May update the rules during execution (Rules object gets updated in-place).
    Extracted files will be named '<mnc_id>_<end_of_year>.csv' and be on the specified directory <write_tables_to>.
    (Above, extracted means the complete pipeline from pdf to standardized, unique CSV file per report.)
    Temporary files will be inside the respective '/extraction/intermediate_files/<mnc_id>_<end_of_year>/' folder, relative to root of the repo.
    They will be named '<mnc_id>_<end_of_year>_<table_number>.csv'."""
    # if quiet_mode, make extraction of each report concurrent (in parallel) (TODO)

    # else, make extraction of each report sequential (below)
    # in any case, make the extractions concurrent within each report (TODO)
    human_bored = quiet_mode
    if force_rewrite:
        shutil.rmtree(write_tables_to_dir)
        os.makedirs(write_tables_to_dir)
    not_extracted = set(reports)
    # only extract when there is an explicit yes
    with futures.ProcessPoolExecutor(max_workers=4) as executor:
        for report in [r for r in reports if r.to_extract][:default_max_reports]:
            report, success, df = extract_one(
                executor,
                report,
                rules,
                input_pdf_directory,
                intervened_dir,
                intermediate_files_dir,
                write_tables_to_dir,
                human_bored,
            )
            if success:
                not_extracted.remove(report)
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
                    print(f"{report} successfully extracted.\n")
            else:
                print(f"{report} failed to extract.\n")

    return not_extracted


def main():

    init_time = datetime.now()
    rules = Rules(args.rules)
    for directory in [args.intermediate_files_dir, args.write_tables_to_dir]:
        os.makedirs(directory, exist_ok=True)
    not_extracted = extract_all_reports(
        get_reports_from_metadata(args.metadata),
        rules,
        default_max_reports=1000,
        force_rewrite=args.force_rewrite,
        quiet_mode=args.quiet,
        input_pdf_directory=args.input_pdf_dir,
        intervened_dir=args.after_intervention_dir,
        intermediate_files_dir=args.intermediate_files_dir,
        write_tables_to_dir=args.write_tables_to_dir,
    )

    rules.write(args.rules)
    if args.write_justifications_to:
        rules.export_justifications_to_csv(args.write_justifications_to)

    print(f" \nNot extracted ({len(not_extracted)}) below:")
    print("\n".join([str(f) for f in not_extracted]))
    print(f" \n Total time spent: {datetime.now() - init_time}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A script to extract CbC data from PDFs."
    )
    parser.add_argument(
        "-f",
        "--force-rewrite",
        action="store_true",
        help="clears output directory before extracting, thus forcing a rewrite of all files.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="do not prompt the user whenever column or jurisdiction names are not standard. Non-standard names will have a trailing '_tocheck' flag.",
    )
    parser.add_argument(
        "-i",
        "--input_pdf_dir",
        default=os.path.join("inputs", "pdf_repository"),
        help="the directory where the PDFs to be extracted can be found.",
    )
    parser.add_argument(
        "-o",
        "--write_tables_to_dir",
        default=os.path.join("outputs", "individual_reports"),
        help="the directory at which the final CSVs for each report will be written.",
    )
    parser.add_argument(
        "-r",
        "--rules",
        default=os.path.join("inputs", "rules.json"),
        help="the path of the rules file.",
    )
    parser.add_argument(
        "-m",
        "--metadata",
        default=os.path.join("inputs", "metadata.json"),
        help="the path of the metadata file.",
    )
    parser.add_argument(
        "--intermediate_files_dir",
        default=os.path.join("intermediate_files"),
        help="the path of the metadata file.",
    )
    parser.add_argument(
        "-j",
        "--write_justifications_to",
        default=None,
        help="the path at which to write the file with the justifications for the rules.",
    )
    parser.add_argument(
        "--after_intervention_dir",
        default=os.path.join("inputs", "files_after_human_intervention"),
        help="the path of the directory with manually edited input CSVs.",
    )
    args = parser.parse_args()

    main()
