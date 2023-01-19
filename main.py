# to be ran from the extraction directory
import pandas as pd
from datetime import datetime
import shutil
from os.path import exists, join
import os
import csv
import sys
import getopt

from table_extraction import get_DataFrames
from table_standardize import unify_CbCR_tables, standardize_dataframe
from cbc_report import CbCReport, get_reports_from_metadata
from rules import Rules
from log import logger

def extract_each_report(
    reports: list[CbCReport],
    rules: Rules,
    input_pdf_directory,
    intervened_dir,
    write_tables_to,
    default_max_reports=100,
    mode="",
    human_bored=False,
):
    """May update the rules during execution (Rules object gets updated in-place).
    Extracted files will be named '<mnc_id>_<end_of_year>.csv' and be on the specified directory <write_tables_to>.
    (Above, extracted means the complete pipeline from pdf to standardized, unique CSV file per report.)
    Temporary files will be inside the respective '/extraction/intermediate_files/<mnc_id>_<end_of_year>/' folder, relative to root of the repo.
    They will be named '<mnc_id>_<end_of_year>_<table_number>.csv'."""
    if mode == "hard" or mode == "h":
        try:
            os.mkdir(write_tables_to)
        except FileExistsError:
            shutil.rmtree(write_tables_to)
            os.mkdir(write_tables_to)
    not_extracted = set(reports)
    for report in reports[:default_max_reports]:
        # only extract when there is an explicit yes
        if report.metadata.get("to_extract", "no").casefold() != "yes":
            continue
        else:
            # 1. check if report already extracted - if so, stop.
            if exists(
                os.path.join(
                    write_tables_to, f"{report.group_name}_{report.end_of_year}.csv"
                )
            ):
                not_extracted.remove(report)
                continue
            # 2. get a CSV version of the Tables
            try:
                logger.info("Extracting %s\n\n",report, exc_info=True)
                # 2a. check if there is a extraction (from pdf to csv) that underwent manual editing.
                manual_path = os.path.join(
                    intervened_dir, f"{report.group_name}_{report.end_of_year}.csv"
                )
                if exists(manual_path):
                    dfs = [pd.read_csv(manual_path, header=None).astype(str)]
                # 2b. otherwise, get the result from the 3rd party software that transforms the pdf tables into CSV (ExtractTable.com).
                # each file is ran by the 3rd party software only once. the results are cached in root/extraction/ExtractTable.com/
                elif exists(
                    os.path.join(input_pdf_directory, report.filename_of_source)
                ):
                    dfs = get_DataFrames(report, input_pdf_directory)
                else:
                    raise FileNotFoundError("Source file not found.")
                # 3. as reports may span across multiple tables, create a dataframe with all the data
                unified_df = unify_CbCR_tables(dfs, report)
                # 4. use the rules from `rules.json` (or another specified file!) to make column names and jurisdiction codes standard.
                # For jurisdiction/column names that cannot be resolved with the current rules, get input from the operator is human_bored == False.
                # As the operator can become bored during a report, update the value of human_bored for the remaining documents (only goes from not bored to bored.)
                human_bored = standardize_dataframe(
                    human_bored, unified_df, report, rules
                )
                # 5. export the final, standardized dataframe to CSV.
                unified_df.to_csv(
                    os.path.join(
                        write_tables_to, f"{report.group_name}_{report.end_of_year}.csv"
                    ),
                    index=False,
                    quoting=csv.QUOTE_NONNUMERIC,
                )
                not_extracted.remove(report)
                print(f"{report} successfully extracted.\n")
            except Exception as exception:
                logger.error("Fatal error on %s :\n%s\n\n",report,exception, exc_info=True)
    return not_extracted


def main(argv):
    # default values
    mode_ = "s"
    quiet_mode = False
    input_pdf_directory = "../../pdf_repository/"
    write_tables_to = join("extracted_tables", "")
    rules_file = join("configuration", "rules.json")
    after_intervention = "./files_after_human_intervention"
    metadata_path = join("configuration", "metadata.json")
    # values specified by operator overwrite defaults
    try:
        opts, _ = getopt.getopt(
            argv,
            "fqi:o:r:m:",
            [
                "min_nb_CbCR_cols=",
                "min_nb_countries=",
                "after_human=",
                "write_tables_to=",
                "rules=",
                "metadata=",
            ],
        )
    except getopt.GetoptError as exception:
        print(str(exception))
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-f":
            mode_ = "h"
        elif opt == "-q":
            quiet_mode = True
        elif opt == "-i":
            input_pdf_directory = arg
        elif opt == "--after_human":
            after_intervention = arg
        elif opt in ["--write_tables_to", "o"]:
            write_tables_to = arg
        elif opt in ["--rules", "-r"]:
            rules_file = arg
        elif opt in ["--metadata", "-m"]:
            metadata_path = arg

    init_time = datetime.now()
    rules = Rules(rules_file)
    not_extracted = extract_each_report(
        get_reports_from_metadata(metadata_path),
        rules,
        default_max_reports=1000,
        mode=mode_,
        human_bored=quiet_mode,
        input_pdf_directory=input_pdf_directory,
        intervened_dir=after_intervention,
        write_tables_to=write_tables_to,
    )
    rules.write(rules_file)
    rules.export_justifications_to_csv("./outputs/justifications.csv")
    print(f" \nNot extracted ({len(not_extracted)}) below:")
    print("\n".join([str(f) for f in not_extracted]))
    print(f" \n Total time spent: {datetime.now() - init_time}\n")


if __name__ == "__main__":
    main(sys.argv[1:])
