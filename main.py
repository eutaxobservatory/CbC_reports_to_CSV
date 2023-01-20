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
    intermediate_files_dir,
    write_tables_to_dir,
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
        shutil.rmtree(write_tables_to_dir)
        os.makedirs(write_tables_to_dir)
    not_extracted = set(reports)
    for report in reports[:default_max_reports]:
        # only extract when there is an explicit yes
        if report.metadata.get("to_extract", "no").casefold() != "yes":
            continue
        else:
            # 1. check if report already extracted - if so, stop.
            if exists(
                os.path.join(
                    write_tables_to_dir, f"{report.group_name}_{report.end_of_year}.csv"
                )
            ):
                not_extracted.remove(report)
                continue
            # 2. get a CSV version of the Tables
            try:
                logger.info("\nExtracting %s\n", report, exc_info=True)
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
                    dfs = get_DataFrames(
                        report,
                        input_pdf_directory,
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
                human_bored = standardize_dataframe(
                    human_bored, unified_df, report, rules
                )
                # 5. export the final, standardized dataframe to CSV.
                unified_df.to_csv(
                    os.path.join(
                        write_tables_to_dir,
                        f"{report.group_name}_{report.end_of_year}.csv",
                    ),
                    index=False,
                    quoting=csv.QUOTE_NONNUMERIC,
                )
                not_extracted.remove(report)
                print(f"{report} successfully extracted.\n")
            except Exception as exception:
                logger.error(
                    "Fatal error on %s :\n%s\n\n", report, exception, exc_info=True
                )
    return not_extracted


def main(argv):
    # default values
    mode_ = "s"
    quiet_mode = False
    input_pdf_dir = "./input/pdf_repository/"
    after_intervention_dir = "input/files_after_human_intervention/"
    intermediate_files_dir = "intermediate_files/"
    write_tables_to_dir = "./output/individual_reports/"
    rules_path = join("configuration", "rules.json")
    metadata_path = join("configuration", "metadata.json")
    write_justifications_to = ""
    HELP_STRING = """(all paths are given relative to the root of the extraction directory):
- -f: (force): forces the deletion and the re-extraction of all files flagged for extraction (as per the metadata file)
- -q (quiet) : this option does not prompt the user whenever column or jurisdiction names are not standard. Instead, non-standard names will have a trailing "_tocheck" flag.
- -i (input): specifies the directory where the PDFs to be extracted can be found. Default value: '../pdf_repository/'.
- -o (output): specifies the path at which the CSV that aggregates data from all extractions will be written. Default value: '../data.csv'.
- --after_human: specifies the directory at which the intermediate CSV that have been manually edited can be found (so that the software can pick it up from there). Default value: './files_after_human_intervention'.
- --write_tables_to: specifies the directory at which the final CSVs for each (MNC,year) pair will be written. The full path for these final files will be `<write_tables_to>/<MNC>_<year>.csv`. Default value: './extracted_tables/'
- --min_nb_CbCR_cols: specifies the minimum number of columns that must contain "CbCR terms" so that a table classifies as a "CbCR table". (motivation: it is often the case that multiple tables exist in the PDF page that contains the CbCR table and not all of them have data on CbCR)
- --rules: specifies the path of the rules file. (See rules.json below) . Default value: './configuration/rules.json'
- --metadata: specifies the path of the metadata file. (See metadata.json below). Default value: './configuration/metadata.json'"""

    # values specified by operator overwrite defaults
    try:
        opts, _ = getopt.getopt(
            argv,
            "hfqi:o:r:m:",
            [
                "min_nb_CbCR_cols=",
                "min_nb_countries=",
                "after_human=",
                "write_tables_to=",
                "rules=",
                "metadata=",
                "intermediate_dir=",
                "justifications=",
                "help",
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
            input_pdf_dir = arg
        elif opt == "--after_human":
            after_intervention_dir = arg
        elif opt in ["--write_tables_to", "o"]:
            write_tables_to_dir = arg
        elif opt in ["--rules", "-r"]:
            rules_path = arg
        elif opt in ["--metadata", "-m"]:
            metadata_path = arg
        elif opt in ["--intermediate_dir"]:
            intermediate_files_dir = arg
        elif opt in ["--justifications"]:
            write_justifications_to = arg
        elif opt in ["-h", "--help"]:
            print(HELP_STRING)
            return

    init_time = datetime.now()
    rules = Rules(rules_path)
    for directory in [intermediate_files_dir, write_tables_to_dir]:
        os.makedirs(directory, exist_ok=True)
    not_extracted = extract_each_report(
        get_reports_from_metadata(metadata_path),
        rules,
        default_max_reports=1000,
        mode=mode_,
        human_bored=quiet_mode,
        input_pdf_directory=input_pdf_dir,
        intervened_dir=after_intervention_dir,
        intermediate_files_dir=intermediate_files_dir,
        write_tables_to_dir=write_tables_to_dir,
    )

    rules.write(rules_path)
    if write_justifications_to:
        rules.export_justifications_to_csv(write_justifications_to)

    print(f" \nNot extracted ({len(not_extracted)}) below:")
    print("\n".join([str(f) for f in not_extracted]))
    print(f" \n Total time spent: {datetime.now() - init_time}\n")


if __name__ == "__main__":
    main(sys.argv[1:])
