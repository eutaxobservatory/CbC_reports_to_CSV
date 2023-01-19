# to be ran from the extraction directory
import csv
import getopt
import os
import sys
from os.path import exists, join
import pandas as pd

from cbc_report import get_reports_from_metadata
from log import logger
from rules import Rules
from table_standardize import standardize_jurisdiction_names
from utils import trim_rows_cols


def concatenate_tables(argv):
    """made separate from the extraction of each report so as to allow more flexibility in the 
    extraction of each report and ensuring greater rigidity
     in the final database comprised of all extracted reports."""
    cumulative_df = []
    aggregate_output_path = ""
    extracted_tables_at = join("extracted_tables", "")
    metadata_path = join("configuration", "metadata.json")
    rules_file = join("configuration", "rules_concat.json")

    try:
        opts, _ = getopt.getopt(argv, "i:o:r:m:", ["rules=", "metadata="])
    except getopt.GetoptError as exception:
        print(str(exception))
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-o":
            aggregate_output_path = arg
        elif opt == "-i":
            extracted_tables_at = arg
        elif opt in ["--rules", "-r"]:
            rules_file = arg
        elif opt in ["--metadata", "-m"]:
            metadata_path = arg
    reports = get_reports_from_metadata(metadata_path)
    rules = Rules(rules_file)

    if aggregate_output_path:
        for report in reports:
            try:
                extracted_report_path = os.path.join(
                    extracted_tables_at, f"{report.group_name}_{report.end_of_year}.csv"
                )
                if not exists(extracted_report_path):
                    logger.error("%s not extracted so not in output file.", report)
                    continue
                dataframe = pd.read_csv(
                    extracted_report_path,
                    dtype={"parent_entity_nace2_core_code": "str"},
                )
                standardize_jurisdiction_names(dataframe, report, rules)
                trim_rows_cols(dataframe)
                cumulative_df.append(dataframe)
            except Exception as exception:
                logger.error(exception)

        pd.concat(cumulative_df).to_csv(
            aggregate_output_path, index=False, quoting=csv.QUOTE_NONNUMERIC
        )
        print(f"\n wrote aggregate CSV to: {aggregate_output_path}")


if __name__ == "__main__":
    concatenate_tables(sys.argv[1:])
