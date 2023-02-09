""" This script concatenates the tables extracted from each report into a single table. It may apply a different set of rules to each table and is particularly useful for allowing less strictness when extracting individual tables and more when concatenating into a single database."""
import csv
import getopt
import os
import sys
from os.path import exists, join
import pandas as pd

from .cbc_report import get_reports_from_metadata
from .exceptions import StandardizationError
from .log import logger
from .rules import Rules
from .standardize_dataframe import apply_rules_to_rows, trim_dataframe

__all__ = ["concatenate_tables"]

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
                    logger.info("%s not extracted so not in output file.", report)
                    continue
                dataframe = pd.read_csv(
                    extracted_report_path,
                    dtype={"parent_entity_nace2_core_code": "str"},
                )
                apply_rules_to_rows(dataframe, report, rules)
                trim_dataframe(dataframe)
                cumulative_df.append(dataframe)
            except StandardizationError as exception:
                logger.error(exception, exc_info=True)

        pd.concat(cumulative_df).to_csv(
            aggregate_output_path, index=False, quoting=csv.QUOTE_NONNUMERIC
        )
        print(f"\n wrote aggregate CSV to: {aggregate_output_path}")


if __name__ == "__main__":
    concatenate_tables(sys.argv[1:])
