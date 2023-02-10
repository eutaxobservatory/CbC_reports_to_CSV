""" This script concatenates the tables extracted from each report into a single table. It may apply a different set of rules to each table and is particularly useful for allowing less strictness when extracting individual tables and more when concatenating into a single database."""
import csv
import os
from os.path import exists
import pandas as pd

from ..cbc_report import CbCReport
from ..exceptions import StandardizationError
from ..log import logger
from ..rules import Rules
from ..standardize_dataframe import apply_rules_to_rows, trim_dataframe

__all__ = ["concatenate_tables"]


def concatenate_tables(
    extracted_tables_at, aggregate_output_path, rules: Rules, reports: list[CbCReport]
):
    """made separate from the extraction of each report so as to allow more flexibility in the
    extraction of each report and ensuring greater rigidity
     in the final database comprised of all extracted reports."""
    cumulative_df = []
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
