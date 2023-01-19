import re
import pandas as pd

from log import logger
from cbc_report import CbCReport
from utils import  count_CbCR_terms, count_countries


def get_non_standard_cols(df: pd.DataFrame):
    """
    heavy lifting is done by standardize_colnames.
    """
    ns_colnames = set(
        filter(lambda x: bool(re.search(r"_tocheck$", str(x))), df.columns)
    )
    logger.info("NUMBER OF NON STD COLS> %s", len(ns_colnames))
    return ns_colnames


def get_non_standard_jurisdiction(df: pd.DataFrame):
    # negative lookahead to extract names that are not 3 letter codes
    a = df["jurisdiction"].str.extractall(r"^(?P<found_jurisdiction>.*)_tocheck$")[
        "found_jurisdiction"
    ]
    return pd.Series(a).to_list()


def check_colnames(column_names: pd.Index):
    """
    sanity check on column names - no NA nor repetitions and at least a character. Throws ValueError exception if not.
    """
    logger.debug([str(s) for s in column_names])
    # check all cols are named appropriately
    is_neat = map(lambda x: bool(re.search(r"[a-zA-z]", str(x))), column_names)

    if (column_names.nunique(dropna=True) != column_names.size) | (not all(is_neat)):
        raise ValueError(
            f"Senseless name(s) for column(s) of first table. Fix before continuing.\n{column_names}"
        )


def not_CbCR_table(df: pd.DataFrame, report: CbCReport) -> bool:
    """
    TODO! This is very inefficient; however, since total time to extract all tables is less than 10s, not really a problem.
    """
    nb_rows, nb_cols = df.shape
    # could add rows but nb_jurisdictions is a good enough proxy?
    too_small = True if nb_cols < report.min_nb_cols else False
    single_string = df.to_string().casefold()
    total_country_cells = 0
    for _, col in df.items():
        total_country_cells += count_countries(col)
    nb_cbcr_terms = count_CbCR_terms(single_string)
    too_few_countries = (
        True if total_country_cells < report.min_nb_jurs_per_table else False
    )
    too_few_CbCR_terms = True if nb_cbcr_terms < report.min_nb_terms else False
    logger.debug(
        "nb_countries: %s \nnb_CbCR_terms: %s \nnv_rows: %s \nnb_cols: %s",
        total_country_cells,
        nb_cbcr_terms,
        nb_rows,
        nb_cols,
    )
    return too_small or too_few_CbCR_terms or too_few_countries
