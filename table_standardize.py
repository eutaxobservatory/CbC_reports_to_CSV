import functools
import re
from itertools import filterfalse

import numpy as np
import pandas as pd
from cbc_report import CbCReport
from log import logger
from rules import Rules
from sanity_checks import (
    check_colnames,
    get_non_standard_cols,
    get_non_standard_jurisdiction,
    not_CbCR_table,
)
from utils import (
    auto_jurisdiction_to_iso3166,
    count_countries,
    neatify,
    orient_tables,
    trim_rows_cols,
    EXCHANGE_RATES,
    PERCENTAGE_FORMAT_RE,
    ETR_FORMAT_RE,
    NOT_NUMERIC_CHARS_RE,
    YEAR_REGEX,
    DOUBLE_DIGITS,
)


def untangle_df_head(df: pd.DataFrame, report: CbCReport) -> pd.DataFrame:
    """
    if more than 2 cells have numbers 2 digits, assume it is within the table
    TODO: better filtering
    Takes the column names out of the table area and puts them as such. Fails if anonymous columns
    """
    header_last_index = -1
    # it CAN be that first row is already data - tables in second pages or so.
    index = 0
    for index, (_, row) in enumerate(df.iterrows()):
        if row.map(lambda x: int(bool(DOUBLE_DIGITS.search(x)))).sum() >= report.min_nb_cols:
            break
        header_last_index = index

    out_df = df.iloc[index:]  # from index on (inclusively) there is data
    if (
        header_last_index != -1
    ):  # no row appears to contain colnames - which can be fine!
        neat_colnames = []
        for _, cells in df.iloc[:index].items():
            # separate cells with spaces
            single_cell = " ".join(filter(lambda x: x != "nan", cells))
            # remove non-word chars and uniform whitespace
            neat_col_name = neatify(single_cell)
            neat_colnames.append(neat_col_name)
        out_df.columns = neat_colnames
    return out_df


def standardize_colnames(df: pd.DataFrame, report: CbCReport, rules: Rules):
    """
    Tries to standardize names of the columns. Works in-place.
    Assumes unique col names (from untangle_df_head). - apart from 'to_drop', which will be dropped.
    does not immediately pop useless columns as that can only be done after table concatenation!

    In general it may be the case that multiple Index-items are the same in the columns Index -
    - eg. multiple are the empty string -, which means care is needed when concatenating the DataFrames.
    """
    # jurisdiction will be automatically assigned so no problem with calling df.jurisdiction before user's intervention
    def find_jurisdiction_location():
        """
        name country-filled col and put it in pos 0.
        errors on columns with duplicated names - including empty!
        """
        pos = -1
        for i, (_, values) in enumerate(df.items()):
            if count_countries(values) >= report.min_nb_jurs_per_table:
                if pos != -1:
                    raise ValueError("Multiple columns w/ jurisdictions.")
                pos = i
        if pos == -1:
            raise ValueError("No column w/ enough country names.")
        else:
            return pos

    columns_to_be = []
    std_colnames_from_rules = rules.get_std_colnames_from_rules()
    column_names = df.columns

    for i, column_name in enumerate(column_names):
        std_name = rules.get_sink_from_strict(report, column_name, "c")
        if not std_name and not column_name:
            std_name = "to_drop"
        if (not std_name) and (column_name in std_colnames_from_rules):
            std_name = column_name
        if not std_name:
            std_name = rules.get_sink_from_regex(report, column_name, "c")

        # important to do it here as the to_drop may come from the rules
        if std_name == "to_drop":
            columns_to_be.append(f"{std_name}_{i}")
        elif std_name:
            columns_to_be.append(f"{std_name}")
        else:
            columns_to_be.append(f"{column_name}_{i}_tocheck")
    # check jurisdiction
    if "jurisdiction" not in columns_to_be:
        i = find_jurisdiction_location()
        columns_to_be[i] = "jurisdiction"
    if pd.Index(columns_to_be).nunique(dropna=True) != df.columns.size:
        raise Exception("Bad indexing of columns in standardize names.")
    df.columns = pd.Index(columns_to_be)


def get_new_rules_from_operator(
    df,
    non_standard_column_names,
    non_standard_jurisdictions,
    report: CbCReport,
    rules: Rules,
):
    """gets operator to intervene. alters dataframe and rules in-place.
    Attention to this play with _tocheck with "get_non_standard_cols" and "standardize_colnames"."""
    company = report.group_name
    year = report.end_of_year
    mnc_id__year = f"{company}_{year}"

    def apply_subs(df: pd.DataFrame, col_subs, jur_subs) -> pd.DataFrame:
        """In place!
        apply substitutions and kill rows and columns as instructed by the user.
        """
        df.columns = df.columns.map(lambda x: col_subs.get(x, x))
        logger.debug(df.head())
        df["jurisdiction"] = df["jurisdiction"].map(lambda x: jur_subs.get(x, x))
        logger.debug(df.head())

    def prompt_common(source, default_options, prompt_text):
        """
        rule_set will be changed while this function runs.()
        """
        while True:
            answer = input(prompt_text)
            if answer.casefold() == "r":
                source = input("Write your source regex (as you would in python):\n")
                source = f"_regex_{source}"
                sink = input("Write your sink:\n")
                mode = input("Write the mode for the rule ('!','#' or '.'):\n")
            elif answer.casefold() == "q":
                return ("", "quit", "")
            else:
                try:
                    match = re.match(r"(.*?)([!#\.])(.*)", answer)
                    mode = match.group(2) if match.group(2) else ""
                    choice = match.group(1)
                    justification = (
                        match.group(3) if match.group(3) else "<no justification>"
                    )
                except AttributeError:
                    continue
                sink = default_options[int(choice)] if choice.isdigit() else choice
            confirm = input(
                f'Assign "{source}" to "{sink}" in mode {mode}? Justification: "{justification}".\n(y/n)'
            )
            if (confirm in ["y", "Y"]) and (mode in ["#", ".", "!"]):
                return (mode, sink, justification)

    def prompt_text_col(odd_name, filename, col_dict: dict):
        substring = "\n".join([f"{n} -> {name}" for n, name in col_dict.items()])
        return f"""
Unknown column name found. Select a standard column name.
Instructions:
"q" to stop updating rules for the whole iteration.
"0" to delete this column.
"1" to leave as is.
"r" to create regex rule.
To assign new standard column:
  Write the column name in snake_case (recommended) or choose a number from the list.
  Then, append one of the following:
    "!" to your option to make it a rule for every CbCR report. eg. 1!
    "#" to make it a rule at the MNC level. eg: 2#
    "." to make it a one-time rule. eg: 3.
  Optionally, write a justification for the rule after the !/#/.  
Finalize by pressing ENTER.

Unknown column name: {odd_name}
File: {filename}
Default options:
{substring}
"""

    def prompt_text_jurisdiction(new_name, filename, jurisdiction_dict: dict):
        substring = "\n\n".join(
            [f"{n} -> {name}" for n, name in jurisdiction_dict.items()]
        )
        return f"""Select a standard jurisdiction name or press enter to leave it as is.
Instructions:

Write "r" to create a regex rule.
Choose "0" to keep jurisdiction name as is.
Choose "1" to delete this row.
Choose "2" for "other" - the chosen default for this DB.
To assign new jurisdiction name, write its ISO 3166-1 alpha-3 code (recommended).
Append "!" to your option to make it a rule for every CbCR report. eg. 1!
Append "#" to make it a rule at the MNC level. eg: 2#
Append "." to make it a one-time rule. eg: 3.
Optionally, write a justification for the rule after the !/#/.  
Write Q to stop updating rules for the whole iteration.
Then, press ENTER.

Unidentified jurisdiction name: {new_name}
File: {filename}
Options:
{substring}
"""

    human_bored = False
    col_subs = {}
    jur_subs = {}
    for source_appended in non_standard_column_names:
        source = re.match(r"(.*)_\d{0,2}_tocheck$", source_appended).group(1)
        col_dict = dict(
            (nb, name)
            for nb, name in enumerate(
                ["to_drop", source] + rules.get_std_colnames_from_rules()
            )
        )
        prompt_text = prompt_text_col(source, mnc_id__year, col_dict)
        mode, sink, justification = prompt_common(source, col_dict, prompt_text)
        rules.write_new_rule(source, mode, sink, justification, "c", report)
        if sink == "quit":
            human_bored = True
            return (col_subs, jur_subs, human_bored)
        else:
            col_subs[source_appended] = sink

    for source in non_standard_jurisdictions:
        jurisdiction_dict = dict(
            (option, name)
            for option, name in enumerate([source, "delete_row", "other"])
        )
        prompt_text = prompt_text_jurisdiction(source, mnc_id__year, jurisdiction_dict)
        mode, sink, justification = prompt_common(
            source, jurisdiction_dict, prompt_text
        )
        rules.write_new_rule(source, mode, sink, justification, "j", report)
        if sink == "quit":
            human_bored = True
            break
        else:
            jur_subs[f"{source}_tocheck"] = sink

    apply_subs(df, col_subs, jur_subs)
    return human_bored


def standardize_jurisdiction_names(
    df: pd.DataFrame, report: CbCReport, rules: Rules
) -> None:
    """In-place!"""

    def clean_country_col():
        try:
            df.jurisdiction = df.jurisdiction.apply(auto_jurisdiction_to_iso3166)
        except Exception as excep:
            logger.exception(excep, exc_info=True)
            raise ValueError("no \"jurisdiction' column found.") from excep

    # columns to drop come from the rules. In "" they are renames as "to_drop"
    clean_country_col()
    old_new_correspondence = {}
    for jur_name in df["jurisdiction"]:
        sink = rules.get_sink_from_strict(report, jur_name, "j")
        if sink:
            old_new_correspondence[jur_name] = sink
        else:
            sink = rules.get_sink_from_regex(report, jur_name, "j")
            if sink:
                old_new_correspondence[jur_name] = sink
        if not re.match(r"^[A-Z]{3}$", jur_name) and not sink:
            old_new_correspondence[jur_name] = f"{jur_name}_tocheck"
    df.jurisdiction = df["jurisdiction"].map(lambda x: old_new_correspondence.get(x, x))


def unify_CbCR_tables(dfs: list[pd.DataFrame], report: CbCReport) -> pd.DataFrame:
    """
    Orient tables, untagle  columns of the first table is standardized and remaining tables' columns are set to match them.
    """
    if not dfs:
        raise ValueError("No tables to unify - must have not passed CbCR test")
    else:
        cbcr_tables = list(
            filterfalse(lambda table: not_CbCR_table(table, report=report), dfs)
        )
        dfs_oriented = orient_tables(cbcr_tables, report)
        untangled_dfs = list(map(lambda x: untangle_df_head(x, report), dfs_oriented))
        for df in untangled_dfs:
            try:
                df.columns = untangled_dfs[0].columns
            except Exception as exc:
                raise Exception(
                    "different tables with different number of columns. Intervention needed."
                ) from exc

        # ignore_index = TRUE >> do not use the index values along the concatenation axis. The resulting axis will be labeled 0, …, n - 1.
        # this ignore_index param is important for the handling of percentages. More importantly, it makes sense.
        return pd.concat(untangled_dfs, ignore_index=True)


def tidy_data(df: pd.DataFrame, report: CbCReport) -> pd.DataFrame:
    def handle_etr(df: pd.DataFrame):
        def percentage_to_rational(x):
            return float(
                ETR_FORMAT_RE.search(
                    re.sub(",", ".", NOT_NUMERIC_CHARS_RE.sub("", str(x)))
                ).group(1)
                / 100
            )

        try:
            df.effective_tax_rate = df.effective_tax_rate.apply(percentage_to_rational)
        except (KeyError, AttributeError, TypeError) as exception:
            # debug
            logger.warning(exception, exc_info=True)
        except Exception as exception:
            logger.error(exception, exc_info=True)
            raise ValueError(f"{exception} from handle ETR") from exception

    def handle_percentages(df: pd.DataFrame):
        def percentage_to_number(x):
            try:
                z = (
                    float(
                        PERCENTAGE_FORMAT_RE.search(
                            re.sub(",", ".", NOT_NUMERIC_CHARS_RE.sub("", str(x)))
                        ).group(1)
                    )
                    / 100
                )
            except AttributeError:
                z = -1
            return z

        try:
            totals = df["total_revenues"]
        except KeyError:
            logger.info("No total_revenue column")
        else:
            try:
                percentages = df["unrelated_revenues"].apply(percentage_to_number)
                if any(percentages > 0):
                    df["unrelated_revenues"] = totals * percentages
            except (KeyError) as e:
                logger.warning(e, exc_info=True)
            try:
                percentages = df["related_revenues"].apply(percentage_to_number)
                if any(percentages > 0):
                    df["related_revenues"] = totals * percentages
            except (KeyError) as e:
                logger.warning(e, exc_info=True)

    def cell_basic_conversion(df: pd.DataFrame):
        """This works inplace
        Takes out commas, replaces parenthesis by a minus sign and transforms strings into numbers.

        come back to this and do it better
        """

        def clean_commas_dots_space(x):
            m = re.match(
                r"(.*?)([\., ]\d{3})([\., ]\d{3})?([\., ]\d{3})?([\., ]\d{3})?(.*)", x
            )
            if m:
                return "".join(
                    [
                        re.sub("[,. ]", "", x)
                        for x in m.groups()[:-1]
                        if isinstance(x, str)
                    ]
                    + [m.groups()[-1]]
                )
            return x

        def replace_brackets(x):
            m = re.match(r"\((.*)\)", x)  # no-lint
            if m:
                return "-" + m.group(1)
            return x

        def compose(*functions):
            """this is composition as normal in mathematics, not R's pipe! compose(f,g) means `f . g `"""
            return functools.reduce(
                lambda f, g: lambda x: f(g(x)), functions, lambda x: x
            )

        eliminate_non_numeric_chars = compose(
            replace_brackets,
            lambda x: re.sub(r"<s>.*?</s>", "", x),
            clean_commas_dots_space,
            lambda x: re.sub(r",", r".", x),
            lambda x: "" if x == "-" else x,
            lambda x: NOT_NUMERIC_CHARS_RE.sub("", x),
        )

        safe_to_coerce = pd.DataFrame(df).drop(
            ["jurisdiction", "commentary", "main_activities"],
            axis="columns",
            errors="ignore",
        )
        newdf = safe_to_coerce.applymap(eliminate_non_numeric_chars).apply(
            pd.to_numeric, errors="ignore"
        )
        df[newdf.columns] = newdf
        logger.debug(df)

    metadata_in_effect = report.metadata
    cell_basic_conversion(df)
    logger.debug("POST CELL")
    logger.debug(df)
    handle_etr(df)
    handle_percentages(df)
    cols = df.columns.drop(
        [
            "jurisdiction",
            "commentary",
            "main_activities",
            "statutory_tax_rate",
            "tax_reconciliation",
        ],
        errors="ignore",
    )
    df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")
    try:
        unit = metadata_in_effect.get("unit")
        cur = metadata_in_effect.get("currency")
        parent_entity_name = metadata_in_effect.get("parent_entity_name")
        nace2_main = metadata_in_effect.get("nace2_main")
        nace2_core_code = metadata_in_effect.get("nace2_core_code")

    except Exception as exc:
        raise KeyError(f"Fatally incomplete metadata.\n{str(exc)}") from exc
    hq = metadata_in_effect.get("parent_jurisdiction", "")
    sector = metadata_in_effect.get("sector", "")
    # try:
    #     df.loc[metadata_in_effect.get('columns_to_flip')].apply(lambda x: -1 * x)
    # except KeyError as e:
    #         logger.warn(
    #             f"No {column_name} column found but present in metadata file.")

    for column_name in metadata_in_effect.get("columns_to_flip"):
        try:
            df[column_name] = df[column_name].apply(lambda x: -1 * x)
        except KeyError:
            logger.warning(
                "No %s column found but present in metadata file.", column_name
            )

    numerics = ["int16", "int32", "int64", "float16", "float32", "float64"]
    df_to_multiply = df.select_dtypes(include=numerics).drop(
        ["employees", "effective_tax_rate"], axis="columns", errors="ignore"
    )
    df[df_to_multiply.columns] = df_to_multiply.applymap(lambda x: int(unit) * x)
    # df.sort_index(axis=1, inplace=True)
    df.insert(0, "currency", np.repeat(cur, len(df)))
    # retrofit years as end_of_year (legacy metadata may just show `2020` instead of `2020.12.31`).
    end_of_year = (
        report.end_of_year + ".12.31"
        if YEAR_REGEX.match(report.end_of_year)
        else report.end_of_year
    )
    df.insert(
        1,
        "multiplier_to_euro",
        np.repeat(float(EXCHANGE_RATES[(cur, end_of_year)]), len(df)),
    )

    if hq:
        df.insert(1, "parent_entity_jurisdiction", np.repeat(hq, len(df)))
    if sector:
        df.insert(2, "parent_entity_bvd_sector", np.repeat(sector, len(df)))
    if nace2_main:
        df.insert(2, "parent_entity_nace2_main", np.repeat(f"{nace2_main}", len(df)))
    if nace2_core_code:
        df.insert(
            2, "parent_entity_nace2_core_code", np.repeat(nace2_core_code, len(df))
        )
    df.insert(0, "end_of_year", np.repeat(end_of_year, len(df)))
    df.insert(0, "parent_entity", np.repeat(str.strip(parent_entity_name), len(df)))
    df.insert(0, "group_name", np.repeat(report.group_name, len(df)))
    df.drop(  # cleaner database - this was a hot fix, can be improved and dealt with sooner
        [
            "statutory_tax_rate",
            "effective_tax_rate",
            "commentary",
            "main_activities",
            "tax_reconciliation",
        ],
        axis="columns",
        errors="ignore",
        inplace=True,
    )


def standardize_dataframe(
    human_bored, df: pd.DataFrame, report: CbCReport, rules: Rules
):
    """in-place: standardizes column names and jurisdictions, gets user input and adds metadata."""
    standardize_colnames(df, report, rules)
    check_colnames(df.columns)
    standardize_jurisdiction_names(df, report, rules)
    if not human_bored:
        human_bored = get_new_rules_from_operator(
            df,
            get_non_standard_cols(df),
            get_non_standard_jurisdiction(df),
            report,
            rules,
        )
    trim_rows_cols(df)
    # before the next step, column names must be made standard as treatment of columns depends on col name.
    # e.g. applying rules to percentages, coercing columns into numeric values.
    tidy_data(df, report)
    return human_bored
