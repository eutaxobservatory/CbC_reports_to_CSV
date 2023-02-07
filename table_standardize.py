import functools
import itertools
import re
from itertools import filterfalse

import numpy as np
import pandas as pd
from cbc_report import CbCReport
from exceptions import IncompatibleTables, NoCbCReportFound, StandardizationError
from log import logger
from rules import Rules
from utils import (
    CBCR_TERMS,
    CONTRY_TO_ISO3166_MAPPING,
    DOUBLE_DIGITS,
    ETR_FORMAT_RE,
    EXCHANGE_RATES,
    ISO3166_ALPHA3,
    NOT_NUMERIC_CHARS_RE,
    PERCENTAGE_FORMAT_RE,
    YEAR_REGEX,
    jurisdiction_to_iso3166,
    neatify,
)


def trim_dataframe(df: pd.DataFrame):
    """Delete (in-place) columns and rows marked for deletion by "apply_rules" or "get_new_rules_from_operator"."""
    df.drop(df[df.jurisdiction == "delete_row"].index, inplace=True)

    for name, _ in df.items():
        if bool(re.search("to_drop", str(name))):
            df.drop(name, axis=1, inplace=True)


def apply_rules_to_rows(df: pd.DataFrame, report: CbCReport, rules: Rules) -> None:
    """In-place. Applies strict rules, then regex rules and allows ISO3166-1 alpha-3 codes. Otherwise the jurisdiction name is appended with `_tocheck`"""
    try:
        df.jurisdiction = df.jurisdiction.apply(jurisdiction_to_iso3166)
    except AttributeError as exc:
        raise StandardizationError("jurisdiction column not found") from exc
    old_new_correspondence = {}
    for jur_name in df["jurisdiction"]:
        sink = rules.get_sink_from_strict(report, jur_name, "j")
        if sink:
            old_new_correspondence[jur_name] = sink
        else:
            sink = rules.get_sink_from_regex(report, jur_name, "j")
            if sink:
                old_new_correspondence[jur_name] = sink
        if (jur_name not in ISO3166_ALPHA3) and not sink:
            old_new_correspondence[jur_name] = f"{jur_name}_tocheck"
    df.jurisdiction = df["jurisdiction"].map(lambda x: old_new_correspondence.get(x, x))


def count_countries(
    smtg: str | pd.Series | pd.DataFrame, include_continents=False, stop_at=None
) -> int:
    """Lazily (by column) count the number of countries in a string, Series or DataFrame."""
    total = 0
    if isinstance(smtg, pd.DataFrame):
        iterator = itertools.chain.from_iterable((column for _, column in smtg.items()))
    elif isinstance(smtg, pd.Series):
        iterator = smtg
    else:
        iterator = [smtg]
    for cell in iterator:
        if (
            cell.upper() in ISO3166_ALPHA3
            or (
                cell.upper() in ["AFRICA", "EUROPE", "AMERICA", "ASIA", "NORTH AMERICA"]
                if include_continents
                else False
            )
            or CONTRY_TO_ISO3166_MAPPING.get(neatify(cell), "")
        ):
            total += 1
        if stop_at and total >= stop_at:
            break
    return total


def count_CbCR_terms(
    smtg: str | pd.Series | pd.DataFrame, stop_at=None, casefold=True
) -> int:
    """Lazily (by row) count the number of CBCR terms in a string."""
    pattern = "|".join(
        r"\b{}".format(word) for word in CBCR_TERMS
    )  # no break after word (to account for superscripts)
    counter = 0
    if isinstance(smtg, pd.DataFrame):
        iterator = itertools.chain.from_iterable((row for _, row in smtg.iterrows()))
    elif isinstance(smtg, pd.Series):
        iterator = smtg
    else:
        iterator = [smtg]
    for cell in iterator:
        if casefold:
            cell = cell.casefold()
        for _ in re.finditer(pattern, str(cell)):
            counter += 1
            if stop_at and counter >= stop_at:
                break
    return counter


def unify_CbCR_tables(dfs: list[pd.DataFrame], report: CbCReport) -> pd.DataFrame:
    """Attempts to concatenate the potentially multiple tables that comprise the report.
    Before doing so, it will attempt to have observations as rows.
    The head of the table will be set as the Index of the Dataframe."""

    def untangle_df_head(df: pd.DataFrame, report: CbCReport) -> pd.DataFrame:
        """Takes the column names out of the table area and puts them as such. Fails if anonymous columns"""
        # if more than 2 cells have numbers 2 digits, assume it is within the table
        # TODO: better filtering
        header_last_index = -1
        # it CAN be that first row is already data - tables in second pages or so.
        index = 0
        for index, (_, row) in enumerate(df.iterrows()):
            if (
                row.map(lambda x: int(bool(DOUBLE_DIGITS.search(x)))).sum()
                >= report.min_nb_cols
            ):
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

    def not_CbCR_table(df: pd.DataFrame, report: CbCReport) -> bool:
        """Identifies if a table is unlikely to a CbCR table, due to being too small, having too few countries or too few CbCR terms."""
        nb_rows, nb_cols = df.shape
        too_small = (
            True if nb_cols < report.min_nb_cols else False
        )  # could add rows but nb_jurisdictions is a good enough proxy?
        total_country_cells = count_countries(
            df, include_continents=True, stop_at=report.min_nb_jurs_per_table
        )
        nb_cbcr_terms = count_CbCR_terms(df, stop_at=report.min_nb_terms)
        too_few_countries = (
            True if total_country_cells < report.min_nb_jurs_per_table else False
        )
        too_few_CbCR_terms = True if nb_cbcr_terms < report.min_nb_terms else False
        logger.info(
            "nb_countries: %s \nnb_CbCR_terms: %s \nnv_rows: %s \nnb_cols: %s",
            total_country_cells,
            nb_cbcr_terms,
            nb_rows,
            nb_cols,
        )
        return too_small or too_few_CbCR_terms or too_few_countries

    def orient_tables(dfs: list[pd.DataFrame], report: CbCReport) -> list[pd.DataFrame]:
        """Rotates the tables if they have observations as columns instead of rows."""

        def is_transposed(df: pd.DataFrame, report: CbCReport) -> bool:
            for _, row in df.items():
                if (
                    count_countries(
                        row,
                        include_continents=True,
                        stop_at=report.min_nb_jurs_per_table,
                    )
                    >= report.min_nb_jurs_per_table
                ):
                    return False
            for _, row in df.iterrows():
                # if count_CbCR_terms(column) >= report.min_nb_cols:
                #     return False
                if (
                    count_countries(
                        row,
                        include_continents=True,
                        stop_at=report.min_nb_jurs_per_table,
                    )
                    >= report.min_nb_jurs_per_table
                ):
                    return True
            raise NoCbCReportFound("\nCan't tell whether transposed.\n")

        try:
            if is_transposed(dfs[0], report):
                logger.info("TRANSPOSing!")
                return list(map(pd.DataFrame.transpose, dfs))
            else:
                logger.info("no transposition.")
                return dfs
        except IndexError as exc:
            raise NoCbCReportFound("No tables to unify after CbCR filtering") from exc

    if not dfs:
        raise NoCbCReportFound("No tables to unify - must have not passed CbCR test")
    else:
        cbcr_tables = list(
            filterfalse(lambda table: not_CbCR_table(table, report=report), dfs)
        )
        dfs_oriented = orient_tables(cbcr_tables, report)
        untangled_dfs = list(map(lambda x: untangle_df_head(x, report), dfs_oriented))
        for df in untangled_dfs:
            try:
                df.columns = untangled_dfs[0].columns
            except ValueError as exc:
                raise IncompatibleTables(
                    "different tables with different number of columns. Intervention needed."
                ) from exc

        # ignore_index = TRUE >> do not use the index values along the concatenation axis. The resulting axis will be labeled 0, …, n - 1.
        # this ignore_index param is important for the handling of percentages. More importantly, it makes sense.
        return pd.concat(untangled_dfs, ignore_index=True)


def standardize_dataframe(
    operator_wont_intervene: bool, df: pd.DataFrame, report: CbCReport, rules: Rules
) -> bool:
    """Standardizes the DataFrame in-place. Makes column names and jurisdiction codes standard (jurisdictions according to ISO3166) and adds metadata to the DataFrames (company name, time interval covered, company's sectors and HQ country, etc.). Returns a flag indicating whether the operator may be further prompted to intervene.
    When standardization requires the operator's input, the function blocks and prompts the user."""

    def apply_rules_to_columns(df: pd.DataFrame, report: CbCReport, rules: Rules):
        """Tries to standardize names of the columns. Works in-place."""
        # jurisdiction will be automatically assigned so no problem with calling df.jurisdiction before user's intervention
        def find_jurisdiction_location() -> int:
            """finds index of column with jurisdictions. Raises an error if there are multiple columns with jurisdictions or if there are no columns with jurisdictions."""
            pos = -1
            for i, (_, values) in enumerate(df.items()):
                if (
                    count_countries(
                        values,
                        include_continents=True,
                        stop_at=report.min_nb_jurs_per_table,
                    )
                    >= report.min_nb_jurs_per_table
                ):
                    if pos != -1:
                        raise StandardizationError("Multiple columns w/ jurisdictions.")
                    pos = i
            if pos == -1:
                raise StandardizationError("No column w/ enough country names.")
            else:
                return pos

        columns_to_be = []
        std_colnames_from_rules = rules.get_std_colnames_from_rules()
        column_names = df.columns
        for i, column_name in enumerate(
            column_names
        ):  # enumerate so that we can distinguish between columns with the same name (e.g. empty string)
            # 1st priority: a strict rule that applies to the column name
            std_name = rules.get_sink_from_strict(report, column_name, "c")
            # 2nd priority: an already std column name
            if (not std_name) and (column_name.lower() in std_colnames_from_rules):
                std_name = column_name.lower()
            # 3rd priority: a regex rule that applies to the column name
            if not std_name:
                std_name = rules.get_sink_from_regex(report, column_name, "c")

            # important to do it here as the to_drop may come from the rules
            if std_name == "to_drop":
                columns_to_be.append(f"{std_name}_{i}")
            elif std_name:
                columns_to_be.append(f"{std_name}")
            else:
                if column_name:
                    columns_to_be.append(f"{column_name}_{i}_tocheck")
                else:
                    columns_to_be.append(
                        ""
                    )  # use empty string as poison pill for checking column names below. error not yet thrown as might be the jurisdiction column
        # check existence of a jurisdiction column
        if "jurisdiction" not in columns_to_be:
            i = find_jurisdiction_location()
            columns_to_be[i] = "jurisdiction"
        # check no repetitions in nor empty in the to be column names
        if (
            pd.Index(columns_to_be).drop("", errors="ignore").nunique(dropna=True)
            != df.columns.size
        ):
            raise StandardizationError(
                "Same column name assigned to different columns or empty column name."
            )
        df.columns = pd.Index(columns_to_be)

    def get_new_rules_from_operator(
        df,
        report: CbCReport,
        rules: Rules,
    ):
        """gets operator to assign values to unknown column and row identifiers. Alters the DataFrame and updates the rules in-place.
        Closely related to "apply_rules_to_columns" and "apply_rules_to_rows"."""

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
            a = df["jurisdiction"].str.extractall(
                r"^(?P<found_jurisdiction>.*)_tocheck$"
            )["found_jurisdiction"]
            return pd.Series(a).to_list()

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
                    source = input(
                        "Write your source regex (as you would in python):\n"
                    )
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
        for source_appended in get_non_standard_cols(df):
            source = re.match(r"(.*)_\d{0,2}_tocheck$", source_appended).group(1)
            col_dict = dict(
                (nb, name)
                for nb, name in enumerate(
                    ["to_drop", source] + rules.get_std_colnames_from_rules()
                )
            )
            prompt_text = prompt_text_col(
                source, f"{report.group_name}_{report.end_of_year}", col_dict
            )
            mode, sink, justification = prompt_common(source, col_dict, prompt_text)
            rules.write_new_rule(source, mode, sink, justification, "c", report)
            if sink == "quit":
                human_bored = True
                return (col_subs, jur_subs, human_bored)
            else:
                col_subs[source_appended] = sink

        for source in get_non_standard_jurisdiction(df):
            jurisdiction_dict = dict(
                (option, name)
                for option, name in enumerate([source, "delete_row", "other"])
            )
            prompt_text = prompt_text_jurisdiction(
                source, f"{report.group_name}_{report.end_of_year}", jurisdiction_dict
            )
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

    def tidy_data(df: pd.DataFrame, report: CbCReport) -> None:
        """Trasforms DataFrame in-place as to get data in tidy format. Cleans cells and ensures values are in units.
        Adds report metadata (multinational's name, time-frame, sectoral info, currency, etc.)."""

        def handle_etr(df: pd.DataFrame):
            def percentage_to_rational(x):
                return float(
                    ETR_FORMAT_RE.search(
                        re.sub(",", ".", NOT_NUMERIC_CHARS_RE.sub("", str(x)))
                    ).group(1)
                    / 100
                )

            try:
                df.effective_tax_rate = df.effective_tax_rate.apply(
                    percentage_to_rational
                )
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
                    r"(.*?)([\., ]\d{3})([\., ]\d{3})?([\., ]\d{3})?([\., ]\d{3})?(.*)",
                    x,
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

        cell_basic_conversion(df)
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

        for column_name in report.columns_to_flip:
            try:
                df[column_name] = df[column_name].apply(lambda x: -1 * x)
            except KeyError as exc:
                raise StandardizationError(
                    f"No {column_name} column found but present in metadata file."
                ) from exc

        numerics = ["int16", "int32", "int64", "float16", "float32", "float64"]
        df_to_multiply = df.select_dtypes(include=numerics).drop(
            ["employees", "effective_tax_rate"], axis="columns", errors="ignore"
        )
        df[df_to_multiply.columns] = df_to_multiply.applymap(lambda x: report.unit_multiplier * x)
        # df.sort_index(axis=1, inplace=True)
        df.insert(0, "currency", np.repeat(report.currency, len(df)))
        # retrofit years as end_of_year (legacy metadata may just show `2020` instead of `2020.12.31`).
        end_of_year = (
            report.end_of_year + ".12.31"
            if YEAR_REGEX.match(report.end_of_year)
            else report.end_of_year
        )
        df.insert(
            1,
            "multiplier_to_euro",
            np.repeat(float(EXCHANGE_RATES[(report.currency, end_of_year)]), len(df)),
        )

        if report.parent_jurisdiction:
            df.insert(1, "parent_entity_jurisdiction", np.repeat(report.parent_jurisdiction, len(df)))
        if report.bvd_sector:
            df.insert(2, "parent_entity_bvd_sector", np.repeat(report.bvd_sector, len(df)))
        if report.nace2_main:
            df.insert(
                2, "parent_entity_nace2_main", np.repeat(f"{report.nace2_main}", len(df))
            )
        if report.nace2_core_code:
            df.insert(
                2, "parent_entity_nace2_core_code", np.repeat(report.nace2_core_code, len(df))
            )
        df.insert(0, "end_of_year", np.repeat(end_of_year, len(df)))
        df.insert(0, "parent_entity", np.repeat(str.strip(report.parent_entity_name), len(df)))
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

    apply_rules_to_columns(df, report, rules)
    apply_rules_to_rows(df, report, rules)
    if not operator_wont_intervene:
        operator_wont_intervene = get_new_rules_from_operator(df, report, rules)
    trim_dataframe(df)
    tidy_data(df, report)
    return operator_wont_intervene
