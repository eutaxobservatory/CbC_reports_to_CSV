import json
import os
import shutil
from collections.abc import Iterable
from os.path import exists
from concurrent import futures
import dill as pickle

import camelot.io as camelot
import pandas as pd
from cbc_report import CbCReport
from ExtractTable import ExtractTable
from log import logger


def camelot_extraction(pickled_read_pdf, fixed_options, options):
    unpickled_read_pdf = pickle.loads(pickled_read_pdf)
    return (options, unpickled_read_pdf(**fixed_options, **options))


def get_DataFrames(
    report: CbCReport,
    pdf_repo_path,
    intermediate_files_dir="intermediate_files/",
    cache=True,
) -> list[pd.DataFrame]:
    """
    Returns results from ET. Extracts with both ET and camelot results into the write_directory.
    """
    # TODO option without cache
    class TableAcc:
        def __init__(self, df: pd.DataFrame, acc: float):
            self.accuracy = acc
            self.table = df

    def cache_extraction(filepath, pages: list[int], method: str):
        """
        method must be either 'et' or 'camelot'.
        Returns differently for ET and camelot, as camelot has multiple alternative methods. TODO?
        """

        def write_extracttable_json(file_path, file_name, pages):
            """
            writes table (gotten from ExtractTable.com) to a csv file in the temp directory.
            """
            print("\n write_extracttable() \n")
            # DO NOT PUT KEY ONLINE PUBLICLY! Save it in a local file.
            et_sess = ExtractTable(api_key=open(".et_key", "r").read())
            # Checks the API Key validity as well as shows associated plan usage
            print(et_sess.check_usage())
            tables = dict()
            l = et_sess.process_file(
                filepath=file_path,
                output_format="dict",
                pages=",".join(map(str, pages)),
            )
            # Checks the API Key validity as well as shows associated plan usage
            print(et_sess.check_usage())

            for tb_number, table in enumerate(l):
                tables[tb_number] = table
            with open(
                os.path.join(cache_directory["et"], f"{file_name}.json"), "w"
            ) as outfile:
                json.dump(tables, outfile)

        def write_camelot_json(file_path, file_name, pages):
            logger.info("write_camelot_json")
            pages = ",".join(map(str, pages))
            # in order to add alternative ways of extracting the pdf, add to the dict below
            # WARNING: row_tol is very tricky and we can't really rely on camelot's accuracy - it tends to give higher values with higher row_tol (by design...) but often ruins the table from a our perspective.

            with futures.ProcessPoolExecutor() as executor:
                pickled_camelot = pickle.dumps(camelot.read_pdf)
                fixed_options = {
                    "pages": pages,
                    "flag_size": True,
                    "strip_tex": "\n",
                    "filepath": file_path,
                }
                options = [
                    {"flavor": "stream", "row_tol": 5},
                    {"flavor": "stream", "row_tol": 2},
                    {"flavor": "lattice", "copy_text": ["h"]},
                ]
                to_do = []
                for option in options:
                    # fixed_options goes to comeback as results come out of order
                    to_do.append(
                        executor.submit(
                            camelot_extraction, pickled_camelot, fixed_options, option
                        )
                    )
                method_extraction = dict()
                for i in futures.as_completed(to_do):
                    # cache as json to have a single file per report, not multiple CSV files.
                    opts, tables = i.result()
                    method_extraction[str(opts)] = list(
                        map(
                            lambda x: {
                                "json_table": x.df.to_dict(),
                                "accuracy": x.accuracy,
                            },
                            tables,
                        )
                    )
            with open(
                os.path.join(cache_directory["camelot"], f"{file_name}.json"), "w"
            ) as outfile:
                json.dump(method_extraction, outfile, indent=4)

        extract_dic = {"camelot": write_camelot_json, "et": write_extracttable_json}
        cache_directory = {
            "camelot": os.path.join(intermediate_files_dir, "camelot_cache"),
            "et": os.path.join(intermediate_files_dir, "ExtractTable.com_cache"),
        }
        os.makedirs(cache_directory["camelot"], exist_ok=True)
        os.makedirs(cache_directory["et"], exist_ok=True)
        file_name = os.path.basename(filepath)
        if not exists(os.path.join(cache_directory[method], f"{file_name}.json")):
            extract_dic[method.casefold()](filepath, file_name, pages)
        with open(
            os.path.join(cache_directory[method], f"{file_name}.json")
        ) as json_file:
            tables = dict(json.load(json_file))
        return tables

    def write_camelot_tables(write_dir, file_path, report: CbCReport) -> None:
        """writes the CSV tables to intermediate files, so that can be used by the operator. If cached results available, they will be used."""
        try:
            tables = cache_extraction(file_path, report.pages, method="camelot")
            extractions = {
                k: list(
                    map(
                        lambda x: TableAcc(
                            pd.DataFrame.from_dict(x["json_table"], dtype=str),
                            x["accuracy"],
                        ),
                        v,
                    )
                )
                for k, v in tables.items()
            }
            best_method = max(
                extractions,
                key=lambda key: sum(
                    map(lambda table: table.accuracy, extractions[key])
                ),
            )

        except Exception as e:
            logger.error(e, exc_info=True)
            raise e
        for i, df in enumerate(map(lambda x: x.table, extractions[best_method])):
            write_path_camelot = os.path.join(
                write_dir,
                f"{report.group_name}_{report.end_of_year}_{str(i)}.csv",
            )
            df.to_csv(write_path_camelot, index=False)

    def extracttable_extraction(file_path: str, pages) -> Iterable[pd.DataFrame]:
        """
        returns iterable of dataframes in 1-to-1 correspondence with tables in pages. PDF to dataframe using extracttable.com
        results cached to save on ET credits.
        """

        tables = cache_extraction(file_path, pages, method="et")
        return (pd.DataFrame.from_dict(v, dtype=str) for _, v in tables.items())

    # files keep the same base name as the directory so that is is more user friendly to transform them into the manual_extraction filename format: suffices to remove the index in the end.
    file_path = "".join([pdf_repo_path, report.filename_of_source])
    # First try camelot (free), on error try ExtractTable (paid)
    # match names before concat to try to pave-over small discrepancies

    subdirectory = f"{report.group_name}_{report.end_of_year}"
    dir_path = os.path.join(
        intermediate_files_dir, "csv_intermediate_tables", subdirectory
    )

    # uninteresting creation of the directory structure
    try:
        os.makedirs(dir_path)
    except FileExistsError:
        shutil.rmtree(dir_path)
        os.makedirs(dir_path)

    # 1. extract using camelot
    try:
        write_dir = os.path.join(dir_path, "camelot")
        os.makedirs(write_dir)
        write_camelot_tables(write_dir, file_path, report)
    except Exception as e:
        logger.warning(e, exc_info=True)
    finally:
        try:
            et_dfs = extracttable_extraction(file_path, report.pages)
        except Exception as e:
            logger.error(e, exc_info=True)
            raise type(e)(f"No ET extraction.\n {e}")
        else:
            tables = []
            for i, df in enumerate(et_dfs):
                write_path = os.path.join(
                    dir_path, f"{report.group_name}_{report.end_of_year}_{str(i)}.csv"
                )
                df.to_csv(write_path, index=False, header=False)
                tables.append(df)
            return tables
