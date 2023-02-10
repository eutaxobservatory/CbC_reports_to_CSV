"""This module contains the functions to extract tables from PDFs and convert them to dataframes. If input is in Excel of equivalent, module is bypassed."""
import abc
import json
import os
import time
from concurrent import futures
from os.path import exists

import camelot.io as camelot
import dill as pickle
import pandas as pd
from ExtractTable import ExtractTable

from .cbc_report import CbCReport
from .log import logger
from .exceptions import ExtractionError


def get_remote_ET_result(et_sess: ExtractTable, file_path, pages):
    t0 = time.time()
    logger.info("ET.com - worker started %s, %s\n%s", file_path, pages, time.time())
    server_res = et_sess.process_file(
        filepath=file_path,
        output_format="dict",
        pages=",".join(map(str, pages)),
    )
    logger.info(
        "ET.com - worker finished %s, %s\ntook %ss", file_path, pages, time.time() - t0
    )
    return server_res


pickled_camelot = pickle.dumps(camelot.read_pdf)


def camelot_extraction(pickled_read_pdf, fixed_options, options):
    t0 = time.time()
    unpickled_read_pdf = pickle.loads(pickled_read_pdf)
    logger.info(
        "camelot_extraction by worker %s %s\n%s",
        fixed_options,
        options,
        time.gmtime(time.time()),
    )
    proc_res = unpickled_read_pdf(**fixed_options, **options)
    logger.info(
        "camelot_extraction by worker done %s %s\ntook %ss",
        fixed_options,
        options,
        time.time() - t0,
    )
    return (options, proc_res)


class AbstractExtractor(abc.ABC):
    def __init__(
        self,
        pdf_repo_path,
        intermediate_files_dir,
        executor: futures.ProcessPoolExecutor,
    ) -> None:
        self.pdf_repo_path = pdf_repo_path
        self.intermediate_files_dir = intermediate_files_dir
        self.executor = executor

    @abc.abstractmethod
    def check_cache(self, report: CbCReport) -> bool:
        """Returns True if the cache exists, False otherwise."""

    @abc.abstractmethod
    def write_cache(self, report: CbCReport, jobs: list[futures.Future] | None) -> None:
        pass

    @abc.abstractmethod
    def submit_jobs(self, report: CbCReport) -> list[futures.Future] | None:
        pass

    @abc.abstractmethod
    def read_cache_write_intermediate_tables(
        self, report: CbCReport, jobs: list[futures.Future]
    ) -> list[pd.DataFrame]:
        pass


class ExtractTableExtractor(AbstractExtractor):
    def __init__(
        self,
        key,
        pdf_repo_path,
        intermediate_files_dir,
        executor: futures.ProcessPoolExecutor,
    ) -> None:
        super().__init__(pdf_repo_path, intermediate_files_dir, executor)
        self.key = key
        self.cache_path = os.path.join(intermediate_files_dir, "ExtractTable.com_cache")
        os.makedirs(self.cache_path, exist_ok=True)

    def check_cache(self, report: CbCReport) -> bool:
        file_name = os.path.basename(report.filename_of_source)
        return exists(os.path.join(self.cache_path, f"{file_name}.json"))

    def write_cache(self, report: CbCReport, jobs: list[futures.Future] | None) -> None:
        file_name = f"{os.path.basename(report.filename_of_source)}.json"
        tables = dict()
        # this is dumb as there is a single job...
        if jobs:
            for job in futures.as_completed(jobs):
                l = job.result()
                logger.info("ET.com - worker finished. \n%s \n%s", report, time.time())
                for tb_number, table in enumerate(l):
                    tables[tb_number] = table
                with open(
                    os.path.join(self.cache_path, file_name), "w", encoding="utf-8"
                ) as outfile:
                    json.dump(tables, outfile)

    def submit_jobs(self, report: CbCReport) -> list[futures.Future] | None:
        if not self.check_cache(report):
            file_path = os.path.join(self.pdf_repo_path, report.filename_of_source)
            pages = report.pages
            logger.info(
                "\nExtracting %s with ExtractTable.com\n", report, exc_info=True
            )
            if self.key:
                et_sess = ExtractTable(api_key=self.key)
            else:
                raise ExtractionError("no ExtractTable.com key provided")
            logger.info("submitting %s to ET", report)
            return [
                self.executor.submit(get_remote_ET_result, et_sess, file_path, pages)
            ]

    def read_cache_write_intermediate_tables(
        self, report: CbCReport, jobs: list[futures.Future] | None
    ) -> list[pd.DataFrame]:
        logger.info("read_cache_write_intermediate_tables ET %s", report)
        self.write_cache(report, jobs)
        # 2d. read the result from ExtractTable.com and write the tables to the intermediate_files directory.
        with open(
            os.path.join(
                self.cache_path, f"{os.path.basename(report.filename_of_source)}.json"
            ),
            "r",
            encoding="utf-8",
        ) as f:
            et_json = json.load(f)
        dfs = []
        subdirectory = f"{report.group_name}_{report.end_of_year}"
        dir_path = os.path.join(
            self.intermediate_files_dir, "csv_intermediate_tables", subdirectory
        )
        os.makedirs(dir_path, exist_ok=True)
        for nb, table in et_json.items():
            df = pd.DataFrame(table)
            df.to_csv(
                os.path.join(
                    dir_path,
                    f"{report.group_name}_{report.end_of_year}_{nb}.csv",
                ),
                index=False,
                header=False,
            )
            dfs.append(df)
        return dfs


class CamelotExtractor(AbstractExtractor):
    def __init__(
        self,
        pdf_repo_path,
        intermediate_files_dir,
        executor: futures.ProcessPoolExecutor,
    ) -> None:
        super().__init__(pdf_repo_path, intermediate_files_dir, executor)
        self.cache_path = os.path.join(intermediate_files_dir, "camelot_cache")
        os.makedirs(self.cache_path, exist_ok=True)

    def check_cache(self, report: CbCReport) -> bool:
        return exists(
            os.path.join(
                self.cache_path,
                f"{os.path.basename(report.filename_of_source)}.json",
            )
        )

    def write_cache(self, report: CbCReport, jobs: list[futures.Future] | None) -> None:
        file_name = f"{os.path.basename(report.filename_of_source)}.json"
        method_extraction = dict()
        if jobs:
            for i in futures.as_completed(jobs):
                # cache as json to have a single file per report, not multiple CSV files.
                opts, tables = i.result()
                logger.info("camelot - worker finished. \n%s \n%s", report, time.time())
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
                os.path.join(self.cache_path, file_name), "w", encoding="utf-8"
            ) as outfile:
                json.dump(method_extraction, outfile, indent=4)

    def submit_jobs(self, report: CbCReport) -> list[futures.Future] | None:
        if not self.check_cache(report):
            file_path = os.path.join(self.pdf_repo_path, report.filename_of_source)
            pages = ",".join(map(str, report.pages))
            # 2c. otherwise, get the result from the 3rd party software that transforms the pdf tables into CSV (ExtractTable.com).
            logger.info("\nExtracting %s with camelot\n", report, exc_info=True)
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
                logger.info("submitting %s", option)
                to_do.append(
                    self.executor.submit(
                        camelot_extraction, pickled_camelot, fixed_options, option
                    )
                )
            return to_do

    def read_cache_write_intermediate_tables(
        self, report: CbCReport, jobs: list[futures.Future] | None
    ) -> list[pd.DataFrame]:
        """writes the CSV tables to intermediate files, so that can be used by the operator. If cached results available, they will be used."""
        logger.info("read_cache_write_intermediate_tables CAMELOT %s", report)
        self.write_cache(report, jobs)
        with open(
            os.path.join(
                self.cache_path, f"{os.path.basename(report.filename_of_source)}.json"
            ),
            "r",
            encoding="utf-8",
        ) as f:
            tables = json.load(f)
        try:
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
        subdirectory = f"{report.group_name}_{report.end_of_year}"
        dir_path = os.path.join(
            self.intermediate_files_dir,
            "csv_intermediate_tables",
            subdirectory,
            "camelot",
        )
        os.makedirs(dir_path, exist_ok=True)
        for i, df in enumerate(map(lambda x: x.table, extractions[best_method])):
            write_path_camelot = os.path.join(
                dir_path,
                f"{report.group_name}_{report.end_of_year}_{str(i)}.csv",
            )
            df.to_csv(write_path_camelot, index=False)


class TableAcc:
    def __init__(self, df: pd.DataFrame, acc: float):
        self.accuracy = acc
        self.table = df


def get_DataFrames(
    key: str,
    report: CbCReport,
    pdf_repo_path,
    executor: futures.ProcessPoolExecutor,
    intermediate_files_dir="intermediate_files",
) -> list[pd.DataFrame]:
    """Returns tables from ExtractTable.com. Both camelot-py and ExtractTable.com CSV files are written to the intermediate_files_dir so that they can be edited by the operator in case automatic standardization is not possible."""

    et_extractor = ExtractTableExtractor(
        key, pdf_repo_path, intermediate_files_dir, executor
    )
    camelot_extractor = CamelotExtractor(
        pdf_repo_path, intermediate_files_dir, executor
    )
    camelot_jobs = camelot_extractor.submit_jobs(report)
    try:
        et_jobs = et_extractor.submit_jobs(report)
    except ExtractionError as exc:
        raise exc
    finally:
        camelot_extractor.read_cache_write_intermediate_tables(report, camelot_jobs)
    return et_extractor.read_cache_write_intermediate_tables(report, et_jobs)
