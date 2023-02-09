""" This is the main script for the extraction module. It is called by the command line interface."""
import argparse
import os
from datetime import datetime

from . import Rules, extract_all_reports, get_reports_from_metadata

if __name__ == "__main__":
    init_time = datetime.now()
    parser = argparse.ArgumentParser(
        description="A script to extract CbC data from PDFs."
    )
    parser.add_argument(
        "-f",
        "--force-rewrite",
        default=False,
        action="store_true",
        help="clears output directory before extracting, thus forcing a rewrite of all files.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        default=False,
        action="store_true",
        help="do not print any intermediate results - only upon trying to extract all files. Useful for extracting multiple reports in concurrently.",
    )
    parser.add_argument(
        "--operator-wont-intervene",
        action="store_true",
        default=False,
        help="do not prompt the operator whenever column or jurisdiction names are not standard. Non-standard names will have a trailing '_tocheck' flag.",
    )
    parser.add_argument(
        "-i",
        "--input_pdf_dir",
        default=os.path.join("inputs", "pdf_repository"),
        help="the directory where the PDFs to be extracted can be found.",
    )
    parser.add_argument(
        "-o",
        "--write_tables_to_dir",
        default=os.path.join("outputs", "individual_reports"),
        help="the directory at which the final CSVs for each report will be written.",
    )
    parser.add_argument(
        "-r",
        "--rules",
        default=os.path.join("inputs", "rules.json"),
        help="the path of the rules file.",
    )
    parser.add_argument(
        "-m",
        "--metadata",
        default=os.path.join("inputs", "metadata.json"),
        help="the path of the metadata file.",
    )
    parser.add_argument(
        "--intermediate_files_dir",
        default=os.path.join("intermediate_files"),
        help="the path of the metadata file.",
    )
    parser.add_argument(
        "-j",
        "--write_justifications_to",
        default=None,
        help="the path at which to write the file with the justifications for the rules.",
    )
    parser.add_argument(
        "--after_intervention_dir",
        default=os.path.join("inputs", "files_after_human_intervention"),
        help="the path of the directory with manually edited input CSVs.",
    )
    args = parser.parse_args()

    rules = Rules(args.rules)

    not_extracted = extract_all_reports(
        get_reports_from_metadata(args.metadata),
        rules,
        default_max_reports=1000,
        force_rewrite=args.force_rewrite,
        operator_wont_intervene=args.operator_wont_intervene,
        input_pdf_directory=args.input_pdf_dir,
        intervened_dir=args.after_intervention_dir,
        intermediate_files_dir=args.intermediate_files_dir,
        write_tables_to_dir=args.write_tables_to_dir,
        quiet=args.quiet,
    )

    rules.write(args.rules)
    if args.write_justifications_to:
        rules.export_justifications_to_csv(args.write_justifications_to)

    print(f" \nNot extracted ({len(not_extracted)}) below:")
    print("\n".join([str(f) for f in not_extracted]))
    print(f" \n Total time spent: {datetime.now() - init_time}\n")
