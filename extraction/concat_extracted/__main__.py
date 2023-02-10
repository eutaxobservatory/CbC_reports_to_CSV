import argparse
from . import concatenate_tables
from .. import get_reports_from_metadata, Rules

parser = argparse.ArgumentParser(description="Concatenate tables.")
parser.add_argument(
    "-o",
    "--aggregate_output_path",
    help="Path to output file. Must be provided.",
)
parser.add_argument(
    "-i",
    "--extracted_tables_at",
    help="Path to directory containing extracted tables. Must be provided.",
)
parser.add_argument(
    "-m",
    "--metadata_path",
    help="Path to metadata file. Must be provided.",
)
parser.add_argument(
    "-r",
    "--rules_file",
    help="Path to rules file. Must be provided.",
)
args = parser.parse_args()

reports_ = get_reports_from_metadata(args.metadata_path)
rules_ = Rules(args.rules_file)
concatenate_tables(
    args.extracted_tables_at, args.aggregate_output_path, rules_, reports_
)
