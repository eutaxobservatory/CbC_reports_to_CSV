#!/bin/bash
export PYTHONPATH=..:$PYTHONPATH.

python3 -m extraction -f -i "inputs/pdfs_to_test/" --write_tables_to "outputs/test_extracted_tables" --metadata "inputs/metadata.json" --rules "inputs/rules.json" --after_intervention_dir "inputs/amended_tables" -j "outputs/justifications.csv"
