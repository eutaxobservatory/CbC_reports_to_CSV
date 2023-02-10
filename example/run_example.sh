#!/bin/bash
export PYTHONPATH=${PYTHONPATH}:..
echo "Running extraction"
python3 -m extraction -f -i "inputs/pdfs_to_test/" --write_tables_to "outputs/extracted_tables" --metadata "inputs/metadata.json" --rules "inputs/rules.json" --after_intervention_dir "inputs/amended_tables" -j "outputs/justifications.csv"
echo "Running concatenation"
python3 -m extraction.concat_extracted -i "outputs/extracted_tables" -o "outputs/concatenated_tables.csv" --metadata "inputs/metadata.json" --rules "inputs/rules_concat.json"
echo "Done."
