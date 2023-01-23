#!/bin/bash

#test runs from root

python3 main.py -f -i "test_input/pdfs_to_test/" --write_tables_to "./outputs/test_extracted_tables" --metadata "test_input/metadata.json" --rules "test_input/rules.json" --after_intervention_dir "test_input/amended_tables" -j "./outputs/justifications.csv"
python3 concat_extracted.py -i "./outputs/test_extracted_tables" -o "./outputs/all_tables_from_test.csv" --metadata "test_input/metadata.json" --rules "test_input/rules_concat.json"