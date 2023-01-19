#!/bin/bash

#test runs from root

python3 main.py -f -i "test_input/pdfs_to_test/" --write_tables_to "./outputs/test_extracted_tables" --metadata "test_input/metadata.json" --rules "test_input/rules.json"
python3 concat_extracted.py -i "./outputs/test_extracted_tables" -o "./outputs/.test_all_tables.csv" --metadata "test_input/metadata.json" --rules "test_input/rules_concat.json"