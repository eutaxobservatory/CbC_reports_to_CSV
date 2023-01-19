# What this code is for
The purpose of the code in this repository is to act as a pipeline that takes multiple PDF files with country-by-country reports by large multinational companies (together with their appropriate metadata - such as year of the report, and multinational-level info) and creates a uniform database comprised of such reports.
## The PDFs as they are
The PDF files present the data in a variety of forms. Input files exist in multiple languages and currencies, with figures shown in units, thousands, millions and even billions - for currencies worth orders of magnitude less than EUR or USD.

Tables with CbC data usually show the countries as rows and the variables of interest as columns, but exceptions exist that need to be handled by the software. In addition, across the reports collected, there is wide variability of the names used to refer to the standard variables. For example, "Unrelated party revenues" can be referred as ‘Third-party revenue’, ‘Revenues from unrelated parties’, ‘Revenues from third party sales’ or ‘Income from sales to third parties’. A similar case is found for jurisdiction names.

A central requirement for the code is that country names and variables must be made uniform and this process should be reproducible - so that changes can be made and the data base updated. Decisions by the operator of the software should be recorded, along with the corresponding justifications.  

# How to use this software
The entry point is 'main.py'. This script allows for the following options to be set (all paths are given relative to the root of the extraction directory):
- -f: (force): forces the deletion and the re-extraction of all files flagged for extraction (as per the metadata file)
- -q (quiet) : this option does not prompt the user whenever column or jurisdiction names are not standard. Instead, non-standard names will have a trailing "_tocheck" flag.
- -i (input): specifies the directory where the PDFs to be extracted can be found. Default value: '../pdf_repository/'.
- -o (output): specifies the path at which the CSV that aggregates data from all extractions will be written. Default value: '../data.csv'.
- --after_human: specifies the directory at which the intermediate CSV that have been manually edited can be found (so that the software can pick it up from there). Default value: './files_after_human_intervention'.
- --write_tables_to: specifies the directory at which the final CSVs for each (MNC,year) pair will be written. The full path for these final files will be `<write_tables_to>/<MNC>_<year>.csv`. Default value: './extracted_tables/'
- --min_nb_CbCR_cols: specifies the minimum number of columns that must contain "CbCR terms" so that a table classifies as a "CbCR table". (motivation: it is often the case that multiple tables exist in the PDF page that contains the CbCR table and not all of them have data on CbCR)
- --rules: specifies the path of the rules file. (See rules.json below) . Default value: './configuration/rules.json'
- --metadata: specifies the path of the metadata file. (See metadata.json below). Default value: './configuration/metadata.json'

'run_again.sh' is an easy way of extracting the final .csv individual tables and the aggregated table. It calls 'main.py' twice, the second time around with a lower number of CbCR columns (so that telenor_2020 gets extracted) and in the soft mode, so that tables extracted before stay untouched. It also takes care of moving into and out of the appropriate directories.

## Basic workflow
The metadata file is read in order to get information regarding which pairs of (MNC,year) will be extracted, regarding the location of the source pdf files and the pages at which the CbCR tables can be found.

The first time a file is ran, the program calls the ExtractTable.com API in order to get the table extracted (this was the PDF to text tool tried that had the best results) into plain-text (as opposed to the binary format of PDF). The response from the server is cached for the future. CAVEAT: the ExtractTable code is not perfect and refuses some of the PDF file is it given - in particular for bigger PDF files. A workaround is to print-to-file just the relevant pages - which must be reflected in the metadata.

Once the tables are in plain-text, the process of standardizing them starts. 

0. Irrelevant tables are discarded. 
1. The column names are disentangled from the rest of the table;
2. The potentially multiple tables that form the whole CbCR are unified. ExtractTable.com returns at least a table per page. CbCRs with many jurisdictions (e.g. Shell) are split into multiple tables that must be re-assembled into a single CSV.
3. Column names and jurisdiction names are made standard and irrelevant (not conforming to CbCR standards) data is deleted - as specified by the current 'rules.json' file.
4. Information relative to exchange rates, sectors of activity, country of the ultimate parent jurisdiction is retrieved and put in the data, so that it conforms to the tidy standard. 
5. Columns are flipped (some reports multiply some columns (e.g. the tax paid) by -1), numeric values are multiplied by the unit of the report (some reports present the figures in thousands or millions of the unit.)

## Adding new files for extraction
Add file to the repository, add metadata ('configure_metadata.py' available if uncomfortable to manually edit the metadata .json file) and either 'run_again.sh' or 'main.py'.
## metadata.json
```
{
    "melia hotels": {
        "default": {
            "parent_entity_name": "MELIA HOTELS INTERNATIONAL, S.A.",
            "sector": "Travel, Personal & Leisure",
            "parent_jurisdiction": "ESP",
            "currency": "EUR",
            "unit": 1000
        },
        "2020": {
            "columns_to_flip": "[]",
            "filename": "2020_meliahotels_CbCR_90.pdf",
            "pages": [
                90
            ],
            "to_extract": "yes"
        },
        "2021": {
            "filename": "ET_2021_meliahotels.pdf",
            "columns_to_flip": "[]",
            "pages": [
                1
            ],
            "to_extract": "yes"
        }
    },
    ...
}
```
Above is an example entry of the metadata.json file for the "melia hotels" MNC. All this fields must be filled in order for the file to be successfully extracted. The configure_metadata.py script can be used to fill these fields in.

 `columns to flip` refer to the columns that should be multiplied by -1 (this is sometimes the case for tax_paid and tax_accrued, but exceptions may exist - as the Barloworld file, for which many columns need flipping.) NB: the names written here should be the final names of the columns, not the ones written in the PDF (e.g. `tax_paid`).
## Filenames throughout the workflow
The name each PDF file is given is not important. The metadata.json file is responsible for linking the PDFs to a (MNC,year) pair. It is sometimes the case that the same PDF has information that will be used in multiple such pairs - information on one MNC, for multiple years (eg. SGR).

(NB: This wasn't the case before and one can encounter legacy files in the repository - before, one file was needed per (MNC, year) pair, so there are links still present in the pdf_repository which are used in the metadata file. This can be cleaned - but not a priority as it doesn't pose a problem.)

Once a (MNC,year) pair references a PDF file, ExtractTable.com will extract the PDF tables into a CSV-ish format. This will get encoded into a json file under the ExtractTable.com directory. The reason why json is used here is so that there is a single file for each pdf - and not a CSV file per table present in the PDF. The results from ExtractTable.com are cached in these JSON files as their API is paid. Each JSON file has the same name as the PDF,  with ".json" appended to it.

Whenever the end-to-end extraction is not possible in one go, the tables obtained from ExtractTable.com can be found in the ./intermediate_files directory - this is a sufficient condition, not a necessary one. Here, they will be under `<MNC>_<year>_<i>.csv` - the last index increasing from 0 onward. Whenever necessary, these files should be edited and merged into a single `<MNC>_<year>_<i>.csv` file to be placed in the directory specified after the `--after_human` option.

As stated above, the final CSV file for each (MNC,year) pair can be found under the directory specified after the `--write_tables_to` option, as `<MNC>_<year>.csv` (The advantage of having the MNC name before is that files regarding the same MNC are close to each other, and they are often examined sequentially.)
## rules.json
You can find below an abridged rules.json file. Is is composed of `column_rules` and `jurisdiction_rules` that are used to replace, respectively, the column and jurisdiction names in any given (MNC,year) pair. This `rules.json` file is automatically edited during the execution on the `main.py` unless the `-q` option is in effect. (Naturally, it can be edited as a raw text file as well.)

Both column and jurisdiction rules can be either 'strict' in the sense that the 'source' must be match exactly or 'regex' rules, that are applicable if the regex is matched. Regexes must be written in conformance to python's `re` module. The `_regex_` part is a flag that indicates to the script it is a regex rule and not a part of the regex. When writing regex rules via the prompt from `main.py`, this will be handled implicitly.

Rules can be `default` rules if they can be used for any (MNC,year) pair, rules that apply to all years for a given MNC or one-off rules that apply to just one (MNC,year) pair.
```
{
    "column_rules": {
        "default": {
            "unrelated party revenues": "unrelated_revenues",
            "related party revenues": "related_revenues",
            "total revenues": "total_revenues",
            "profit loss before income tax": "profit_before_tax",
            "income tax paid on cash basis": "tax_paid",
            "income tax accrued current year": "tax_accrued",
            ...
        },
        "Nationalgrid": {
            "default": {
                "total": "total_revenues"
            }
        },
        "Inditex": {
            "default": {
                "income tax": "tax_paid",
                "profit before taxes consolidation": "profit_before_tax"
            }
        },
        ...
    },
    "jurisdiction_rules": {
        "default": {
            "_regex_.*total.*": "delete_row",
            "_regex_^sum .*": "delete_row",
            "_regex_.*(elimination)|(group).*": "delete_row",
            "rest of world": "other",
            "<empty>": "delete_row"
        },
        "Inditex": {
            "default": {
                "total america": "delete_row",
                "other america": "other america",
                "total asia and row": "delete_row",
                "other asia": "other asia",
                "total europe": "delete_row",
                "other europe": "other europe",
                "after consolidation": "delete_row",
                "consolidation": "delete_row",
                "before consolidation": "delete_row"
            }
        },
        "Aegon": {
            "default": {
                "_regex_(europe)|(asia)|(americas)": "delete_row"
            }
        },
        "Barloworld": {
            "default": {
                "siberia": "RUS"
            }
        },
        ...
        }
    }
}
```

## misc. (wip)
caveats:
untangling the column names from the data is not perfect - it may happen that (as was the case for ASTM 2020) the first row doesn't have enough numbers and is overlooked
it is also possible that rows that are column names have many digits and are mislabeled as data (empresas copec) - this requires human intervention but is easy to catch as there wouldnt be colnames or senseless "_tocheck" colnames. The first case is more tricky.

DSM 21: files_after_human_intervention: income tax row added manually and 4M unaccounted for in the Americas were split 2-2 between north and south americas (total income tax paid in americas was  71, with 39 in the USA and 28 in South america, so the split seems reasonable)