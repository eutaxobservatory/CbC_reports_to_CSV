# What this code is for
The purpose of the code in this repository is to act as a pipeline that takes multiple PDF files with country-by-country reports by large multinational companies (together with their appropriate metadata - such as year of the report, and multinational-level info) and creates a uniform database comprised of such reports.
## The PDFs as they are
The PDF files present the data in a variety of forms. Input files exist in multiple languages and currencies, with figures shown in units, thousands, millions and even billions - for currencies worth orders of magnitude less than EUR or USD.

Tables with CbC data usually show the countries as rows and the variables of interest as columns, but exceptions exist that need to be handled by the software. In addition, across the reports collected, there is wide variability of the names used to refer to the standard variables. For example, "Unrelated party revenues" can be referred as ‘Third-party revenue’, ‘Revenues from unrelated parties’, ‘Revenues from third party sales’ or ‘Income from sales to third parties’. A similar case is found for jurisdiction names.

A central requirement for the code is that country names and variables must be made uniform and this process should be reproducible - so that changes can be made and the data base updated. Decisions by the operator of the software should be recorded, along with the corresponding justifications.  

# How to use this software
## Requirements
To run this software (and, in particular, the test) you will need python 3.10, an API key for extracttable.com (for the examples in example, i put it in `example/.et_key`) and the python libraries specified in `requirements.txt`. Ten free API credits for extracttable.com can be requested [here](https://www.extracttable.com/signup/trial.html).

See the `example` folder for how to run it. `extraction` is a package, and `python3 -m extraction -h` instructs on how to run it.



## Basic workflow
The metadata file is read in order to get information regarding which reports to extract, where they can be found and, for each file, which pages are important, and the year the data is relative to. The metadata file can also include other information to be written to the output - location of the multinational, sectors of activity, _et cetera_.

The extraction from PDF to CSV is done with [camelot-py](https://pypi.org/project/camelot-py/) and [ExtractTable.com](https://www.extracttable.com). By default, the responses from these services is cached.

Then, the process of standardizing the data begins. There are two situations in which the operator is required to act:
1. Multiple, incompatible tables have been found and the software in incapable of deciding which are the ones with CbCR data. In this case the operator should start from the tables stored in the intermediate files and select one (potentially by concatenating information on multiple tables) to be used as the single input table.
2. The column names of the tables or the names of the jurisdictions are not standard and no applicable rule exists for making it so. This causes the program to prompt the operator for a standardized name or an instruction stating that such column/row should be deleted.

Each report is extracted individually. `concat_extracted.py` allows the operator to get a single CSV with the information of all the reports. Further, it takes as input a second set of rules, allowing a more strict standardization: it may be the case that is is ok to have rows with data on aggregations for the individual extractions but that these rows should be dropped for the final CSV.

## Filenames throughout the workflow
The final CSV file pertaining to a given report will be named `<multinational>_<end_of_year>.csv`. Intermediate files will be named `<multinational>_<end_of_year>_<i>.csv`, as there are potentially multiple such files (one per table found by ExtractTable.com and camelot-py).


## Standardizing rules
Two sets for rules are used when extracting a report: `column_rules` and `jurisdiction_rules`.

Both column and jurisdiction rules can be either 'strict' in the sense that the 'source' must be match exactly or 'regex' rules, that are applicable if the regex is matched. 

When introducing new rules, the operator can specify their scope: rules can be applicable to any report, to all reports of a given multinational or to just one report.
