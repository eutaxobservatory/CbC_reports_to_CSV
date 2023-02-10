import sys, time, os.path

sys.path.append("..") # Adds higher directory to python modules path to import extraction

from extraction import extract_all_reports, get_reports_from_metadata, Rules, concatenate_tables

init_time = time.time()
write_tables_to_dir=os.path.join(
        "outputs", "extracted_tables"
    )
reports = get_reports_from_metadata(
        os.path.join( "inputs", "metadata.json")
    )
rules = Rules(os.path.join( "inputs", "rules.json"))
rules_concat = Rules(os.path.join( "inputs", "rules_concat.json"))
not_extracted = extract_all_reports(
    reports,
    rules,
    default_max_reports=1000,
    force_rewrite=True,
    operator_wont_intervene=False,
    input_pdf_directory=os.path.join( "inputs", "pdfs_to_test"),
    intervened_dir=os.path.join(
        "inputs",
        "amended_tables",
    ),
    intermediate_files_dir=os.path.join( "intermediate_files"
    ),
    write_tables_to_dir=write_tables_to_dir,
    quiet=False,
)

rules.write(os.path.join( "inputs", "rules.json"))
rules.export_justifications_to_csv(os.path.join(
       "outputs", "justifications.csv"
    ))

print(f" \nNot extracted ({len(not_extracted)}) below:")
print("\n".join([str(f) for f in not_extracted]))
print(f" \n Total time spent: {time.time() - init_time}\n")

concatenate_tables(write_tables_to_dir, os.path.join("outputs", "concatenated_tables.csv"),rules_concat,reports)