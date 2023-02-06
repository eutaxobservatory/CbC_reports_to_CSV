import json


class CbCReport:
    def __init__(self, group_name, end_of_year, metadata: dict) -> None:
        self.group_name = group_name
        self.end_of_year = end_of_year
        self.metadata = metadata

    def __str__(self) -> str:
        return f"CbCReport(group_name = {self.group_name},end_of_period = {self.end_of_year}, to_extract = {self.to_extract})"

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.group_name},{self.end_of_year},{self.metadata})"

    @property
    def to_extract(self) -> bool:
        try:
            return self.metadata['to_extract'].casefold() == 'yes'
        except KeyError:
            return False
    @property
    def pages(self):
        try:
            return self.metadata.get('pages')
        except KeyError:
            raise AttributeError(f"{self} has no `pages` metadata.")
    @property
    def filename_of_source(self):
        try:
            return self.metadata.get('filename')
        except KeyError:
            raise AttributeError(f"{self} has no `filename` metadata.")
    
    @property
    def min_nb_cols(self):
        try:
            return self.metadata['min_nb_cols']
        except KeyError:
            return 2
    
    @property
    def min_nb_terms(self):
        try:
            return self.metadata['min_nb_terms']
        except KeyError:
            return 2

    @property
    def min_nb_jurs_per_table(self):
        try:
            return self.metadata['min_nb_jurs_per_table']
        except KeyError:
            return 2


def get_reports_from_metadata(metadata_path : str) -> list[CbCReport]:
    """"Reads the metadata.json file and returns a list of CbCReport objects."""
    reports = []
    with open(metadata_path, mode='r') as infile:
        all_metadata = json.load(infile)
    for mnc, value in all_metadata.items():
        mnc_all_metadata = dict(value)
        try:
            default_values = dict(mnc_all_metadata.pop('default'))
        except KeyError:
            default_values = dict()
        for end_of_year, report_specific_metadata in mnc_all_metadata.items():
            report_metadata = default_values.copy()
            report_metadata.update(report_specific_metadata)
            reports.append(CbCReport(mnc, end_of_year, report_metadata))
    return reports

