import json
from exceptions import MetadataError


class CbCReport:
    """Represents a Country-by-Country report to be extracted. It is comprised of metadata information to be used during the extraction process, such as the name of the group, the end of the reporting period, the base units used in the report, the currency, the parent entity name, the NACE2 main code, the NACE2 core code, the columns to flip, the BVD sector, the parent jurisdiction, and the pages where the tables are located. It also contains the filename of the source PDF file."""
    def __init__(self, group_name, end_of_year, metadata: dict) -> None:
        try:
            self.group_name = group_name
            self.end_of_year = end_of_year
            self.metadata = metadata
            if self.to_extract:
                self.unit_multiplier = int(metadata.get("unit"))
                self.currency = metadata.get("currency")
                self.parent_entity_name = metadata.get("parent_entity_name", None)
                self.nace2_main = metadata.get("nace2_main", None)
                self.nace2_core_code = metadata.get("nace2_core_code", None)
                self.columns_to_flip = self.metadata.get('columns_to_flip', None)
                self.bvd_sector = self.metadata.get('bvd_sector', None)
                self.parent_jurisdiction = self.metadata.get('parent_jurisdiction', None)
        except KeyError as exc:
            raise MetadataError(f"Missing metadata for {self} : {exc}") from exc

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
        except KeyError as exc:
            raise MetadataError(f"{self} has no `pages` metadata.") from exc
    @property
    def filename_of_source(self):
        try:
            return self.metadata.get('filename')
        except KeyError as exc:
            raise MetadataError(f"{self} has no `filename` metadata.") from exc
    
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


def get_reports_from_metadata(metadata : str) -> list[CbCReport]:
    """"Reads the metadata.json file and returns a list of CbCReport objects."""
    reports = []
    try:
        all_metadata = json.loads(metadata)
    except json.decoder.JSONDecodeError:
        with open(metadata, mode='r', encoding="utf-8") as infile:
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

