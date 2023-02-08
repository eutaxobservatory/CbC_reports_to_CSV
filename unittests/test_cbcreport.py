import os.path
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from cbc_report import CbCReport, get_reports_from_metadata


class TestCBCReport(unittest.TestCase):
    def setUp(self):
        self.reports = get_reports_from_metadata(
            """
{
    "acciona": {
        "2020.12.31": {
            "columns_to_flip": [],
            "unit": "1000000",
            "currency": "EUR",
            "pages": [
                1
            ],
            "filename": "extracts/.2020_Acciona_CbCR_1.pdf",
            "to_extract": "yes"
        },
        "default": {
            "currency": "GBP",
            "bvd_sector": "Construction",
            "parent_jurisdiction": "ESP",
            "parent_entity_name": "ACCIONA SA",
            "nace2_main": "F - Construction",
            "nace2_core_code": "4120"
        }
    },
    "bhp": {
        "2020.06.30": {
            "columns_to_flip": [],
            "unit": "1000000",
            "currency": "USD",
            "pages": [
                9
            ],
            "filename": "2020_BHP_CbCR_9.pdf",
            "to_extract": "yes"
        },
        "default": {
            "bvd_sector": "Mining & Extraction",
            "parent_jurisdiction": "AUS",
            "parent_entity_name": "BHP GROUP LIMITED",
            "nace2_main": "B - Mining and quarrying",
            "nace2_core_code": "0510"
        }
    },
    "bp": {
        "2020.12.31": {
            "columns_to_flip": [],
            "unit": "1",
            "currency": "USD",
            "pages": [
                29,
                30,
                31,
                32
            ],
            "filename": "2020_BP_CbCR_29-32.pdf",
            "to_extract": "yes"
        },
        "default": {
            "parent_jurisdiction": "GBR",
            "parent_entity_name": "BP PLC",
            "nace2_main": "C - Manufacturing",
            "nace2_core_code": "1920"
        }
    },
    "inpost": {
        "2020": {
            "columns_to_flip": [],
            "unit": "1000000",
            "currency": "PLN",
            "pages": [
                106
            ],
            "filename": "2020_Inpost_CbCR_106.pdf",
            "to_extract": "yes"
        },
        "default": {
            "bvd_sector": "Retail",
            "parent_jurisdiction": "LUX",
            "parent_entity_name": "INPOST S.A.",
            "nace2_main": "G - Wholesale and retail trade; repair of motor vehicles and motorcycles",
            "nace2_core_code": "4791"
        }
    },
    "newmont": {
        "2021.12.31": {
            "pages": [
                1,
                2,
                3
            ],
            "filename": "extracts/.2021_newmont_1-3.pdf",
            "to_extract": "yes"
        },
        "default": {
            "columns_to_flip": [
                "tax_accrued"
            ],
            "bvd_sector": "Mining & Extraction",
            "unit": "1000000",
            "parent_jurisdiction": "USA",
            "currency": "USD",
            "parent_entity_name": "NEWMONT CORPORATION",
            "nace2_main": "C - Manufacturing",
            "nace2_core_code": "2441"
        }
    },
    "buzzi unicem": {
        "2020": {
            "pages": [
                98,
                99
            ],
            "filename": "2020_buzziunicem_CbCR_98-99.pdf",
            "to_extract": "yes"
        },
        "default": {
            "bvd_sector": "Leather, Stone, Clay & Glass products",
            "parent_jurisdiction": "ITA",
            "unit": "1",
            "currency": "EUR",
            "columns_to_flip": [],
            "parent_entity_name": "BUZZI UNICEM S.P.A.",
            "nace2_main": "C - Manufacturing",
            "nace2_core_code": "2351"
        }
    },
    "eni": {
        "2018": {
            "columns_to_flip": [],
            "unit": "1000",
            "currency": "EUR",
            "pages": [
                12,
                13
            ],
            "filename": "2018_ENI_CbCR_12_13.pdf",
            "to_extract": "yes"
        },
        "default": {
            "bvd_sector": "Mining & Extraction",
            "parent_jurisdiction": "ITA",
            "parent_entity_name": "ENI S.P.A.",
            "nace2_main": "B - Mining and quarrying",
            "nace2_core_code": "0610"
        }
    }
}"""
        )

    def test_get_report(self):
        acciona = self.reports[0]
        bp = self.reports[2]
        self.assertEqual(acciona.currency, "EUR")
        self.assertEqual(acciona.unit_multiplier, 1000000)
        self.assertEqual(bp.bvd_sector, None)
        self.assertEqual(acciona.bvd_sector, "Construction")

