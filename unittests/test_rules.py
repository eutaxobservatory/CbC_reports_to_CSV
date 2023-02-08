import os.path
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from cbc_report import CbCReport, get_reports_from_metadata
from rules import Rules
from exceptions import RulesError


class TestRules(unittest.TestCase):
    def setUp(self):
        self.reports = get_reports_from_metadata(
            """
{
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
    }
    }"""
        )
        self.rules = Rules(
            """{
    "column_rules": {
        "default": {
            "corporate income taxes accrued": "tax_paid",
            "_regex_^total.*": "foobar"

        },

        "bp": {
            "default": {
                "commentary see country analysis": "commentary",
                "corporate taxes accrued": "tax_accrued",
                "tangible assets": "tangible_assets",
                "corporate income taxes accrued": "tax_accrued"
            }
        }
    },
    "jurisdiction_rules": {
        "default": {},

        "bp": {
            "default": {
                "other middle east region": {
                    "sink": "other middle east region",
                    "justification": "<no justification>"
                }
            },
            "2020.12.31": {
                "middle east regionb": {
                    "sink": "other middle east",
                    "justification": "<no justification>"
                },
                "north africa region": {
                    "sink": "north africa region",
                    "justification": "<no justification>"
                }
            }
        }
    }
}"""
    )

    def test_strict_sink(self):
        bp_report = self.reports[0]
        sink = self.rules.get_sink_from_strict(
            bp_report, "corporate income taxes accrued", "c"
        )
        self.assertEqual(sink, "tax_accrued")
    def test_regex_sink(self):
        bp_report = self.reports[0]
        sink = self.rules.get_sink_from_regex(
            bp_report, "totalioio", "c"
        )
        self.assertEqual(sink, "foobar")
    def test_regex_sink2(self):
        bp_report = self.reports[0]
        sink = self.rules.get_sink_from_regex(
            bp_report, " totalx", "c"
        )
        self.assertEqual(sink, None)

    def test_Rules__init__(self):
        self.assertRaises(RulesError, Rules, "foo")
    
    def test_Rules__init__bad_string_input(self):
        self.assertRaises(RulesError, Rules, """{
    "column_rules": {
        "default": {
            "corporate income taxes accrued": "tax_paid",
            "_regex_^total.*": "foobar"

        },

        "bp": {
            "default": {
                "commentary see country analysis": "commentary",
                "corporate taxes accrued": "tax_accrued",
                "tangible assets": "tangible_assets",
                "corporate income taxes accrued": "tax_accrued"
            }
        }
    }
    }""")
