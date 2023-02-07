import csv
import re
from itertools import filterfalse, tee
from os.path import join

import pandas as pd
import pycountry
from log import logger

pd.set_option("display.max_columns", None)


PATH_EXCHANGE_RATES = join("configuration", "rolling_avg_rate.csv")
PATH_COUNTRY_NAME_TO_ISO3166 = join("configuration", "countryish_names_to_code.csv")

with open(PATH_COUNTRY_NAME_TO_ISO3166, mode="r") as infile:  # country_fix
    CONTRY_TO_ISO3166_MAPPING = dict(
        (row[0].casefold(), row[1]) for row in csv.reader(infile) if row[1]
    )
with open(PATH_EXCHANGE_RATES, mode="r") as infile:  # exchange_rate
    EXCHANGE_RATES = dict(((row[0], row[1]), row[3]) for row in csv.reader(infile))


ISO3166_ALPHA3 = set([i.alpha_3 for i in pycountry.countries])
ISO3166_ALPHA3.add("XKX")  # Kosovo

# Regexes compilation

STAND_ALONE_CHAR_RE = re.compile(r"(\s\S\s*$)|(\smn\s*$)")
CHARS_TO_PURGE_FROM_COLUMN_NAMES_RE = re.compile(r"[^a-zA-Z ']")
NOT_NUMERIC_CHARS_RE = re.compile(r"[^0-9\(\)\-\.%,]")
PERCENTAGE_FORMAT_RE = re.compile(r"(\d+[.,]?\d*)\w?%")
ETR_FORMAT_RE = re.compile(r"(-?\d+[\.]?\d*)")
ENGLISH_COLUMN_TERMS = [
    "tax",
    "related",
    "income",
    "employee",
    "unrelated",
    "third",
    "tangible",
    "assets",
    "party",
    "parties",
    "accrued",
    "profit",
    "revenue",
]
ITALIAN_COLUMN_TERMS = ["imposte", "pagate", "reddito", "utile"]
CBCR_TERMS = ENGLISH_COLUMN_TERMS + ITALIAN_COLUMN_TERMS
YEAR_REGEX = re.compile(r"^\d{4}$")
DOUBLE_DIGITS = re.compile(r"\d{2}")


def partition(pred, iterable):
    "Use a predicate to partition entries into false entries and true entries"
    # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = tee(iterable)
    return filterfalse(pred, t1), filter(pred, t2)


def jurisdiction_to_iso3166(z):
    x = neatify(z)
    if x.upper() in ISO3166_ALPHA3:
        return x.upper()
    c = CONTRY_TO_ISO3166_MAPPING.get(x, "")
    # dont allow empty lines or they get assigned GBR.
    if x and not c and (z.upper() not in ISO3166_ALPHA3):
        try:
            logger.debug(my_search_fuzzy(pycountry.countries, x))
            c = my_search_fuzzy(pycountry.countries, x)[0][0]
        except:
            # don't bother if fails, more attempts to ensue.
            pass
    # "<empty> so that it is easier to extract with sanity_checks.get_non_standard_jurisdiction"
    return c if c else (x if x else "<empty>")


def neatify(arg):

    """Returns a string with all non-[ascii-alphanumeric] characters removed, and all words lowercased."""
    return " ".join(
        STAND_ALONE_CHAR_RE.sub(
            " ", CHARS_TO_PURGE_FROM_COLUMN_NAMES_RE.sub(" ", arg)
        ).split()
    ).casefold()


def my_search_fuzzy(self, query):
    """
    copied from github of pycountry but ignores subdivisions and cuts through uninteresting things to us.
    """
    query = pycountry.remove_accents(query.strip().lower())
    if query in ["africa", "america", "europe"]:
        return [(query, 51)]
    # A country-code to points mapping for later sorting countries
    # based on the query's matching incidence.
    results = {}

    def add_result(country, points):
        results.setdefault(country.alpha_3, 0)
        results[country.alpha_3] += points

    # Prio 1: exact matches on country names
    try:
        add_result(self.lookup(query), 50)
    except LookupError:
        pass

    # Prio 3: partial matches on country names
    for candidate in self:
        # Higher priority for a match on the common name
        for v in [
            candidate._fields.get("name"),
            candidate._fields.get("official_name"),
            candidate._fields.get("comment"),
        ]:
            if v is None:
                continue
            v = pycountry.remove_accents(v.lower())
            if query in v:
                # This prefers countries with a match early in their name
                # and also balances against countries with a number of
                # partial matches and their name containing 'new' in the
                # middle
                add_result(candidate, max([5, 30 - (2 * v.find(query))]))
                break

    if not results:
        raise LookupError(query)

    results = [
        (x[0], x[1])
        # self.get(alpha_2=x[0])
        # sort by points first, by alpha2 code second, and to ensure stable
        # results the negative value allows us to sort reversely on the
        # points but ascending on the country code.
        for x in sorted(results.items(), key=lambda x: (-x[1], x[0]))
    ]
    return results
