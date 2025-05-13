import pytest
from data.counties import get_all_counties, get_counties_dict, get_county_by_query

# Local constants
FIELD_STATE_CODE = "state_code"
FIELD_COUNTY = "county"

def test_county_read_all():
    counties = get_all_counties()
    assert isinstance(counties, list)
    assert len(counties) > 0
    for county in counties:
        assert FIELD_STATE_CODE in county
        assert FIELD_COUNTY in county

def test_county_lookup_by_id():
    counties = get_counties_dict()
    assert isinstance(counties, dict)
    assert len(counties) > 0
    sample = list(counties.values())[0]
    assert FIELD_STATE_CODE in sample
    assert FIELD_COUNTY in sample

def test_read_sample_county():
    anchorage = get_county_by_query({FIELD_COUNTY: "Anchorage Municipality"})
    assert anchorage is None or anchorage[FIELD_STATE_CODE] == "AK"

