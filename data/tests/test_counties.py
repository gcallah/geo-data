import pytest
from data.counties import get_all_counties, get_counties_dict, get_county_by_query

def test_county_read_all():
    counties = get_all_counties()
    assert isinstance(counties, list)
    assert len(counties) > 0
    for county in counties:
        assert "state_code" in county
        assert "county" in county

def test_county_lookup_by_id():
    counties = get_counties_dict()
    assert isinstance(counties, dict)
    assert len(counties) > 0
    sample = list(counties.values())[0]
    assert "state_code" in sample
    assert "county" in sample

def test_read_sample_county():
    anchorage = get_county_by_query({"county": "Anchorage Municipality"})
    assert anchorage is None or anchorage["state_code"] == "AK"
