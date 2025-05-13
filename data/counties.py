from .db_connect import read_all, read_one, read_as_dict

# Local constants
COL_COUNTIES = "counties"
FIELD_COUNTY = "county"

def get_all_counties():
    return read_all(COL_COUNTIES)

def get_counties_dict():
    return read_as_dict(COL_COUNTIES, key_field=FIELD_COUNTY)

def get_county_by_query(query):
    return read_one(COL_COUNTIES, query)
