from fastapi import FastAPI, Query
from typing import Optional
from data.counties import get_all_counties, get_county_by_query
from data.states import get_all_states, get_state_by_query

app = FastAPI()

@app.get("/states")
def read_states():
    return get_all_states()

@app.get("/states/{state_code}")
def read_state_by_code(state_code: str):
    try:
        print("Looking up:", state_code)
        result = get_state_by_query({"state_code": state_code.upper()})
        print("RESULT:", result)
        if result is None:
            return {"error": f"No state found for {state_code}"}
        result.pop("_id", None)  #REMOVE ObjectId
        return result
    except Exception as e:
        print("ERROR OCCURRED:", str(e))
        return {"error": "Internal error", "details": str(e)}


@app.get("/counties")
def read_counties(
    state_code: Optional[str] = Query(None),
    min_population: Optional[int] = Query(None),
    max_population: Optional[int] = Query(None),
    min_area: Optional[float] = Query(None),
    max_area: Optional[float] = Query(None),
    has_metro_area: Optional[bool] = Query(None),
    sort_by: Optional[str] = Query(None, description="Options: 'population', 'area_sq_miles'"),
    descending: bool = Query(False),
    limit: Optional[int] = Query(50),
    offset: Optional[int] = Query(0)
):
    counties = get_all_counties()

    #filters
    if state_code:
        counties = [c for c in counties if c.get("state_code") == state_code.upper()]
    if min_population is not None:
        counties = [c for c in counties if c.get("population", 0) >= min_population]
    if max_population is not None:
        counties = [c for c in counties if c.get("population", 0) <= max_population]
    if min_area is not None:
        counties = [c for c in counties if c.get("area_sq_miles", 0) >= min_area]
    if max_area is not None:
        counties = [c for c in counties if c.get("area_sq_miles", 0) <= max_area]
    if has_metro_area is not None:
        if has_metro_area:
            counties = [c for c in counties if c.get("metro_area")]
        else:
            counties = [c for c in counties if not c.get("metro_area")]

    if sort_by in {"population", "area_sq_miles"}:
        counties.sort(key=lambda x: x.get(sort_by, 0), reverse=descending)

    return counties[offset:offset + limit]

@app.get("/counties/{county_name}")
def read_county_by_name(county_name: str):
    result = get_county_by_query({"county": county_name})
    if result:
        result.pop("_id", None)  #REMOVE ObjectId
    return result

