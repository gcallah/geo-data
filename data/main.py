from fastapi import FastAPI, Query, HTTPException, Request
from typing import Optional
from data.counties import get_all_counties, get_county_by_query
from data.states import get_all_states, get_state_by_query

app = FastAPI()


@app.get("/states")
def read_states(request: Request):
    states = get_all_states()
    if not states:
        raise HTTPException(status_code=404, detail="No states found")

    origin = str(request.base_url).rstrip("/")

    for state in states:
        code = state.get("state_code", "")
        state["county_link"] = f"{origin}/counties?state_code={code}"
        state.pop("_id", None)

    return states


@app.get("/states/{state_code}")
def read_state_by_code(state_code: str, request: Request):
    try:
        result = get_state_by_query({"state_code": state_code.upper()})
        if result is None:
            raise HTTPException(status_code=404, detail=f"No state found for {state_code}")
        result.pop("_id", None)

        origin = str(request.base_url).rstrip("/")
        result["county_link"] = f"{origin}/counties?state_code={state_code.upper()}"

        return result
    except Exception:
        raise HTTPException(status_code=404, detail="Internal server error")


@app.get("/counties")
def read_counties(
    request: Request,
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

    filtered = counties[offset:offset + limit]

    if not filtered:
        raise HTTPException(status_code=404, detail="No counties matched the filters")

    origin = str(request.base_url).rstrip("/")
    for county in filtered:
        name = county.get("county", "").replace(" ", "%20")
        county["county_link"] = f"{origin}/counties/{name}"
        county.pop("_id", None)

    return filtered


@app.get("/counties/{county_name}")
def read_county_by_name(county_name: str, request: Request):
    result = get_county_by_query({"county": county_name})
    if not result:
        raise HTTPException(status_code=404, detail=f"County '{county_name}' not found")
    result.pop("_id", None)

    origin = str(request.base_url).rstrip("/")
    name = result.get("county", "").replace(" ", "%20")
    result["county_link"] = f"{origin}/counties/{name}"

    return result




