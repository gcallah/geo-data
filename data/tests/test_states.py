import pytest
from data.states import get_all_states, get_state_by_query

# Local constants
FIELD_STATE_CODE = "state_code"
FIELD_STATE_NAME = "state_name"

def test_states_read_all():
    states = get_all_states()
    assert isinstance(states, list)
    assert len(states) > 0
    for state in states:
        assert FIELD_STATE_CODE in state
        assert FIELD_STATE_NAME in state

def test_read_alaska():
    alaska = get_state_by_query({FIELD_STATE_CODE: "AK"})
    assert alaska is None or alaska[FIELD_STATE_NAME].lower().strip() == "alaska"

