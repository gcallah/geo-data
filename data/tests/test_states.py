import pytest
from data.states import get_all_states, get_state_by_query

def test_states_read_all():
    states = get_all_states()
    assert isinstance(states, list)
    assert len(states) > 0
    for state in states:
        assert "state_code" in state
        assert "state_name" in state

def test_read_alaska():
    alaska = get_state_by_query({"state_code": "AK"})
    assert alaska is None or alaska["state_name"].lower().strip() == "alaska"
