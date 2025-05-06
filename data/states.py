from .db_connect import read_all, read_one, COL_STATES

def get_all_states():
    return read_all(COL_STATES)

def get_state_by_query(query):
    print("QUERY RECEIVED:", query)
    from .db_connect import get_collection, COL_STATES  # import this temporarily
    state = get_collection(COL_STATES).find_one(query)
    print("FOUND STATE:", state)
    return state

