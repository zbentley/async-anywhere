from async_anywhere.state_tracker import StateTracker, State


class MyStateTracker(StateTracker):
    Initial = State(initial=True)
    Second = State(sources=[Initial])
    Third = State(sources=[Initial])

def test_basic_state_tracker():
    inst = MyStateTracker()
    assert inst.state is inst.Initial
    assert inst.state == inst.Initial