from async_anywhere import myfunc


def test_first():
    assert 1 == 1, "OK"

def test_second():
    assert myfunc() == 1, "OK"