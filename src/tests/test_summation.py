import pytest

a = 9
b = 10


@pytest.fixture()
def x():
    tmp = 5
    return tmp


def sum(a, b):
    return a + b


@pytest.mark.one
@pytest.mark.xfail
def test_sum(x):
    result = sum(9, 1)

    assert result == 100
    assert result > x


@pytest.mark.one
@pytest.mark.skip
def test_sample(x):
    result = 10

    assert result == 10
    assert result > x
