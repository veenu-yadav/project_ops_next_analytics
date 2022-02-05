a = 9
b = 10

def sum(a, b):
    return a + b

def test_sum():
    result = sum(9, 1)

    assert result == 10
    assert result > 5
