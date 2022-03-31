from portfolio.Database import DB

ES_HOSTNAME = None
db = DB(ES_HOSTNAME)


def test_input():
    print("Given an integer, db read must return a dict")
    assert type(db.read("AAPL", 1923123)) == list


def test_multiple_input():
    print("Given an integer, db read must return a dict")
    assert type(db.read("AAPL", [1923123, 192312])) == list


def test_multiple_input_length():
    print("Given an integer, db read must return a dict")
    T = [1923123, 192312]
    out = db.read("AAPL", T)
    assert len(out) == len(T)


