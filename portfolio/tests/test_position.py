from portfolio.Portfolio import Position
from portfolio import utils as utils

NOW = utils.today()


def test_output_input_length_match():
    for input_ in [[NOW], [NOW, NOW +1]]:
        pos = Position('AAPLE', input_)
        assert len(pos.value_at(input_)) == len(input_)
