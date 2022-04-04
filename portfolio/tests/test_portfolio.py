from portfolio import utils

def test_return_integer():
    # distance between Start and stop times should return an integer when divided by number of seconds in a day.
    # step size check.
    pass

    # (utils.today() - parser.parse("20200101T000000 UTC").timestamp()) /  (60*60*24)

#  TODO: date columns must be string
#  TODO: asset return at purchase date must be 100%
#  TODO: asset time course table must have as many columns as the number of shares.
#  TODO: Transaction table must have indices from 0 to the shape[0] of the table.
#  TODO: Two times the same ticker must result in two different columns in the time-course table.
#  TODO: there should never be row filled ALL columns with nan.
#  TODO: When same ticker added twice the lot number should be iterated.
#  TODO: When same ticker added twice the asset price must be the mean of them