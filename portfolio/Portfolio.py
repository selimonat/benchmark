class DB:
    def __init__(self):
        self.connection = []

    def write(self, ticker, data):
        return 1

    def read(self, ticker, date):
        return 1


class Position:
    def __init__(self, ticker, date):
        self.ticker = ticker
        self.open_at = date
        self.value_at_open = DB.read(ticker, date)
        self.value_now = DB.read(ticker)

    def returns(self, date):
        """
            Returns returns as percentage for all dates.
        :param date:
        :return:
        """
        pass





class Portfolio:
    def __init__(self):
        self.position = []
    def