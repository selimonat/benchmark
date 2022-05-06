from typing import AnyStr, List
from elasticsearch_dsl import Q


def matcher(ticker: AnyStr):
    """
    Basic query to fetch all matching tickers.
    """
    return Q('match', ticker=ticker)


def time_filter(ticker: List, dates: List):
    """
    Returns ticker values at specific dates.
    """
    return {"query": {"bool": {"must": [{"terms": {"ticker": ['MA']}},
                                        {"terms": {"date": [1579478400, 1579564800, 1579651200, 1579737600]}}]}}}
# Q('bool', must=[Q('terms', date=['1579564800','1579651200']), Q('terms', **{'ticker.keyword':['MA','GOOG']})])