from diagrams import Cluster, Diagram
from diagrams.elastic.elasticsearch import Elasticsearch, Kibana
from diagrams.programming.language import Python
from diagrams.generic.storage import Storage
from diagrams.programming.language import Bash


with Diagram("", show=False):
    connector = Python("DB connector\n(portfolio.DataBase)")
    portfolio = Python("Portfolio backend\n(set of tickers)\n(portfolio.Portfolio)")
    parser = Python("Export file Parser\n(portfolio.Parser)")
    position = Python("Positions\n(portfolio.Position)")
    ticker = Python("Set of positions\n(portfolio.Ticker)")
    file = Storage("Portfolio file export")
    cli = Bash("CLI")
    with Cluster("Services"):
        downloader = Python("Downloader Bot\n(downloader)")
        es = Elasticsearch("Elasticsearch")
        kibana = Kibana('Monitoring')

    downloader >> connector >> es << kibana
    file >> parser >> position >> ticker >> portfolio >> connector << es
    cli >> portfolio