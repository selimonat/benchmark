from diagrams import Cluster, Diagram
from diagrams.elastic.elasticsearch import Elasticsearch, Kibana
from diagrams.programming.language import Python
from diagrams.generic.storage import Storage


with Diagram("", show=False):
    connector = Python("DB connector\n(portfolio.DataBase)")
    portfolio = Python("Portfolio Backend\n(portfolio.Portfolio)")
    parser = Python("Export File Parser\n(portfolio.Parser")
    file = Storage("Portfolio File Export")
    with Cluster("Services"):
        downloader = Python("Downloader Bot\n(downloader)")
        es = Elasticsearch("Elasticsearch")
        kibana = Kibana('Monitoring')

    downloader >> connector >> es << kibana
    file >> parser << connector << es
    portfolio << parser
