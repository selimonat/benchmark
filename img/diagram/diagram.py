from diagrams import Cluster, Diagram
from diagrams.elastic.elasticsearch import Elasticsearch, Kibana
from diagrams.programming.language import Python
from diagrams.generic.storage import Storage


with Diagram("", show=False):
    connector = Python("DB connector")
    portfolio = Python("Portfolio Backend")
    parser = Python("Export File Parser")
    file = Storage("Portfolio File Export")
    with Cluster("Services"):
        downloader = Python("Data Downloader")
        es = Elasticsearch("ES")
        kibana = Kibana('Monitoring')

    downloader >> connector >> es << kibana
    file >> parser << connector << es
    portfolio << parser
