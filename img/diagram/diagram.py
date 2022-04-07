from diagrams import Cluster, Diagram
from diagrams.elastic.elasticsearch import Elasticsearch, Kibana
from diagrams.programming.language import Python
from diagrams.generic.storage import Storage


with Diagram("Event processing", show=False):
    connector = Python("DB connector")
    portfolio = Python("Portfolio")
    parser = Python("Parser")
    file = Storage("Portfolio Export")
    with Cluster("Services"):
        downloader = Python("Downloader")
        es = Elasticsearch("ES")
        kibana = Kibana('Monitoring')

    downloader >> connector >> es << kibana
    file >> parser << connector << es
    portfolio << parser
