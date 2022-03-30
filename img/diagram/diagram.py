from diagrams import Cluster, Diagram
from diagrams.elastic.elasticsearch import Elasticsearch, Kibana
from diagrams.programming.language import Python


with Diagram("Event processing", show=False):
    with Cluster("Containers"):
        downloader = Python("Downloader")
        es = Elasticsearch("ES")
        kibana = Kibana('Monitoring')

    downloader >> es << kibana
