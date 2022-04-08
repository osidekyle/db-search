import elasticsearch
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host' : 'localhost', 'port' : 9200, 'scheme' : 'http'}], max_retries=30,
                       retry_on_timeout=True, request_timeout=30)
print(es.ping())


while True:
    query = input("Enter Search Query: ")
    resp = es.search(index="news_index", body={"query":{"fuzzy": {"title":{"value" : query, "fuzziness": 2}}}})
    for hit in resp['hits']['hits']:
        print(hit['_source']['title'])

