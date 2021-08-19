import argparse
import logging
import sys

from es_client import EsClient, EsApiCall

log = logging.getLogger()
log.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(levelname)s %(asctime)s - %(message)s")

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(formatter)

log.addHandler(stdout_handler)


def fill_up(es_api):
    count = 1
    for i in range(0, 10):
        index_name = "test_index-2021.{}".format(i)
        log.info("fill index {}".format(index_name))
        for d in range(count, count + 100):
            document = dict(
                message="message num {}".format(d)
            )
            es_api.post(path="/{}/_doc".format(index_name), data=document)
        count = d + 1


def main():
    url = "http://localhost:9200"
    es_client = EsClient(es_url=url, log=log)
    es_api = es_client.get_api()

    indices, code = es_api.get(path="/_cat/indices")

    #
    compact = []
    for index in indices:
        logging.info("Index: {}, Docsize: {}".format(index['index'], index['docs.count']))
        compact.append(index['index'])

    cmd = {
        "source": {
            "index": compact
        },
        "dest": {
            "index": "optimized-000001"
        }
    }
    # es_api.post(path="/_reindex", data=cmd)


    # all_docs = get_all_docs(es_api, "optimized-000001")
    # logging.info(len(all_docs))
    # es_api.delete(path="/test_index-*")
    fill_up(es_api)

def get_all_docs(es_api,index):
    cmd = {
        "query": {
            "match_all": {}
        }
    }
    all_docs = []
    docs, code = es_api.post(path="/{}/_search?scroll=1m".format(index), data=cmd)
    scroll_id = docs['_scroll_id']
    logging.debug(docs['_scroll_id'])
    for doc in docs['hits']['hits']:
        logging.debug(doc)
        all_docs.append(doc)

    while docs['hits']['hits']:
        cmd={ "scroll" : "1m", "scroll_id" : scroll_id }
        docs, code  = es_api.post(path="/_search/scroll", data=cmd)
        scroll_id = docs['_scroll_id']

        logging.debug(docs['_scroll_id'])
        for doc in docs['hits']['hits']:
            logging.debug(doc)
            all_docs.append(doc)

    return all_docs

if __name__ == '__main__':
    main()
