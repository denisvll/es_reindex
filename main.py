#!/usr/bin/env python3
import argparse
import logging
import sys
import time

from es_client import EsClient, EsApiCall

log = logging.getLogger()
log.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(levelname)s %(asctime)s - %(message)s")

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

log.addHandler(stdout_handler)


class App():
    def __init__(self, src: list, dest: str, rm_old: bool, dry_run: bool, no_wait: bool, elastic_api: EsApiCall):
        self.src = src
        self.dest = dest
        self.rm_old = rm_old
        self.elastic_api = es_api
        self.dry_run = dry_run
        self.no_wait = no_wait

    def run(self):
        self._get_indices()
        self._optimize()

    @staticmethod
    def sizeof_fmt(num, suffix='B'):
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    def _get_indices(self):
        log.info('Get indices')
        self.src_idx_list = []
        for index in self.src:
            res, code = self.elastic_api.get(path="/_cat/indices/{}".format(index))
            for res_index in res:
                self.src_idx_list.append(res_index['index'])

        if len(self.src_idx_list) == 0:
            log.info("Source indices is not found, exiting")
            sys.exit(1)

        log.debug(self.src_idx_list)

    def _get_inices_size(self, idx_list: list):
        total_size = 0
        for index in idx_list:
            stat, code = self.elastic_api.get(path="/{}/_stats".format(index))
            logging.debug(stat['_all']['total']['store'])
            total_size += stat['_all']['total']['store']['size_in_bytes']

        return total_size

    def _is_index_exist(self, index):
        result, code = self.elastic_api.head(path="/{}".format(index))
        logging.debug(result)

        if code != 200:
            return False
        else:
            return True

    def _setup_new_index(self, mapping_source):
        if not self._is_index_exist(self.dest):
            primary_shard = 1
            replica = 0
            mapping, code = self.elastic_api.get(path="/{}/_mapping".format(mapping_source))
            mapping = mapping[mapping_source]['mappings']
            settings = {
                "settings": {
                    "index": {
                        "codec": "best_compression",
                        "refresh_interval": "5s",
                        "number_of_shards": primary_shard,
                        "number_of_replicas": replica,
                    }
                },
                "mappings": mapping,
            }
            logging.debug(settings)
            self.elastic_api.put(path="/{}".format(self.dest), data=settings)
        else:
            log.info("Dest Index '{}' already exist.. exiting".format(self.dest))
            sys.exit(1)

    def _delete(self, idx_list: list):
        for index in idx_list:
            self.elastic_api.delete(path="/{}".format(index))

    def _optimize(self):
        log.info('Start optimize')
        total_size = self.sizeof_fmt(self._get_inices_size(self.src_idx_list))
        log.info("Size of source indices: {}".format(total_size))

        cmd = {
            "source": {
                "index": self.src_idx_list
            },
            "dest": {
                "index": self.dest
            }
        }

        if self.no_wait:
            query = "/_reindex?slices=5&wait_for_completion=false"
        else:
            query = "/_reindex?slices=5"

        if not self.dry_run:
            self._setup_new_index(self.src_idx_list[0])

            response, response_code = self.elastic_api.post(path=query, data=cmd)
            log.debug(response)
            if response_code != 200:
                log.error("Something goes wrong, exiting")
                sys.exit(1)
            if self.rm_old:
                log.info("delete old indices")
                self._delete(self.src_idx_list)
        else:
            log.info("DRY RUN")
            log.info(cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Reindex multiply indices into one')
    parser.add_argument('-s', '--src', help='source indices (index-*, index-0, index-1)', required=True)
    parser.add_argument('-d', '--dest', help='destination index (new_index_1)', required=True)
    parser.add_argument('-u', '--user', help='Username', required=True)
    parser.add_argument('-p', '--password', help='Password', required=True)
    parser.add_argument('--url', help='Elasticsearch url https://localhost:9200', required=True)
    parser.add_argument('--no-wait', help='don\'t wait until indices will be reindex', action='store_true', required=False)
    parser.add_argument('--delete', help='delete source indices', action='store_true',required=False)
    parser.add_argument('--dry-run', help='dry run', action='store_true', required=False)

    args = parser.parse_args()
    index_src = args.src.split(',')
    index_dest = args.dest
    delete_old = args.delete
    url = args.url
    dry_run = args.dry_run
    user = args.user
    password = args.password
    no_wait = args.no_wait

    if no_wait and delete_old:
        log.error("Unable use no-wait and delete simultaneously ")
        sys.exit(1)

    log.debug("Source: {}, Destination: {}, Delete old: {}, URL: {}".format(index_src, index_dest, delete_old, url))

    es_client = EsClient(es_url=url, log=log, user=user, password=password)
    es_api = es_client.get_api()
    app = App(
        src=index_src,
        dest=index_dest,
        rm_old=delete_old,
        no_wait=no_wait,
        dry_run=dry_run,
        elastic_api=es_api
    )
    app.run()
