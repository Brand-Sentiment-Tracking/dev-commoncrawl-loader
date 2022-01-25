import json
import requests
import logging

from urllib.parse import urljoin

from .record import CommonCrawlRecord

class CommonCrawlRecordLoader:

    CC_SERVER_URL = "https://commoncrawl.s3.amazonaws.com/"
    CDX_SERVER_URL = "http://index.commoncrawl.org/"
    COLLECTION_INFO = "collinfo.json"
    SEARCH_FORMAT = "json"

    def __init__(self, collection_name=None):
        self.__collections = self.load_collections()

        if collection_name is None:
            self.collection_name = self.latest_collection()
        else:
            self.collection_name = collection_name

        self.__last_search_results = None
        self.__last_download = None

    @property
    def cdx_server_url(self):
        return CommonCrawlRecordLoader.CDX_SERVER_URL

    @property
    def collection_info(self):
        return CommonCrawlRecordLoader.COLLECTION_INFO

    @property
    def search_format(self):
        return CommonCrawlRecordLoader.SEARCH_FORMAT

    @property
    def cc_server_url(self):
        return CommonCrawlRecordLoader.CC_SERVER_URL

    @property
    def collections(self):
        return self.__collections

    @property
    def collection_ids(self):
        return list(map(lambda c: c["id"], self.collections))

    @property
    def collection_name(self):
        return self.__collection_name

    @collection_name.setter
    def collection_name(self, name):
        collection_ids = map(lambda c: c["id"], self.collections)

        if name not in list(collection_ids):
            raise ValueError(f"Collection '{name}' unavailable from CDX.")

        self.__collection_name = name

    @property
    def last_search_results(self):
        return self.__last_search_results

    @property
    def last_download(self):
        return self.__last_download

    def load_collections(self):
        collections_url = urljoin(self.cdx_server_url, self.collection_info)
        response = requests.get(collections_url)

        collections = response.json()

        if len(collections) == 0:
            logging.warn("No collections available from CDX.")

        return collections

    def latest_collection(self):
        return max(self.collection_ids)

    def __search_payload(self, pattern):
        return {
            "url": pattern,
            "output": self.search_format,
        }

    def search(self, pattern):
        payload = self.__search_payload(pattern)
        collection_route = f"{self.collection_name}-index"
        collection_url = urljoin(self.cdx_server_url, collection_route)

        response = requests.get(collection_url, params=payload)

        if response.ok:
            response_body = response.text.strip()
            response_json = "[" + response_body.replace("\n", ",") + "]"
            self.__last_search_results = json.loads(response_json)
        else:
            code = response.status_code
            logging.warn(f"CDX Server returned a bad status code ({code}).")
            self.__last_search_results = None

        return self.last_search_results

    def save_search(self, filename, indent=4):
        if self.last_search_results is None:
            logging.info("No search result available to save.")
            return

        with open(filename, 'w') as fp:
            json.dump(self.last_search_results, fp, indent=indent)

    def __get_byte_index(self, record):
        byte_start = int(record["offset"])
        byte_end = byte_start + int(record["length"]) - 1

        return byte_start, byte_end

    def __download_header(self, byte_index):
        start, end = byte_index
        return {"Range": f"bytes={start}-{end}"}

    def download_record(self, record):
        if int(record.get("status")) != 200:
            logging.info("Record returned a bad status code when crawled.")

        if record.get("encoding") != "UTF-8":
            logging.warn("Record is not encoded from UTF-8. This may cause "
                         "issues when decoding to a string.")

        byte_index = self.__get_byte_index(record)
        headers = self.__download_header(byte_index)

        record_cc_url = urljoin(self.cc_server_url, record["filename"])
        response = requests.get(record_cc_url, headers=headers)

        if response.ok:
            self.__last_download = CommonCrawlRecord(
                'warc', record["url"], record_cc_url, response.content)
        else:
            logging.warn(f"Failed to download record from '{record_cc_url}' "
                         f"(status code {response.status_code}).")

            self.__last_download = CommonCrawlRecord(
                'warc', record["url"], record_cc_url, None)

        return self.last_download
