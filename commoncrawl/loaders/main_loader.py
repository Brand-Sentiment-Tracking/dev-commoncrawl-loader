import io
import json
import requests
import logging
import pprint

from urllib.parse import urljoin

from warcio.archiveiterator import ArchiveIterator
from newspaper import Article

from ..record import CommonCrawlRecord


class CCMainRecordLoader:

    CC_DOMAIN = "https://data.commoncrawl.org"
    CDX_DOMAIN = "http://index.commoncrawl.org"
    COLLECTION_INFO = "collinfo.json"
    SEARCH_FORMAT = "json"

    def __init__(self, article_callback=None, collection_name=None):
        self.article_callback = article_callback \
            if article_callback is not None \
            else self.__empty_callback

        self.__collections = self.load_collections()

        if collection_name is None:
            self.collection_name = self.latest_collection()
        else:
            self.collection_name = collection_name

        self.__last_search_results = None
        self.__last_download = None

    @property
    def article_callback(self):
        return self.__article_callback

    @article_callback.setter
    def article_callback(self, func):
        if not callable(func):
            raise ValueError("Article callback is not a function.")

        self.__article_callback = func

    def __empty_callback(article):
        return

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
        collections_url = urljoin(self.CDX_DOMAIN, self.COLLECTION_INFO)
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
            "output": self.SEARCH_FORMAT,
        }

    def search(self, pattern):
        payload = self.__search_payload(pattern)
        collection_route = f"{self.collection_name}-index"
        collection_url = urljoin(self.CDX_DOMAIN, collection_route)
        response = requests.get(collection_url, params=payload)

        if response.ok:
            body = response.text.strip().splitlines()
            search_results = list(map(json.loads, body))
            self.__last_search_results = search_results
        else:
            code = response.status_code
            logging.warn(f"CDX Server returned a bad status code ({code}).")
            if response.json():
                logging.info("The response returned the following:\n"
                             f"{pprint.pformat(response.json())}")
            
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

        record_cc_url = urljoin(self.CC_DOMAIN, record["filename"])
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

    def __parse_warc(self, text):
        with io.BytesIO(text.encode()) as stream:
            records = ArchiveIterator(stream)

            article = next(records)
            content = article.content_stream().read()

        content_type = article.http_headers.get_header("Content-Type")

        if article.rec_type != "response":
            raise Exception("WARC file is not an article")

        elif "text/html" not in content_type:
            raise Exception("WARC file does not contain HTML.")

        self.html = content.decode("utf-8")
        self.warc = article