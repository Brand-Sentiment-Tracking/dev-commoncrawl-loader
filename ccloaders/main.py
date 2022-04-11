import re
import json
import requests
import logging
import langdetect

from typing import Callable, Optional
from itertools import product

from datetime import datetime

from urllib3.response import HTTPResponse
from urllib.parse import urljoin

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from newspaper import Article
from newspaper.utils import get_available_languages


class CCMainArticleLoader:
    """Load and parse articles from CommonCrawl News Archive.

    Note:
        This is for extracting articles from the CC-NEWS collection. For the
        CC-MAIN collections, use the `CCMainRecordLoader` class.

    Args:
        article_callback (callable): A function that is called once an article
            has been extracted.
    """
    CC_DOMAIN = "https://data.commoncrawl.org"
    COLLINFO_URL = "http://index.commoncrawl.org/collinfo.json"

    COLLECTION_RE = re.compile(r"^(?P<month>\w+)\s(?P<year>\d{4})\sIndex$")
    CONTENT_RE = re.compile(r"^(?P<mime>[\w\/]+);\s?charset=(?P<charset>.*)$")

    SUPPORTED_LANGUAGES = get_available_languages()

    def __init__(self, article_callback=None):
        # Set article_callback to the empty callback if no function is passed
        self.article_callback = article_callback \
            if article_callback is not None \
            else self.__empty_callback

        self.__collections = self.load_collections()

        self.patterns = list()

        self.__start_date = None
        self.__end_date = None

        self.reset_counts()

    @property
    def article_callback(self) -> Callable[[Article, datetime], None]:
        """`callable`: Called once an article has been extracted.

        Note:
            The callback is passed one argument: the article as a
                `newspaper.Article` object.

        The setter method will throw a ValueError if the new callback is not
        a function.
        """
        return self.__article_callback

    @article_callback.setter
    def article_callback(self, func: Callable[[Article, datetime], None]):
        if not callable(func):
            raise ValueError("Article callback is not a function.")

        self.__article_callback = func

    def __empty_callback(article: Article):
        """Default function when an article_callback isn't specified.

        Note:
            This function does nothing.

        Args:
            article (Article): The extracted article.
        """
        return

    @property
    def patterns(self) -> "list[str]":
        """`list` of `str` containing the url patterns to match the
        article URL against when filtering.

        The setter method will throw a ValueError if the new patterns is not a
        list of strings.
        """
        return self.__patterns

    @patterns.setter
    def patterns(self, patterns: "list[str]"):
        if type(patterns) != list:
            raise ValueError("URL patterns is not a list.")
        elif any(map(lambda x: type(x) != str, patterns)):
            raise ValueError("Not all URL patterns are strings.")

        self.__patterns = patterns

    @property
    def start_date(self) -> datetime:
        """`datetime`: The starting date to filter the articles between.

        The setter method will throw a ValueError if the new date is not a
        `datetime` object or it is later than the end date.
        """
        return self.__start_date

    @start_date.setter
    def start_date(self, start_date: datetime):
        if type(start_date) != datetime:
            raise ValueError("Start date isn't type 'datetime'.")
        elif start_date >= self.end_date:
            raise ValueError("Start date is on or after the end date.")

        self.__start_date = start_date

    @property
    def end_date(self) -> datetime:
        """`datetime`: The ending date to filter the articles between.

        The setter method will throw a ValueError if the new date is not a
        `datetime` object or it is in the future.
        """
        return self.__end_date

    @end_date.setter
    def end_date(self, end_date: datetime):
        if type(end_date) != datetime:
            raise ValueError("End date isn't type 'datetime'.")
        elif end_date >= datetime.now():
            raise ValueError("End date is in the future.")

        self.__end_date = end_date

    @property
    def extracted(self) -> int:
        """`int`: The number of articles successfully extracted."""
        return self.__extracted

    @property
    def discarded(self) -> int:
        """`int`: The number of articles discarded before extraction."""
        return self.__discarded

    @property
    def errored(self) -> int:
        """`int`: The number of articles that errored during extraction."""
        return self.__errored

    def reset_counts(self):
        """Reset the counters for extracted/discarded/errored to zero."""
        self.__extracted = 0
        self.__discarded = 0
        self.__errored = 0

    @property
    def collections(self):
        return self.__collections

    def load_collections(self):
        response = requests.get(self.COLLINFO_URL)
        collections = response.json()

        if len(collections) == 0:
            logging.warn("No collections available from CDX.")

        return {coll["id"]: coll for coll in collections}

    def __filter_collections_by_dates(self):
        collections = list()

        for collection in self.collections.values():
            match = self.COLLECTION_RE.match(collection["name"])

            if match is None:
                continue

            year = int(match.group("year"))

            if year >= self.start_date.year \
                    and year <= self.end_date.year:

                collections.append(collection)

        return collections

    def __search_payload(self, pattern):
        from_timestamp = self.start_date.strftime("%Y%m%d%H%M%S")
        to_timestamp = self.end_date.strftime("%Y%m%d%H%M%S")

        return {
            "url": pattern,
            "output": "json",
            "filter": "mime:text/html",
            "from": from_timestamp,
            "to": to_timestamp
        }

    def search(self, patterns, start_date, end_date):
        self.patterns = patterns
        self.end_date = end_date
        self.start_date = start_date

        collections = self.__filter_collections_by_dates()
        search_results = list()

        for pattern, collection in product(self.patterns, collections):
            logging.info(f"Querying {collection['id']} with '{pattern}'.")
            payload = self.__search_payload(pattern)
            collection_url = collection["cdx-api"]

            response = requests.get(collection_url, params=payload)

            if not response.ok:

                if response.status_code != 404:
                    logging.info(f"Failed to access '{collection_url}' "
                                 f"(status code {response.status_code}).")

                continue

            body = response.text.strip().splitlines()
            search_results.extend(list(map(json.loads, body)))

        return search_results

    def __is_valid_record(self, record: ArcWarcRecord) -> bool:
        """Checks whether a warc record should be extracted to an article.

        This is done by checking:
        - The record type is a response.
        - Its MIME type is `text/html` and its charset is UTF-8.
        - The source URL matches one of the url patterns.

        Args:
            record (ArcWarcRecord): The record to evaluate.

        Returns:
            bool: True if the record is valid and should be extracted to an
                article. False otherwise.
        """
        if record.rec_type != "response":
            return False

        source_url = record.rec_headers.get_header("WARC-Target-URI")
        content_string = record.http_headers.get_header('Content-Type')

        if source_url is None or content_string is None:
            return False

        content = self.CONTENT_RE.match(content_string)

        if content is None or content.group("mime") != "text/html" \
                or content.group("charset").lower() != "utf-8":

            return False

        return True

    def extract_article(self, url: str, html: str, language: str,
                        date_crawled: datetime):
        """Extracts the article from its html and update counters.

        Once successfully extracted, it is then passed to `article_callback`.

        Note:
            If the extraction process fails, the article will be discarded.

        Args:
            url (str): The source URL of the article.
            html (str): The complete HTML structure of the record.
            language (str): The two-char language code of the record.
        """
        if language not in self.SUPPORTED_LANGUAGES:
            logging.debug(f"Language not supported for '{url}'")
            self.__discarded += 1
            return

        article = Article(url, language=language)

        try:
            article.download(input_html=html)
            article.parse()
            self.__extracted += 1
        # Blanket error catch here. Should be made more specific
        except Exception as e:
            logging.warn(str(e))
            self.__errored += 1

        # Conditional here so exceptions in the callback are still raised
        if article.is_parsed:
            self.article_callback(article, date_crawled)

    def __parse_records(self, warc: HTTPResponse):
        """Iterate through articles from a warc file.

        Each record is loaded using warcio, and extracted if:
        - It is a valid news article (see __is_valid_record)
        - Its source URL matches one of the patterns.
        - The detected language is supported by newspaper.

        Args:
            warc (HTTPResponse): The complete warc file as a stream.
        """
        records = ArchiveIterator(warc)
        record = next(records)

        url = record.rec_headers.get_header("WARC-Target-URI")
        iso_date = record.rec_headers.get_header("WARC-Date")

        date_crawled = datetime.fromisoformat(iso_date)

        if not self.__is_valid_record(record):
            logging.debug(f"Ignoring '{url}'")
            self.__discarded += 1
            return

        try:
            html = record.content_stream().read().decode("utf-8")
            language = langdetect.detect(html)
        except Exception:
            logging.debug(f"Couldn't decode '{url}'")
            self.__errored += 1
            return

        self.extract_article(url, html, language, date_crawled)

    def __load_warc(self, warc_path: str, headers: Optional[dict] = None):
        """Downloads and parses a warc file for article extraction.

        Note:
            If the response returns a bad status code, the method will exit
            without parsing the warc file.

        Args:
            warc_path (str): The route of the warc file to be downloaded (not
                including the CommonCrawl domain).
        """
        warc_url = urljoin(self.CC_DOMAIN, warc_path)
        logging.debug(f"Downloading '{warc_url}'")

        response = requests.get(warc_url, headers=headers, stream=True)

        if response.ok:
            self.__parse_records(response.raw)
        else:
            logging.warn(f"Failed to download warc from '{warc_url}' "
                         f"(status code {response.status_code}).")

    def __get_byte_index(self, record):
        byte_start = int(record["offset"])
        byte_end = byte_start + int(record["length"]) - 1

        return byte_start, byte_end

    def __download_header(self, byte_index):
        return dict(Range=f"bytes={byte_index[0]}-{byte_index[1]}")

    def __download_single_article(self, record_metadata):
        byte_index = self.__get_byte_index(record_metadata)
        headers = self.__download_header(byte_index)
        warc_path = record_metadata.get("filename")

        self.__load_warc(warc_path, headers=headers)

    def download_articles(self, patterns: "list[str]", start_date: datetime,
                          end_date: datetime):
        """Downloads and extracts articles from CC-NEWS.

        Articles are extracted only if:
        - The source URL matches one of the URL patterns.
        - The article was crawled between the start and end dates.

        Args:
            patterns (list[str]): List of URL patterns the article must match.
            start_date (datetime): The earliest date the article must have
                been crawled.
            end_date (datetime): The latest date the article must have been
                crawled by.
        """
        search_results = self.search(patterns, start_date, end_date)

        for record_metadata in search_results:
            self.__download_single_article(record_metadata)
