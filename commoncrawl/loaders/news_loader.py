import re
import gzip
import requests
import logging

from datetime import datetime
from dateutil.rrule import rrule, MONTHLY

from os.path import join as path_join
from fnmatch import fnmatch

from warcio.archiveiterator import ArchiveIterator
from newspaper import Article


class CCNewsRecordLoader:

    CC_DOMAIN = "https://data.commoncrawl.org"
    CC_NEWS_ROUTE = path_join(CC_DOMAIN, "crawl-data", "CC-NEWS")
    WARC_PATHS = "warc.paths.gz"

    WARC_FILE_RE = re.compile(r"CC-NEWS-(?P<time>\d{14})-(?P<serial>\d{5})")

    def __init__(self, article_callback=None):

        self.article_callback = article_callback \
            if article_callback is not None \
            else self.__empty_callback

        self.urls = list()

        self.__start_date = None
        self.__end_date = None

    @property
    def article_callback(self):
        return self.__article_callback

    @article_callback.setter
    def article_callback(self, func):
        if not callable(func):
            raise ValueError("Article callback must be a function.")

        self.__article_callback = func

    @property
    def urls(self):
        return self.__urls

    @urls.setter
    def urls(self, urls):
        if type(urls) != list or any(map(lambda x: type(x) != str, urls)):
            raise ValueError("URL patterns is not a list of strings.")

        self.__urls = urls

    @property
    def start_date(self):
        return self.__start_date

    @start_date.setter
    def start_date(self, start_date):
        if type(start_date) != datetime:
            raise ValueError("Start date isn't type 'datetime'.")
        elif start_date >= self.end_date:
            raise ValueError("Start date is on or after the end date.")

        self.__start_date = start_date

    @property
    def end_date(self):
        return self.__end_date

    @end_date.setter
    def end_date(self, end_date):
        if type(end_date) != datetime:
            raise ValueError("End date isn't type 'datetime'.")
        elif end_date >= datetime.now():
            raise ValueError("End date is in the future.")

        self.__end_date = end_date

    def __empty_callback(article):
        return

    def __load_warc_paths(self, month, year):
        paths_url = path_join(self.CC_NEWS_ROUTE, str(year),
                              str(month).zfill(2),
                              self.WARC_PATHS)

        response = requests.get(paths_url)

        if response.ok:
            content = gzip.decompress(response.content)
            filenames = content.decode("utf-8").splitlines()
        else:
            logging.warn(f"Failed to download paths from '{paths_url}' "
                         f"(status code {response.status_code}).")

            filenames = list()

        return filenames

    def __is_within_date(self, warc_file):
        match = CCNewsRecordLoader.WARC_FILE_RE.search(warc_file)

        if match is None:
            logging.debug(f"Ignoring '{warc_file}'.")
            return False

        time = match.group("time")
        crawl_date = datetime.strptime(time, "%Y%m%d%H%M%S")

        return crawl_date >= self.start_date \
            and crawl_date < self.end_date

    def __filter_warc_paths(self, filenames):
        return list(filter(self.__is_within_date, filenames))

    def __retrieve_warc_paths(self):
        filenames = list()

        for dt in rrule(MONTHLY, self.start_date, until=self.end_date):
            logging.info(f"Downloading warc paths for {dt}")
            filenames.extend(self.__load_warc_paths(dt.month, dt.year))

        return self.__filter_warc_paths(filenames)

    def __is_valid_record(self, record):
        if record.rec_type != "response":
            return False

        record_url = record.rec_headers.get_header("WARC-Target-URI")
        content = record.http_headers.get_header('Content-Type')

        if content != "text/html" or record_url is None:
            return False

        return any(map(lambda url: fnmatch(record_url, url), self.urls))

    def __parse_warcs(self, response):
        for record in ArchiveIterator(response.raw, arc2warc=True):
            url = record.rec_headers.get_header("WARC-Target-URI")
            if not self.__is_valid_record(record):
                logging.debug(f"Ignoring '{url}'")
                continue

            article = Article(url)

            try:
                html = record.content_stream().read()
                article.download(input_html=html.decode("utf-8"))
                article.parse()

            except Exception as e:
                logging.error(repr(e))

            self.article_callback(article)

    def __load_warc(self, path):
        warc_url = path_join(self.CC_DOMAIN, path)
        response = requests.get(warc_url, stream=True)

        if response.ok:
            self.__parse_warcs(response)
        else:
            logging.warn(f"Failed to download warc from '{warc_url}' "
                         f"(status code {response.status_code}).")

    def download_articles(self, urls, start_date, end_date):
        self.urls = urls

        self.end_date = end_date
        self.start_date = start_date

        warc_paths = self.__retrieve_warc_paths()

        for warc in warc_paths:
            self.__load_warc(warc)
