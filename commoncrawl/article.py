import io
import logging

from warcio import ArchiveIterator
from boilerpy3.extractors import ArticleExtractor
from dateutil.parser import parse as parse_date


class Article:

    def __init__(self, name, warc_text, url):

        self.name = name

        self.warc = None
        self.url = None
        self.date = None

        self.html = ""
        self.text = ""

        self.extractor = ArticleExtractor()

        self.__parse_warc(warc_text)
        self.__get_html_content()
        self.__set_metadata(url)

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

    def __set_metadata(self, url):
        self.url = self.warc.rec_headers.get_header("WARC-Target-URI")

        if self.url != url:
            logging.warn("The URLs passed from CC and in the WARC file "
                         "do not match:\n"
                         f"    {self.url} != {url}\n"
                         "    Selecting the URL from the WARC file.")

        date_extracted = self.warc.rec_headers.get_header("WARC-Date")
        self.date = parse_date(date_extracted)

    def __get_html_content(self):
        self.text = self.extractor.get_content(self.html)


if __name__ == "__main__":

    with open("www_bbc_co_uk_news.warc", "r") as f:
        warc_string = f.read()

    article = Article("test_article", warc_string,
                      "https://www.bbc.co.uk/news")

    print(article.text)
    print(article.date)
