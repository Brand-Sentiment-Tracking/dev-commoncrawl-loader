import io
import pprint
import logging
import dateutil

from datetime import date
from dateutil.parser import parse as date_parse


from boilerpy3.extractors import ArticleExtractor
from warcio import ArchiveIterator

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

            for i, article in enumerate(ArchiveIterator(stream)):
                if i > 0:
                    raise Exception("Found multiple WARC files in text.")

                if article.rec_type != "response":
                    raise Exception("WARC file is not an article")
                elif "text/html" not in article.http_headers.get_header("Content-Type"):
                    raise Exception("WARC file does not contain HTML.")

                content = article.content_stream()

                self.html = content.read().decode("utf-8")
                self.warc = article

    def __set_metadata(self, url):
        self.url = self.warc.rec_headers.get_header("WARC-Target-URI")

        if self.url != url:
            logging.warn("The URLs passed from CC and in the WARC file" 
                        f"do not match:\n    {self.url} != {url}\n"
                         "    Selecting the URL from the WARC file.")
        
        date_extracted = self.warc.rec_headers.get_header("WARC-Date")
        self.date = date_parse(date_extracted)
        #self.date = date.fromisoformat(date_extracted)


    def __get_html_content(self):
        self.text = self.extractor.get_content(self.html)

if __name__ == "__main__":

    with open("www_bbc_co_uk_news.warc", "r") as f:
        warc_string = f.read()

    article = Article("test_article", warc_string, "https://www.bbc.co.uk/news")

    print(article.text)
    print(article.date)