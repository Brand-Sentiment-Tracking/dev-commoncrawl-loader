import logging

from datetime import datetime, timedelta
from commoncrawl.loaders import CCNewsRecordLoader


def print_article(article):
    logging.info(article.title)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    end_date = datetime.today()
    start_date = end_date - timedelta(days=1)
    valid_urls = ["*"]

    loader = CCNewsRecordLoader(print_article)
    loader.download_articles(valid_urls, start_date, end_date)
