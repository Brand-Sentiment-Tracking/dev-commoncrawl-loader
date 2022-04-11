import logging

from datetime import datetime, timedelta
from newspaper import Article

from ccloaders.news import CCNewsArticleLoader


def print_article(article: Article, date_crawled: datetime):
    logging.info(article.title)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    end_date = datetime.today() - timedelta(days=150)
    start_date = end_date - timedelta(days=75)
    valid_urls = ["*business*"]

    loader = CCNewsArticleLoader(print_article)
    loader.download_articles(valid_urls, start_date, end_date)
