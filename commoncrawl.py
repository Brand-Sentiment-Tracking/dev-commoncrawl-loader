import logging
import pprint

from datetime import datetime, timedelta
from commoncrawl.loaders import CCNewsRecordLoader


def print_article(article):
    pprint.pprint(article.__dict__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)

    end_date = datetime.today()
    start_date = end_date - timedelta(days=1)
    valid_urls = ["*"]

    loader = CCNewsRecordLoader(print_article)
    loader.download_articles(valid_urls, start_date, end_date)

    """
    for record_data in results:
        record = loader.download_record(record_data)
        article = record.create_article()

        record.save()

        print(article.text)

        break
    """
