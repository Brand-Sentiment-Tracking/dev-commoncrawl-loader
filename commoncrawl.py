import logging
import pprint

from commoncrawl import CommonCrawlRecordLoader

if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    loader = CommonCrawlRecordLoader()

    # loader.collection_name = "CC-MAIN-2021-49"
    results = loader.search("https://bbc.co.uk/news/")

    pprint.pprint(results)

    for record_data in results:
        record = loader.download_record(record_data)
        article = record.create_article()

        record.save()

        print(article.text)

        break
