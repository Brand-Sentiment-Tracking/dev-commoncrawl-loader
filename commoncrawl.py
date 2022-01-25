import logging
import pprint

from commoncrawl import CommonCrawlRecordLoader

if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    loader = CommonCrawlRecordLoader()
    results = loader.search("https://bbc.co.uk/news/")

    pprint.pprint(results)

    for record_data in results:
        record = loader.download_record(record_data)
        record.save()

        break
