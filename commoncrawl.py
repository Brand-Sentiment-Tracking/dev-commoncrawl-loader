import logging
import pprint

from commoncrawl import CommonCrawlRecordLoader
from commoncrawl import WARCConverter

if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    domain = "https://bbc.co.uk/news/"
    loader = CommonCrawlRecordLoader()
    results = loader.search(domain)

    pprint.pprint(results)

    converter = WARCConverter()

    for record_data in results:
        record = loader.download_record(record_data)
        record.save()
        
        filename = "www_bbc_co_uk_news.warc" # temp hard code
        data_dict = converter.convert(filename)
        break

    print(data_dict)