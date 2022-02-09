
from fileinput import filename
from boilerpy3 import extractors
from warcio.archiveiterator import ArchiveIterator


class WARCConverter:

    def __init__(self, attribute=None):
        self.attribute = attribute


    def convert(self, filename):
        with open(filename, 'rb') as stream:

            data_dict = {}
            for record in ArchiveIterator(stream):

                # Parse record into dictionary
                data_dict["WARC-Target-URI"] = record.rec_headers.get_header('WARC-Target-URI')
                data_dict["WARC-Date"] = record.rec_headers.get_header("WARC-Date")
                
                # Extract article text using boilerpy
                html = record.content_stream().read().decode("utf-8")
                extractor = extractors.ArticleExtractor()
                content = extractor.get_content(html)
                data_dict["Text"] = content

            return data_dict


if __name__ == "__main__":
    converter = WARCConverter()
    filename = "..\www_bbc_co_uk_news.warc"
    data_dict = converter.convert(filename)
    print(data_dict)