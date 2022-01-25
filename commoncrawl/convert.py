
from fileinput import filename
from boilerpipe.extract import Extractor
from warcio.archiveiterator import ArchiveIterator


class WARCConverter:

    def __init__(self, attribute=None):
        self.attribute = attribute


    def convert(self, filename):
        with open(filename, 'rb') as stream:
            for record in ArchiveIterator(stream):

                # Parse record into dictionary
                data_dict = {}
                data_dict["WARC-Target-URI"] = record.rec_headers.get_header('WARC-Target-URI')
                data_dict["WARC-Date"] = record.rec_headers.get_header("WARC-Date")
                
                # Extract article text using boilerpipe
                html = record.content_stream().read()
                extractor = Extractor(extractor='ArticleSentencesExtractor', html=html)
                extracted_text = extractor.getText()
                data_dict["Text"] = extracted_text

                return data_dict




if __name__ == "__main__":
    converter = WARCConverter()
    filename = "..\www_bbc_co_uk_news.warc"
    data_dict = converter.convert(filename)
    print(data_dict)