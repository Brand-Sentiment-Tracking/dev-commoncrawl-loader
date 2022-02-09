from ast import Pass
import io
import warcio
import boilerpy3

from warcio.recordloader import ArcWarcRecordLoader

class Article:

    def __init__(self, name, warc_text, url):

        self.loader = ArcWarcRecordLoader()

        self.name = name

        self.text = warc_text
        self.warc = self.__parse_warc_string()
        
        self.url = url

    def __parse_warc_string(self):
        stream = io.BytesIO(self.text)
        self.warc = self.loader.parse_record_stream(stream)
        stream.close()

if __name__ == "__main__":

    pass