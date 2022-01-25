import logging
import gzip
import re
import os

from urllib.parse import urlparse


class CommonCrawlRecord:

    def __init__(self, format, url, cc_url, data):
        self.__format = format
        self.__url = url
        self.__cc_url = cc_url
        self.__data = data

        self.name = self.create_filename(url)

        self.__is_zipped = True

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, new_name):
        filename = re.sub(r"\W+", "_", new_name)
        self.__name = f"{filename}.{self.format}"

    @property
    def format(self):
        return self.__format

    @property
    def url(self):
        return self.__url

    @property
    def cc_url(self):
        return self.__cc_url

    @property
    def data(self, zip=False):
        if zip:
            return self.__zip()

        return self.__unzip().decode("utf-8")

    @property
    def is_zipped(self):
        return self.__is_zipped

    def create_filename(self, url_string):
        url = urlparse(url_string)
        return f"{url.netloc}/{url.path}"

    def __unzip(self):
        if self.__data and self.__is_zipped:
            return gzip.decompress(self.__data)

        return self.__data

    def unzip(self):
        self.__data = self.__unzip()
        self.__is_zipped = False

    def __zip(self):
        if self.__data and not self.__is_zipped:
            return gzip.compress(self.__data)

        return self.__data

    def zip(self):
        self.__data = self.__zip()
        self.__is_zipped = True

    def save(self, filename=None, directory="./", zip=False):
        if self.__data is None:
            logging.info("No data available to save.")
            return

        if filename is None:
            filename = self.name

        if zip:
            filename = f"{filename}.gz"
            record_data = self.__zip()
        else:
            record_data = self.__unzip()

        if not os.path.exists(directory):
            logging.info(f"Creating a new directory '{directory}'.")
            os.mkdir(directory)

        filepath = os.path.join(directory, filename)

        if os.path.isfile(filepath):
            commit = input(f"'{filepath}' already exists. Overwrite? [y/n]: ")

            if commit != 'y':
                return

        with open(filepath, 'wb') as f:
            f.write(record_data)

        logging.info(f"Saved record to '{filepath}'.")
