# -*- coding: utf-8 -*-

import bing
import os
from multiprocessing import Pool, Value
from functools import partial
from urllib import unquote_plus
import requests
try:
    # Python 3
    from urllib.parse import urlparse, parse_qs
except ImportError:
    # Python 2
    from urlparse import urlparse, parse_qs


KEY = 'D2xfDfBc+Go+ODdcF/pb6/H5CuFv26Y0eemhwRbiFxg'
USER_AGENT = 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; FDM; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 1.1.4322)'
NUM_PROCESSES = 8
CHUNK_SIZE = 32000

DESTINATION_PATH = '/Users/constantinm/Documents/DS/test'


def download_file(path, url, chunk_size=CHUNK_SIZE, display=None):
    success = False
    o = urlparse(url)
    url_path_segments = o.path.split('/')
    filename = url_path_segments[-1]
    # check for a query string
    filename_parts = filename.split('?')
    filename = filename_parts[0]
    filename = unquote_plus(filename)
    filepath = os.path.join(path, filename)

    try:
        r = requests.get(url)
        if r.status_code == requests.codes.ok:
            try:
                with open(filepath, 'wb') as fd:
                    for chunk in r.iter_content(chunk_size):
                        fd.write(chunk)
                with bing.counter.get_lock():
                    bing.counter.value += 1
                if display:
                    display(filepath)
                success = True
            except IOError as e:
                print 'error writing %s' % filepath
        else:
            print 'error downloading from %s (%s)' % (url, r.status_code)
    except requests.exceptions.ConnectionError as e:
        print 'error downlaoding from %s (%s)' % (url, e)
    return success


def download_file2(from_to, chunk_size=CHUNK_SIZE, display=None):
    success = False
    url = from_to[0]
    path = from_to[1]

    o = urlparse(url)
    url_path_segments = o.path.split('/')
    filename = url_path_segments[-1]
    # check for a query string
    filename_parts = filename.split('?')
    filename = filename_parts[0]
    filename = unquote_plus(filename)
    filepath = os.path.join(path, filename)

    try:
        r = requests.get(url)
        if r.status_code == requests.codes.ok:
            try:
                with open(filepath, 'wb') as fd:
                    for chunk in r.iter_content(chunk_size):
                        fd.write(chunk)
                with bing.counter.get_lock():
                    bing.counter.value += 1
                if display:
                    display(from_to)
                success = True
            except IOError as e:
                print 'error writing %s' % filepath
        else:
            print 'error downloading from %s (%s)' % (url, r.status_code)
    except (requests.exceptions.ConnectionError, requests.exceptions.TooManyRedirects) as e:
        print 'error downloading from %s (%s)' % (url, e)
    except Exception as e:
        print 'general error downloading from %s (%s)' % (url, e)
    return success


def _init_process(counter):
    bing.counter = counter


def main():
    bing = Bing(KEY)
    query = "sunshine"
    filetypes = [(Bing.PDF_FILE_TYPE,10), (Bing.IMAGE_FILE_TYPE,10)]
    bing.execute(DESTINATION_PATH, query, filetypes)


class Bing(object):
    url_base = 'https://api.datamarket.azure.com/Data.ashx/Bing/Search/'

    IMAGE_FILE_TYPE = 'IMAGE'
    PDF_FILE_TYPE = 'PDF'
    DOC_FILE_TYPE = 'DOC'
    PPT_FILE_TYPE = 'PPT'
    HTM_FILE_TYPE = 'HTM'
    HTML_FILE_TYPE = 'HTML'
    RTF_FILE_TYPE = 'RTF'
    TEXT_FILE_TYPE = 'TEXT'
    TXT_FILE_TYPE = 'TXT'
    XLS_FILE_TYPE = 'XLS'

    def __init__(self, key, user_agent=USER_AGENT):
        self.key = key
        self.user_agent = user_agent
        self.credentials = (':%s' % self.key).encode('base64')[:-1]
        self.auth = 'Basic %s' % self.credentials

    def get_headers(self):
        return {
            'Authorization': self.auth,
            'User-Agent': self.user_agent,
        }

    @staticmethod
    def quote(s):
        return "'%s'" % s

    def search(self, query, file_type, count=100, page_size=50):
        url = Bing.url_base
        kwargs = {
            'Query': Bing.quote(query),
            '$top': count,
            '$format': 'json'
        }
        if file_type == Bing.IMAGE_FILE_TYPE:
            url += 'Image'
        else:
            url += 'Web'
            kwargs['WebFileType'] = Bing.quote(file_type)

        result_list = []
        page = 0
        while count - page * page_size > 0:
            result_page = []
            if page > 0:
                kwargs['$skip'] = page * page_size
            r = requests.get(url, params=kwargs, headers=self.get_headers())
            try:
                result_page = r.json()['d']['results']
                result_list.extend(result_page)
                page += 1
            except ValueError as e:
                print 'error getting query results: %s' % e
                print 'result:\n%s' % r.text
                break
        return result_list

    def get_files(self, query, file_type, count=100):
        results = self.search(query, file_type, count=count)
        if results is None:
            raise StopIteration
        for item in results:
            if file_type == Bing.IMAGE_FILE_TYPE:
                yield item['MediaUrl']
            else:
                yield item['Url']

    def execute(self, dest_path, query, types, display=None):
        urls = []
        for filetype, count in types:
            for url in self.get_files(query, filetype, count=count):
                urls.append(url)

        counter = Value('I', 0)
        pool = Pool(processes=NUM_PROCESSES, initializer=_init_process, initargs=(counter, ))
        # partial_download_file = partial(Bing.download_file, DESTINATION_PATH)
        partial_download_file = partial(download_file, dest_path, chunk_size=CHUNK_SIZE, display=display)
        pool.map(partial_download_file, urls)
        pool.close()
        pool.join()
        return counter.value

    def execute2(self, multiplefrom_to_list, display=None):
        from_to_list = []
        for multiplefrom_to in multiplefrom_to_list:
            src = multiplefrom_to['src']
            dst = multiplefrom_to['dst']
            from_to_list.extend(zip(src, [dst] * len(src)))

        counter = Value('I', 0)
        pool = Pool(processes=NUM_PROCESSES, initializer=_init_process, initargs=(counter, ))
        # partial_download_file = partial(Bing.download_file, DESTINATION_PATH)
        partial_download_file = partial(download_file2, chunk_size=CHUNK_SIZE, display=display)
        pool.map(partial_download_file, from_to_list)
        pool.close()
        pool.join()
        return counter.value


if __name__ == "__main__":
    main()