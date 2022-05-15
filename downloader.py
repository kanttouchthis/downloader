import os
from regex import A
import requests
import concurrent.futures
from urllib.parse import urlparse, unquote


class Downloader(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, max_workers=10, *args, **kwargs):
        super().__init__(max_workers=max_workers, *args, **kwargs)
        self.session = self.get_session()

    def get_session(self, *args, **kwargs):
        self.session = requests.Session(*args, **kwargs)
        return self.session

    def download(self, urls, filepaths=None, timeout=None, return_only=False, **kwargs):
        if type(urls) is str:
            if filepaths is None:
                filepaths = self.get_filename(urls)
            filepaths = [filepaths]
            urls = [urls]
        if return_only:
            return self.map(lambda u: self._download(u, **kwargs), urls, timeout=timeout)
        if type(filepaths) is str:
            filepaths = [os.path.join(filepaths, filename) for filename in self.get_filenames(urls)]
        elif filepaths is None:
            filepaths = self.get_filenames(urls)
        return self.map(lambda u, f: self.download_to_file(u, f, **kwargs), urls, filepaths, timeout=timeout)

    def download_to_file(self, url, filepath, **kwargs):
        _, response = self._download(url, stream=True, **kwargs)
        self.write_response_chunked(response, filepath)
        return url, response, filepath

    def _download(self, url, stream=False, **kwargs):
        response = self.session.get(url, stream=stream, **kwargs)
        return url, response

    @staticmethod
    def write_response_chunked(response, filepath):
        dir = os.path.dirname(os.path.abspath(filepath))
        os.makedirs(dir, exist_ok=True)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=128):
                f.write(chunk)

    @staticmethod
    def get_filename(url):
        return os.path.basename(unquote(urlparse(url).path))

    @staticmethod
    def get_filenames(urls):
        return [Downloader.get_filename(url) for url in urls]

    def close(self, wait=True):
        self.shutdown(wait=wait)
        self.session.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def __call__(self, *args, **kwargs):
        return self.download(*args, **kwargs)


def download(*args, **kwargs):
    return Downloader()(*args, **kwargs)

#download("https://speed.hetzner.de/100MB.bin") for testing
