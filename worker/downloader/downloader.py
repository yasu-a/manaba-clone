import datetime
import urllib.error
from abc import ABCMeta, abstractmethod
from typing import NamedTuple, Iterable, Optional

import app_logging
import opener


class DownloadingEntry(NamedTuple):
    title: str
    url: str
    timestamp: datetime.datetime


class DownloaderBase(metaclass=ABCMeta):
    logger = app_logging.create_logger()

    def __init__(
            self,
            *,
            url_opener: opener.URLOpenerUtilMethodsMixin
    ):
        self.__opener = url_opener

    @abstractmethod
    def _create_downloading_entry(
            self,
            *,
            url: str,
            title: str,
            timestamp: datetime.datetime
    ) -> DownloadingEntry:
        raise NotImplementedError()

    @abstractmethod
    def _iter_downloading_entry_parameters(self) -> Iterable[dict]:
        raise NotImplementedError()

    def iter_downloading_entry(self) -> Iterable[DownloadingEntry]:
        for param in self._iter_downloading_entry_parameters():
            downloading_entry = self._create_downloading_entry(**param)
            yield downloading_entry

    def execute_download(self, dl_entry: DownloadingEntry) -> Optional[bytes]:
        try:
            content = self.__opener.urlopen_bytes(dl_entry.url)
        except urllib.error.HTTPError:
            content = None
        return content

    @abstractmethod
    def _setup_download(self, dl_entry: DownloadingEntry) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def _process_content(self, dl_entry: DownloadingEntry, content: bytes):
        raise NotImplementedError()

    def download_all(self):
        for dl_entry in self.iter_downloading_entry():
            self.logger.info(f'processing download: {dl_entry._asdict()}')
            setup_result = self._setup_download(dl_entry)
            self.logger.info(f' proceed_downloading: {setup_result}')
            if not setup_result:
                continue
            content = self.execute_download(dl_entry)
            if content:
                self.logger.info(f' retrieved content with length {len(content)}')
            else:
                self.logger.info(f' failed to get content')
            self._process_content(dl_entry, content)
