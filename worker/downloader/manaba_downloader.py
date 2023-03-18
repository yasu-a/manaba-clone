import datetime
import re
import urllib.error
import urllib.parse
from typing import Iterable

import dateutil.parser
from bs4 import BeautifulSoup

import app_logging
import model
import model.crawl
import model.crawl
import model.downloader
import model.scrape
import opener
from sessctx import SessionContext
from .downloader import DownloaderBase, DownloadingEntry


class ManabaAttachmentDownloader(DownloaderBase):
    logger = app_logging.create_logger()

    def __init__(
            self,
            *,
            session_context: SessionContext,
            url_opener: opener.URLOpenerUtilMethodsMixin
    ):
        super().__init__(url_opener=url_opener)
        self.__sc = session_context

    def _create_downloading_entry(
            self,
            *,
            url: str,
            title: str,
            timestamp: datetime.datetime
    ) -> DownloadingEntry:
        components = urllib.parse.urlparse(url)
        # noinspection PyProtectedMember
        components = components._replace(query='')
        url = urllib.parse.urlunparse(components)

        m = re.fullmatch(r'(.*?)(\s-\s(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}))?', title)
        title, _, timestamp_str = m.groups()
        if timestamp_str is not None:
            # update timestamp with its attachment timestamp
            timestamp = dateutil.parser.parse(timestamp_str)

        return DownloadingEntry(
            url=url,
            title=title,
            timestamp=timestamp
        )

    def _iter_downloading_entry_parameters(self) -> Iterable[dict]:
        def iter_node_classes(cls):
            subclasses = cls.__subclasses__()
            if subclasses:
                for subclass in subclasses:
                    yield from iter_node_classes(subclass)
            else:
                yield cls

        def iter_tables_with_body():
            for cls in iter_node_classes(model.scrape.SQLScraperModelBase):
                # TODO: using mixin looks better; fields: `url`, `body`, `timestamp`
                if hasattr(cls, 'body'):
                    yield cls

        with self.__sc(do_commit=False) as session:
            for table_cls in iter_tables_with_body():
                query = session.query(table_cls) \
                    .with_entities(table_cls.url, table_cls.body, table_cls.timestamp)
                for url, body, timestamp in query:
                    soup = BeautifulSoup(body, features='lxml')
                    for anchor in soup.select('div.inlineaf-description > a'):
                        anchor_url = urllib.parse.urljoin(url, anchor.attrs['href'].strip())
                        title = anchor.text.strip()
                        timestamp = timestamp
                        yield dict(url=anchor_url, title=title, timestamp=timestamp)

    def _setup_download(self, dl_entry: DownloadingEntry) -> bool:
        with self.__sc(do_commit=False) as session:
            entry_exists = model.downloader.Attachment.check_entry_exists(
                session,
                url=dl_entry.url,
                timestamp=dl_entry.timestamp
            )
            proceed_downloading = not entry_exists
            return proceed_downloading

    def _process_content(self, dl_entry: DownloadingEntry, content: bytes):
        with self.__sc() as session:
            model.downloader.Attachment.put_entry_from_parameters(
                session,
                title=dl_entry.title,
                url=dl_entry.url,
                content=content,
                timestamp=dl_entry.timestamp
            )
