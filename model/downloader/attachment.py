import datetime
import os.path
import urllib.parse

from sqlalchemy.orm import Session
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME, BLOB

from .base import SQLDownloaderModelBase


class Attachment(SQLDownloaderModelBase):
    id = Column(INTEGER, primary_key=True)

    title = Column(TEXT)
    datatype = Column(TEXT)
    url = Column(TEXT)
    content = Column(BLOB)
    timestamp = Column(DATETIME)

    @classmethod
    def check_entry_exists(
            cls,
            session: Session,
            *,
            url: str,
            timestamp: datetime.datetime
    ) -> bool:
        dup_entry = cls.find_duplication(
            session,
            values=dict(url=url, timestamp=timestamp)
        )

        return dup_entry is not None

    @classmethod
    def put_entry_from_parameters(
            cls,
            session: Session,
            *,
            title: str,
            url: str,
            content: bytes,
            timestamp: datetime.datetime
    ) -> 'Attachment':
        components = urllib.parse.urlparse(url)
        _, datatype = os.path.splitext(components.path)

        dup_entry = cls.find_duplication(
            session,
            values=dict(url=url, timestamp=timestamp)
        )
        if dup_entry is not None:
            return dup_entry

        entry = cls(
            title=title,
            datatype=datatype,
            url=url,
            content=content,
            timestamp=timestamp
        )

        session.add(entry)

        return entry
