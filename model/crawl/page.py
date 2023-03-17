from typing import Optional, Type

from sqlalchemy.orm import Session
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, DATETIME, UnicodeText

from model import create_timestamp
from .base import SQLCrawlerModelBase
from .common import string_hash_63


# TODO: Before working on the following TODO, investigate duplications of content hash on the table.
# TODO: Change mapping into content_hash -> content; store timestamp on Task, not on this table.
#       This change can reduce the size of database.
class PageContent(SQLCrawlerModelBase):
    id = Column(INTEGER, primary_key=True, nullable=False)
    timestamp = Column(DATETIME, nullable=False)
    content = Column(UnicodeText)
    content_hash = Column(INTEGER, nullable=False)

    @classmethod
    def new_record(
            cls: Type['PageContent'],
            session: Session,
            *,
            content: Optional[str]
    ) -> 'PageContent':
        entry = cls(
            timestamp=create_timestamp(),
            content=content,
            content_hash=string_hash_63(content)
        )
        session.add(entry)
        return entry
