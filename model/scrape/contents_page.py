from typing import Optional, Any

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Session
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME, UnicodeText

import model.crawl
from model.common import SQLDataModelMixin
from model.scrape import SQLScraperDataModelBase
from .course import Course
from .soup_parser import SoupParser


class CourseContentsPageSoupParser(SoupParser):
    @property
    def title(self):
        elm = self._soup.select_one('.contentbody-left > h1')
        if elm is None:
            return None
        return elm.text.strip()

    @property
    def body(self):
        elm = self._soup.select_one('.contentbody-left')
        if elm is None:
            return None
        inner_html \
            = elm.decode_contents(formatter="html")
        return inner_html


class CourseContentsPage(SQLDataModelMixin, SQLScraperDataModelBase):
    id = Column(INTEGER, primary_key=True)

    contents_page_list_id = Column(INTEGER, ForeignKey('course_contents_page_list.id'))
    timestamp = Column(DATETIME)
    url = Column(TEXT)

    title = Column(TEXT)
    body = Column(UnicodeText)

    @classmethod
    def find_duplication(
            cls,
            session: Session,
            *,
            values: dict[str, Any]
    ):
        query = session.query(cls)
        for name, value in values.items():
            attribute = getattr(cls, name)
            query = query.where(attribute == value)
        duplicated_entry: Optional[cls] = query.first()
        return duplicated_entry

    @classmethod
    def exists(
            cls,
            session: Session,
            *,
            task_entry: model.crawl.Task
    ) -> bool:
        dup_entry = cls.find_duplication(
            session,
            values=dict(
                timestamp=task_entry.timestamp,
                url=task_entry.lookup.url
            )
        )

        return dup_entry is not None

    @classmethod
    def from_task_entry(
            cls,
            *,
            task_entry: model.crawl.Task
    ) -> 'Course':
        soup_parser = CourseContentsPageSoupParser.from_html(task_entry.page.content)

        entry = cls(
            timestamp=task_entry.timestamp,
            url=task_entry.lookup.url,
            **soup_parser.extract_properties('title', 'body')
        )

        return entry

    @classmethod
    def insert_from_task_entry(
            cls,
            session: Session,
            *,
            task_entry: model.crawl.Task
    ) -> bool:
        entry_exists = cls.exists(
            session,
            task_entry=task_entry
        )
        if entry_exists:
            return False

        entry = cls.from_task_entry(
            task_entry=task_entry
        )

        session.add(entry)

        return True
