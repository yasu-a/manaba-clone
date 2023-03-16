import re
from typing import Optional, Any

import dateutil.parser
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Session, relationship
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME

import model.crawl
from model import SQLDataModelMixin, SQLDataModelBase
from .course import Course
from .soup_parser import SoupParser


class CourseContentsPageListSoupParser(SoupParser):
    @property
    def title(self):
        elm = self._soup.select_one('h1.contents > a')
        if elm is None:
            return None
        return elm.text.strip()

    @property
    def release_date(self):
        elm = self._soup.select_one('.contents-modtime')
        if elm is None:
            return None
        text = elm.text.strip()
        m = re.search(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}', text)
        string = m.group()
        return dateutil.parser.parse(string)


class CourseContentsPageList(SQLDataModelMixin, SQLDataModelBase):
    id = Column(INTEGER, primary_key=True)

    course_id = Column(INTEGER, ForeignKey('course.id'))
    timestamp = Column(DATETIME)
    url = Column(TEXT)

    title = Column(TEXT)
    release_date = Column(DATETIME)

    contents_pages = relationship(
        'CourseContentsPage',
        backref='course_contents_page_list',
        lazy="joined"
    )

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
        soup_parser = CourseContentsPageListSoupParser.from_html(task_entry.page.content)

        entry = cls(
            timestamp=task_entry.timestamp,
            url=task_entry.lookup.url,
            **soup_parser.extract_properties('title', 'release_date')
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
