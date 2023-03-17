import re

import dateutil.parser
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME

import model.crawl
from .base import SQLScraperModelBase, ParentModelEntries
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


class CourseContentsPageList(SQLScraperModelBase):
    id = Column(INTEGER, primary_key=True)

    course_id = Column(INTEGER, ForeignKey('course.id'))
    timestamp = Column(DATETIME)
    url = Column(TEXT)

    title = Column(TEXT)
    release_date = Column(DATETIME)

    contents_page_entries = relationship(
        'CourseContentsPage',
        backref='course_contents_page_list',
        lazy='joined'
    )

    @classmethod
    def _soup_parser(cls) -> type[SoupParser]:
        return CourseContentsPageListSoupParser

    @classmethod
    def _create_entry_from_task_entry(
            cls: type['SQLScraperModelBase'], *,
            task_entry: model.crawl.Task,
            soup_parser: SoupParser
    ) -> 'SQLScraperModelBase':
        entry = cls(
            timestamp=task_entry.timestamp,
            url=task_entry.lookup.url,
            **soup_parser.extract_properties('title', 'release_date')
        )

        return entry

    def _set_parent_model_entry(
            self,
            parent_model_entries: ParentModelEntries
    ):
        # TODO: move attribute `id` in every scraper models to SQLScraperModelBase
        self.course_id = parent_model_entries['Course'].id
