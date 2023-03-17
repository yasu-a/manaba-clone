from sqlalchemy import ForeignKey
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME, UnicodeText

import model.crawl
from .base import SQLScraperModelBase, ParentModelEntries
from .soup_parser import SoupParser


class CourseNewsSoupParser(SoupParser):
    @property
    def title(self):
        elm = self._soup.select_one('h2.msg-subject')
        if elm is None:
            return None
        return elm.text.strip()

    @property
    def body(self):
        elm = self._soup.select_one('.msg-text')
        if elm is None:
            return None
        inner_html \
            = elm.decode_contents(formatter="html")
        return inner_html


class CourseNews(SQLScraperModelBase):
    id = Column(INTEGER, primary_key=True)

    course_id = Column(INTEGER, ForeignKey('course.id'))
    timestamp = Column(DATETIME)
    url = Column(TEXT)

    title = Column(TEXT)
    body = Column(UnicodeText)

    @classmethod
    def _soup_parser(cls) -> type[SoupParser]:
        return CourseNewsSoupParser

    @classmethod
    def _create_entry_from_task_entry(
            cls: type['SQLScraperModelBase'], *,
            task_entry: model.crawl.Task,
            soup_parser: SoupParser
    ) -> 'SQLScraperModelBase':
        entry = cls(
            timestamp=task_entry.timestamp,
            url=task_entry.lookup.url,
            **soup_parser.extract_properties('title', 'body')
        )

        return entry

    def _set_parent_model_entry(
            self,
            parent_model_entries: ParentModelEntries
    ):
        self.course_id = parent_model_entries['Course'].id
