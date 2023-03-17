from sqlalchemy import ForeignKey
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME, UnicodeText

import model.crawl
from .base import SQLScraperModelBase, ParentModelEntries
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


class CourseContentsPage(SQLScraperModelBase):
    id = Column(INTEGER, primary_key=True)

    contents_page_list_id = Column(INTEGER, ForeignKey('course_contents_page_list.id'))
    timestamp = Column(DATETIME)
    url = Column(TEXT)

    title = Column(TEXT)
    body = Column(UnicodeText)

    @classmethod
    def _soup_parser(cls) -> type[SoupParser]:
        return CourseContentsPageSoupParser

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
        self.contents_page_list_id = parent_model_entries['CourseContentsPageList'].id
