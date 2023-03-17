import re
from typing import Iterable, Optional, Any

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Session, relationship
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME

import model.crawl
from .base import SQLScraperModelBase
from .soup_parser import SoupParser


class CourseInstructor(SQLScraperModelBase):
    id = Column(INTEGER, primary_key=True)

    course_id = Column(INTEGER, ForeignKey('course.id'))
    name = Column(TEXT)

    @classmethod
    def list_entries_from_string(
            cls,
            *,
            string: str,
    ) -> Iterable['CourseInstructor']:
        return [
            CourseInstructor(**field)
            for field in cls.iter_fields_from_string(string=string)
        ]

    @classmethod
    def iter_fields_from_string(cls, string: str) -> Iterable[dict]:
        assert isinstance(string, str)
        string = string.strip()

        for part in string.split('、'):
            part = part.strip()
            if not part:
                continue

            yield dict(
                name=part
            )


class CourseSchedule(SQLScraperModelBase):
    id = Column(INTEGER, primary_key=True)

    course_id = Column(INTEGER, ForeignKey('course.id'))
    year = Column(INTEGER)
    semester = Column(INTEGER)
    weekday = Column(INTEGER)
    period = Column(INTEGER)

    YEAR_NONE = 1111

    @classmethod
    def list_entries_from_string(
            cls,
            *,
            string: str
    ) -> Iterable['CourseSchedule']:
        return [
            CourseSchedule(**field)
            for field in cls.iter_fields_from_string(string)
        ]

    @classmethod
    def iter_fields_from_string(cls, string: str, *, year: str = None) -> Iterable[dict]:
        assert isinstance(string, str)
        string = string.strip()

        parts = re.findall(r'\S+', string)

        if len(parts) != 0:
            head, *rest = parts
            if re.fullmatch(r'\d+', head):
                assert year is None
                year = int(head)
                parts = rest

        if len(parts) != 0:
            head, *rest = parts
            if head == '通年':
                for new_string in [
                    ' '.join(['前期', *rest]),
                    ' '.join(['後期', *rest])
                ]:
                    yield from cls.iter_fields_from_string(new_string, year=year)
            else:
                assert len(head) == 2
                head_first, head_second = head
                assert head_first in '前後'
                assert head_second in '期複'
                semester = {'前': 0, '後': 1}[head_first]

                assert len(rest) % 2 == 0
                it = iter(rest)
                for weekday_str, period_str in zip(it, it):
                    weekday = '月火水木金土日'.index(weekday_str)
                    period = int(re.fullmatch(r'(\d+)時限', period_str).group(1))
                    yield dict(
                        year=year,
                        semester=semester,
                        weekday=weekday,
                        period=period
                    )


class CourseSoupParser(SoupParser):
    @property
    def name(self):
        return self._soup.select_one('#coursename').attrs['title'].strip()

    @property
    def schedules(self):
        string = self._soup.select_one('.coursedata-info').text.strip()
        # TODO: simplify parameter
        return CourseSchedule.list_entries_from_string(string=string)

    @property
    def instructors(self):
        string = self._soup.select_one('.courseteacher').attrs['title'].strip()
        # TODO: simplify parameter
        return CourseInstructor.list_entries_from_string(string=string)


class Course(SQLScraperModelBase):
    id = Column(INTEGER, primary_key=True)

    url = Column(TEXT)
    timestamp = Column(DATETIME)
    name = Column(TEXT)

    # TODO: relationship with back_populates
    schedules = relationship('CourseSchedule', backref='course', lazy="joined")
    instructors = relationship('CourseInstructor', backref='course', lazy="joined")

    # course_news = relationship('CourseNews', backref='course', lazy="joined")
    # course_contents = relationship('CourseContents', backref='course', lazy="joined")

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
    ) -> Optional['Course']:
        if task_entry.page is None:
            return None

        soup_parser = CourseSoupParser.from_html(task_entry.page.content)

        entry = cls(
            timestamp=task_entry.timestamp,
            url=task_entry.lookup.url,
            **soup_parser.extract_properties('name', 'schedules', 'instructors')
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
