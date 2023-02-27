import re
from typing import Iterable, Optional, Type, TypeVar

from sqlalchemy import ForeignKey, desc
from sqlalchemy.orm import Session, relationship
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME

from model.common import SQLDataModelMixin, create_model_parameters
from model.scrape import SQLScraperDataModelBase
from persistent_hash import persistent_hash

T = TypeVar('T')

__all__ = 'CourseInstructor', 'CourseSchedule', 'Course'


class CourseInstructor(SQLDataModelMixin, SQLScraperDataModelBase):
    course_id = Column(INTEGER, ForeignKey('course.id'))
    name = Column(TEXT)

    FIELD_NAMES = {'name'}

    @classmethod
    def insert(cls: Type[T], session: Session, field: dict, *, course: 'Course') \
            -> Optional[T]:
        assert isinstance(field, dict)
        assert set(field.keys()) == cls.FIELD_NAMES

        instructor = cls(**field, course=course)
        session.add(instructor)
        return instructor

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


class CourseSchedule(SQLDataModelMixin, SQLScraperDataModelBase):
    course_id = Column(INTEGER, ForeignKey('course.id'))
    year = Column(INTEGER)
    semester = Column(INTEGER)
    weekday = Column(INTEGER)
    period = Column(INTEGER)

    FIELD_NAMES = {'semester', 'weekday', 'period', 'year'}

    YEAR_NONE = 1111

    @classmethod
    def insert(cls: Type[T], session: Session, field: dict, *, course: 'Course') \
            -> Optional[T]:
        assert isinstance(field, dict)
        assert set(field.keys()) == cls.FIELD_NAMES

        if field['year'] == cls.YEAR_NONE:
            field['year'] = None

        schedule = cls(**field, course=course)
        session.add(schedule)
        return schedule

    @classmethod
    def iter_fields_from_string(cls, year: int, string: str) -> Iterable[dict]:
        assert isinstance(string, str)
        string = string.strip()

        parts = re.findall(r'\S+', string)

        if len(parts) != 0:
            head, *rest = parts
            if head == '通年':
                for new_string in [
                    ' '.join(['前期', *rest]),
                    ' '.join(['後期', *rest])
                ]:
                    yield from cls.iter_fields_from_string(year, new_string)
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


class Course(SQLDataModelMixin, SQLScraperDataModelBase):
    timestamp = Column(DATETIME)
    hash = Column(INTEGER)
    key = Column(TEXT)
    name = Column(TEXT)

    # TODO: relationship with back_populates
    schedules = relationship('CourseSchedule', backref='course')
    instructors = relationship('CourseInstructor', backref='course')
    course_news = relationship('CourseNews', backref='course')
    course_contents = relationship('CourseContents', backref='course')

    FIELD_NAMES = {'key', 'name', 'year', 'schedules', 'instructors'}

    @classmethod
    def get_latest_entry_with_same_hash(cls, session: Session, field: dict) -> Optional['Course']:
        assert isinstance(field, dict)
        assert set(field.keys()) == cls.FIELD_NAMES

        field_hash = persistent_hash(field)

        course_with_same_hash = session.query(Course).filter(
            Course.hash == field_hash
        ).order_by(
            desc(Course.timestamp)
        ).first()

        return course_with_same_hash

    @classmethod
    def insert(cls: Type[T], session: Session, field: dict) -> T:
        assert isinstance(field, dict)
        assert set(field.keys()) == cls.FIELD_NAMES

        course_with_same_hash = cls.get_latest_entry_with_same_hash(session, field)
        if course_with_same_hash is not None:
            cls.logger.info(f'insertion cancelled {field!r}')
            return course_with_same_hash

        course = cls(
            **create_model_parameters(
                field,
                timestamp=True,
                field_hash=True,
                keys=['key', 'name']
            )
        )
        session.add(course)

        for schedule_field \
                in CourseSchedule.iter_fields_from_string(field['year'], field['schedules']):
            CourseSchedule.insert(session, schedule_field, course=course)

        for instructor_field \
                in CourseInstructor.iter_fields_from_string(field['instructors']):
            CourseInstructor.insert(session, instructor_field, course=course)

        cls.logger.info(f'insertion done {field!r}')

        return course
