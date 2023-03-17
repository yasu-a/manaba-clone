from sqlalchemy import ForeignKey, desc
from sqlalchemy.orm import Session
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME

from model import create_model_parameters
from persistent_hash import persistent_hash
from .base import SQLScraperModelBase
from .course import Course


class CourseNews(SQLScraperModelBase):
    course_id = Column(INTEGER, ForeignKey('course.id'))
    timestamp = Column(DATETIME)
    hash = Column(INTEGER)
    key = Column(TEXT)
    title = Column(TEXT)
    sender_name = Column(TEXT)
    release_date = Column(DATETIME)

    FIELD_NAMES = {'key', 'title', 'sender_name', 'release_date'}

    @classmethod
    def get_latest_entry_with_same_hash(cls, session: Session, field: dict) -> bool:
        assert isinstance(field, dict)
        assert set(field.keys()) == cls.FIELD_NAMES

        field_hash = persistent_hash(field)

        course_news_with_same_hash = session.query(Course).filter(
            Course.hash == field_hash
        ).order_by(
            desc(Course.timestamp)
        ).first()

        return course_news_with_same_hash

    @classmethod
    def insert(cls, session: Session, field: dict, *, course: Course):
        assert isinstance(field, dict)
        assert set(field.keys()) == cls.FIELD_NAMES

        course_news_with_same_hash = cls.get_latest_entry_with_same_hash(session, field)
        if course_news_with_same_hash is not None:
            cls.logger.info(f'insertion cancelled {field!r}')
            return course_news_with_same_hash

        course_news = cls(
            **create_model_parameters(
                field,
                timestamp=True,
                field_hash=True
            ),
            course=course
        )
        session.add(course_news)

        cls.logger.info(f'insertion done {field!r}')

        return course_news
