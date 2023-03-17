from sqlalchemy import ForeignKey
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, TEXT, DATETIME

from .base import SQLScraperModelBase


class CourseNews(SQLScraperModelBase):
    course_id = Column(INTEGER, ForeignKey('course.id'))
    timestamp = Column(DATETIME)
    hash = Column(INTEGER)
    key = Column(TEXT)
    title = Column(TEXT)
    sender_name = Column(TEXT)
    release_date = Column(DATETIME)
