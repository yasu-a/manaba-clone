import hashlib
from typing import Optional, Type, TypeVar, NamedTuple

from sqlalchemy import ForeignKey, desc, func, case
from sqlalchemy.orm import Session, relationship
from sqlalchemy.schema import Column, Index
from sqlalchemy.types import INTEGER, TEXT, DATETIME, UnicodeText

from crawl.url_mapping import MappedURL
from model.common import SQLDataModelMixin, create_timestamp
from .base import SQLCrawlerDataModelBase

T = TypeVar('T')


def string_hash_63(string: Optional[str]) -> int:
    if string is None:
        return 1
    bytes_digest = hashlib.sha3_256(string.encode('utf-8')).digest()
    return int.from_bytes(bytes_digest[:8], byteorder='big') >> 1


class CrawlingSession(SQLDataModelMixin, SQLCrawlerDataModelBase):
    id = Column(INTEGER, primary_key=True, nullable=False)
    timestamp = Column(DATETIME)

    @classmethod
    def get_new_session(
            cls: Type['CrawlingSession'],
            session: Session,
    ) -> 'CrawlingSession':
        entry = cls(timestamp=create_timestamp())
        session.add(entry)
        return entry

    @classmethod
    def get_resumed_session(
            cls: Type['CrawlingSession'],
            session: Session,
    ) -> 'CrawlingSession':
        entry = session.query(CrawlingSession).order_by(
            desc(CrawlingSession.timestamp)
            # desc(CrawlingSession.timestamp)
        ).limit(1).first()
        if entry is None:
            raise ValueError('no entries to be resumed')
        return entry


# noinspection PyShadowingBuiltins
class Lookup(SQLDataModelMixin, SQLCrawlerDataModelBase):
    id = Column(INTEGER, primary_key=True, nullable=False)
    url = Column(TEXT, unique=True)
    mapper_name = Column(TEXT)

    id_index = Index('id')
    url_index = Index('url')

    __UNSPECIFIED = object()

    @classmethod
    def _is_specified(cls, obj):
        return obj is not cls.__UNSPECIFIED

    @classmethod
    def _is_unspecified(cls, obj):
        return obj is cls.__UNSPECIFIED

    @classmethod
    def lookup(
            cls,
            session: Session,
            *,
            id=__UNSPECIFIED,
            url=__UNSPECIFIED
    ) -> 'Lookup':
        if (cls._is_specified(id) and cls._is_specified(url)) \
                or (cls._is_unspecified(id) and cls._is_unspecified(url)):
            raise ValueError('either \'id\' or \'url\' should be specified')

        if cls._is_specified(id):
            mapper_name = None
            filter_predicate = Lookup.id == id
        elif cls._is_specified(url):
            if isinstance(url, MappedURL):
                mapper_name = url.mapper_name
                url = url.url
            else:
                mapper_name = None
            filter_predicate = Lookup.url == url
        else:
            assert False

        entry = session.query(Lookup).filter(
            filter_predicate
        ).first()

        if entry is not None:
            return entry

        if cls._is_specified(id):
            raise ValueError(f'unregistered {id=!r}')
        elif cls._is_specified(url):
            if mapper_name is None and url is not None:
                raise ValueError('new url entry must have non-null mapper_name')
            entry = cls(
                id=string_hash_63(url),
                url=url,
                mapper_name=mapper_name
            )
        else:
            assert False

        session.add(entry)

        return entry


class URLLinkage(NamedTuple):
    url: Optional[str]
    back_url: str


# noinspection PyPep8
class Task(SQLDataModelMixin, SQLCrawlerDataModelBase):
    id = Column(INTEGER, primary_key=True, nullable=False)
    session_id = Column(INTEGER, ForeignKey('crawling_session.id'), nullable=False)
    url_id = Column(INTEGER, ForeignKey('lookup.id'), nullable=False)
    back_url_id = Column(INTEGER, ForeignKey('lookup.id'))
    timestamp = Column(DATETIME, nullable=False)
    page_id = Column(INTEGER, ForeignKey('page_content.id'))

    crawling_session = relationship('CrawlingSession', foreign_keys=[session_id])
    lookup = relationship('Lookup', foreign_keys=[url_id])
    back_lookup = relationship('Lookup', foreign_keys=[back_url_id])
    page = relationship('PageContent', foreign_keys=[page_id])

    @classmethod
    def add_initial_url(
            cls: Type['Task'],
            session: Session,
            *,
            crawling_session: CrawlingSession,
            initial_mapped_url: MappedURL
    ) -> bool:
        entry_count = session.query(Task).filter(
            Task.crawling_session == crawling_session
        ).count()

        if entry_count > 0:
            return False

        cls.new_record(
            session=session,
            crawling_session=crawling_session,
            lookup=Lookup.lookup(
                session,
                url=initial_mapped_url
            ),
            back_lookup=Lookup.lookup(
                session,
                url=None
            )
        )
        return True

    @classmethod
    def new_record(
            cls: Type['Task'],
            session: Session,
            *,
            crawling_session: CrawlingSession,
            lookup: Lookup,
            back_lookup: Lookup
    ) -> 'Task':
        entry_count = session.query(Task).filter(
            (Task.crawling_session == crawling_session) &
            (Task.lookup == lookup) &
            (Task.back_lookup == back_lookup)
        ).count()

        if entry_count > 0:
            raise ValueError('all tasks in the same crawling session should be unique')

        assert lookup.url is not None

        entry = cls(
            crawling_session=crawling_session,
            lookup=lookup,
            back_lookup=back_lookup,
            timestamp=create_timestamp(),
            page=None
        )
        session.add(entry)

        return entry

    # noinspection PyComparisonWithNone,PyPep8
    @classmethod
    def open_task(
            cls: Type['Task'],
            session: Session,
            *,
            crawling_session: CrawlingSession,
    ) -> Optional['Task']:
        # noinspection PyPep8
        entry = session.query(Task).filter(
            (Task.crawling_session == crawling_session) &
            (Task.page == None)
        ).order_by(
            desc(Task.timestamp)
        ).limit(1).first()

        if entry is None:
            return None

        return entry

    @classmethod
    def close_task(
            cls: Type['Task'],
            session: Session,
            *,
            task: 'Task',
            content: Optional[str]
    ) -> None:
        task.page = PageContent.new_record(
            session,
            content=content
        )

    @classmethod
    def fill_pages(
            cls: Type['Task'],
            session: Session,
            *,
            crawling_session: CrawlingSession,
    ) -> int:
        # noinspection PyTypeChecker
        task_with_page_iter = session.query(Task, PageContent).filter(
            (Task.crawling_session == crawling_session) &
            (Task.page_id == PageContent.id)
        )

        lookup_to_page = {}
        for task, page in task_with_page_iter:
            lookup_to_page[task.url_id] = page.id

        no_tasks_with_page = len(lookup_to_page) == 0
        if no_tasks_with_page:
            return 0

        # noinspection PyTypeChecker,PyComparisonWithNone,PyPep8
        row_count = session.query(Task).filter(
            (Task.crawling_session == crawling_session) &
            (Task.page == None) &
            (Task.url_id.in_(lookup_to_page.keys()))
        ).update({
            Task.page_id: case(
                lookup_to_page,
                value=Task.url_id
            )
        }, synchronize_session='fetch')

        return row_count


# TODO: Before working on the following TODO, investigate duplications of content hash on the table.
# TODO: Change mapping into content_hash -> content; store timestamp on Task, not on this table.
#       This change can reduce the size of database.
class PageContent(SQLDataModelMixin, SQLCrawlerDataModelBase):
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


def info_dict(
        session: Session,
        *,
        crawling_session: CrawlingSession
) -> dict[str, object]:
    # noinspection PyPep8
    return {
        'tasks without content':
            session.query(func.count(Task.id)).filter(
                (Task.page_id == None) &
                (Task.crawling_session == crawling_session)
            ).scalar(),
        'tasks with content':
            session.query(func.count(Task.id)).filter(
                (Task.page_id != None) &
                (Task.crawling_session == crawling_session)
            ).scalar(),
        'pages in db':
            session.query(func.count(PageContent.id)).scalar(),
        'lut size':
            session.query(func.count(Lookup.id)).scalar(),
    }
