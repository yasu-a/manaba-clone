import hashlib
from typing import Literal, Optional, Type, TypeVar, NamedTuple, Union, Iterable

from sqlalchemy import ForeignKey, func, case, distinct, asc, desc, and_
from sqlalchemy.orm import Session, relationship, aliased
from sqlalchemy.schema import Column, Index
from sqlalchemy.types import INTEGER, TEXT, DATETIME, UnicodeText

from model.common import SQLDataModelMixin, create_timestamp
from worker.crawl.page_family import GroupedURL
from .base import SQLCrawlerDataModelBase

T = TypeVar('T')


def string_hash_63(string: Optional[str]) -> int:
    if string is None:
        return 1
    bytes_digest = hashlib.sha3_256(string.encode('utf-8')).digest()
    return int.from_bytes(bytes_digest[:8], byteorder='big') >> 1


# TODO: adding index on timestamp may improve 'get_resumed_session'???
class Job(SQLDataModelMixin, SQLCrawlerDataModelBase):
    id = Column(INTEGER, primary_key=True, nullable=False)
    timestamp = Column(DATETIME)

    def __int__(self):
        return self.id

    @classmethod
    def get_new_session(
            cls,
            session: Session,
    ) -> 'Job':
        entry = cls(timestamp=create_timestamp())
        session.add(entry)
        return entry

    @classmethod
    def get_session_by_id(
            cls,
            session: Session,
            *,
            job_id: int
    ) -> 'Job':
        entry = session.query(Job).where(Job.id == job_id).first()
        return entry

    @classmethod
    def get_job(
            cls,
            session: Session,
            *,
            state: Literal['finished', 'unfinished'],
            order: Literal['latest', 'oldest']
    ) -> 'Job':
        session_ids_unfinished = session.query(
            distinct(Task.job_id)
        ).join(
            Job
        ).where(
            Task.page_id.is_(None)
        )

        if state == 'unfinished':
            target_session_ids = session_ids_unfinished
        elif state == 'finished':
            session_ids_finished = session.query(
                distinct(Task.job_id)
            ).where(
                Task.job_id.not_in(session_ids_unfinished)
            )
            target_session_ids = session_ids_finished
        else:
            raise ValueError('parameter \'state\' must be either "finished" or "unfinished"')

        order_func = {'latest': desc, 'oldest': asc}.get(order)
        if order_func is None:
            raise ValueError('parameter \'order\' must be either "latest" or "oldest"')

        entry = session.query(
            Job
        ).where(
            Job.id.in_(target_session_ids)
        ).order_by(
            order_func(Job.timestamp)
        ).limit(1).first()

        return entry


# noinspection PyShadowingBuiltins
class Lookup(SQLDataModelMixin, SQLCrawlerDataModelBase):
    id = Column(INTEGER, primary_key=True, nullable=False)
    url = Column(TEXT, unique=True)
    group_name = Column(TEXT)

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
            group_name = None
            filter_predicate = Lookup.id == id
        elif cls._is_specified(url):
            if isinstance(url, GroupedURL):
                group_name = url.group_name
                url = url.url
            else:
                group_name = None
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
            if group_name is None and url is not None:
                raise ValueError('new url entry must have non-null group_name')
            entry = cls(
                id=string_hash_63(url),
                url=url,
                group_name=group_name
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

    job_id = Column(INTEGER, ForeignKey('job.id'), nullable=False)
    url_id = Column(INTEGER, ForeignKey('lookup.id'), nullable=False)
    back_url_id = Column(INTEGER, ForeignKey('lookup.id'))
    timestamp = Column(DATETIME, nullable=False)
    page_id = Column(INTEGER, ForeignKey('page_content.id'))

    job = relationship('Job', foreign_keys=[job_id], lazy="joined")
    lookup = relationship('Lookup', foreign_keys=[url_id], lazy="joined")
    back_lookup = relationship('Lookup', foreign_keys=[back_url_id], lazy="joined")
    page = relationship('PageContent', foreign_keys=[page_id], lazy="joined")

    @classmethod
    def list_group_names(
            cls,
            session: Session,
            *,
            job: Union[Job, int]
    ) -> set[str]:
        query = session.query(Lookup.group_name).distinct().join(
            Task,
            Task.url_id == Lookup.id
        ).where(
            Task.job_id == int(job)
        )

        return {row[0] for row in query.all()}

    @classmethod
    def add_initial_url(
            cls,
            session: Session,
            *,
            job: Job,
            initial_mapped_url: GroupedURL,
            force_append: bool = False
    ) -> bool:
        entry_count = session.query(Task).filter(
            and_(
                Task.job == job,
                Task.page_id.is_not(None)
            )
        ).count()

        if not force_append and entry_count > 0:
            return False

        cls.new_record(
            session=session,
            job=job,
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
            cls,
            session: Session,
            *,
            job: Job,
            lookup: Lookup,
            back_lookup: Lookup
    ) -> 'Task':
        entry_count = session.query(Task).filter(
            (Task.job == job) &
            (Task.lookup == lookup) &
            (Task.back_lookup == back_lookup)
        ).count()

        if entry_count > 0:
            raise ValueError('all tasks in the same crawling session should be unique')

        assert lookup.url is not None

        entry = cls(
            job=job,
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
            cls,
            session: Session,
            *,
            job: Job,
    ) -> Optional['Task']:
        entry = session.query(Task).filter(
            and_(
                Task.job == job,
                Task.page.is_(None)
            )
        ).order_by(
            desc(Task.timestamp)
        ).limit(1).first()

        if entry is None:
            return None

        return entry

    @classmethod
    def close_task(
            cls,
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
            cls,
            session: Session,
            *,
            job: Job,
    ) -> int:
        sub_query = session.query(Task).with_entities(
            Task.url_id
        ).where(
            Task.job == job
        )

        task_with_page_iter = session.query(Task).where(
            and_(
                Task.url_id.in_(sub_query),
                Task.job == job,
                Task.page_id.is_not(None)
            )
        )

        lookup_to_page = {}
        for task in task_with_page_iter:
            lookup_to_page[task.url_id] = task.page_id

        no_tasks_with_page = len(lookup_to_page) == 0
        if no_tasks_with_page:
            return 0

        row_count = session.query(Task).filter(
            and_(
                Task.job == job,
                Task.page.is_(None),
                Task.url_id.in_(lookup_to_page.keys())
            )
        ).update({
            Task.page_id: case(
                lookup_to_page,
                value=Task.url_id
            )
        }, synchronize_session='fetch')

        return row_count

    # TODO: USE THIS!!!
    @classmethod
    def session_query(
            cls,
            session: Session,
            *,
            job: Union[Job, int],
    ):
        query = session.query(cls).where(
            cls.job_id == int(job)
        )

        return query

    @classmethod
    def iter_roots(
            cls,
            session: Session,
            *,
            job: Union[Job, int]
    ) -> Iterable['Task']:
        back_lookup = aliased(Lookup)

        query = session.query(Task).where(
            Task.job_id == int(job)
        ).join(
            back_lookup,
            back_lookup.id == Task.back_url_id
        ).where(
            back_lookup.url.is_(None)
        )

        yield from query

    @classmethod
    def iter_next(
            cls,
            session: Session,
            *,
            base_task: 'Task'
    ) -> Iterable['Task']:
        back_task = aliased(cls)

        query = session.query(cls).where(
            cls.job == base_task.job
        ).join(
            back_task,
            back_task.url_id == cls.back_url_id
        ).where(
            back_task.id == base_task.id
        ).where(
            back_task.job == base_task.job
        )

        yield from query


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
        job: Job
) -> dict[str, object]:
    # noinspection PyPep8
    return {
        'unfinished_task_count':
            session.query(func.count(Task.id)).filter(
                and_(
                    Task.page_id.is_(None),
                    Task.job == job
                )
            ).scalar(),
        'finished_task_count':
            session.query(func.count(Task.id)).filter(
                and_(
                    Task.page_id.is_not(None),
                    Task.job == job
                )
            ).scalar(),
        'whole_page_count':
            session.query(func.count(PageContent.id)).scalar(),
        'whole_lookup_count':
            session.query(func.count(Lookup.id)).scalar(),
    }
