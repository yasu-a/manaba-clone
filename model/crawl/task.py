from typing import Optional, Union, Iterable, TYPE_CHECKING

from sqlalchemy import ForeignKey, case, desc, and_
from sqlalchemy.orm import Session, relationship, aliased
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, DATETIME

from model import SQLDataModelMixin, SQLDataModelBase, create_timestamp
from worker.crawl.page_family import GroupedURL
from .lookup import Lookup
from .page import PageContent

if TYPE_CHECKING:
    from .job import Job


# noinspection PyPep8
class Task(SQLDataModelMixin, SQLDataModelBase):
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
            job: Union['Job', int]
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
            job: 'Job',
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
            job: 'Job',
            lookup: Lookup,
            back_lookup: Lookup
    ) -> 'Task':
        entry_count = session.query(Task).filter(
            (Task.job == job) &
            (Task.lookup == lookup) &
            (Task.back_lookup == back_lookup)
        ).count()

        if entry_count > 0:
            raise ValueError('all tasks in the same job should be unique')

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
            job: 'Job',
    ) -> Optional['Task']:
        entry = session.query(Task).filter(
            and_(
                Task.job == job,
                Task.page_id.is_(None)
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
            job: 'Job',
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
                Task.page_id.is_(None),
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
            job: Union['Job', int],
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
            job: Union['Job', int]
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
