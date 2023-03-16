from typing import Literal

from sqlalchemy import distinct, asc, desc
from sqlalchemy.orm import Session
from sqlalchemy.schema import Column
from sqlalchemy.types import INTEGER, DATETIME

from model import SQLDataModelMixin, SQLDataModelBase, create_timestamp
from .task import Task


# TODO: adding index on timestamp may improve 'get_resumed_session'???
class Job(SQLDataModelMixin, SQLDataModelBase):
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
        job_ids_unfinished = session.query(
            distinct(Task.job_id)
        ).join(
            Job
        ).where(
            Task.page_id.is_(None)
        )

        if state == 'unfinished':
            target_job_ids = job_ids_unfinished
        elif state == 'finished':
            job_ids_finished = session.query(
                distinct(Task.job_id)
            ).where(
                Task.job_id.not_in(job_ids_unfinished)
            )
            target_job_ids = job_ids_finished
        else:
            raise ValueError('parameter \'state\' must be either "finished" or "unfinished"')

        order_func = {'latest': desc, 'oldest': asc}.get(order)
        if order_func is None:
            raise ValueError('parameter \'order\' must be either "latest" or "oldest"')

        entry = session.query(
            Job
        ).where(
            Job.id.in_(target_job_ids)
        ).order_by(
            order_func(Job.timestamp)
        ).limit(1).first()

        return entry
