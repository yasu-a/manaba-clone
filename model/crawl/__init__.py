from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from .job import Job
from .lookup import Lookup
from .page import PageContent
from .task import Task


# noinspection PyShadowingNames
def info_dict(
        session: Session,
        *,
        job: Job
) -> dict[str, object]:
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
