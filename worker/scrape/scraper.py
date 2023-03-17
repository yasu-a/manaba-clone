from typing import Literal

from sqlalchemy.orm import Session

import model.crawl
import model.scrape
from sessctx import SessionContext
from .group_handler import GroupHandlerMixin, group_handler


# noinspection PyUnresolvedReferences
class ManabaGroupHandlerImpl:
    @staticmethod
    def implement_group_handler(
            group_name=None, scraper_model_class=None, /, *, ignore=False, return_value=False
    ):
        # noinspection PyUnusedLocal
        @group_handler(group_name=group_name)
        def impl(self, *, task_entry: model.crawl.Task, session: Session) -> bool:
            if ignore:
                return return_value

            result = scraper_model_class.insert_from_task_entry(
                session,
                task_entry=task_entry
            )
            assert isinstance(result, bool)
            return result

        return impl

    # TODO: after updating python to 3.10, remove '.__func__' and noinspection; see:
    #  https://stackoverflow.com/questions/12718187/python-version-3-9-calling-class-staticmethod
    #  -within-the-class-body
    course_list_handler = implement_group_handler.__func__(
        'course_list',
        ignore=True,
        return_value=True  # TDOO: need it?
    )
    course_handler = implement_group_handler.__func__(
        'course',
        model.scrape.Course
    )
    contents_list_handler = implement_group_handler.__func__(
        'course_contents_list',
        ignore=True
    )
    contents_page_list_handler = implement_group_handler.__func__(
        'course_contents_page_list',
        model.scrape.CourseContentsPageList
    )
    contents_page_handler = implement_group_handler.__func__(
        'course_contents_page',
        model.scrape.CourseContentsPage
    )
    news_list_handler = implement_group_handler.__func__(
        'course_news_list',
        ignore=True
    )
    news_handler = implement_group_handler.__func__(
        'course_news',
        ignore=True
    )


class ManabaScraper(GroupHandlerMixin, ManabaGroupHandlerImpl):
    def __init__(
            self,
            session_context: SessionContext,
    ):
        super().__init__()

        self.__sc = session_context

        self.__active_job_id = None

    def set_active_job(
            self,
            state: Literal['finished', 'unfinished'],
            order: Literal['latest', 'oldest']
    ) -> int:
        with self.__sc() as session:
            job = model.crawl.Job.get_job(
                session=session,
                state=state,
                order=order
            )
            self.__active_job_id = job.id
        return self.__active_job_id

    def scrape(self, session: Session, task_entry: model.crawl.Task):
        # TODO: remove handler's return values
        self.handle_by_group_name(task_entry, session)

        for next_task_entry in model.crawl.Task.iter_next(
                session,
                base_task=task_entry
        ):
            self.scrape(session, next_task_entry)

    def scrape_all(self):
        with self.__sc() as session:
            for root_task in model.crawl.Task.iter_roots(
                    session,
                    job=self.__active_job_id
            ):
                self.scrape(session, root_task)
