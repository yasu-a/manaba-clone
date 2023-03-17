from typing import Literal, Optional

import sqlalchemy.exc
from sqlalchemy.orm import Session

import app_logging
import model.crawl
import model.scrape
import model.scrape.base
from sessctx import SessionContext
from worker.crawl.manaba_family import ManabaPageFamily
from .group_handler import GroupHandlerMixin, group_handler


# noinspection PyUnresolvedReferences
class ManabaGroupHandlerImpl:
    @staticmethod
    def implement_group_handler(
            group_name=None, scraper_model_class=None, /, *, ignore=False
    ):
        # noinspection PyUnusedLocal
        @group_handler(group_name=group_name)
        def impl(
                self,
                *,
                session: Session,
                task_entry: model.crawl.Task,
                parent_model_entries: model.scrape.base.ParentModelEntries
        ) -> Optional[model.scrape.base.SQLScraperModelBase]:
            if ignore:
                return None

            model_entry = scraper_model_class.insert_from_task_entry(
                session,
                task_entry=task_entry,
                parent_model_entries=parent_model_entries
            )
            assert model_entry is None \
                   or isinstance(model_entry, model.scrape.base.SQLScraperModelBase)
            return model_entry

        return impl

    # TODO: after updating python to 3.10, remove '.__func__' and noinspection; see:
    #  https://stackoverflow.com/questions/12718187/python-version-3-9-calling-class-staticmethod
    #  -within-the-class-body
    course_list_handler = implement_group_handler.__func__(
        ManabaPageFamily.course_list.name,  # TODO: generate from PageGroup directly
        ignore=True,
    )
    course_handler = implement_group_handler.__func__(
        ManabaPageFamily.course.name,
        model.scrape.Course
    )
    contents_list_handler = implement_group_handler.__func__(
        ManabaPageFamily.course_contents_list.name,
        ignore=True
    )
    contents_page_list_handler = implement_group_handler.__func__(
        ManabaPageFamily.course_contents_page_list.name,
        model.scrape.CourseContentsPageList
    )
    contents_page_handler = implement_group_handler.__func__(
        ManabaPageFamily.course_contents_page.name,
        model.scrape.CourseContentsPage
    )
    news_list_handler = implement_group_handler.__func__(
        ManabaPageFamily.course_news_list.name,
        ignore=True
    )
    news_handler = implement_group_handler.__func__(
        ManabaPageFamily.course_news.name,
        model.scrape.CourseNews
    )


class ManabaScraper(GroupHandlerMixin, ManabaGroupHandlerImpl):
    logger = app_logging.create_logger()

    def __init__(
            self,
            session_context: SessionContext,
            max_process_count=None
    ):
        super().__init__()

        self.__sc = session_context

        self.__active_job_id = None

        self.__max_process_count = max_process_count
        self.__process_count = 0

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

    def scrape(
            self,
            *,
            session: Session,
            task_entry: model.crawl.Task,
            parent_model_entries: model.scrape.base.ParentModelEntries
    ):
        # TODO: remove handler's return values
        current_model_entry = self.handle_by_group_name(
            session=session,
            task_entry=task_entry,
            parent_model_entries=parent_model_entries
        )

        self.__process_count += 1
        if self.__max_process_count and self.__process_count >= self.__max_process_count:
            return

        for next_task_entry in model.crawl.Task.iter_next(
                session,
                base_task=task_entry
        ):
            self.scrape(
                session=session,
                task_entry=next_task_entry,
                parent_model_entries=parent_model_entries.add(current_model_entry)
            )

    def scrape_all(self):
        with self.__sc() as session:
            for root_task in model.crawl.Task.iter_roots(
                    session,
                    job=self.__active_job_id
            ):
                self.scrape(
                    session=session,
                    task_entry=root_task,
                    parent_model_entries=model.scrape.base.ParentModelEntries()
                )

    @classmethod
    def __drop_all_scraper_tables(cls, session: Session):
        for base_subclass in model.scrape.base.SQLScraperModelBase.__subclasses__():
            try:
                query = session.query(base_subclass)
            except sqlalchemy.exc.ArgumentError:
                pass
            else:
                query.delete()
                cls.logger.info(f'DROPPED {base_subclass.__tablename__}')

    def reset_database(self):
        with self.__sc() as session:
            self.__drop_all_scraper_tables(session)
