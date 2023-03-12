from typing import Literal

from sqlalchemy.orm import Session

import model.crawl
import model.scrape
from sessctx import SessionContext
from .mapper_handler import MapperHandlerMixin, mapper_handler


# noinspection PyUnresolvedReferences
class ManabaMapperHandlerImpl:
    @staticmethod
    def implement_mapper_name_handler(
            mapper_name=None, scraper_model_class=None, /, *, ignore=False
    ):
        @mapper_handler(mapper_name=mapper_name)
        def impl(self, *, task_entry: model.crawl.Task, scraper_session: Session) -> bool:
            if ignore:
                return False

            result = scraper_model_class.insert_from_task_entry(
                scraper_session,
                task_entry=task_entry
            )
            assert isinstance(result, bool)
            return result

        return impl

    # TODO: after updating python to 3.10, remove '.__func__' and noinspection; see:
    #  https://stackoverflow.com/questions/12718187/python-version-3-9-calling-class-staticmethod
    #  -within-the-class-body
    course_list_handler = implement_mapper_name_handler.__func__(
        'course_list',
        ignore=True
    )
    course_handler = implement_mapper_name_handler.__func__(
        'course',
        model.scrape.Course
    )
    contents_list_handler = implement_mapper_name_handler.__func__(
        'course_contents_list',
        ignore=True
    )
    contents_page_list_handler = implement_mapper_name_handler.__func__(
        'course_contents_page_list',
        model.scrape.CourseContentsPageList
    )
    contents_page_handler = implement_mapper_name_handler.__func__(
        'course_contents_page',
        model.scrape.CourseContentsPage
    )
    news_list_handler = implement_mapper_name_handler.__func__(
        'course_news_list',
        ignore=True
    )
    news_handler = implement_mapper_name_handler.__func__(
        'course_news',
        ignore=True
    )


class ManabaScraper(MapperHandlerMixin, ManabaMapperHandlerImpl):
    def __init__(
            self,
            crawler_session_context: SessionContext,
            scraper_session_context: SessionContext
    ):
        super().__init__()

        self.__crawler_sc = crawler_session_context
        self.__scraper_sc = scraper_session_context

        self.__active_session_id = None

    def set_active_crawling_session(
            self,
            state: Literal['finished', 'unfinished'],
            order: Literal['latest', 'oldest']
    ) -> int:
        with self.__crawler_sc() as session:
            crawling_session = model.crawl.CrawlingSession.get_session(
                session=session,
                state=state,
                order=order
            )
            self.__active_session_id = crawling_session.id
        return self.__active_session_id

    def scrape(self, crawler_session: Session, scraper_session: Session,
               task_entry: model.crawl.Task):
        handled = self.handle_by_mapper_name(task_entry, scraper_session)

        if handled:
            for next_task_entry in model.crawl.Task.iter_next(
                    crawler_session,
                    base_task=task_entry
            ):
                self.scrape(crawler_session, scraper_session, next_task_entry)

    def scrape_all(self):
        crawler_sessctx = self.__crawler_sc(do_commit=False)
        scraper_sessctx = self.__scraper_sc()
        with crawler_sessctx as crawler_session, scraper_sessctx as scraper_session:
            for root_task in model.crawl.Task.iter_roots(
                    crawler_session,
                    crawling_session=self.__active_session_id
            ):
                self.scrape(crawler_session, scraper_session, root_task)

            # query = query.limit(200)  # TODO: REMOVE THIS LINE!!!!!!
            #
            # for task_entry in query:
            #     self.handle_by_mapper_name(task_entry, scraper_session)
