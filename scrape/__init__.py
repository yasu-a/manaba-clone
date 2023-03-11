from functools import lru_cache
from functools import lru_cache
from typing import Literal

import app_logging
import model.crawl
import model.scrape
from sessctx import SessionContext


class CrawlerDBReader:
    def __init__(
            self,
            crawler_session_context: SessionContext
    ):
        self.__sc = crawler_session_context

    @lru_cache
    def read_soup(self, value):
        with self.__sc() as session:
            session.query(model.crawl.Task.page.content)


def mapper_name_handler(*, mapper_name: str):
    def decorator(func):
        setattr(func, '_mapper_name_handler', {'mapper_name': mapper_name})
        return func

    return decorator


class MapperNameBasedHandlerMixin:
    logger = app_logging.create_logger()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __find_mapper_name_handler(self, mapper_name: str):
        for name in dir(self):
            obj = getattr(self, name)
            param = getattr(obj, '_mapper_name_handler', None)
            if param is None:
                continue
            if param['mapper_name'] == mapper_name:
                return obj

    def handle_by_mapper_name(self, task_entry: model.crawl.Task):
        mapper_name = task_entry.lookup.mapper_name
        handler = self.__find_mapper_name_handler(mapper_name)
        if handler:
            self.logger.debug(f'handling {mapper_name}')
            handler(task_entry)
        else:
            self.logger.debug(f'ignored handling {mapper_name}')


# noinspection PyPep8
class ManabaScraper(MapperNameBasedHandlerMixin):
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

    @staticmethod
    def implement_mapper_name_handler(mapper_name, scraper_model_class):
        @mapper_name_handler(mapper_name=mapper_name)
        def impl(self, task_entry: model.crawl.Task):
            with self.__scraper_sc() as session:
                scraper_model_class.insert_from_task_entry(
                    session,
                    task_entry=task_entry
                )

        return impl

    # TODO: after updating python to 3.10, remove '.__func__' and remove noinspection; see:
    #  https://stackoverflow.com/questions/12718187/python-version-3-9-calling-class-staticmethod
    #  -within-the-class-body
    # noinspection PyUnresolvedReferences
    handle_course = implement_mapper_name_handler.__func__(
        'course',
        model.scrape.Course
    )
    # TODO: add handler for course_contents_page_list <- current
    # noinspection PyUnresolvedReferences
    handle_contents_page = implement_mapper_name_handler.__func__(
        'course_contents_page',
        model.scrape.CourseContentsPage
    )

    def scrape_all(self):
        with self.__crawler_sc() as session:
            query = session.query(model.crawl.Task).where(
                model.crawl.Task.session_id == self.__active_session_id
            )
            for task_entry in query:
                self.handle_by_mapper_name(task_entry)

    # # TODO: move into model.crawl.Task
    # def iter_course_entries(
    #         self,
    #         session_id: int = None,
    # ) -> Iterable[model.crawl.Task]:
    #     session_id = session_id or self.__active_session_id
    #
    #     with self.__crawler_sc() as crawler_session:
    #         query = model.crawl.Task.query_joined(
    #             crawler_session,
    #             session_id=session_id,
    #             target_mapper_name='course'
    #         )
    #
    #         yield from query

    # # TODO: iterate page requests to retrieve 100 or more items
    # COURSE_NEWS_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/{course_key}_news?start=1&pagelen=100'
    #
    # def iter_course_news(self, course_key: str) -> Iterable[dict]:
    #     url = self.COURSE_NEWS_URL_FORMAT.format(course_key=course_key)
    #     soup = self.__url_opener.urlopen_soup(url)
    #
    #     def extract_field_from_table_row(tr):
    #         title_td, sender_td, release_date_td = tr.find_all('td')
    #         title_anchor = title_td.find('a')
    #         title = title_anchor.text.strip()
    #         key = title_anchor.attrs['href']
    #         try:
    #             sender_name = sender_td.find_all('a')[1].text.strip()
    #         except IndexError:
    #             sender_name = sender_td.text.strip()
    #         release_date = datetime.datetime.strptime(
    #             release_date_td.text.strip(),
    #             '%Y-%m-%d %H:%M'
    #         )
    #
    #         return dict(
    #             key=key,
    #             title=title,
    #             sender_name=sender_name,
    #             release_date=release_date
    #         )
    #
    #     elm_div = soup.select_one('div.contentbody-s div.description')
    #     if elm_div and 'ニュースはありません' in elm_div.text:
    #         return
    #
    #     elm_table = soup.select_one('table.stdlist')
    #     for elm_tr in elm_table.select('tr:not(.title)'):
    #         yield extract_field_from_table_row(elm_tr)
    #
    # COURSE_CONTENTS_LIST_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/{course_key}_page'
    #
    # def iter_course_contents(self, course_key: str) -> Iterable[dict]:
    #     url = self.COURSE_CONTENTS_LIST_URL_FORMAT.format(course_key=course_key)
    #     soup = self.__url_opener.urlopen_soup(url)
    #
    #     def extract_field_from_table_row(tr):
    #         title_td, release_date_td = tr.find_all('td')
    #         title_anchor = title_td.find('a')
    #         title = title_anchor.text.strip()
    #         key = title_anchor.attrs['href']
    #         release_date = datetime.datetime.strptime(
    #             release_date_td.text.split('\n')[2].strip(),
    #             '%Y-%m-%d %H:%M'
    #         )
    #
    #         return dict(
    #             key=key,
    #             title=title,
    #             release_date=release_date
    #         )
    #
    #     for elm_tr in soup.select('table.contentslist tr'):
    #         yield extract_field_from_table_row(elm_tr)
    #
    # COURSE_CONTENTS_URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/{course_contents_key}'
    #
    # def retrieve_course_contents(self, course_contents_key: str) -> dict:
    #     url = self.COURSE_CONTENTS_URL_FORMAT.format(course_contents_key=course_contents_key)
    #     soup = self.__url_opener.urlopen_soup(url)
    #
    #     elm_div = soup.select('div.contentbody-left div.articletext')
    #     inner_html = elm_div.decode_contents(formatter='html')
    #
    #     return dict(
    #         inner_html=inner_html
    #     )
