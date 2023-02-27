import abc
import urllib.error
import urllib.parse
from typing import Iterable, Optional

import bs4

import app_logging
import model.crawl
import opener
from sessctx import SessionContext
from .url_mapping import *


class AbstractCrawler:
    @classmethod
    def map_url(cls, url: str) -> Optional[MappedURL]:
        return MappedURL(url=url, mapper_name='<unset>')

    def retrieve_content_and_soup(self, url: str) -> tuple[str, bs4.BeautifulSoup]:
        raise NotImplementedError()

    @classmethod
    def iterate_anchor_full_url(cls, source_url: str, soup: bs4.BeautifulSoup) \
            -> Iterable[str]:
        for tag_anchor in soup.find_all('a'):
            anchor_url = tag_anchor.attrs.get('href')
            if anchor_url is None:
                continue
            full_url = urllib.parse.urljoin(source_url, anchor_url)
            yield full_url

    @classmethod
    def iter_next_urls(cls, source_url: str, soup: bs4.BeautifulSoup) \
            -> Iterable[MappedURL]:
        it = cls.iterate_anchor_full_url(source_url, soup)
        it = map(cls.map_url, it)
        it = filter(None, it)
        urls_unique = set(it)
        yield from urls_unique

    def initialize(self, initial_urls: list[str]) -> None:
        raise NotImplementedError()

    def crawl_once(self) -> bool:
        raise NotImplementedError()

    def crawl(self) -> None:
        while True:
            crawling_executed = self.crawl_once()
            if not crawling_executed:
                break


# TODO: use mixin
class DatabaseBasedCrawler(AbstractCrawler, metaclass=abc.ABCMeta):
    logger = app_logging.create_logger()

    def __init__(self, session_context: SessionContext):
        self.__session_context = session_context
        self.__crawling_session = None

    def initialize(self, initial_urls: list[str]) -> None:
        with self.__session_context() as session:
            crawling_session \
                = model.crawl.CrawlingSession.get_new_session(session)

            for initial_url in initial_urls:
                mapped_url = self.map_url(initial_url)
                if mapped_url is None:
                    self.logger.warning('initial_url mapped to None')
                    continue

                model.crawl.Task.add_initial_url(
                    session,
                    crawling_session=crawling_session,
                    initial_mapped_url=mapped_url
                )

    def crawl_once(self) -> bool:
        crawling_executed = False

        self.logger.info('begin crawling')

        with self.__session_context() as session:
            # TODO: most of cpu time in this function spent here
            crawling_session \
                = model.crawl.CrawlingSession.get_resumed_session(session)
            self.logger.info(f'crawling session acquired: {crawling_session=}')

            fill_count = model.crawl.Task.fill_pages(
                session,
                crawling_session=crawling_session
            )
            self.logger.info(f'page fill: {fill_count=}')

            task = model.crawl.Task.open_task(
                session,
                crawling_session=crawling_session
            )
            self.logger.info(f'task open: {task=}')

            if task is not None:
                current_url = task.lookup.url

                try:
                    content, soup = self.retrieve_content_and_soup(current_url)
                    self.logger.info(f'content retrieved: {len(content)=}')
                # TODO: distribute error handlings to each classes
                except (urllib.error.HTTPError, FileNotFoundError) as e:
                    content, soup = None, None
                    self.logger.info(f'{e} occurred while retrieving content')
                else:
                    new_task_count = 0
                    for mapped_url \
                            in self.iter_next_urls(current_url, soup):
                        model.crawl.Task.new_record(
                            session,
                            crawling_session=crawling_session,
                            lookup=model.crawl.Lookup.lookup(
                                session,
                                url=mapped_url
                            ),
                            back_lookup=model.crawl.Lookup.lookup(
                                session,
                                url=current_url
                            )
                        )
                        new_task_count += 1
                    self.logger.info(f'new tasks added: {new_task_count=}')

                model.crawl.Task.close_task(
                    session,
                    task=task,
                    content=content
                )
                self.logger.info(f'task closed: {task=}')

                crawling_executed = True

            # with self.__session_context() as session:
            #     crawling_session \
            #         = model.crawl.CrawlingSession.get_resumed_session(session)

            info_dict = model.crawl.info_dict(
                session,
                crawling_session=crawling_session
            )
            self.logger.info(f'[summary] {crawling_executed=}, {info_dict=!r}')

        self.logger.info('end crawling')

        return crawling_executed


# TODO: use mixin
class OpenerBasedCrawler(DatabaseBasedCrawler):
    def __init__(
            self,
            session_context: SessionContext,
            url_opener: opener.URLOpenerUtilMethodsMixin
    ):
        super().__init__(session_context=session_context)

        self.__url_opener = url_opener

    def retrieve_content_and_soup(self, url: str) -> tuple[str, bs4.BeautifulSoup]:
        return self.__url_opener.urlopen_string_and_soup(url)


class HomeCoursePeriod(tuple):
    __URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/home_{period}?chglistformat=list'

    def __add__(self, other):
        new_tuple = super().__add__(other)
        return type(self)(new_tuple)

    def iter_home_course_urls(self):
        for period_string in self:
            url = self.__URL_FORMAT.format(period=period_string)
            yield url


class ManabaCrawler(OpenerBasedCrawler):
    __url_mapper = ManabaURLMapper()

    def __init__(
            self,
            session_context: SessionContext,
            url_opener: opener.URLOpenerUtilMethodsMixin
    ):
        super().__init__(
            session_context=session_context,
            url_opener=url_opener
        )

    @classmethod
    def map_url(cls, url: str) -> Optional[MappedURL]:
        return cls.__url_mapper.map(url)

    PERIOD_CURRENT = HomeCoursePeriod([''])
    PERIOD_PAST = HomeCoursePeriod(['_past'])
    PERIOD_FUTURE = HomeCoursePeriod(['_upcoming'])
    PERIOD_ALL = PERIOD_CURRENT + PERIOD_PAST + PERIOD_FUTURE

    def initialize_by_period(self, period: HomeCoursePeriod = PERIOD_ALL):
        self.initialize(initial_urls=list(period.iter_home_course_urls()))
