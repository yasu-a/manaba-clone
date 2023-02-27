import abc
import re
import urllib.error
import urllib.parse
from typing import Iterable, Optional, Callable, Union

import bs4

import app_logging
import model.crawl
import opener
from sessctx import SessionContext


class AbstractCrawler:
    @classmethod
    def map_anchor_url(cls, url: str) -> Optional[str]:
        return url

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
    def iterate_available_anchor_full_url(cls, source_url: str, soup: bs4.BeautifulSoup) \
            -> Iterable[str]:
        urls = cls.iterate_anchor_full_url(source_url, soup)
        urls = map(cls.map_anchor_url, urls)
        urls = filter(None, urls)
        urls_unique = set(urls)
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

            model.crawl.Task.set_initial_urls(
                session,
                crawling_session=crawling_session,
                initial_urls=initial_urls
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
                    for next_url \
                            in self.iterate_available_anchor_full_url(current_url, soup):
                        model.crawl.Task.new_record(
                            session,
                            crawling_session=crawling_session,
                            lookup=model.crawl.Lookup.lookup(
                                session,
                                url=next_url
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

        with self.__session_context() as session:
            crawling_session \
                = model.crawl.CrawlingSession.get_resumed_session(session)

            info_dict = model.crawl.info_dict(
                session,
                crawling_session=crawling_session
            )
            self.logger.info(f'[summary] {crawling_executed=} {info_dict=!r}')

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


URLMappingFunctionType \
    = Callable[[str, urllib.parse.ParseResult], Union[str, urllib.parse.ParseResult]]


class URLMapper:
    logger = app_logging.create_logger()

    @classmethod
    def path_regex_params(cls) -> dict[str, dict[str, dict[str, object]]]:
        raise NotImplementedError()

    @classmethod
    def test_path_regex(cls, url: str) -> Optional[dict]:
        parse_result = urllib.parse.urlparse(url)
        domain, path = parse_result.netloc, parse_result.path
        regex_to_params = cls.path_regex_params().get(domain) or {}
        for regex, params in regex_to_params.items():
            if re.fullmatch(regex, path):
                return params
        return None

    @classmethod
    def apply_mappers(
            cls,
            source_url: str,
            mappers: Iterable[URLMappingFunctionType]
    ) -> str:
        url = source_url
        for mapper in mappers:
            parse_result = urllib.parse.urlparse(url)
            mapping_result = mapper(url, parse_result)
            if isinstance(mapping_result, urllib.parse.ParseResult):
                mapping_result = urllib.parse.urlunparse(mapping_result)
            if not isinstance(mapping_result, str):
                raise ValueError('invalid type of value returned from mapper')
            url = mapping_result
        return url

    @classmethod
    def map(cls, url: str) -> Optional[str]:
        params = cls.test_path_regex(url)
        if params is None:
            cls.logger.debug(f'mapper filtered {url!r}')
            return None

        url_mappers = params.get('url_mapper') or []
        if callable(url_mappers):
            url_mappers = [url_mappers]

        mapped_url = cls.apply_mappers(url, url_mappers)
        cls.logger.debug(f'mapper mapped {url!r} -> {mapped_url!r}')
        return mapped_url


class ManabaURLMapper(URLMapper):
    @staticmethod
    def home_course_url_mapper(url: str, parse_result: urllib.parse.ParseResult):
        query_mapping = dict(urllib.parse.parse_qsl(parse_result.query))
        query_mapping['chglistformat'] = 'list'
        new_parse_result = parse_result._replace(query=urllib.parse.urlencode(query_mapping))
        return new_parse_result

    @staticmethod
    def normalize_start_and_page_len_query(url: str, parse_result: urllib.parse.ParseResult):
        query_mapping = dict(urllib.parse.parse_qsl(parse_result.query))
        if 'start' in query_mapping and 'pagelen' in query_mapping:
            query_mapping['start'] = '1'
            query_mapping['pagelen'] = '100'
        new_parse_result = parse_result._replace(query=urllib.parse.urlencode(query_mapping))
        return new_parse_result

    @staticmethod
    def remove_header_fragment(url: str, parse_result: urllib.parse.ParseResult):
        new_parse_result = parse_result._replace(fragment=None)
        return new_parse_result

    @classmethod
    def path_regex_params(cls) -> dict[str, dict[str, dict[str, object]]]:
        return {
            'room.chuo-u.ac.jp': {
                r'/ct/home_course(_[a-z]*)?': {
                    'url_mapper': [
                        cls.home_course_url_mapper
                    ]
                },
                r'/ct/course_\d+': {},
                r'/ct/course_\d+_news': {
                    'url_mapper': [
                        cls.normalize_start_and_page_len_query
                    ]
                },
                r'/ct/course_\d+_news_\d+': {},
                r'/ct/page_\d+c\d+': {
                    'url_mapper': [
                        cls.remove_header_fragment
                    ]
                },
                r'/ct/page_\d+c\d+_\d+': {
                    'url_mapper': [
                        cls.remove_header_fragment
                    ]
                }
            }
        }


class HomeCoursePeriod(tuple):
    __URL_FORMAT = 'https://room.chuo-u.ac.jp/ct/home_{period}?chglistformat=list'

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
    def map_anchor_url(cls, url: str) -> Optional[str]:
        return cls.__url_mapper.map(url)

    PERIOD_CURRENT = HomeCoursePeriod([''])
    PERIOD_PAST = HomeCoursePeriod(['_past'])
    PERIOD_FUTURE = HomeCoursePeriod(['_upcoming'])
    PERIOD_ALL = PERIOD_CURRENT + PERIOD_PAST + PERIOD_FUTURE

    def initialize_by_period(self, period: HomeCoursePeriod = PERIOD_ALL):
        initial_urls = []
        for initial_url in period.iter_home_course_urls():
            initial_urls.append(initial_url)
        self.initialize(initial_urls=initial_urls)
