import urllib.error
import urllib.parse
from abc import ABCMeta, abstractmethod

import bs4

import model.crawl
import opener
from sessctx import SessionContext
from .page_family import *


class AbstractCrawler(metaclass=ABCMeta):
    @abstractmethod
    def _page_family(self) -> type[PageFamily]:
        raise NotImplementedError()

    def _group_url(self, url: str) -> Optional[GroupedURL]:
        return self._page_family().apply_maps(url)

    @abstractmethod
    def _retrieve_content_and_soup(self, url: str) -> tuple[str, bs4.BeautifulSoup]:
        raise NotImplementedError()

    @staticmethod
    def __iterate_anchor_full_url(source_url: str, soup: bs4.BeautifulSoup) \
            -> Iterable[str]:
        for tag_anchor in soup.find_all('a'):
            anchor_url = tag_anchor.attrs.get('href')
            if anchor_url is None:
                continue
            full_url = urllib.parse.urljoin(source_url, anchor_url)
            yield full_url

    @abstractmethod
    def _iter_next_grouped_urls(self, source_url: str, soup: bs4.BeautifulSoup, **kwargs) \
            -> Iterable[GroupedURL]:
        assert kwargs == {}
        it = self.__iterate_anchor_full_url(source_url, soup)
        it = map(self._group_url, it)
        it = filter(None, it)
        urls_unique = set(it)
        yield from urls_unique

    @abstractmethod
    def initialize_tasks(self, initial_urls: list[str]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def _process_job(self, **kwargs) -> bool:
        raise NotImplementedError()

    def crawl(self, **kwargs) -> None:
        while True:
            crawling_executed = self._process_job(**kwargs)
            if not crawling_executed:
                break


# TODO: use mixin
class DatabaseBasedCrawler(AbstractCrawler, metaclass=ABCMeta):
    logger = app_logging.create_logger()

    def __init__(self, session_context: SessionContext):
        self.__session_context = session_context

    def initialize_tasks(self, initial_urls: list[str]) -> None:
        with self.__session_context() as session:
            job = model.crawl.Job.get_new_session(session)

            for initial_url in initial_urls:
                mapped_url = self._group_url(initial_url)
                if mapped_url is None:
                    self.logger.warning(f'{initial_url=!r} mapped to None')
                    continue

                model.crawl.Task.add_initial_url(
                    session,
                    job=job,
                    initial_mapped_url=mapped_url
                )

    # noinspection PyMethodOverriding
    def _iter_next_grouped_urls(
            self,
            source_url: str,
            soup: bs4.BeautifulSoup,
            current_grouped_url: GroupedURL,
            **kwargs
    ) -> Iterable[GroupedURL]:
        it = super()._iter_next_grouped_urls(source_url, soup)
        for child_grouped_url in it:
            if child_grouped_url.url == current_grouped_url.url:
                continue
            if not child_grouped_url.is_child_of(
                    current_grouped_url,
                    page_family=self._page_family()
            ):
                continue
            yield child_grouped_url

    RESUME_LATEST = 'latest'
    RESUME_OLDEST = 'oldest'

    def _process_job(self, resume_state) -> bool:
        crawling_executed = False

        self.logger.info('CRAWLING SESSION BEGIN')

        with self.__session_context() as session:
            # TODO: most of cpu time in this function spent here
            job = model.crawl.Job.get_job(
                session,
                state='unfinished',
                order=resume_state
            )
            self.logger.info(f'crawling session acquired: {job=}')

            fill_count = model.crawl.Task.fill_pages(
                session,
                job=job
            )
            self.logger.info(f'page fill: {fill_count=}')

            task = model.crawl.Task.open_task(
                session,
                job=job
            )
            self.logger.debug(f'task open: {task=}')

            if task is not None:
                current_grouped_url = GroupedURL(
                    url=task.lookup.url,
                    group_name=task.lookup.group_name
                )
                current_url = current_grouped_url.url

                try:
                    content, soup = self._retrieve_content_and_soup(current_url)
                    self.logger.info(f'content retrieved: {len(content)=}')
                # TODO: distribute error handles to each classes
                except (urllib.error.HTTPError, FileNotFoundError) as e:
                    content, soup = None, None
                    self.logger.info(f'{e} occurred while retrieving content')
                else:
                    new_task_count = 0
                    for grouped_url in self._iter_next_grouped_urls(
                            source_url=current_url,
                            soup=soup,
                            current_grouped_url=current_grouped_url
                    ):
                        model.crawl.Task.new_record(
                            session,
                            job=job,
                            lookup=model.crawl.Lookup.lookup(
                                session,
                                url=grouped_url
                            ),
                            back_lookup=model.crawl.Lookup.lookup(
                                session,
                                url=current_url
                            )
                        )
                        new_task_count += 1
                    self.logger.debug(f'new tasks added: {new_task_count=}')

                model.crawl.Task.close_task(
                    session,
                    task=task,
                    content=content
                )
                self.logger.debug(f'task closed: {task=}')

                crawling_executed = True

            info_dict = model.crawl.info_dict(
                session,
                job=job
            )
            info_dict |= {'crawling_executed': crawling_executed}
            self.logger.info('\n'.join(
                f'[SUMMARY] {k!s:30s} {v!r:>8s}' for k, v in info_dict.items()
            ))

            self.logger.info('CRAWLING SESSION END')

        return crawling_executed


# TODO: use mixin
class OpenerBasedCrawler(DatabaseBasedCrawler, metaclass=ABCMeta):
    def __init__(
            self,
            session_context: SessionContext,
            url_opener: opener.URLOpenerUtilMethodsMixin
    ):
        super().__init__(session_context=session_context)

        self.__url_opener = url_opener

    def _retrieve_content_and_soup(self, url: str) -> tuple[str, bs4.BeautifulSoup]:
        return self.__url_opener.urlopen_string_and_soup(url)
