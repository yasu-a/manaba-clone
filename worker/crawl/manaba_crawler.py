from typing import Optional

import model.crawl
import opener
from sessctx import SessionContext
from .crawler import OpenerBasedCrawler
from .manaba_family import ManabaPageFamily
from .page_family import GroupedURL, PageFamily


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
    def __init__(
            self,
            session_context: SessionContext,
            url_opener: opener.URLOpenerUtilMethodsMixin
    ):
        super().__init__(
            session_context=session_context,
            url_opener=url_opener
        )

    def _page_family(self) -> type[PageFamily]:
        return ManabaPageFamily

    def _group_url(self, url: str) -> Optional[GroupedURL]:
        return self._page_family().apply_maps(url)

    PERIOD_CURRENT = HomeCoursePeriod([''])
    PERIOD_PAST = HomeCoursePeriod(['_past'])
    PERIOD_FUTURE = HomeCoursePeriod(['_upcoming'])
    PERIOD_ALL = PERIOD_CURRENT + PERIOD_PAST + PERIOD_FUTURE

    def initialize_tasks_by_period(self, period: HomeCoursePeriod = PERIOD_ALL):
        self.initialize_tasks(initial_urls=list(period.iter_home_course_urls()))

    # TODO: move this method into super class and resolve duplicated lines
    def force_initialize_unfinished_oldest(self, period: HomeCoursePeriod = PERIOD_ALL):
        initial_urls = list(period.iter_home_course_urls())

        with self.__session_context() as session:
            job = model.crawl.Job.get_job(
                session,
                state='finished',
                order='oldest'
            )

            if job is None:
                raise ValueError('finished tasks not found')

            for initial_url in initial_urls:
                mapped_url = self._group_url(initial_url)
                if mapped_url is None:
                    self.logger.warning(f'{initial_url=!r} mapped to None')
                    continue

                try:
                    model.crawl.Task.add_initial_url(
                        session,
                        job=job,
                        initial_mapped_url=mapped_url,
                        force_append=True
                    )
                except ValueError as e:
                    # TODO: make new exception
                    if 'should be unique' not in e.args[0]:
                        raise
                    self.logger.warning(f'force append initial url failed: {initial_url=}')
                else:
                    self.logger.info(f'force append initial url success: {initial_url=}')
