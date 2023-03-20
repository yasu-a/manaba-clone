from unittest import TestCase

from sqlalchemy.orm import aliased

import app_logging
import model
import model.crawl
import opener
import worker.crawl
from generate_html import create_test_case, TestCaseGenerationFailureError
from worker.crawl.page_family import PageFamily, page_group_with_domain

# TODO: organize code

logger = app_logging.create_logger()
app_logging.set_level(app_logging.INFO)

USE_MEMORY_DB = True

# TODO: use polymorphism

# noinspection PyUnresolvedReferences,PyProtectedMember
if model.creator._SCG is not model.creator.SQLiteSessionContextGenerator:
    USE_MEMORY_DB = False

# noinspection PyUnresolvedReferences,PyProtectedMember
if model.creator._SCG is model.creator.SQLiteSessionContextGenerator:
    DB_NAME = 'debug.db'
elif model.creator._SCG is model.creator.MySQLSessionContextGenerator:
    DB_NAME = 'manaba_clone_debug'


class PageFamilyForDebug(PageFamily):
    with page_group_with_domain(domain='') as debug_page_group:
        node = debug_page_group(
            path_pattern=r'.*',
            parent='node'
        )


class CrawlerForDebug(worker.crawl.OpenerBasedCrawler):
    def _page_family(self) -> type[PageFamily]:
        return PageFamilyForDebug


class TestOpenerBasedCrawler(TestCase):
    @classmethod
    def add_test_for_seed(cls, source_method_name: str, seed: int):
        func = getattr(cls, source_method_name)

        def wrapper(*args, **kwargs):
            return func(*args, **(kwargs | {'seed': seed}))

        new_method_name = f'test_{source_method_name}_for_seed_{seed}'
        setattr(cls, new_method_name, wrapper)

    def crawl(self, seed):
        try:
            files, answers = create_test_case(
                num_htmls=40,
                num_links_mean=10,
                num_links_sigma=10,
                seed=seed
            )
        except TestCaseGenerationFailureError as e:
            logger.error(e)
            return

        session_context = model.create_session_context(
            ':memory:' if USE_MEMORY_DB else DB_NAME
        )

        with opener.MemoryURLOpener(
                files=files
        ) as url_opener:
            crawler = CrawlerForDebug(
                session_context=session_context,
                url_opener=url_opener
            )

            crawler.initialize_tasks(
                initial_urls=['0.html']
            )

            crawler.crawl(resume_state=crawler.RESUME_LATEST)

        with session_context() as session:
            lst = fetch_answers(session)

            for task in lst:
                name = task.lookup.url
                back_name = task.back_lookup.url
                content = task.page.content
                key = back_name, name
                entry = answers.pop(key)
                logger.info(f'{name=}, {back_name=}, {content and len(content)=}, {entry=}')
                self.assertTrue(entry.content == content)

            self.assertFalse(answers)


def setup_test():
    for seed in range(10):
        TestOpenerBasedCrawler.add_test_for_seed(
            'crawl',
            seed=seed
        )


setup_test()


def fetch_answers(session):
    job = model.crawl.Job.get_job(
        session,
        state='finished',
        order='latest'
    )

    lookup = aliased(model.crawl.Lookup)
    back_lookup = aliased(model.crawl.Lookup)

    query = session.query(
        model.crawl.Task
    ).filter(
        model.crawl.Task.job_id == job.id
    ).join(
        lookup,
        model.crawl.Task.url_id == lookup.id
    ).join(
        back_lookup,
        model.crawl.Task.back_url_id == back_lookup.id
    ).join(
        model.crawl.PageContent,
        model.crawl.Task.page_id == model.crawl.PageContent.id
    )

    lst = query.all()

    return lst
