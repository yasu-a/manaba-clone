from pprint import pprint

from sqlalchemy.orm import *

import app_logging
import model.crawl
import model.scrape
from sessctx import SessionContext
from testing_tools import *

logger = app_logging.create_logger()

CRAWLING_DATABASE_PATH = 'db/crawl.db'
SRAPEING_DATABASE_PATH = 'db/scrape.db'

crawler_session_context = SessionContext.create_instance(
    CRAWLING_DATABASE_PATH,
    model.crawl.SQLCrawlerDataModelBase
)

scraper_session_context = SessionContext.create_instance(
    SRAPEING_DATABASE_PATH,
    model.scrape.SQLScraperDataModelBase
)


@test(enabled=True)
def test_1():
    with crawler_session_context(do_commit=False) as session:
        query = session.query(model.crawl.Task)
        print()
        print(query)
        print()
        result = query.first()
        pprint(result.as_dict())
        print()


@test(enabled=True)
def test_2():
    with crawler_session_context(do_commit=False) as session:
        base_task = aliased(model.crawl.Task)
        next_task = aliased(model.crawl.Task)

        query = session.query(base_task).join(
            next_task,
            base_task.url_id == next_task.back_url_id
        ).where(
            base_task.session_id == 1
        ).where(
            next_task.session_id == 1
        )

        print()
        print(query)
        print()
        for r in query.limit(10).all():
            pprint(r.as_dict())
        print(query.count())


if __name__ == '__main__':
    run_tests(list(globals().items()), run_last_only=True)
