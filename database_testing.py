from pprint import pprint

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


if __name__ == '__main__':
    run_tests(list(globals().items()), run_last_only=True)
