import app_logging
import model.crawl
import model.scrape
from sessctx import SessionContext
from worker import scrape

logger = app_logging.create_logger()

CRAWLING_DATABASE_PATH = 'db/crawl.db'
SCRAPING_DATABASE_PATH = 'db/scrape.db'

crawler_session_context = SessionContext.create_instance(
    CRAWLING_DATABASE_PATH,
    model.crawl.SQLCrawlerDataModelBase
)

scraper_session_context = SessionContext.create_instance(
    SCRAPING_DATABASE_PATH,
    model.scrape.SQLScraperDataModelBase
)


def main():
    logger.info('main')

    mnb = scrape.ManabaScraper(
        crawler_session_context=crawler_session_context,
        scraper_session_context=scraper_session_context
    )

    # noinspection PyUnusedLocal
    job = mnb.set_active_job(
        state='finished',
        order='oldest'
    )

    mnb.scrape_all()


if __name__ == '__main__':
    main()
