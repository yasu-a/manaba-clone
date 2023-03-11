import app_logging
import model.crawl
import model.scrape
import scrape
from sessctx import SessionContext

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


def main():
    logger.info('main')

    mnb = scrape.ManabaScraper(
        crawler_session_context=crawler_session_context,
        scraper_session_context=scraper_session_context
    )

    crawling_session_id = mnb.set_active_crawling_session(
        state='finished',
        order='oldest'
    )

    with scraper_session_context() as scraper_session:
        # logger.info('iterating courses')
        # for crawling_entry in mnb.iter_course_entries():
        #     logger.info(f'course retrieved {crawling_entry}')
        #     course = model.scrape.Course.from_scraping_entry(
        #         scraper_session,
        #         crawling_entry=crawling_entry
        #     )
        #     logger.info(f'{course=}')
        #     scraper_session.add(course)
        mnb.scrape_all()


if __name__ == '__main__':
    main()
