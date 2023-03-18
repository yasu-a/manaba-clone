import app_logging
import model.scrape
import worker.scrape

logger = app_logging.create_logger()

session_context = model.create_session_context()


def main():
    app_logging.set_level(app_logging.INFO)

    logger.info('scraper main')

    mnb = worker.scrape.ManabaScraper(
        session_context=session_context,
        max_process_count=None
    )

    # noinspection PyUnusedLocal
    job = mnb.set_active_job(
        state='finished',
        order='oldest'
    )

    mnb.reset_database()
    mnb.scrape_all()


if __name__ == '__main__':
    main()
