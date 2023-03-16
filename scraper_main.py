import app_logging
import model.scrape
from worker import scrape

logger = app_logging.create_logger()

session_context = model.create_session_context()


def main():
    logger.info('main')

    mnb = scrape.ManabaScraper(
        session_context=session_context,
    )

    # noinspection PyUnusedLocal
    job = mnb.set_active_job(
        state='unfinished',
        order='oldest'
    )

    mnb.scrape_all()


if __name__ == '__main__':
    main()
